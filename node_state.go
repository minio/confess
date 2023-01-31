// Copyright (c) 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/confess/node"
	"github.com/minio/confess/ops"
	"github.com/minio/confess/tests"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
)

// TestStats
type TestStats struct {
	TotalTests        uint64        `json:"totalTests"`
	TotalFailures     uint64        `json:"totalFailures"`
	TotalBytesWritten uint64        `json:"totalBytes"`
	LatencyCumulative time.Duration `json:"latencycumulative"`
	PeakLatency       time.Duration `json:"peakLatency"`
}

func (s TestStats) AvgLatency() time.Duration {
	if s.TotalTests == 0 {
		return 0
	}
	return s.LatencyCumulative / time.Duration(s.TotalTests)
}

type TestMetrics struct {
	startTime time.Time
	numTests  int32
	numFailed int32
	PutStats  TestStats
	GetStats  TestStats
	HeadStats TestStats
	ListStats TestStats
}
type TestInfo struct {
	Test  tests.Tester
	Retry tests.RetryInfo
}
type nodeState struct {
	node.NodeSlc
	hc     *healthChecker
	cliCtx *cli.Context
	resCh  chan interface{}
	testCh chan TestInfo
	doneCh chan struct{}
	// track offline nodes
	nlock      sync.RWMutex
	offlineMap map[string]bool

	registry *tests.Registry
	// stats
	model   model
	metrics TestMetrics
	wg      sync.WaitGroup
}

func newNodeState(ctx *cli.Context) *nodeState {
	var endpoints []string
	var nodes []*node.Node
	now := time.Now()
	for _, hostStr := range ctx.Args() {
		hosts := strings.Split(hostStr, ",")
		for _, host := range hosts {
			if len(host) == 0 {
				continue
			}
			if !ellipses.HasEllipses(host) {
				endpoints = append(endpoints, host)
				continue
			}
			patterns, perr := ellipses.FindEllipsesPatterns(host)
			if perr != nil {
				console.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", patterns, perr))
			}
			for _, lbls := range patterns.Expand() {
				endpoints = append(endpoints, strings.Join(lbls, ""))
			}
		}
	}

	hcMap := make(map[string]*hcClient)
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSuffix(endpoint, slashSeparator)
		target, err := url.Parse(endpoint)
		if err != nil {
			console.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", endpoint, err))
		}
		if target.Scheme == "" {
			target.Scheme = "http"
		}
		if target.Scheme != "http" && target.Scheme != "https" {
			console.Fatalln("unexpected scheme %s, should be http or https, please use '%s --help'",
				endpoint, ctx.App.Name)
		}
		if target.Host == "" {
			console.Fatalln(fmt.Errorf("missing host address %s, please use '%s --help'",
				endpoint, ctx.App.Name))
		}
		clnt, err := getClient(ctx, target)
		if err != nil {
			console.Fatalln(fmt.Errorf("could not initialize client for %s",
				endpoint))
		}
		hcClient, err := newHCClient(target)
		if err != nil {
			console.Fatalln(fmt.Errorf("could not initialize client for %s", endpoint))
		}
		hcMap[target.Host] = &hcClient

		n := &node.Node{
			EndpointURL: target,
			Client:      clnt,
			HCFn:        hcClient.isOffline,
		}
		nodes = append(nodes, n)
	}
	var pfxes []string
	for i := 0; i < 10; i++ {
		pfxes = append(pfxes, fmt.Sprintf("confess/%s/pfx%d", now.Format("01_02_06_15:04"), i))
	}
	return &nodeState{
		metrics:    TestMetrics{startTime: now},
		offlineMap: make(map[string]bool),
		hc:         newHealthChecker(ctx, hcMap),
		cliCtx:     ctx,
		resCh:      make(chan interface{}, 10000),
		testCh:     make(chan TestInfo, 100),
		doneCh:     make(chan struct{}, 1),
		NodeSlc: node.NodeSlc{
			Prefixes: pfxes,
			Nodes:    nodes,
			Bucket:   ctx.String("bucket"),
		},
		model:    initialModel(),
		registry: tests.LoadRegistry(),
	}
}

// disallow test on object locked bucket
func (n *nodeState) checkBucket(bucket string) error {
	for _, node := range n.Nodes {
		if n.hc.isOffline(node.EndpointURL) {
			continue
		}
		_, _, _, _, err := node.Client.GetObjectLockConfig(context.Background(), bucket)
		if err == nil {
			return fmt.Errorf("%s is object lock enabled. Please use another bucket", bucket)
		}
		aerr := minio.ToErrorResponse(err)
		switch aerr.Code {
		case "NoSuchBucket":
			return fmt.Errorf("bucket `%s` does not exist ", bucket)
		default:
			vcfg, err := node.Client.GetBucketVersioning(context.Background(), bucket)
			if err == nil {
				n.VersioningEnabled = vcfg.Enabled()
			}
			return nil
		}
	}
	return nil
}

// addWorker creates a new worker to process op sequence
func (n *nodeState) addWorker(ctx context.Context) {
	n.wg.Add(1)
	// Add a new worker.
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-n.testCh:
				if !ok {
					return
				}
				t.Test.Run(ctx, n.NodeSlc, n.resCh, t.Retry)
			}
		}
	}()
}

// returns latest health check status for a node
func (n *nodeState) hcStatus(epURL *url.URL) (s statusChg) {
	if epURL == nil {
		return
	}
	s.epURL = epURL
	n.nlock.Lock()
	_, ok := n.offlineMap[epURL.Host]
	switch {
	case !ok && n.hc.isOffline(epURL):
		n.offlineMap[epURL.Host] = true
		s.status = "offline"
	case !n.hc.isOffline(epURL) && ok:
		delete(n.offlineMap, epURL.Host)
		s.status = "online"
	}
	n.nlock.Unlock()
	return s
}

type statusChg struct {
	epURL     *url.URL
	status    string
	timestamp time.Time
}

func (s statusChg) String() string {
	return fmt.Sprintf("%s is %s", s.epURL.Host, s.status)
}

// initialize clients, health check and tests...
func (n *nodeState) init(ctx context.Context) {
	if n == nil {
		return
	}
	for i := 0; i < concurrency; i++ {
		n.addWorker(ctx)
	}

	go func() {
		logFile := n.cliCtx.String("output")
		if logFile == "" {
			logFile = fmt.Sprintf("%s%s.txt", "confess", time.Now().Format(".01-02-2006-15-04-05"))
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			console.Fatalln("unable to write 'confess' log", err)
			return
		}
		f.WriteString(getHeader(n.cliCtx))
		fwriter := bufio.NewWriter(f)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-globalContext.Done():
				p := tea.NewProgram(initialModel(), tea.WithoutSignalHandler())
				go func() {
					if _, err := p.Run(); err != nil {
						os.Exit(1)
					}
				}()
				for rs := range n.resCh {
					if res, ok := rs.(ops.Result); ok {
						if res.Err != nil && !errors.Is(res.Err, context.Canceled) && !errors.Is(res.Err, ops.ErrNodeOffline) {
							if _, err := f.WriteString(res.String() + "\n"); err != nil {
								console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
							}
						}
					}

					if res, ok := rs.(tests.Result); ok {
						if res.Err != nil && !errors.Is(res.Err, context.Canceled) && !errors.Is(res.Err, ops.ErrNodeOffline) {
							if _, err := f.WriteString(res.String() + "\n"); err != nil {
								console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
							}
						}
					}
				}

				if _, err := f.WriteString(n.summaryMsg()); err != nil {
					console.Fatalf("unable to write summary to 'confess' log: %s\n", err)
				}
				fwriter.Flush()
				f.Close()
				ops.Cleanup(n.Bucket, n.Prefixes, n.Nodes) // finish clean up
				n.model.quitting = true
				console.Println(n.statusBar())

				n.doneCh <- struct{}{}
				return
			case rs, ok := <-n.resCh:
				if !ok {
					return
				}
				res, ok := rs.(ops.Result)
				var statusChg statusChg
				eraseOnce := false
				if ok {
					statusChg = n.hcStatus(res.Node)
					if statusChg.status != "" {
						statusChg.timestamp = res.EndTime
						eraseOnce = n.printWithErase(eraseOnce, statusChg.Render())
						if _, err := f.WriteString(fmt.Sprintf("%s %s %s\n", res.StartTime.Format(time.RFC3339Nano), res.EndTime.Format(time.RFC3339Nano), statusChg.String())); err != nil {
							console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
						}
					}
					if res.Err != nil {
						if !errors.Is(res.Err, ops.ErrNodeOffline) && !res.RetryRequest && !res.AbortTest {
							if _, err := f.WriteString(res.String() + "\n"); err != nil {
								console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
							}
							eraseOnce = n.printWithErase(eraseOnce, n.printRow(res))
						}
					}

				}
				if res, ok := rs.(tests.Result); ok {
					statusChg = n.hcStatus(res.Node)
					if statusChg.status != "" {
						statusChg.timestamp = res.EndTime
						eraseOnce = n.printWithErase(eraseOnce, statusChg.Render())
					}
					totsuccess := atomic.LoadInt32(&n.metrics.numTests) - atomic.LoadInt32(&n.metrics.numFailed)
					if !errors.Is(res.Err, ops.ErrNodeOffline) {
						// summarize the stats here
						if res.Status == tests.Started {
							atomic.AddInt32(&n.metrics.numTests, 1)
						}
						if res.Err != nil {
							if !res.AbortTest && !res.IsRetry {
								atomic.AddInt32(&n.metrics.numFailed, 1)
							}
							if res.IsRetry {
								test := n.registry.GetTestByName(res.TestName)
								n.queueTest(ctx, TestInfo{Test: test, Retry: res.Retry})
							}
						} else {
							totsuccess++
						}
						if n.metrics.numFailed >= int32(n.cliCtx.Int("fail-after")) {
							console.Println(whiteStyle.Render("Too many failures, shutting down..."))
							globalCancel()
						}
					}
				}
				select { // fix status bar flicker
				case <-ticker.C:
					n.printWithErase(eraseOnce, n.statusBar())
				default:
					if eraseOnce {
						n.printWithErase(eraseOnce, n.statusBar())
					}
				}
			}
		}
	}()
}

// prints line to console - erase status bar if erased is true
func (n *nodeState) printWithErase(erased bool, msg string) bool {
	if !erased {
		console.Eraseline()
	}

	console.Print(msg + "\r")
	return true
}

func (n *nodeState) queueTest(ctx context.Context, ti TestInfo) {
	select {
	case <-ctx.Done():
		return
	default:
		n.testCh <- ti
	}
}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			close(n.testCh)
			return
		default:
			if n.hc.allOffline() {
				continue
			}
			test := n.registry.GetRandomTest()
			for i := 0; i < test.Concurrency(); i++ {
				n.queueTest(ctx, TestInfo{Test: test})
			}
		}
	}
}

// wait on workers to finish
func (n *nodeState) finish() {
	<-globalContext.Done()
	n.wg.Wait()
	close(n.resCh)

	<-n.doneCh
}

const (
	// Global error exit status.
	globalErrorExitStatus = 1

	// Global CTRL-C (SIGINT, #2) exit status.
	globalCancelExitStatus = 130

	// Global SIGKILL (#9) exit status.
	globalKillExitStatus = 137

	// Global SIGTERM (#15) exit status
	globalTerminatExitStatus = 143
)

// trapSignals traps the registered signals and cancel the global context.
func (n *nodeState) trapSignals(sig ...os.Signal) {
	// channel to receive signals.
	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)

	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigCh, sig...)

	var s os.Signal
exitfor:
	for {
		select {
		case s = <-sigCh:
			signal.Stop(sigCh)
			break exitfor
		default:
			var duration time.Duration
			var timer *time.Timer
			if n.cliCtx.IsSet("duration") {
				duration = n.cliCtx.Duration("duration")
				timer = time.NewTimer(duration)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-sigCh:
					signal.Stop(sigCh)
				}
				break exitfor
			}
			// Wait for the signal.
		}
	}
	// Cancel the global context - wait for cleanup and final summary to be printed to logfile and screen
	globalCancel()
	n.wg.Wait()
	<-n.doneCh // wait on signal that last operation status summary written to log

	var exitCode int
	if s != nil {
		switch s.String() {
		case "interrupt":
			exitCode = globalCancelExitStatus
		case "killed":
			exitCode = globalKillExitStatus
		case "terminated":
			exitCode = globalTerminatExitStatus
		default:
			exitCode = globalErrorExitStatus
		}
	}
	os.Exit(exitCode)
}

// for logging and console

// return summary for logfile
func (n *nodeState) summaryMsg() string {
	success := atomic.LoadInt32(&n.metrics.numTests) - atomic.LoadInt32(&n.metrics.numFailed)
	return fmt.Sprintf("Test succeeded=%d Tests Failed=%d Duration=%s\n", success, atomic.LoadInt32(&n.metrics.numFailed), humanize.RelTime(n.metrics.startTime, time.Now(), "", ""))
}

func (n *nodeState) statusBar() string {
	width := globalTermWidth - 2
	if width > 100 {
		width = 100
	}
	statusKey := statusStyle.Render(" STATUS")
	success := successStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numTests)-atomic.LoadInt32(&n.metrics.numFailed)))
	failures := failedStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numFailed)))
	bar := statusKey + statusTextStyle.Render("  Tests succeeded=") + success +
		statusTextStyle.Render("Tests Failed=") + failures +
		statusTextStyle.Render("Duration= "+humanize.RelTime(n.metrics.startTime, time.Now(), "", ""))
	return statusBarStyle.Width(width).Render(bar)
}

func (n *nodeState) printRow(r ops.Result) string {
	errMsg := fmt.Sprintf("%s failed %s test", r.Path, r.TestName)
	columnA := fmt.Sprintf("%s | %s | ", n.metrics.startTime.Format("2006-01-02 15:04:05"), nodeStyle.Render(r.Node.Host))
	width := globalTermWidth - len(columnA) - 2
	if width <= 0 {
		width = len(columnA)
	}
	height := 3 + len(errMsg)/width
	columnB := errMsgStyle.MaxWidth(width).Width(width).Height(height).Render(lipgloss.JoinHorizontal(lipgloss.Left, errMsg))
	if globalTermWidth > 0 {
		baseStyle = baseStyle.MaxWidth(globalTermWidth - 2)
	}
	return baseStyle.Width(globalTermWidth - 2).Render(lipgloss.JoinHorizontal(lipgloss.Left, columnA, columnB))
}

func (s statusChg) Render() string {
	columnB := ""
	switch s.status {
	case "online":
		columnB = " is " + advisory("online")
	case "offline":
		columnB = " is " + warn("offline")
	default:
		return ""
	}
	columnA := s.timestamp.Format("2006-01-02 15:04:05") + " | " + nodeStyle.Render(s.epURL.Host) + " | "
	return baseStyle.Width(globalTermWidth).Render(lipgloss.JoinHorizontal(lipgloss.Left, columnA, columnB) + "\n")
}

type model struct {
	spinner  spinner.Model
	quitting bool
	err      error
}

func initialModel() model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	return model{spinner: s}
}

func (m model) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.quitting {
		return m, tea.Quit
	}
	var cmd tea.Cmd
	m.spinner, cmd = m.spinner.Update(msg)
	return m, cmd
}

func (m model) View() string {
	if m.err != nil {
		return m.err.Error()
	}
	str := fmt.Sprintf("\n   %s Cleaning up....", m.spinner.View())
	if m.quitting {
		return str + "\n"
	}
	return str
}

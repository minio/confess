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
	"math/rand"
	"net/http"
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// node represents an endpoint to S3 object store
type node struct {
	endpointURL *url.URL
	client      *minio.Client
}

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
type nodeState struct {
	Prefixes []string
	nodes    []*node
	hc       *healthChecker
	cliCtx   *cli.Context
	resCh    chan testResult
	testCh   chan OpSequence
	doneCh   chan struct{}
	// track offline nodes
	nlock      sync.RWMutex
	offlineMap map[string]bool

	// stats
	model   model
	metrics TestMetrics
	wg      sync.WaitGroup
}

func newNodeState(ctx *cli.Context) *nodeState {
	var endpoints []string
	var nodes []*node
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
		n := &node{
			endpointURL: target,
			client:      clnt,
		}
		nodes = append(nodes, n)
		hcClient, err := newHCClient(target)
		if err != nil {
			console.Fatalln(fmt.Errorf("could not initialize client for %s", endpoint))
		}
		hcMap[target.Host] = &hcClient
	}
	var pfxes []string
	for i := 0; i < 10; i++ {
		pfxes = append(pfxes, fmt.Sprintf("confess_%s/pfx%d", now.Format("01-02-2006-15-04-05"), i))
	}
	return &nodeState{
		metrics:    TestMetrics{startTime: now},
		offlineMap: make(map[string]bool),
		nodes:      nodes,
		hc:         newHealthChecker(ctx, hcMap),
		cliCtx:     ctx,
		resCh:      make(chan testResult, 100),
		testCh:     make(chan OpSequence, 1000),
		doneCh:     make(chan struct{}, 1),
		Prefixes:   pfxes,
		model:      initialModel(),
	}
}

// disallow test on object locked bucket
func (n *nodeState) checkBucket(bucket string) error {
	for _, node := range n.nodes {
		if n.hc.isOffline(node.endpointURL) {
			continue
		}
		_, _, _, _, err := node.client.GetObjectLockConfig(context.Background(), bucket)
		if err == nil {
			return fmt.Errorf("%s is object lock enabled. Please use another bucket", bucket)
		}
		aerr := minio.ToErrorResponse(err)
		switch aerr.Code {
		case "NoSuchBucket":
			return fmt.Errorf("bucket `%s` does not exist ", bucket)
		default:
			return nil
		}
	}
	return nil
}

func (n *nodeState) getRandomPfx() string {
	idx := rand.Intn(len(n.Prefixes))
	return n.Prefixes[idx]
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
				for op := range n.testCh {
					if op.Retry.OK {
						n.resCh <- testResult{
							Err:       op.Retry.Err,
							Path:      fmt.Sprintf("%s/%s", n.cliCtx.String("bucket"), op.Retry.ObjInfo.Key),
							TestName:  op.Retry.TestName,
							StartTime: time.Now(),
							EndTime:   time.Now(),
						}
					}
				}
				return
			case ops, ok := <-n.testCh:
				if !ok {
					return
				}
				ops.Test(ctx, ops.Retry)
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
	epURL  *url.URL
	status string
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

				for res := range n.resCh {
					if res.Err != nil && !errors.Is(res.Err, context.Canceled) {
						if _, err := f.WriteString(res.String() + "\n"); err != nil {
							console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
						}
					}
				}

				if _, err := f.WriteString(n.summaryMsg()); err != nil {
					console.Fatalf("unable to write summary to 'confess' log: %s\n", err)
				}
				fwriter.Flush()
				f.Close()
				n.cleanup() // finish clean up
				n.model.quitting = true
				n.printSummary()
				n.doneCh <- struct{}{}
				return
			case res, ok := <-n.resCh:
				if !ok {
					return
				}
				eraseOnce := false
				statusChg := n.hcStatus(res.Node)
				if statusChg.status != "" {
					eraseOnce = n.printWithErase(eraseOnce, statusChg.Render())
					if _, err := f.WriteString(fmt.Sprintf("%s %s %s\n", res.StartTime.Format(time.RFC3339Nano), res.EndTime.Format(time.RFC3339Nano), statusChg.String())); err != nil {
						console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
					}
				}
				if res.Err != nil {
					if !errors.Is(res.Err, errNodeOffline) || !res.RetryRequest || !res.AbortTest {
						if _, err := f.WriteString(res.String() + "\n"); err != nil {
							console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
						}
					}
				}

				totsuccess := atomic.LoadInt32(&n.metrics.numTests) - atomic.LoadInt32(&n.metrics.numFailed)
				if !errors.Is(res.Err, errNodeOffline) {
					// summarize the stats here
					atomic.AddInt32(&n.metrics.numTests, 1)
					if res.Err != nil {
						if !res.RetryRequest && !res.AbortTest {
							atomic.AddInt32(&n.metrics.numFailed, 1)
						}
					} else {
						totsuccess++
					}
					if res.Err != nil && !res.RetryRequest && !res.AbortTest {
						eraseOnce = n.printWithErase(eraseOnce, n.printRow(res))
					}
					n.updateMetrics(ctx, res)
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
func (n *nodeState) updateMetrics(ctx context.Context, res testResult) {
	switch res.Method {
	case http.MethodHead:
		n.metrics.HeadStats.TotalTests++
		if res.Err != nil {
			n.metrics.HeadStats.TotalFailures++
		} else {
			n.metrics.HeadStats.TotalBytesWritten += uint64(res.data.Size)
		}
	case http.MethodGet:
		n.metrics.GetStats.TotalTests++
		if res.Err != nil {
			n.metrics.GetStats.TotalFailures++
		} else {
			n.metrics.GetStats.TotalBytesWritten += uint64(res.data.Size)
		}
	case ListType:
		n.metrics.ListStats.TotalTests++
		if res.Err != nil {
			n.metrics.ListStats.TotalFailures++
		} else {
			n.metrics.ListStats.TotalBytesWritten += uint64(res.data.Size)
		}
	}
}

func (n *nodeState) printSummary() {
	var s strings.Builder

	table := tablewriter.NewWriter(&s)
	table.SetAutoWrapText(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(true)
	table.SetRowLine(false)
	addRow := func(s string) {
		table.Append([]string{s})
	}
	title := metricsTitle
	_ = addRow
	addRowF := func(format string, vals ...interface{}) {
		s := fmt.Sprintf(format, vals...)
		table.Append([]string{s})
	}
	uiSuccessStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("118")).
		Bold(true)
	uiFailedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("190")).
		Bold(true)
	success := uiSuccessStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numTests)-atomic.LoadInt32(&n.metrics.numFailed)))
	failures := uiFailedStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numFailed)))

	addRowF(title("Total Operations succeeded:")+"   %s;"+title(" Total failed ")+" %s in %s", success, failures, humanize.RelTime(n.metrics.startTime, time.Now(), "", ""))
	addRow("-------------------------------------- Confess Run Statistics -------------------------------------------")
	addRowF(title("GET:  ") + fmt.Sprintf("  %s operations; %s failed",
		whiteStyle.Render(humanize.Comma(int64(n.metrics.GetStats.TotalTests))),
		whiteStyle.Render(humanize.Comma(int64(n.metrics.GetStats.TotalFailures)))))
	addRowF(title("HEAD: ") + fmt.Sprintf("  %s operations; %s failed",
		whiteStyle.Render(humanize.Comma(int64(n.metrics.HeadStats.TotalTests))),
		whiteStyle.Render(humanize.Comma(int64(n.metrics.HeadStats.TotalFailures)))))
	addRowF(title("LIST: ") + fmt.Sprintf("  %s operations; %s failed",
		whiteStyle.Render(humanize.Comma(int64(n.metrics.ListStats.TotalTests))),
		whiteStyle.Render(humanize.Comma(int64(n.metrics.ListStats.TotalFailures)))))

	table.Render()
	console.Println(s.String())
}
func metricsTitle(s string) string {
	return console.Colorize("metrics-title", s)
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
	return fmt.Sprintf("Operations succeeded=%d Operations Failed=%d Duration=%s\n", success, atomic.LoadInt32(&n.metrics.numFailed), humanize.RelTime(n.metrics.startTime, time.Now(), "", ""))
}

func (n *nodeState) statusBar() string {
	width := 108
	statusKey := statusStyle.Render(" STATUS")
	success := successStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numTests)-atomic.LoadInt32(&n.metrics.numFailed)))
	failures := failedStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.metrics.numFailed)))
	bar := statusKey + statusTextStyle.Render("  Operations succeeded=") + success +
		statusTextStyle.Render("Operations Failed=") + failures +
		statusTextStyle.Render("Duration= "+humanize.RelTime(n.metrics.startTime, time.Now(), "", ""))
	return statusBarStyle.Width(width).Render(bar)
}

var titleCase = cases.Title(language.English)

func (n *nodeState) printRow(r testResult) string {
	errMsg := fmt.Sprintf(" %s failed %s consistency", r.Path, titleCase.String(r.Method))
	columnA := "  " + nodeStyle.Render(r.Node.Host) + " | "
	columnB := errMsgStyle.Width(80).Render(lipgloss.JoinVertical(lipgloss.Left, errMsg))
	return bassStyle.Render(lipgloss.JoinHorizontal(lipgloss.Top, columnA, columnB))
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
	columnA := "  " + nodeStyle.Render(s.epURL.Host) + " | "
	return bassStyle.Render(lipgloss.JoinHorizontal(lipgloss.Top, columnA, columnB))

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

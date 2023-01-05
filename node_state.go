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
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
)

// node represents an endpoint to S3 object store
type node struct {
	endpointURL *url.URL
	client      *minio.Client
}

type nodeState struct {
	Prefixes    []string
	nodes       []*node
	hc          *healthChecker
	cliCtx      *cli.Context
	resCh       chan testResult
	testCh      chan OpSequence
	doneCh      chan struct{}
	printHeader bool
	// track offline nodes
	nlock      sync.RWMutex
	offlineMap map[string]bool

	// stats
	startTime time.Time
	numTests  int32
	numFailed int32

	wg sync.WaitGroup
}

func newNodeState(ctx *cli.Context) *nodeState {
	var endpoints []string
	var nodes []*node
	//minio.MaxRetry = 1

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
		pfxes = append(pfxes, fmt.Sprintf("confess/pfx%d", i))
	}
	return &nodeState{
		startTime:  time.Now(),
		offlineMap: make(map[string]bool),
		nodes:      nodes,
		hc:         newHealthChecker(ctx, hcMap),
		cliCtx:     ctx,
		resCh:      make(chan testResult, 100),
		testCh:     make(chan OpSequence, 1000),
		doneCh:     make(chan struct{}, 1),
		Prefixes:   pfxes,
	}
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
func (s statusChg) Render() string {
	switch s.status {
	case "online":
		return divider + baseStyle.Render(s.epURL.Host) + " is " + advisory("online") + "\n"
	case "offline":
		return divider + baseStyle.Render(s.epURL.Host) + " is " + warn("offline") + "\n"
	default:
		return ""
	}
}

// initialize clients, health check and tests...
func (n *nodeState) init(ctx context.Context) {
	if n == nil {
		return
	}
	for i := 0; i < concurrency; i++ {
		n.addWorker(ctx)
	}
	n.wg.Add(1)

	n.printHeader = true
	go func() {
		defer n.wg.Done()
		logFile := fmt.Sprintf("%s%s.txt", "confess", time.Now().Format(".01-02-2006-15-04-05"))
		if n.cliCtx.IsSet("output") {
			logFile = fmt.Sprintf("%s/%s", n.cliCtx.String("output"), logFile)
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
				if _, err := f.WriteString(n.summaryMsg()); err != nil {
					console.Fatalf("unable to write summary to 'confess' log: %s\n", err)
				}
				fwriter.Flush()
				f.Close()
				n.cleanup() // finish clean up
				console.Println(n.statusBar())
				n.doneCh <- struct{}{}
				return
			case res, ok := <-n.resCh:
				if !ok {
					return
				}
				eraseOnce := false
				statusChg := n.hcStatus(res.Node)
				if statusChg.status != "" {
					eraseOnce = printWithErase(eraseOnce, statusChg.Render())
					if _, err := f.WriteString(fmt.Sprintf("%s %s %s\n", res.StartTime.Format(time.RFC3339Nano), res.EndTime.Format(time.RFC3339Nano), statusChg.String())); err != nil {
						console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
					}
				}
				if res.Err != nil {
					if !errors.Is(res.Err, errNodeOffline) && !res.RetryRequest {
						if _, err := f.WriteString(res.String() + "\n"); err != nil {
							console.Fatalf("unable to write to 'confess' log for %s: %s\n", res, err)
						}
					}
				}

				totsuccess := atomic.LoadInt32(&n.numTests) - atomic.LoadInt32(&n.numFailed)
				if !errors.Is(res.Err, errNodeOffline) {
					// summarize the stats here
					atomic.AddInt32(&n.numTests, 1)
					if res.Err != nil {
						if !res.RetryRequest {
							atomic.AddInt32(&n.numFailed, 1)
						}
					} else {
						totsuccess++
					}
					if res.Err != nil {
						if n.printHeader {
							block := lipgloss.PlaceHorizontal(80, lipgloss.Center, "Consistency Errors"+divider)
							row := lipgloss.JoinHorizontal(lipgloss.Left, block)
							eraseOnce = printWithErase(eraseOnce, row)
							n.printHeader = false
						} else {
							eraseOnce = printWithErase(eraseOnce, n.printRow(res))
						}
					}
					select { // fix status bar flicker
					case <-ticker.C:
						printWithErase(eraseOnce, n.statusBar())
					default:
						if eraseOnce {
							printWithErase(eraseOnce, n.statusBar())
						}
					}
				}
			}
		}
	}()
}

// wait on workers to finish
func (n *nodeState) finish(ctx context.Context) {
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
		default:
			var duration time.Duration
			var timer *time.Timer
			if n.cliCtx.IsSet("duration") {
				duration = n.cliCtx.Duration("duration")
				timer = time.NewTimer(duration)
				defer timer.Stop()
				<-timer.C
				break exitfor
			}
			// Wait for the signal.
		case s = <-sigCh:
			signal.Stop(sigCh)
			break exitfor
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
	success := atomic.LoadInt32(&n.numTests) - atomic.LoadInt32(&n.numFailed)
	return fmt.Sprintf("Operations succeeded=%d Operations Failed=%d Duration=%s\n", success, atomic.LoadInt32(&n.numFailed), humanize.RelTime(n.startTime, time.Now(), "", ""))
}

func (n *nodeState) statusBar() string {
	width := 96
	statusKey := statusStyle.Render("STATUS")
	success := successStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.numTests)-atomic.LoadInt32(&n.numFailed)))
	failures := failedStyle.Render(fmt.Sprintf("%d ", atomic.LoadInt32(&n.numFailed)))
	bar := statusKey + statusTextStyle.Render("  Operations succeeded=") + success +
		statusTextStyle.Render("Operations Failed=") + failures +
		statusTextStyle.Render("Duration= "+humanize.RelTime(n.startTime, time.Now(), "", ""))
	return statusBarStyle.Width(width).Render(bar)
}

func (n *nodeState) maxNodeLen() int {
	max := 0
	for _, node := range n.nodes {
		if max < len(node.endpointURL.Host) {
			max = len(node.endpointURL.Host)
		}
	}
	return max
}

func (n *nodeState) printRow(r testResult) string {
	maxNodeLen := n.maxNodeLen()
	cols := getColumns()
	msg1 := fmt.Sprintf("%s %s %s",
		cols[0].Style.Width(maxNodeLen).Render(r.Node.Host), cols[2].Style.Width(15).Render(r.FuncName), cols[1].Style.Width(48).Render(r.Path))
	msg2 := cols[3].Style.Width(60).Render(lipgloss.JoinVertical(lipgloss.Left, strings.TrimSpace(r.Err.Error())))
	return lipgloss.JoinHorizontal(lipgloss.Top, msg1, msg2)
}

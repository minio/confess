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
	minio.MaxRetry = 1

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

	hcMap := make(map[string]epHealth)
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
		hcMap[target.Host] = epHealth{
			Endpoint: target.Host,
			Scheme:   target.Scheme,
			Online:   true,
		}
	}
	var pfxes []string
	for i := 0; i < 10; i++ {
		pfxes = append(pfxes, fmt.Sprintf("prefix%d", i))
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
				n.runOpSeq(ctx, ops)
			}
		}
	}()
}

// returns latest health check status for a node
func (n *nodeState) hcStatus(epURL *url.URL) string {
	var statusChg string
	n.nlock.Lock()
	_, ok := n.offlineMap[epURL.Host]
	switch {
	case !ok && n.hc.isOffline(epURL):
		n.offlineMap[epURL.Host] = true
		statusChg = "offline"
	case !n.hc.isOffline(epURL) && ok:
		delete(n.offlineMap, epURL.Host)
		statusChg = "online"
	}
	n.nlock.Unlock()

	switch statusChg {
	case "online":
		return divider + baseStyle.Render(epURL.Host) + " is " + advisory("online") + "\n"
	case "offline":
		return divider + baseStyle.Render(epURL.Host) + " is " + warn("offline") + "\n"
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
		logFile := fmt.Sprintf("%s%s", "confess_log", time.Now().Format(".01-02-2006-15-04-05"))
		if n.cliCtx.IsSet("output") {
			logFile = fmt.Sprintf("%s/%s", n.cliCtx.String("output"), logFile)
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			console.Fatalln("could not create confess_log", err)
			return
		}
		f.WriteString(getHeader(n.cliCtx))
		fwriter := bufio.NewWriter(f)

		for {
			select {
			case <-globalContext.Done():
				if _, err := f.WriteString(n.summaryMsg()); err != nil {
					console.Errorln(fmt.Sprintf("Error writing summary to confess_log: %s", err.Error()))
					os.Exit(1)
				}
				fwriter.Flush()
				f.Close()
				n.doneCh <- struct{}{}
				return
			case res, ok := <-n.resCh:
				if !ok {
					return
				}
				eraseOnce := false
				statusChg := n.hcStatus(res.Node)
				if statusChg != "" {
					eraseOnce = printWithErase(eraseOnce, statusChg)
				}
				if res.Err != nil {
					// log node offline|online toggle status or real errors
					if !errors.Is(res.Err, errNodeOffline) || statusChg != "" {
						if _, err := f.WriteString(res.String() + "\n"); err != nil {
							console.Errorln(fmt.Sprintf("Error writing to confess_log for "+res.String(), err))
							os.Exit(1)
						}
					}
				}

				totsuccess := atomic.LoadInt32(&n.numTests) - atomic.LoadInt32(&n.numFailed)
				if !errors.Is(res.Err, errNodeOffline) {
					// summarize the stats here
					atomic.AddInt32(&n.numTests, 1)
					if res.Err != nil {
						atomic.AddInt32(&n.numFailed, 1)
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
				}
				printWithErase(eraseOnce, n.statusBar())
			}
		}
	}()
}

// wait on workers to finish
func (n *nodeState) finish(ctx context.Context) {
	<-globalContext.Done()
	n.wg.Wait()
	close(n.resCh)
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

	// Wait for the signal.
	s := <-sigCh

	// Once signal has been received stop signal Notify handler.
	signal.Stop(sigCh)
	// Cancel the global context
	globalCancel()
	<-n.doneCh // wait on signal that last operation status summary written to log
	var exitCode int
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
	bar := statusKey + statusTextStyle.Render("  Operations succeeded=") + success + statusTextStyle.Render("Operations Failed=") + failures + statusTextStyle.Render("Duration= "+humanize.RelTime(n.startTime, time.Now(), "", ""))
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
	errWidth := min(globalTermWidth-(cols[2].Width+cols[1].Width), len(r.Err.Error()))
	return fmt.Sprintf("%-*s %-*s %-*s %-*s\n",
		maxNodeLen, cols[0].Style.Render(r.Node.Host),
		cols[2].Width, cols[2].Style.Render(r.FuncName),
		cols[1].Width, cols[1].Style.Render(r.Path),
		errWidth, cols[3].Style.Render(r.Err.Error()[:errWidth]))
}

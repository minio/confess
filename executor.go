// Copyright (c) 2023 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	testspkg "github.com/minio/confess/tests"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/ellipses"
)

var slashSeparator = "/"

// Config represents the confess executor configuration.
type Config struct {
	Hosts        []string      `json:"hosts"`
	AccessKey    string        `json:"accessKey"`
	SecretKey    string        `json:"secretKey"`
	Insecure     bool          `json:"insecure,omitempty"`
	Region       string        `json:"region,omitempty"`
	UseSignV2    bool          `json:"useSignV2"`
	Bucket       string        `json:"bucket"`
	FailAfter    int64         `json:"failAfter"`
	Duration     time.Duration `json:"duration"`
	ObjectsCount int           `json:"objectsCount"`
	Concurrency  int           `json:"concurrency"`
	Logger       utils.Logger  `json:"-"`
}

// Validate - validates the config provided.
func (c Config) Validate() error {
	if len(c.Hosts) == 0 {
		return errors.New("empty hosts")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("accessKey and secretKey must be set")
	}
	if c.Bucket == "" {
		return errors.New("bucket name should not be empty")
	}
	return nil
}

// Executor represents the test executor.
type Executor struct {
	Bucket               string
	Clients              []*minio.Client
	EnableVersionedTests bool
	Stats                *testspkg.Stats
	Duration             time.Duration
	FailAfter            int64
	ObjectsCount         int
	Concurrency          int
	Logger               utils.Logger
	sLock                sync.Mutex
}

// NewExecutor returns the instance of the type Executor.
func NewExecutor(ctx context.Context, config Config) (*Executor, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var endpoints []string
	for _, hostStr := range config.Hosts {
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
				log.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", patterns, perr))
			}
			for _, values := range patterns.Expand() {
				endpoints = append(endpoints, values...)
			}
		}
	}
	var clients []*minio.Client
	for _, endpoint := range endpoints {
		clnt, err := newClient(ctx, endpoint, config)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize client for %s; %v", endpoint, err)
		}
		clients = append(clients, clnt)
	}
	client, err := utils.GetRandomClient(clients)
	if err != nil {
		return nil, err
	}
	// Check if the bucket has object lock enabled.
	_, _, _, _, err = client.GetObjectLockConfig(ctx, config.Bucket)
	switch {
	case err != nil && minio.ToErrorResponse(err).Code != "ObjectLockConfigurationNotFoundError":
		return nil, fmt.Errorf("unable to get object lock config; %v", err)
	case err == nil:
		return nil, errors.New("object locking is enabled on this bucket, please use a different one")
	}
	// Check if the bucket has versioning enabled.
	versionConfig, err := client.GetBucketVersioning(ctx, config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("unable to get the bucket info; %v", err)
	}
	return &Executor{
		Bucket:               config.Bucket,
		Clients:              clients,
		EnableVersionedTests: versionConfig.Enabled(),
		Duration:             config.Duration,
		FailAfter:            config.FailAfter,
		Concurrency:          config.Concurrency,
		ObjectsCount:         config.ObjectsCount,
		Logger:               config.Logger,
		Stats:                testspkg.NewStats(),
	}, nil
}

// Execute Tests will execute the tests parallelly and send the progress to the TUI.
func (e *Executor) ExecuteTests(ctx context.Context, tests []testspkg.Test, teaProgram *tea.Program) error {
	defer func() {
		teaProgram.Send(notification{
			waitForCleanup: true,
		})
		e.tearDownTestSuite(context.Background(), tests, teaProgram)
	}()
	ctx, cancel := context.WithTimeout(ctx, e.Duration)
	defer cancel()

	// Monitor the failure count w.r.t fail-after value.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				teaProgram.Send(notification{
					waitForCleanup: true,
				})
				cancel()
				return
			case <-ticker.C:
				_, failedAPIs := e.Stats.APIInfo()
				_, failedTests := e.Stats.TestInfo()
				if failedAPIs+failedTests > e.FailAfter {
					teaProgram.Send(notification{
						err:            errors.New("exceeded fail-after count"),
						waitForCleanup: true,
					})
					cancel()
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Start executing tests.
			e.runTestSuite(ctx, tests, teaProgram)
		}
	}

	return nil
}

func (e *Executor) executeTest(ctx context.Context, test testspkg.Test) (err error) {
	defer func() {
		err = test.TearDown(ctx)
		if err != nil {
			if utils.IsContextError(err) || errors.Is(err, utils.ErrAllTargetsOffline) {
				err = nil
				return
			}
			err = fmt.Errorf("Error while tearing down '%s' test; %v", test.Name(), err)
		}
	}()
	if err := test.Init(ctx, testspkg.Config{
		Clients:      e.Clients,
		Bucket:       e.Bucket,
		Logger:       e.Logger,
		ObjectsCount: e.ObjectsCount,
		Concurrency:  e.Concurrency,
	}, e.Stats); err != nil {
		if utils.IsContextError(err) {
			return nil
		}
		return fmt.Errorf("Error while initializing '%s' test; %v", test.Name(), err)
	}
	if err := test.Setup(ctx); err != nil {
		if utils.IsContextError(err) {
			return nil
		}
		return fmt.Errorf("Error while setting up '%s' test; %v", test.Name(), err)
	}
	if err := test.Run(ctx); err != nil {
		if utils.IsContextError(err) {
			return nil
		}
		return fmt.Errorf("Error while running '%s' test; %v", test.Name(), err)
	}
	return
}

func (e *Executor) tearDownTestSuite(ctx context.Context, tests []testspkg.Test, teaProgram *tea.Program) {
	var wg sync.WaitGroup
	for i := range tests {
		wg.Add(1)
		go func(test testspkg.Test) {
			defer wg.Done()
			if err := test.TearDown(ctx); err != nil && !errors.Is(err, utils.ErrAllTargetsOffline) {
				teaProgram.Send(notification{
					err: fmt.Errorf("unable to cleanup test %s; %v", test.Name(), err),
				})
			}
		}(tests[i])
	}
	wg.Wait()
	return
}

func (e *Executor) runTestSuite(ctx context.Context, tests []testspkg.Test, teaProgram *tea.Program) {
	var wg sync.WaitGroup
	for i := range tests {
		wg.Add(1)
		go func(test testspkg.Test) {
			defer wg.Done()
			if err := e.executeTest(ctx, test); err != nil {
				teaProgram.Send(notification{
					err: err,
				})
			}
		}(tests[i])
	}
	wg.Wait()
	return
}

// MonitorNodeHealth repeatedly checks is the nodes are offline or not and prints it to the console screen and the log file.
func (e *Executor) MonitorNodeHealth(ctx context.Context, teaProgram *tea.Program) {
	offlineMap := make(map[string]time.Time, len(e.Clients))
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := range e.Clients {
				urlStr := e.Clients[i].EndpointURL().String()
				offlineT, ok := offlineMap[urlStr]
				switch {
				case e.Clients[i].IsOffline() && !ok:
					offlineMap[urlStr] = time.Now()
					logMsg := urlStr + " is offline"
					teaProgram.Send(notification{
						log: logMsg,
					})
					e.Logger.Log("\n" + logMsg + "\n")
				case e.Clients[i].IsOnline() && ok:
					logMsg := urlStr + " is back online in " + time.Since(offlineT).String()
					teaProgram.Send(notification{
						log: logMsg,
					})
					e.Logger.Log("\n" + logMsg + "\n")
					delete(offlineMap, urlStr)
				}
			}
		}
	}
}

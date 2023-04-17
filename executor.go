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
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/minio/confess/tests"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
)

var slashSeparator = "/"

// Signature denotes the s3 signature type.
type Signature string

const (
	// SignatureV4 denotes s3v4 algorithm
	SignatureV4 Signature = "s3v4"
	// SignatureV2 denotes s3v2 algorithm
	SignatureV2 Signature = "s3v2"
)

// Config represents the confess executor configuration.
type Config struct {
	Hosts      []string      `json:"hosts"`
	AccessKey  string        `json:"accessKey"`
	SecretKey  string        `json:"secretKey"`
	Insecure   bool          `json:"insecure,omitempty"`
	Region     string        `json:"region,omitempty"`
	Signature  Signature     `json:"signature"`
	Bucket     string        `json:"bucket"`
	OutputFile string        `json:"outputFile"`
	FailAfter  int64         `json:"failAfter"`
	Duration   time.Duration `json:"duration"`
}

// Test interface defines the test.
type Test interface {
	Name() string
	Init(ctx context.Context, config tests.Config, stats *tests.Stats) error
	Setup(ctx context.Context) error
	Run(ctx context.Context) error
	TearDown(ctx context.Context) error
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
	LogFile              string
	Stats                *tests.Stats
	Duration             time.Duration
	FailAfter            int64
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
				console.Fatalln(fmt.Errorf("unable to parse input arg %s: %s", patterns, perr))
			}
			for _, values := range patterns.Expand() {
				endpoints = append(endpoints, strings.Join(values, ""))
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
		LogFile:              config.OutputFile,
		Duration:             config.Duration,
		FailAfter:            config.FailAfter,
		Stats:                &tests.Stats{},
	}, nil
}

// Execute Tests will execute the tests parallelly and send the progress to the TUI.
func (e *Executor) ExecuteTests(ctx context.Context, tests []Test, teaProgram *tea.Program) error {
	f, err := os.OpenFile(e.LogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to open the log file %s; %v", e.LogFile, err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(ctx, e.Duration)
	defer cancel()

	// Monitor the failure count w.r.t fail-after value.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				totalCount, successCount := e.Stats.Info()
				failureCount := totalCount - successCount
				if failureCount > e.FailAfter {
					teaProgram.Send(notification{
						err: errors.New("exceeded fail-after count"),
					})
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range tests {
		wg.Add(1)
		go func(test Test) {
			defer wg.Done()
			if err = e.executeTest(ctx, test, f); err != nil {
				teaProgram.Send(notification{
					err: err,
				})
			}
		}(tests[i])
	}
	wg.Wait()

	return nil
}

func (e *Executor) executeTest(ctx context.Context, test Test, logFile *os.File) (err error) {
	defer func() {
		err = test.TearDown(ctx)
		if err != nil {
			err = fmt.Errorf("Error while tearing down '%s' test; %v", test.Name(), err)
		}
	}()
	if err := test.Init(ctx, tests.Config{
		Clients: e.Clients,
		Bucket:  e.Bucket,
		LogFile: logFile,
	}, e.Stats); err != nil {
		return fmt.Errorf("Error while initializing '%s' test; %v", test.Name(), err)
	}
	if err := test.Setup(ctx); err != nil {
		return fmt.Errorf("Error while setting up '%s' test; %v", test.Name(), err)
	}
	if err := test.Run(ctx); err != nil {
		return fmt.Errorf("Error while running '%s' test; %v", test.Name(), err)
	}
	return
}

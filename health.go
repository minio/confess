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
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/madmin-go"
)

const (
	defaultHealthCheckDuration = 1 * time.Second
)

type healthChecker struct {
	mutex    sync.RWMutex
	hc       map[string]epHealth
	hcClient *madmin.AnonymousClient
}

// epHealth struct represents health of an endpoint.
type epHealth struct {
	Endpoint string
	Scheme   string
	Online   bool
}

// isOffline returns current liveness result of an endpoint. Add endpoint to
// healthcheck map if missing and default to online status
func (c *healthChecker) isOffline(ep *url.URL) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if h, ok := c.hc[ep.Host]; ok {
		return !h.Online
	}
	go c.initHC(ep)
	return false
}

func (c *healthChecker) allOffline() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	for _, h := range c.hc {
		if h.Online {
			return false
		}
	}
	return true
}

func (c *healthChecker) initHC(ep *url.URL) {
	c.mutex.Lock()
	c.hc[ep.Host] = epHealth{
		Endpoint: ep.Host,
		Scheme:   ep.Scheme,
		Online:   true,
	}
	c.mutex.Unlock()
}

func newHealthChecker(ctx *cli.Context, m map[string]epHealth) *healthChecker {
	hc := healthChecker{
		hc:       m,
		hcClient: newHCClient(ctx),
	}
	go hc.heartBeat(globalContext)
	return &hc
}

// newHCClient initializes an anonymous client for performing health check on the remote endpoints
func newHCClient(ctx *cli.Context) *madmin.AnonymousClient {
	clnt, e := madmin.NewAnonymousClientNoEndpoint()
	if e != nil {
		log.Fatal(fmt.Errorf("WARNING: Unable to initialize health check client"))
		return nil
	}
	tr := clientTransport(ctx, false)
	clnt.SetCustomTransport(tr)
	return clnt
}

func (h *healthChecker) heartBeat(ctx context.Context) {
	hcTimer := time.NewTimer(defaultHealthCheckDuration)
	defer hcTimer.Stop()
	for {
		select {
		case <-hcTimer.C:
			h.mutex.RLock()
			var eps []madmin.ServerProperties
			for _, ep := range h.hc {
				eps = append(eps, madmin.ServerProperties{Endpoint: ep.Endpoint, Scheme: ep.Scheme})
			}
			h.mutex.RUnlock()

			if len(eps) > 0 {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				m := map[string]epHealth{}
				for result := range h.hcClient.Alive(cctx, madmin.AliveOpts{}, eps...) {
					var online bool
					if result.Error == nil {
						online = result.Online
					}
					m[result.Endpoint.Host] = epHealth{
						Endpoint: result.Endpoint.Host,
						Scheme:   result.Endpoint.Scheme,
						Online:   online,
					}
				}
				cancel()
				h.mutex.Lock()
				h.hc = m
				h.mutex.Unlock()
			}
			hcTimer.Reset(defaultHealthCheckDuration)
		case <-ctx.Done():
			return
		}
	}
}

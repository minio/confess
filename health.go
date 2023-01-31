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
	"crypto/tls"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/cli"
	"golang.org/x/net/publicsuffix"
)

const (
	defaultHealthCheckDuration = 2 * time.Second
)

type healthChecker struct {
	mutex sync.RWMutex
	hc    map[string]*hcClient
}

// hcClient struct represents health of an endpoint.
type hcClient struct {
	EndpointURL *url.URL
	// Scheme      string
	Online     int32
	httpClient *http.Client
}

// isOffline returns current liveness result of an endpoint. Add endpoint to
// healthcheck map if missing and default to online status
func (c *healthChecker) isOffline(ep *url.URL) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if h, ok := c.hc[ep.Host]; ok {
		return !h.isOnline()
	}
	return false
}

func (c *healthChecker) allOffline() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	for _, h := range c.hc {
		if h.isOnline() {
			return false
		}
	}
	return true
}

func newHealthChecker(ctx *cli.Context, m map[string]*hcClient) *healthChecker {
	hc := healthChecker{
		hc: m,
	}
	go hc.heartBeat(globalContext)
	return &hc
}

func (h *hcClient) setOffline() {
	atomic.StoreInt32(&h.Online, 0)
}

func (h *hcClient) setOnline() {
	atomic.StoreInt32(&h.Online, 1)
}
func (h *hcClient) isOnline() bool {
	return atomic.LoadInt32(&h.Online) == 1
}
func (h *hcClient) isOffline() bool {
	return atomic.LoadInt32(&h.Online) == 0
}

type epHealth struct {
	Online   bool
	Endpoint *url.URL
	Error    error
}

// healthCheck - background routine which checks if a backend is up or down.
func (h *healthChecker) heartBeat(ctx context.Context) {
	hcTimer := time.NewTimer(defaultHealthCheckDuration)
	defer hcTimer.Stop()
	for {
		select {
		case <-hcTimer.C:
			for result := range h.healthCheck(ctx) {
				var online bool
				if result.Error == nil || result.Online {
					online = result.Online
				}
				if hcClient, ok := h.hc[result.Endpoint.Host]; ok {
					if online {
						hcClient.setOnline()
					} else {
						hcClient.setOffline()
					}
					h.mutex.Lock()
					h.hc[result.Endpoint.Host] = hcClient
					h.mutex.Unlock()
				}
			}
			hcTimer.Reset(defaultHealthCheckDuration)
		case <-ctx.Done():
			return
		}
	}
}
func (h *healthChecker) healthCheck(ctx context.Context) (resultsCh chan epHealth) {
	resultsCh = make(chan epHealth)
	go func() {
		defer close(resultsCh)
		var wg sync.WaitGroup
		wg.Add(len(h.hc))
		for _, hcClient := range h.hc {
			hcClient := hcClient

			go func() {
				defer wg.Done()
				err := hcClient.healthCheck(ctx, resultsCh)
				if err != nil {
					resultsCh <- epHealth{
						Error: err,
					}
					return
				}
			}()
		}
		wg.Wait()
	}()
	return resultsCh
}

func (hc *hcClient) healthCheck(ctx context.Context, resultsCh chan epHealth) error {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hc.EndpointURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := hc.httpClient.Do(req)
	if err == nil {
		// Drain the connection.
		io.Copy(ioutil.Discard, resp.Body)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	result := epHealth{
		Endpoint: hc.EndpointURL,
	}
	if err != nil || (err == nil && resp.StatusCode != http.StatusForbidden) {
		// observed an error, take the backend down.
	} else {
		result.Online = true
	}
	if err != nil {
		result.Error = err
	}

	select {
	case <-ctx.Done():
		return nil
	case resultsCh <- result:
	}
	return nil
}

// DefaultTransport - this default transport is similar to
// http.DefaultTransport but with additional param  DisableCompression
// is set to true to avoid decompressing content with 'gzip' encoding.
var DefaultTransport = func(secure bool) http.RoundTripper {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:       5 * time.Second,
			KeepAlive:     15 * time.Second,
			FallbackDelay: 100 * time.Millisecond,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		ResponseHeaderTimeout: 60 * time.Second,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	}

	if secure {
		tr.TLSClientConfig = &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		}
	}
	return tr
}

const (
	Offline = 0
	Online  = 1
)

// newHCClient creates a new health check client
func newHCClient(epURL *url.URL) (hc hcClient, err error) {
	// Initialize cookies to preserve server sent cookies if any and replay
	// them upon each request.
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return hc, err
	}

	// Save endpoint URL, user agent for future uses.
	hc.EndpointURL = epURL

	// default online to true
	atomic.StoreInt32(&hc.Online, Online)
	// Instantiate http client and bucket location cache.
	hc.httpClient = &http.Client{
		Jar:       jar,
		Transport: DefaultTransport(epURL.Scheme == "https"),
	}

	return hc, nil
}

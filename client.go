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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/console/pkg"
	md5simd "github.com/minio/md5-simd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/net/http2"
)

func newClient(ctx context.Context, endpoint string, config Config) (*minio.Client, error) {
	endpoint = strings.TrimSuffix(endpoint, slashSeparator)
	targetURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the endpoint %s; %v", endpoint, err)
	}
	if targetURL.Scheme == "" {
		targetURL.Scheme = "http"
	}
	creds := credentials.NewStaticV4(config.AccessKey, config.SecretKey, "")
	if config.UseSignV2 {
		creds = credentials.NewStaticV2(config.AccessKey, config.SecretKey, "")
	}
	isSecured := targetURL.Scheme == "https"
	clnt, err := minio.New(targetURL.Host, &minio.Options{
		Creds:        creds,
		Secure:       isSecured,
		Region:       config.Region,
		BucketLookup: minio.BucketLookupAuto,
		CustomMD5:    md5simd.NewServer().NewHash,
		Transport:    clientTransport(ctx, isSecured, config),
	})
	if err != nil {
		return nil, err
	}
	// start healthcheck on the endpoint
	cancelFn, err := clnt.HealthCheck(2 * time.Second)
	if err != nil {
		return nil, err

	}
	go func() {
		<-ctx.Done()
		cancelFn()
	}()
	clnt.SetAppInfo("confess", pkg.Version)
	return clnt, nil
}

// clientTransport returns a new http configuration
// used while communicating with the host.
func clientTransport(ctx context.Context, enableTLS bool, config Config) *http.Transport {
	// For more details about various values used here refer
	// https://golang.org/pkg/net/http/#Transport documentation
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   1024,
		WriteBufferSize:       32 << 10, // 32KiB moving up from 4KiB default
		ReadBufferSize:        32 << 10, // 32KiB moving up from 4KiB default
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Go net/http automatically unzip if content-type is
		// gzip disable this feature, as we are always interested
		// in raw stream.
		DisableCompression: true,
	}
	if enableTLS {
		// Keep TLS config.
		tr.TLSClientConfig = &tls.Config{
			RootCAs:            mustGetSystemCertPool(),
			InsecureSkipVerify: config.Insecure,
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		}
		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		http2.ConfigureTransport(tr)
	}

	return tr
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/console/pkg"
	md5simd "github.com/minio/md5-simd"
	"github.com/minio/pkg/console"
	"golang.org/x/net/http2"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var slashSeparator = "/"

// clientTransport returns a new http configuration
// used while communicating with the host.
func clientTransport(ctx *cli.Context, enableTLS bool) *http.Transport {
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
			InsecureSkipVerify: ctx.Bool("insecure"),
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

// getClient creates a client with the specified host and the options set in the context.
func getClient(ctx *cli.Context, hostURL *url.URL) (*minio.Client, error) {
	var creds *credentials.Credentials
	switch strings.ToUpper(ctx.String("signature")) {
	case "S3V4":
		// if Signature version '4' use NewV4 directly.
		creds = credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), "")
	case "S3V2":
		// if Signature version '2' use NewV2 directly.
		creds = credentials.NewStaticV2(ctx.String("access-key"), ctx.String("secret-key"), "")
	default:
		console.Fatalln(errors.New("unknown signature method. S3V2 and S3V4 is available"), strings.ToUpper(ctx.String("signature")))
	}
	cl, err := minio.New(hostURL.Host, &minio.Options{
		Creds:        creds,
		Secure:       hostURL.Scheme == "https",
		Region:       ctx.String("region"),
		BucketLookup: minio.BucketLookupAuto,
		CustomMD5:    md5simd.NewServer().NewHash,
		Transport:    clientTransport(ctx, hostURL.Scheme == "https"),
	})
	if err != nil {
		return nil, err
	}
	cl.SetAppInfo("confess", pkg.Version)
	return cl, nil
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

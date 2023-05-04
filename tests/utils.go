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

package tests

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	xnet "github.com/minio/pkg/net"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Config represents the test config
type Config struct {
	Clients      []*minio.Client
	Bucket       string
	Logger       utils.Logger
	ObjectsCount int
	Concurrency  int
}

func newRandomReader(seed, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

// read data from file if it exists or optionally create a buffer of particular size
func reader(size int64) io.ReadCloser {
	return ioutil.NopCloser(newRandomReader(size, size))
}

func logMessage(testName string, client *minio.Client, msg string) string {
	message := fmt.Sprintf("[%v]", time.Now().Format("2006-01-02T15:04:05"))

	if testName != "" {
		message += "[" + testName + "]"
	}
	if client != nil && client.EndpointURL() != nil {
		message += "[" + client.EndpointURL().String() + "]"
	}
	if msg != "" {
		message += " " + msg
	}

	return "\n" + message + "\n"
}

func isIgnored(err error) bool {
	return utils.IsContextError(err) || xnet.IsNetworkOrHostDown(err, false)
}

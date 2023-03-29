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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Config represents the test config
type Config struct {
	Clients []*minio.Client
	Bucket  string
	LogFile *os.File
}

func newRandomReader(seed, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

// read data from file if it exists or optionally create a buffer of particular size
func reader(size int64) io.ReadCloser {
	return ioutil.NopCloser(newRandomReader(size, size))
}

func log(ctx context.Context, f *os.File, testName, clientURL, msg string) error {
	_, err := f.WriteString(fmt.Sprintf("[%s][%s] %s", testName, clientURL, msg) + "\n")
	return err
}

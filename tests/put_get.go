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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
)

const objectCountForPutGetTest = 50

// PutGetTest uploads the objects using a random client and then
// Gets the objects from each node to verify if the get output
// is consistent accross nodes.
type PutGetTest struct {
	clients           []*minio.Client
	logFile           *os.File
	bucket            string
	objectsWithSha256 map[string][]byte
	apiStats          *Stats
}

func (t *PutGetTest) bucketName() string {
	return t.bucket
}

// Name - name of the test.
func (t *PutGetTest) Name() string {
	return "PutGet"
}

// Init initialized the test.
func (t *PutGetTest) Init(ctx context.Context, config Config, stats *Stats) error {
	t.clients = config.Clients
	t.logFile = config.LogFile
	t.bucket = config.Bucket
	t.objectsWithSha256 = make(map[string][]byte, objectCountForPutGetTest)
	t.apiStats = stats
	return nil
}

// Setup prepares the test by uploading the objects to the test bucket.
func (t *PutGetTest) Setup(ctx context.Context) error {
	client, err := utils.GetRandomClient(t.clients)
	if err != nil {
		return err
	}
	for i := 0; i < objectCountForPutGetTest; i++ {
		object := fmt.Sprintf("confess/%s/%s/pfx-%d", t.Name(), time.Now().Format("01_02_06_15:04"), i)
		hasher := sha256.New()
		tr := io.TeeReader(reader(humanize.MiByte*5), hasher)
		if _, err := put(ctx, putConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: object,
			reader:     tr,
			opts:       minio.PutObjectOptions{},
		}, t.apiStats); err != nil {
			if err := log(ctx, t.logFile, t.Name(), client.EndpointURL().String(), fmt.Sprintf("unable to put the object %s; %v", object, err)); err != nil {
				return err
			}
		} else {
			t.objectsWithSha256[object] = hasher.Sum(nil)
		}
	}
	return nil
}

// Run executes the test by verifying if the get output is
// consistent accross the nodes.
func (t *PutGetTest) Run(ctx context.Context) error {
	var offlineNodes int
	var errFound bool
	for i := 0; i < len(t.clients); i++ {
		if t.clients[i].IsOffline() {
			offlineNodes++
			if err := log(
				ctx,
				t.logFile,
				t.Name(),
				t.clients[i].EndpointURL().String(),
				fmt.Sprintf("the node %s is offline", t.clients[i].EndpointURL().String()),
			); err != nil {
				return err
			}
			continue
		}
		if err := t.verify(ctx, t.clients[i]); err != nil {
			if err := log(
				ctx,
				t.logFile,
				t.Name(),
				t.clients[i].EndpointURL().String(),
				fmt.Sprintf("unable to verify %s test; %v", t.Name(), err),
			); err != nil {
				return err
			}
			errFound = true
		}

	}
	if offlineNodes == len(t.clients) {
		return errors.New("all nodes are offline")
	}
	if errFound {
		return errors.New("get inconstent across nodes")
	}
	return nil
}

// TearDown removes the uploaded objects from the test bucket.
func (t *PutGetTest) TearDown(ctx context.Context) error {
	client, err := utils.GetRandomClient(t.clients)
	if err != nil {
		return err
	}
	return removeObjects(ctx, removeObjectsConfig{
		client:     client,
		bucketName: t.bucket,
		logFile:    t.logFile,
		listOpts: minio.ListObjectsOptions{
			Recursive: true,
			Prefix:    fmt.Sprintf("confess/%s/", t.Name()),
		},
	}, t.apiStats)
}

func (t *PutGetTest) verify(ctx context.Context, client *minio.Client) error {
	for objectName, sha256sum := range t.objectsWithSha256 {
		object, err := get(ctx, getConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: objectName,
			opts:       minio.GetObjectOptions{},
		}, t.apiStats)
		if err != nil {
			return err
		}
		// TODO: Verify ETAG
		_, err = object.Stat()
		if err != nil {
			return err
		}
		hasher := sha256.New()
		_, err = io.Copy(hasher, object)
		if err != nil {
			return err
		}
		if !bytes.Equal(sha256sum, hasher.Sum(nil)) {
			return fmt.Errorf("sha256 mismatch for object %s", objectName)
		}
	}
	return nil
}

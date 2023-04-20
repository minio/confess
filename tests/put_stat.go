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
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	xnet "github.com/minio/pkg/net"
)

const objectCountForPutStatTest = 50

// PutStatTest uploads the objects using a random client and then
// Stats the objects from each node to verify if the stat output
// is consistent accross nodes.
type PutStatTest struct {
	clients             []*minio.Client
	logFile             *os.File
	bucket              string
	uploadedObjectNames sync.Map
	apiStats            *Stats
	concurrency         int
}

func (t *PutStatTest) bucketName() string {
	return t.bucket
}

// Name - name of the test.
func (t *PutStatTest) Name() string {
	return "PutStat"
}

// Init initialized the test.
func (t *PutStatTest) Init(ctx context.Context, config Config, stats *Stats) error {
	t.clients = config.Clients
	t.logFile = config.LogFile
	t.bucket = config.Bucket
	t.uploadedObjectNames = sync.Map{}
	t.apiStats = stats
	t.concurrency = config.Concurrency
	return nil
}

// Setup prepares the test by uploading the objects to the test bucket.
func (t *PutStatTest) Setup(ctx context.Context) error {
	return runFn(ctx, objectCountForPutStatTest, t.concurrency, func(ctx context.Context, index int) error {
		client, err := utils.GetRandomClient(t.clients)
		if err != nil {
			return err
		}
		object := fmt.Sprintf("confess/%s/%s/pfx-%d", t.Name(), time.Now().Format("01_02_06_15:04"), index)
		if _, err := put(ctx, putConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: object,
			size:       humanize.MiByte * 5,
			opts:       minio.PutObjectOptions{},
		}, t.apiStats); err != nil {
			if err := log(ctx,
				t.logFile,
				t.Name(),
				client.EndpointURL().String(),
				fmt.Sprintf("unable to put the object %s; %v", object, err)); err != nil {
				return err
			}
		} else {
			t.uploadedObjectNames.Store(object, struct{}{})
		}
		return nil
	})
}

// Run executes the test by verifying if the stat output is
// consistent accross the nodes.
func (t *PutStatTest) Run(ctx context.Context) error {
	var offlineNodes atomic.Int64
	var errFound atomic.Bool
	if err := runFn(ctx, len(t.clients), t.concurrency, func(ctx context.Context, index int) error {
		if t.clients[index].IsOffline() {
			offlineNodes.Add(1)
			if err := log(
				ctx,
				t.logFile,
				t.Name(),
				t.clients[index].EndpointURL().String(),
				fmt.Sprintf("the node %s is offline", t.clients[index].EndpointURL().String()),
			); err != nil {
				return err
			}
			return nil
		}
		if err := t.verify(ctx, t.clients[index]); err != nil {
			if !xnet.IsNetworkOrHostDown(err, true) {
				errFound.CompareAndSwap(false, true)
			} else {
				offlineNodes.Add(1)
			}
			if err := log(
				ctx,
				t.logFile,
				t.Name(),
				t.clients[index].EndpointURL().String(),
				fmt.Sprintf("unable to verify %s; %v", t.Name(), err),
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if offlineNodes.Load() == int64(len(t.clients)) {
		return errors.New("all nodes are offline")
	}
	if errFound.Load() {
		return errors.New("stat inconsistent across nodes")
	}
	return nil
}

// TearDown removes the uploaded objects from the test bucket.
func (t *PutStatTest) TearDown(ctx context.Context) error {
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

func (t *PutStatTest) verify(ctx context.Context, client *minio.Client) (err error) {
	t.uploadedObjectNames.Range(func(key, _ any) bool {
		objectName := key.(string)
		_, err = stat(ctx, statConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: objectName,
			opts:       minio.StatObjectOptions{},
		}, t.apiStats)
		return err == nil
	})
	return
}

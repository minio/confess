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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	xnet "github.com/minio/pkg/net"
)

// PutListTest uploads the objects using a random client and then
// Lists the objects from each node to verify if the list output
// is consistent accross nodes.
type PutListTest struct {
	clients             []*minio.Client
	logger              utils.Logger
	bucket              string
	uploadedObjectNames sync.Map
	apiStats            *Stats
	concurrency         int
	objectsCount        int
	initialized         bool
}

func (t *PutListTest) bucketName() string {
	return t.bucket
}

// Name - name of the test.
func (t *PutListTest) Name() string {
	return "PutList"
}

// Init initialized the test.
func (t *PutListTest) Init(ctx context.Context, config Config, stats *Stats) error {
	t.clients = config.Clients
	t.logger = config.Logger
	t.bucket = config.Bucket
	t.uploadedObjectNames = sync.Map{}
	t.apiStats = stats
	t.concurrency = config.Concurrency
	t.objectsCount = config.ObjectsCount
	t.initialized = true
	return nil
}

// Setup prepares the test by uploading the objects to the test bucket.
func (t *PutListTest) Setup(ctx context.Context) error {
	if !t.initialized {
		return errTestNotInitialized
	}
	return runFn(ctx, t.objectsCount, t.concurrency, func(ctx context.Context, index int) error {
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
			if !utils.IsContextError(err) {
				t.logger.V(3).Log(logMessage(t.Name(), client, err.Error()))
			}
			if !xnet.IsNetworkOrHostDown(err, false) {
				return err
			}
		} else {
			t.uploadedObjectNames.Store(object, struct{}{})
		}
		return nil
	})
}

// Run executes the test by verifying if the list output is
// consistent accross the nodes.
func (t *PutListTest) Run(ctx context.Context) error {
	if !t.initialized {
		return errTestNotInitialized
	}
	var offlineNodes atomic.Int64
	var errFound atomic.Bool
	if err := runFn(ctx, len(t.clients), t.concurrency, func(ctx context.Context, index int) error {
		if t.clients[index].IsOffline() {
			offlineNodes.Add(1)
			return nil
		}
		if err := t.verify(ctx, t.clients[index]); err != nil {
			switch {
			case utils.IsContextError(err):
				return err
			case xnet.IsNetworkOrHostDown(err, false):
				offlineNodes.Add(1)
			default:
				errFound.CompareAndSwap(false, true)
				if err := t.logger.Log(
					logMessage(
						t.Name(),
						t.clients[index],
						fmt.Sprintf("unable to verify %s test; %v", t.Name(), err),
					),
				); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.logger.V(3).Log(logMessage(t.Name(), nil, err.Error()))
		return err
	}
	if offlineNodes.Load() == int64(len(t.clients)) {
		return errors.New("all nodes are offline")
	}
	if errFound.Load() {
		return errors.New("list inconsistent across nodes")
	}
	return nil
}

// TearDown removes the uploaded objects from the test bucket.
func (t *PutListTest) TearDown(ctx context.Context) error {
	if !t.initialized {
		// No clean up required.
		return nil
	}
	for {
		client, err := utils.GetRandomClient(t.clients)
		if err != nil {
			t.logger.V(4).Log(logMessage(t.Name(), client, err.Error()))
			return err
		}
		if err := removeObjects(ctx, removeObjectsConfig{
			client:     client,
			bucketName: t.bucket,
			listOpts: minio.ListObjectsOptions{
				Recursive: true,
				Prefix:    fmt.Sprintf("confess/%s/", t.Name()),
			},
			skipErrStat: t.logger.CurrentV() < 4,
		}, t.apiStats); err != nil {
			t.logger.V(4).Log(logMessage(t.Name(), client, err.Error()))
			if xnet.IsNetworkOrHostDown(err, false) {
				continue
			}
			return err
		}
		return nil
	}
}

func (t *PutListTest) verify(ctx context.Context, client *minio.Client) (err error) {
	objInfoList, err := list(ctx, listConfig{
		client:     client,
		bucketName: t.bucket,
		opts: minio.ListObjectsOptions{
			Recursive: true,
			Prefix:    fmt.Sprintf("confess/%s/", t.Name()),
		},
	}, t.apiStats)
	if err != nil {
		return err
	}
	if !t.match(objInfoList) {
		return fmt.Errorf("list results were found to be inconsistent in client %s", client.EndpointURL().String())
	}
	return nil
}

func (t *PutListTest) match(objInfoList []minio.ObjectInfo) bool {
	uploadedObjectNames := make(map[string]struct{})
	t.uploadedObjectNames.Range(func(key, _ any) bool {
		uploadedObjectNames[key.(string)] = struct{}{}
		return true
	})
	objNamesInResult := make(map[string]struct{}, len(objInfoList))
	for _, objInfo := range objInfoList {
		objNamesInResult[objInfo.Key] = struct{}{}
	}
	return reflect.DeepEqual(uploadedObjectNames, objNamesInResult)
}

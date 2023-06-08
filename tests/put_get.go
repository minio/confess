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
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
	xnet "github.com/minio/pkg/net"
)

// PutGetTest uploads the objects using a random client and then
// Gets the objects from each node to verify if the get output
// is consistent accross nodes.
type PutGetTest struct {
	clients             []*minio.Client
	logFile             *os.File
	bucket              string
	objectsWithMetaData sync.Map
	stats               *Stats
	concurrency         int
	objectsCount        int
	logger              utils.Logger
	initialized         bool
}

// metaData to store the uploaded object's info
// which will be used for verification.
type metaData struct {
	sha256 []byte
	size   int64
	etag   string
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
	t.logger = config.Logger
	t.bucket = config.Bucket
	t.objectsWithMetaData = sync.Map{}
	t.stats = stats
	t.concurrency = config.Concurrency
	t.objectsCount = config.ObjectsCount
	t.initialized = true
	return nil
}

// Setup prepares the test by uploading the objects to the test bucket.
func (t *PutGetTest) Setup(ctx context.Context) error {
	if !t.initialized {
		return errTestNotInitialized
	}
	return runFn(ctx, t.objectsCount, t.concurrency, func(ctx context.Context, index int) error {
		client, err := utils.GetRandomClient(t.clients)
		if err != nil {
			t.logger.V(3).Log(logMessage(t.Name(), nil, err.Error()))
			return err
		}
		object := fmt.Sprintf("confess/%s/%s/pfx-%d", t.Name(), time.Now().Format("01_02_06_15:04"), index)
		hasher := sha256.New()
		tr := io.TeeReader(reader(humanize.MiByte*5), hasher)
		if objectInfo, err := put(ctx, putConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: object,
			reader:     tr,
			opts:       minio.PutObjectOptions{},
		}, t.stats); err != nil {
			if !utils.IsContextError(err) {
				t.logger.V(3).Log(logMessage(t.Name(), client, err.Error()))
			}
			if !xnet.IsNetworkOrHostDown(err, false) {
				return err
			}
		} else {
			t.objectsWithMetaData.Store(object, metaData{
				sha256: hasher.Sum(nil),
				size:   objectInfo.Size,
				etag:   objectInfo.ETag,
			})
		}
		return nil
	})
}

// Run executes the test by verifying if the get output is
// consistent accross the nodes.
func (t *PutGetTest) Run(ctx context.Context) error {
	if !t.initialized {
		return errTestNotInitialized
	}
	var offlineNodes atomic.Int64
	var errFound atomic.Bool
	if err := runFn(ctx,
		len(t.clients),
		t.concurrency,
		func(ctx context.Context, index int) error {
			if t.clients[index].IsOffline() {
				offlineNodes.Add(1)
				return nil
			}
			t.stats.incrementTotalTests()
			if err := t.verify(ctx, t.clients[index]); err != nil {
				switch {
				case utils.IsContextError(err):
					return err
				case xnet.IsNetworkOrHostDown(err, false):
					offlineNodes.Add(1)
				default:
					errFound.CompareAndSwap(false, true)
					t.stats.incrementFailedTests()
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
		},
	); err != nil {
		t.logger.V(3).Log(logMessage(t.Name(), nil, err.Error()))
		return err
	}
	if offlineNodes.Load() == int64(len(t.clients)) {
		return errors.New("all nodes are offline")
	}
	if errFound.Load() {
		return errors.New("get inconsistent across nodes")
	}
	return nil
}

// TearDown removes the uploaded objects from the test bucket.
func (t *PutGetTest) TearDown(ctx context.Context) error {
	if !t.initialized {
		// No clean up required.
		return nil
	}
	for {
		client, err := utils.GetRandomClient(t.clients)
		if err != nil {
			t.logger.V(4).Log(logMessage(t.Name(), nil, err.Error()))
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
		}, t.stats); err != nil {
			t.logger.V(4).Log(logMessage(t.Name(), client, err.Error()))
			if xnet.IsNetworkOrHostDown(err, false) {
				continue
			}
			return err
		}
		return nil
	}
}

func (t *PutGetTest) verify(ctx context.Context, client *minio.Client) (err error) {
	t.objectsWithMetaData.Range(func(key, value any) bool {
		objectName := key.(string)
		savedMetaData := value.(metaData)
		var object *minio.Object
		object, err = get(ctx, getConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: objectName,
			opts:       minio.GetObjectOptions{},
		}, t.stats)
		if err != nil {
			return false
		}
		var objectInfo minio.ObjectInfo
		objectInfo, err = object.Stat()
		if err != nil {
			return false
		}
		if objectInfo.ETag != savedMetaData.etag {
			err = fmt.Errorf("ETag mismatch for object %s", objectName)
			return false
		}
		if objectInfo.Size != savedMetaData.size {
			err = fmt.Errorf("size mismatch for object %s", objectName)
			return false
		}
		hasher := sha256.New()
		_, err = io.Copy(hasher, object)
		if err != nil {
			return false
		}
		if !bytes.Equal(savedMetaData.sha256, hasher.Sum(nil)) {
			err = fmt.Errorf("sha256 mismatch for object %s", objectName)
			return false
		}
		return true
	})
	return
}

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

package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/stats"
	"github.com/minio/confess/utils"
	"github.com/minio/minio-go/v7"
)

const objectCountForPutListTest = 50

type PutListTest struct {
	clients             []*minio.Client
	logFile             *os.File
	bucket              string
	uploadedObjectNames map[string]struct{}
	apiStats            *stats.APIStats
}

func (t *PutListTest) bucketName() string {
	return t.bucket
}

func (t *PutListTest) Name() string {
	return "PutList"
}

func (t *PutListTest) Init(ctx context.Context, config Config, stats *stats.APIStats) error {
	t.clients = config.Clients
	t.logFile = config.LogFile
	t.bucket = config.Bucket
	t.uploadedObjectNames = make(map[string]struct{}, objectCountForPutListTest)
	t.apiStats = stats
	return nil
}

func (t *PutListTest) PreRun(ctx context.Context) error {
	client, err := utils.GetRandomClient(t.clients)
	if err != nil {
		return err
	}
	for i := 0; i < objectCountForPutListTest; i++ {
		object := fmt.Sprintf("confess/%s/%s/pfx-%d", t.Name(), time.Now().Format("01_02_06_15:04"), i)
		if _, err := put(ctx, putConfig{
			client:     client,
			bucketName: t.bucket,
			objectName: object,
			size:       humanize.MiByte * 5,
			opts:       minio.PutObjectOptions{},
		}, t.apiStats); err != nil {
			if err := log(ctx, t.logFile, t.Name(), client.EndpointURL().String(), fmt.Sprintf("unable to put the object %s; %v", object, err)); err != nil {
				return err
			}
		} else {
			t.uploadedObjectNames[object] = struct{}{}
		}
	}
	return nil
}

func (t *PutListTest) Run(ctx context.Context) error {
	var offlineNodes int
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
		objInfoList, err := list(ctx, listConfig{
			client:     t.clients[i],
			bucketName: t.bucket,
			opts: minio.ListObjectsOptions{
				Recursive: true,
				Prefix:    fmt.Sprintf("confess/%s/", t.Name()),
			},
		}, t.apiStats)
		if err != nil {
			if err := log(
				ctx,
				t.logFile,
				t.Name(),
				t.clients[i].EndpointURL().String(),
				fmt.Sprintf("unable to list; %v", err),
			); err != nil {
				return err
			}
		}
		if !t.match(objInfoList) {
			return fmt.Errorf("list results were found to be inconsistent in client %s", t.clients[i].EndpointURL().String())
		}
	}
	if offlineNodes == len(t.clients) {
		return errors.New("all nodes are offline")
	}
	return nil
}

func (t *PutListTest) PostRun(ctx context.Context) error {
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

func (t *PutListTest) match(objInfoList []minio.ObjectInfo) bool {
	objNamesInResult := make(map[string]struct{}, len(objInfoList))
	for _, objInfo := range objInfoList {
		objNamesInResult[objInfo.Key] = struct{}{}
	}
	return reflect.DeepEqual(t.uploadedObjectNames, objNamesInResult)
}

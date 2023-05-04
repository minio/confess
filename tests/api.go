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
	"io"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

var (
	errNilClient = errors.New("client is nil")
)

// Stats denotes the statistical information on the API requests made.
type Stats struct {
	TotalCount   atomic.Int64
	SuccessCount atomic.Int64
}

func (stats *Stats) String() string {
	total, success := stats.Info()
	return fmt.Sprintf("Total: %d\nSuccess: %d\nFailed: %d", total, success, total-success)
}

func (stats *Stats) increment(success bool) {
	stats.TotalCount.Add(1)
	if success {
		stats.SuccessCount.Add(1)
	}
}

// Info returns the stat info.
func (stats *Stats) Info() (total, success int64) {
	return stats.TotalCount.Load(), stats.SuccessCount.Load()
}

type putConfig struct {
	client     *minio.Client
	bucketName string
	objectName string
	size       int64
	reader     io.Reader
	opts       minio.PutObjectOptions
}

func put(ctx context.Context, config putConfig, stats *Stats) (info minio.UploadInfo, err error) {
	defer func() {
		if !isIgnored(err) {
			stats.increment(err == nil)
		}
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	if config.reader == nil {
		reader := reader(config.size)
		defer reader.Close()
		config.reader = reader
	}
	return config.client.PutObject(ctx, config.bucketName, config.objectName, config.reader, config.size, config.opts)
}

type listConfig struct {
	client     *minio.Client
	bucketName string
	opts       minio.ListObjectsOptions
}

func list(ctx context.Context, config listConfig, stats *Stats) (objInfo []minio.ObjectInfo, err error) {
	defer func() {
		if !isIgnored(err) {
			stats.increment(err == nil)
		}
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	var objList []minio.ObjectInfo
	for object := range config.client.ListObjects(ctx, config.bucketName, config.opts) {
		if object.Err != nil {
			return nil, object.Err
		}
		objList = append(objList, object)
	}
	return objList, nil
}

type statConfig struct {
	client     *minio.Client
	bucketName string
	objectName string
	opts       minio.StatObjectOptions
}

func stat(ctx context.Context, config statConfig, stats *Stats) (info minio.ObjectInfo, err error) {
	defer func() {
		if !isIgnored(err) {
			stats.increment(err == nil)
		}
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	return config.client.StatObject(ctx, config.bucketName, config.objectName, config.opts)
}

type getConfig struct {
	client     *minio.Client
	bucketName string
	objectName string
	opts       minio.GetObjectOptions
}

func get(ctx context.Context, config getConfig, stats *Stats) (object *minio.Object, err error) {
	defer func() {
		if !isIgnored(err) {
			stats.increment(err == nil)
		}
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	return config.client.GetObject(ctx, config.bucketName, config.objectName, config.opts)
}

type removeObjectsConfig struct {
	client     *minio.Client
	bucketName string
	listOpts   minio.ListObjectsOptions
}

func removeObjects(ctx context.Context, config removeObjectsConfig, stats *Stats) (err error) {
	objects, err := list(ctx, listConfig{
		client:     config.client,
		bucketName: config.bucketName,
		opts:       config.listOpts,
	}, stats)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := removeObject(ctx, removeObjectConfig{
			client:     config.client,
			bucketName: config.bucketName,
			objectKey:  object.Key,
			removeOpts: minio.RemoveObjectOptions{},
		}, stats); err != nil {
			return nil
		}
	}
	return nil
}

type removeObjectConfig struct {
	client     *minio.Client
	bucketName string
	objectKey  string
	removeOpts minio.RemoveObjectOptions
}

func removeObject(ctx context.Context, config removeObjectConfig, stats *Stats) (err error) {
	defer func() {
		if !isIgnored(err) {
			stats.increment(err == nil)
		}
	}()
	return config.client.RemoveObject(ctx, config.bucketName, config.objectKey, config.removeOpts)
}

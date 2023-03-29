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
	"os"

	"github.com/minio/confess/stats"
	"github.com/minio/minio-go/v7"
)

var (
	errNilClient = errors.New("client is nil")
)

type putConfig struct {
	client     *minio.Client
	bucketName string
	objectName string
	size       int64
	opts       minio.PutObjectOptions
}

func put(ctx context.Context, config putConfig, stats *stats.APIStats) (info minio.UploadInfo, err error) {
	defer func() {
		stats.IncrementStats(&stats.Puts, err == nil)
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	reader := reader(config.size)
	defer reader.Close()
	return config.client.PutObject(ctx, config.bucketName, config.objectName, reader, config.size, config.opts)
}

type listConfig struct {
	client     *minio.Client
	bucketName string
	opts       minio.ListObjectsOptions
}

func list(ctx context.Context, config listConfig, stats *stats.APIStats) (objInfo []minio.ObjectInfo, err error) {
	defer func() {
		stats.IncrementStats(&stats.Lists, err == nil)
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

func stat(ctx context.Context, config statConfig, stats *stats.APIStats) (info minio.ObjectInfo, err error) {
	defer func() {
		stats.IncrementStats(&stats.Heads, err == nil)
	}()
	if config.client == nil {
		err = errNilClient
		return
	}
	return config.client.StatObject(ctx, config.bucketName, config.objectName, config.opts)
}

type removeObjectsConfig struct {
	client     *minio.Client
	bucketName string
	logFile    *os.File
	listOpts   minio.ListObjectsOptions
}

func removeObjects(ctx context.Context, config removeObjectsConfig, stats *stats.APIStats) (err error) {
	objects, err := list(ctx, listConfig{
		client:     config.client,
		bucketName: config.bucketName,
		opts:       config.listOpts,
	}, stats)
	if err != nil {
		return err
	}
	var removeErrFound bool
	for _, object := range objects {
		err = config.client.RemoveObject(ctx, config.bucketName, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			removeErrFound = true
			if err = log(ctx, config.logFile, "", config.client.EndpointURL().String(), "Failed to remove "+object.Key+", error: "+err.Error()); err != nil {
				return
			}
		}
		stats.IncrementStats(&stats.Deletes, err == nil)
	}
	if len(objects) > 0 && removeErrFound {
		return errors.New("unable to remove few objects")
	}
	return nil
}

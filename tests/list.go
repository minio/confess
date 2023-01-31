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
	"time"

	"github.com/minio/confess/node"
	"github.com/minio/confess/ops"
	"github.com/minio/minio-go/v7"
)

func (t *Test) setupListOp() func(expectedCount int, oi minio.ObjectInfo) ops.Op {
	return func(expectedCount int, oi minio.ObjectInfo) ops.Op {
		var o ops.ListOpts
		o.ListObjectsOptions = minio.ListObjectsOptions{
			Prefix:       oi.Key,
			Recursive:    true,
			WithVersions: true,
		}
		o.TestListFn = ops.CheckListConsistency
		op := ops.Op{
			ObjectInfo:    oi,
			TestName:      t.Name,
			Type:          ops.ListType,
			Opts:          o,
			ExpectedCount: expectedCount,
			Prefix:        oi.Key,
		}
		return op
	}
}

type PutListCheckTest1 struct {
	GenericTest
}

func (t PutListCheckTest1) Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retryInfo RetryInfo) {
	t.NodeSlc = nodeSlc
	startTime := time.Now()
	var (
		oi1           minio.ObjectInfo
		retry         bool
		err           error
		expectedCount = 1
	)
	// create 2 versions of an object
	op := t.setupPutOp(false, minio.PutObjectOptions{})(t.genObjName())
	oi1, retry, err = t.prepareFn(ctx, op, resCh)
	if err != nil { //abandon test
		return
	}
	if t.NodeSlc.VersioningEnabled {
		// create another version
		op := t.setupPutOp(false, minio.PutObjectOptions{})(oi1.Key)
		_, retry, err = t.prepareFn(ctx, op, resCh)
		if err != nil { //abandon test
			return
		}
		expectedCount++
	}

	if !retry {
		t.markTestStartFn(ctx, startTime, resCh)
	}
	// test whether both versions are listed on each node
	op = t.TestListOpFn(expectedCount, oi1) // consistency test
	opResCh := t.start(ctx, op)
	var lastRes ops.Result
	var saw int
	for res := range opResCh {
		if res.Err != nil {
			lastRes = res
		}
		saw++
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	if saw != expectedCount && !errors.Is(lastRes.Err, context.Canceled) && !errors.Is(lastRes.Err, ops.ErrNodeOffline) {
		lastRes.Err = fmt.Errorf("mismatch in number of versions: expected %d , saw %d for %s", expectedCount, saw, fmt.Sprintf("%s/%s", nodeSlc.Bucket, oi1.Key))
	}
	// mark test as pass|fail
	t.markTestStatusFn(ctx, startTime, lastRes, resCh)

	// cleanup
	delOpts := ops.DelOpts{
		RemoveObjectOptions: minio.RemoveObjectOptions{ForceDelete: true},
		BaseOpts: ops.BaseOpts{
			Bucket: nodeSlc.Bucket,
			Object: oi1.Key,
		},
	}
	ops.Delete(ctx, delOpts, nodeSlc.Nodes[len(nodeSlc.Nodes)-1])
}

func (t PutListCheckTest1) Concurrency() int {
	return t.Concurrent
}

func newPutListCheckTest1() PutListCheckTest1 {
	t := PutListCheckTest1{}
	t.Test = Test{
		Name:       "PutListCheckTest1",
		Concurrent: 1000,
	}
	t.SetupOpFn = t.setupPutOp(false, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	t.TestListOpFn = t.setupListOp()
	return t
}

type PutListCheckTest2 PutListCheckTest1

func newPutListCheckTest2() PutListCheckTest2 {
	t := PutListCheckTest2{}
	t.Test = Test{
		Name:       "PutListCheckTest2",
		Concurrent: 1000,
	}
	// set up multipart object
	t.SetupOpFn = t.setupPutOp(true, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	t.TestListOpFn = t.setupListOp()
	return t
}
func (t PutListCheckTest2) Concurrency() int {
	return t.Concurrent
}
func (t PutListCheckTest2) Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retryInfo RetryInfo) {
	t.NodeSlc = nodeSlc
	startTime := time.Now()
	var (
		oi1           minio.ObjectInfo
		retry         bool
		err           error
		expectedCount = 1
	)
	// create 2 versions of an object
	op := t.setupPutOp(true, minio.PutObjectOptions{})(t.genObjName())
	oi1, retry, err = t.prepareFn(ctx, op, resCh)
	if err != nil { //abandon test
		return
	}
	if t.NodeSlc.VersioningEnabled {
		// create another version
		op := t.setupPutOp(true, minio.PutObjectOptions{})(oi1.Key)
		_, retry, err = t.prepareFn(ctx, op, resCh)
		if err != nil { //abandon test
			return
		}
		expectedCount++
	}

	if !retry {
		t.markTestStartFn(ctx, startTime, resCh)
	}
	// test whether both versions are listed on each node
	op = t.TestListOpFn(expectedCount, oi1) // consistency test
	opResCh := t.start(ctx, op)
	var lastRes ops.Result
	var saw int
	for res := range opResCh {
		if res.Err != nil {
			lastRes = res
		}
		saw++
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	if saw != expectedCount && !errors.Is(lastRes.Err, context.Canceled) && !errors.Is(lastRes.Err, ops.ErrNodeOffline) {
		lastRes.Err = fmt.Errorf("mismatch in number of versions: expected %d , saw %d for %s", expectedCount, saw, fmt.Sprintf("%s/%s", nodeSlc.Bucket, oi1.Key))
	}
	// mark test as pass|fail
	t.markTestStatusFn(ctx, startTime, lastRes, resCh)

	// cleanup
	delOpts := ops.DelOpts{
		RemoveObjectOptions: minio.RemoveObjectOptions{ForceDelete: true},
		BaseOpts: ops.BaseOpts{
			Bucket: nodeSlc.Bucket,
			Object: oi1.Key,
		},
	}
	ops.Delete(ctx, delOpts, nodeSlc.Nodes[len(nodeSlc.Nodes)-1])
}

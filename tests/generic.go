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
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/node"
	"github.com/minio/confess/ops"
	"github.com/minio/minio-go/v7"
)

type GenericTest struct {
	Test
	SetupOpFn    func(string) ops.Op
	TestOpFn     func(oi minio.ObjectInfo) ops.Op
	TestListOpFn func(expectedCount int, oi minio.ObjectInfo) ops.Op
	CleanupOpFn  func(oi minio.ObjectInfo) ops.Op
}

func (t *GenericTest) setupPutOp(multipart bool, opts minio.PutObjectOptions) func(string) ops.Op {
	optsFn := func(key string) ops.Op {
		typ := http.MethodPut
		sz := int64(humanize.KiByte * 4)
		if multipart {
			typ = ops.MultipartType
			sz = getMultipartObjSize(len(t.NodeSlc.Nodes))
		}
		op := ops.Op{
			TestName: t.Name,
			Type:     typ,
			Opts: ops.PutOpts{
				BaseOpts: ops.BaseOpts{
					Bucket:  t.NodeSlc.Bucket,
					NodeIdx: rand.Intn(len(t.NodeSlc.Nodes)),
					Object:  key,
					Size:    sz,
				},
				PutObjectOptions: opts,
			},
		}
		return op
	}
	return optsFn
}

func (t *GenericTest) setupGetOp() func(oi minio.ObjectInfo) ops.Op {
	return func(oi minio.ObjectInfo) ops.Op {
		var o ops.GetOpts
		o.SetMatchETag(oi.ETag)
		o.GetObjectOptions.VersionID = oi.VersionID
		o.ObjInfo = oi
		o.TestGetFn = ops.CheckGetConsistency
		op := ops.Op{
			ObjectInfo: oi,
			TestName:   t.Name,
			Type:       http.MethodGet,
			Opts:       o,
		}
		return op
	}
}

func (t *GenericTest) setupStatOp() func(oi minio.ObjectInfo) ops.Op {
	return func(oi minio.ObjectInfo) ops.Op {
		var o ops.StatOpts
		o.SetMatchETag(oi.ETag)
		o.StatObjectOptions.VersionID = oi.VersionID
		o.ObjInfo = oi
		o.TestStatFn = ops.CheckStatConsistency
		op := ops.Op{
			ObjectInfo: oi,
			TestName:   t.Name,
			Type:       http.MethodHead,
			Opts:       o,
		}
		return op
	}
}
func (t *GenericTest) setupDelOp() func(oi minio.ObjectInfo) ops.Op {
	return func(oi minio.ObjectInfo) ops.Op {
		opts := ops.DelOpts{
			RemoveObjectOptions: minio.RemoveObjectOptions{
				VersionID: oi.VersionID,
			},
		}
		return ops.Op{
			ObjectInfo: oi,
			TestName:   t.Name,
			Type:       http.MethodDelete,
			Opts:       opts,
		}
	}
}

func (t *GenericTest) start(ctx context.Context, op ops.Op) chan ops.Result {
	var resCh = make(chan ops.Result, len(t.NodeSlc.Nodes))
	defer close(resCh)
	//run the test across all nodes
	var wg sync.WaitGroup
	for i := 0; i < len(t.NodeSlc.Nodes); i++ {
		wg.Add(1)
		go func(i int, op ops.Op) {
			defer wg.Done()
			opts := op.SetAddlOpts(t.NodeSlc.Bucket, i, op.ObjectInfo)
			gres := ops.Run(ctx, t.NodeSlc.Bucket, i, op, t.NodeSlc.Nodes, opts)
			select {
			case resCh <- gres:
			case <-ctx.Done():
				return
			}
		}(i, op)
	}
	wg.Wait()
	return resCh
}
func (t *GenericTest) prepareFn(ctx context.Context, op ops.Op, resCh chan interface{}) (oi minio.ObjectInfo, retry bool, err error) {
	if t.IsRetry {
		return t.Retry.ObjInfo, t.IsRetry, nil
	}
	popts := op.Opts.(ops.PutOpts)
	res := ops.Run(ctx, t.NodeSlc.Bucket, popts.NodeIdx, op, t.NodeSlc.Nodes, popts)
	select {
	case resCh <- res:
	case <-ctx.Done():
		return
	}
	return res.Data, false, res.Err
}
func (t *GenericTest) cleanupFn(ctx context.Context, op ops.Op, resCh chan interface{}) {
	idx := rand.Intn(len(t.NodeSlc.Nodes))
	opts := op.SetAddlOpts(t.NodeSlc.Bucket, idx, op.ObjectInfo)
	res := ops.Run(ctx, t.NodeSlc.Bucket, idx, op, t.NodeSlc.Nodes, opts)
	select {
	case resCh <- res:
	case <-ctx.Done():
		return
	}
}

// Simple test that runs a setup, single operation run concurrently across nodes, followed by a teardown
func (t *GenericTest) Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retryInfo RetryInfo) {
	t.NodeSlc = nodeSlc
	startTime := time.Now()
	oname := t.genObjName()
	op := t.SetupOpFn(oname)
	oi, retry, err := t.prepareFn(ctx, op, resCh)
	if err != nil { //abandon test
		return
	}
	if !retry {
		t.markTestStartFn(ctx, startTime, resCh)
	}
	op = t.TestOpFn(oi) // setuo consistency test

	opResCh := t.start(ctx, op)
	var lastErr ops.Result
	var requeue bool
	for res := range opResCh {
		if res.Err != nil {
			lastErr = res
		}
		if res.RetryRequest {
			requeue = true
		}
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	lastErr.RetryRequest = requeue
	// mark test as pass|fail
	t.markTestStatusFn(ctx, startTime, lastErr, resCh)
	t.cleanupFn(ctx, t.CleanupOpFn(oi), resCh)
}

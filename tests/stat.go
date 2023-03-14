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
	"math/rand"
	"net/http"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/node"
	"github.com/minio/confess/ops"
	"github.com/minio/minio-go/v7"
)

// PutStatCheck creates an object (version), then tests if metadata returned by HEAD is consistent across all
// available nodes
func PutStatCheck() *GenericTest {
	t := &GenericTest{
		Test: Test{
			Name:       "PutStatCheck",
			Concurrent: 1000,
		},
	}
	t.SetupOpFn = t.setupPutOp(false, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	// this function sets up options for stat check
	t.TestOpFns = []TestOpFn{t.setupStatOp()}
	// this function sets up options needed for the teardown - performs versioned delete.
	t.CleanupOpFn = t.setupDelOp()
	return t
}

// PutStatCheck2 creates a multipart object - one part per node, then tests if version is consistent across all
// available nodes
func PutStatCheck2() *GenericTest {
	t := &GenericTest{
		Test: Test{
			Name:       "PutStatCheck2",
			Concurrent: 1000,
		},
	}
	t.SetupOpFn = t.setupPutOp(true, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	// this function sets up options needed for stat consistency verification.
	t.TestOpFns = []TestOpFn{
		t.setupStatOp(),
	}
	// this function sets up options needed for the teardown - performs versioned delete.
	t.CleanupOpFn = t.setupDelOp()
	return t
}

type PutStatCheck3 struct {
	GenericTest
}

// PutStatCheck3 creates an object version, tests version consistency across nodes, then deletes the version.
// Another stat is performed across nodes to verify if the version has been truly deleted
func (t PutStatCheck3) Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retryInfo RetryInfo) {
	t.NodeSlc = nodeSlc
	startTime := time.Now()
	var (
		oi1   minio.ObjectInfo
		retry bool
		err   error
	)
	// create a version of an object
	putOp := ops.Op{
		TestName: t.Name,
		Type:     http.MethodPut,
		Opts: ops.PutOpts{
			BaseOpts: ops.BaseOpts{
				Bucket:  t.NodeSlc.Bucket,
				NodeIdx: rand.Intn(len(t.NodeSlc.Nodes)),
				Object:  t.genObjName(),
				Size:    humanize.KByte * 4,
			},
			PutObjectOptions: minio.PutObjectOptions{},
		},
	}
	oi1, retry, err = t.prepareFn(ctx, putOp, resCh)
	if err != nil { //abandon test
		return
	}

	if !retry {
		t.markTestStartFn(ctx, startTime, resCh)
	}
	// test whether stat succeeds on each node
	var o ops.StatOpts
	o.SetMatchETag(oi1.ETag)
	o.StatObjectOptions.VersionID = oi1.VersionID
	o.ObjInfo = oi1
	statOp := ops.Op{
		ObjectInfo: oi1,
		TestName:   t.Name,
		Type:       http.MethodHead,
		Opts:       o,
	}

	opResCh := t.start(ctx, statOp)
	var lastRes ops.Result
	for res := range opResCh {
		if res.Err != nil {
			lastRes = res
		}
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}

	var delOpts ops.DelOpts
	delOpts.RemoveObjectOptions = minio.RemoveObjectOptions{
		VersionID: oi1.VersionID}
	// delete object version
	delOp := ops.Op{
		ObjectInfo: oi1,
		TestName:   t.Name,
		Type:       http.MethodDelete,
		Opts:       delOpts,
	}
	opResCh = t.start(ctx, delOp)
	for res := range opResCh {
		if res.Err != nil {
			lastRes = res
		}
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	// stat the object again on each node and check if it is indeed deleted.
	// test whether stat succeeds on each node
	opResCh = t.start(ctx, statOp)
	for res := range opResCh {
		if res.Err == nil {
			res.Err = fmt.Errorf("object version %s/%s (%s) still exists on node %s lastErr=%w", t.NodeSlc.Bucket, oi1.Key, oi1.VersionID, res.Node, lastRes.Err)
			lastRes = res
			break
		}
		errResp := minio.ToErrorResponse(res.Err)
		if errResp.Code == "NoSuchKey" { // ignore NoSuchKey error, this is what we want to see
			res.Err = nil
		}
		lastRes = res
		select {
		case resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	lastRes.RetryRequest = false
	// mark test as pass|fail
	t.markTestStatusFn(ctx, startTime, lastRes, resCh)
	// cleanup already done - noop
}
func newPutStatCheck3() PutStatCheck3 {
	return PutStatCheck3{
		GenericTest: GenericTest{
			Test: Test{
				Name:       "PutStatCheck3",
				Concurrent: 1000,
			},
		},
	}
}
func (t PutStatCheck3) Concurrency() int {
	return t.Concurrent
}

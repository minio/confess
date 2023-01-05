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
package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

type Op struct {
	minio.ObjectInfo
	Type          string
	Prefix        string
	ExpectedCount int // for listing
	NodeIdx       int // node at which to run operation
}

type RetryInfo struct {
	OK      bool // retries GET|HEAD|LIST operation
	ObjInfo minio.ObjectInfo
}
type OpSequence struct {
	Test  func(ctx context.Context, r RetryInfo)
	Retry RetryInfo
}

type testResult struct {
	Method       string        `json:"method"`
	FuncName     string        `json:"funcName"`
	Path         string        `json:"path"`
	Node         *url.URL      `json:"node"`
	Err          error         `json:"err,omitempty"`
	Latency      time.Duration `json:"duration"`
	Offline      bool          `json:"offline"`
	RetryRequest bool          `json:"retry"`
	TestFn       func(context.Context)
	data         minio.ObjectInfo `json:"-"`
	StartTime    time.Time        `json:"startTime"`
	EndTime      time.Time        `json:"endTime"`
}

func (r *testResult) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return fmt.Sprintf("%s %s %s: %s %s %s %s", r.StartTime.Format(time.RFC3339Nano), r.EndTime.Format(time.RFC3339Nano), r.Node, r.Path, r.Method, r.FuncName, errMsg)
}

var (
	errNodeOffline = fmt.Errorf("node is offline")
)

// prints line to console - erase status bar if erased is true
func printWithErase(erased bool, msg string) bool {
	if !erased {
		console.Eraseline()
	}
	console.Print(msg + "\r")
	return true
}

func (n *nodeState) queueTest(ctx context.Context, op OpSequence) {
	select {
	case <-ctx.Done():
		return
	default:
		n.testCh <- op
	}
}

// generate a sequence of S3 API operations
func (n *nodeState) generateTest() (seq OpSequence) {
	tIdx := rand.Intn(6)
	switch tIdx {
	case 0:
		seq.Test = n.testHeadWithVersion
	case 1:
		seq.Test = n.testGetWithVersion
	case 2:
		seq.Test = n.testListObject
	case 3:
		seq.Test = n.testHeadAfterMultipartUpload
	case 4:
		seq.Test = n.testGetAfterMultipartUpload
	case 5:
		seq.Test = n.testListAfterMultipartUpload
	}
	return

}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			close(n.testCh)
			return
		default:
			seq := n.generateTest()
			n.queueTest(ctx, seq)
		}
	}
}

func (n *nodeState) genObjName() string {
	pfx := n.getRandomPfx()
	return fmt.Sprintf("%s/%s", pfx, shortuuid.New())
}
func (n *nodeState) testGetWithVersion(ctx context.Context, retry RetryInfo) {

	op := Op{
		Type: http.MethodPut,
		ObjectInfo: minio.ObjectInfo{
			Key:  n.genObjName(),
			Size: 4 * humanize.KiByte,
		}}
	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
	} else {
		res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
		select {
		case n.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	var requeue bool

	if res.Err == nil {
		op := Op{
			Type: http.MethodGet,
			ObjectInfo: minio.ObjectInfo{
				Key:       res.data.Key,
				VersionID: res.data.VersionID,
				ETag:      res.data.ETag,
				Size:      res.data.Size,
			},
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				gres := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- gres:
				case <-ctx.Done():
					return
				}
				if gres.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testGetWithVersion,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}
			}(i, op)
		}
		wg.Wait()
		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *nodeState) testGetAfterMultipartUpload(ctx context.Context, retry RetryInfo) {
	mpartSize := len(n.nodes) * humanize.MiByte * 5
	op := Op{
		Type: "MULTIPART",
		ObjectInfo: minio.ObjectInfo{
			Key:  n.genObjName(),
			Size: int64(mpartSize),
		}}
	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
	} else {
		res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
		select {
		case n.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	var requeue bool
	if res.Err == nil && res.FuncName == "CompleteMultipartUpload" {
		op := Op{
			Type: http.MethodGet,
			ObjectInfo: minio.ObjectInfo{
				Key:       res.data.Key,
				VersionID: res.data.VersionID,
				ETag:      res.data.ETag,
				Size:      res.data.Size,
			},
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
				if res2.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testGetAfterMultipartUpload,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}
			}(i, op)
		}
		wg.Wait()
		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *nodeState) testHeadWithVersion(ctx context.Context, retry RetryInfo) {
	op := Op{
		Type: http.MethodPut,
		ObjectInfo: minio.ObjectInfo{
			Key:  n.genObjName(),
			Size: 4 * humanize.KiByte,
			UserMetadata: map[string]string{
				"Customkey": "extra  spaces  in   value",
				"Key2":      "Confess",
			},
		}}

	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
	} else {
		res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
		select {
		case n.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	var requeue bool

	if res.Err == nil {
		op.Type = http.MethodHead
		res.data.UserMetadata = op.UserMetadata
		op.ObjectInfo = res.data
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
				if res2.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testHeadWithVersion,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}
			}(i, op)
		}
		wg.Wait()
		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *nodeState) testHeadAfterMultipartUpload(ctx context.Context, retry RetryInfo) {
	mpartSize := len(n.nodes) * humanize.MiByte * 5
	op := Op{
		Type: "MULTIPART",
		ObjectInfo: minio.ObjectInfo{
			Key:  n.genObjName(),
			Size: int64(mpartSize),
			UserMetadata: map[string]string{
				"Customkey": "extra  spaces  in   value",
				"Key2":      "Confess",
			},
		}}

	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
	} else {
		res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
		select {
		case n.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	var requeue bool

	if res.Err == nil && res.FuncName == "CompleteMultipartUpload" {
		op.Type = http.MethodHead
		res.data.UserMetadata = op.UserMetadata
		op.ObjectInfo = minio.ObjectInfo{
			Key:       res.data.Key,
			VersionID: res.data.VersionID,
			ETag:      res.data.ETag,
			Size:      res.data.Size,
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
				if res2.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testHeadAfterMultipartUpload,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}
			}(i, op)
		}
		wg.Wait()
		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *nodeState) testListObject(ctx context.Context, retry RetryInfo) {
	count := 0
	object := n.genObjName()
	op := Op{
		Type: http.MethodPut,
		ObjectInfo: minio.ObjectInfo{
			Key:  object,
			Size: 4 * humanize.KiByte,
		}}

	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
		count += 1
	} else {
		for i := 0; i < 2; i++ {
			res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
			if res.Err == nil {
				count++
			}
		}
	}
	var requeue bool

	if count > 0 {
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			op := Op{
				Prefix:        object,
				NodeIdx:       i,
				Type:          "LIST",
				ExpectedCount: count,
			}
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
				if res2.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testListObject,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}

			}(i, op)
		}
		wg.Wait()

		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}
func (n *nodeState) testListAfterMultipartUpload(ctx context.Context, retry RetryInfo) {
	mpartSize := len(n.nodes) * humanize.MiByte * 5
	op := Op{
		Type: "MULTIPART",
		ObjectInfo: minio.ObjectInfo{
			Key:  n.genObjName(),
			Size: int64(mpartSize),
		}}
	var res testResult
	if retry.OK {
		res.data = retry.ObjInfo
	} else {
		res = n.runTest(ctx, rand.Intn(len(n.nodes)), op)
		select {
		case n.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
	var requeue bool

	if (res.Err == nil && res.FuncName == "CompleteMultipartUpload") || retry.OK {
		op.Type = http.MethodHead
		res.data.UserMetadata = op.UserMetadata
		op.ObjectInfo = minio.ObjectInfo{
			Key:       res.data.Key,
			VersionID: res.data.VersionID,
			ETag:      res.data.ETag,
			Size:      res.data.Size,
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			op := Op{
				Prefix:        res.data.Key,
				NodeIdx:       i,
				Type:          "LIST",
				ExpectedCount: 1,
			}
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
				if res2.RetryRequest {
					go n.queueTest(ctx, OpSequence{
						Test:  n.testListAfterMultipartUpload,
						Retry: RetryInfo{OK: true, ObjInfo: res.data},
					})
					requeue = true
				}
			}(i, op)
		}
		wg.Wait()

		if !requeue {
			op.Type = http.MethodDelete
			res := n.runTest(ctx, rand.Intn(len(n.nodes)), op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (n *nodeState) runTest(ctx context.Context, idx int, op Op) (res testResult) {
	bucket := n.cliCtx.String("bucket")
	node := n.nodes[idx]
	startTime := time.Now().UTC()

	if n.hc.isOffline(node.endpointURL) {
		res = testResult{
			Offline:   true,
			Err:       errNodeOffline,
			Node:      node.endpointURL,
			StartTime: startTime,
			EndTime:   time.Now().UTC(),
		}
		return
	}
	defer func() {
		res.StartTime = startTime
		res.EndTime = time.Now().UTC()
		if res.Err == nil {
			return
		}
		errResp := minio.ToErrorResponse(res.Err)
		if minio.IsNetworkOrHostDown(res.Err, false) ||
			n.hc.isOffline(node.endpointURL) ||
			errResp.StatusCode == http.StatusServiceUnavailable {
			res.Err = errNodeOffline
			res.RetryRequest = true
			res.Node = node.endpointURL
			return
		}
		switch {
		case errResp == minio.ErrorResponse{}:
			res.RetryRequest = true
		case errResp.Code == "InternalError":
			res.RetryRequest = true
		}
	}()
	switch op.Type {
	case http.MethodPut:
		select {
		case <-ctx.Done():
			return
		default:
			popts := putOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				Size:    op.Size,
			}
			popts.UserMetadata = op.UserMetadata
			res = n.put(ctx, popts)
			return
		}
	case http.MethodGet:
		select {
		default:
			res = n.get(ctx, getOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				ObjInfo: op.ObjectInfo,
			})
			return
		case <-ctx.Done():
			return
		}
	case http.MethodHead:
		select {
		default:

			res = n.stat(ctx, statOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				ObjInfo: op.ObjectInfo,
			})
			return
		case <-ctx.Done():
			return
		}
	case http.MethodDelete:
		select {
		default:
			res = n.delete(ctx, delOpts{
				Bucket:              bucket,
				Object:              op.Key,
				RemoveObjectOptions: minio.RemoveObjectOptions{VersionID: op.VersionID},
				NodeIdx:             idx,
			})
			return
		case <-ctx.Done():
			return
		}
	case "LIST":
		select {
		default:
			res = n.list(ctx, op.ExpectedCount, listOpts{
				Bucket:  bucket,
				Prefix:  op.Prefix,
				NodeIdx: idx,
			})
			return
		case <-ctx.Done():
			return
		}
	case "MULTIPART":
		select {
		default:
			popts := putOpts{
				Bucket: bucket,
				Object: op.Key,
				Size:   op.Size,
			}
			popts.UserMetadata = op.UserMetadata
			res = n.multipartPut(ctx, popts)
			return
		case <-ctx.Done():
			return
		}
	default:
	}

	return res
}

func (n *nodeState) put(ctx context.Context, o putOpts) (res testResult) {
	start := time.Now()
	reader := getDataReader(o.Size)
	defer reader.Close()
	node := n.nodes[o.NodeIdx]
	res = testResult{
		Method:   http.MethodPut,
		FuncName: "PutObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     node.endpointURL,
	}
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	oi, err := node.client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), o.PutObjectOptions)

	return testResult{
		Method:   http.MethodPut,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "PutObject",
		Err:      err,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		Latency:  time.Since(start),
		data:     toObjectInfo(oi),
	}
}

func (n *nodeState) multipartPut(ctx context.Context, o putOpts) (res testResult) {
	start := time.Now()
	reader := getDataReader(o.Size)
	defer reader.Close()
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	res = testResult{
		Method:   http.MethodPost,
		FuncName: "NewMultipartUpload",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     node.endpointURL,
	}
	var uploadedParts []minio.CompletePart
	c := minio.Core{Client: node.client}

	uploadID, err := c.NewMultipartUpload(context.Background(), o.Bucket, o.Object, o.PutObjectOptions)
	if err != nil {
		res.Err = err
		return
	}

	defer func() {
		if err != nil {
			aerr := c.AbortMultipartUpload(ctx, o.Bucket, o.Object, uploadID)
			res.Err = aerr
			res.FuncName = "AbortMultipartUpload"
			res.Method = http.MethodPost
			return
		}
	}()
	mpartSize := o.Size
	partSize := int64(humanize.MiByte * 5)
	res.FuncName = "PutObjectPart"

	for i, node := range n.nodes {
		clnt := minio.Core{Client: node.client}
		if n.hc.isOffline(node.endpointURL) {
			continue
		}
		var (
			pInfo minio.ObjectPart
		)
		mpartSize -= int64(partSize)
		if i == len(n.nodes) && mpartSize > 0 {
			partSize += int64(mpartSize)
		}
		reader := getDataReader(partSize)
		defer reader.Close()

		pInfo, err = clnt.PutObjectPart(ctx, o.Bucket, o.Object, uploadID, i+1, reader, partSize, "", "", o.ServerSideEncryption)
		if err != nil {
			res.Err = err
			return
		}
		if pInfo.Size != partSize {
			err = fmt.Errorf("part size mismatch: got %d, want %d", pInfo.Size, partSize)
			res.Err = err
			return
		}
		uploadedParts = append(uploadedParts, minio.CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	var etag string
	etag, err = c.CompleteMultipartUpload(ctx, o.Bucket, o.Object, uploadID, uploadedParts, minio.PutObjectOptions{})

	return testResult{
		Method:   http.MethodPut,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "CompleteMultipartUpload",
		Err:      err,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		Latency:  time.Since(start),
		data: minio.ObjectInfo{
			Key:          o.Object,
			ETag:         etag,
			Size:         o.Size,
			UserMetadata: o.UserMetadata,
		},
	}
}
func (n *nodeState) get(ctx context.Context, o getOpts) (res testResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	res = testResult{
		Method:   http.MethodGet,
		FuncName: "GetObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     n.nodes[o.NodeIdx].endpointURL,
	}
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	opts := minio.GetObjectOptions{}
	opts.SetMatchETag(o.ObjInfo.ETag)
	obj, err := node.client.GetObject(ctx, o.Bucket, o.Object, opts)
	var oi minio.ObjectInfo
	if err == nil {
		_, err = io.Copy(ioutil.Discard, obj)
		if err == nil {
			oi, err = obj.Stat()
		}
	}
	return testResult{
		Method:   http.MethodGet,
		FuncName: "GetObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:      err,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		Latency:  time.Since(start),
		data:     oi,
	}
}

func (n *nodeState) stat(ctx context.Context, o statOpts) (res testResult) {
	start := time.Now()
	node := n.nodes[o.NodeIdx]
	res = testResult{
		Method: http.MethodHead,
		Path:   fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:   n.nodes[o.NodeIdx].endpointURL,
	}
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	opts := minio.StatObjectOptions{VersionID: o.VersionID}
	opts.SetMatchETag(o.ObjInfo.ETag)
	oi, err := node.client.StatObject(ctx, o.Bucket, o.Object, opts)
	if err == nil {
		// compare ETag, size and version id if available
		if oi.ETag != o.ObjInfo.ETag ||
			(oi.VersionID != o.ObjInfo.VersionID && o.ObjInfo.VersionID != "") ||
			oi.Size != o.ObjInfo.Size {
			err = fmt.Errorf("metadata mismatch: %s, %s, %s,%s, %d, %d", oi.ETag, o.ObjInfo.ETag, oi.VersionID, o.ObjInfo.VersionID, oi.Size, o.Size)
		}
	}
	if err == nil && len(o.ObjInfo.UserMetadata) > 0 {
		if !reflect.DeepEqual(o.ObjInfo.UserMetadata, oi.UserMetadata) {
			err = fmt.Errorf("metadata mismatch of user defined metadata")
		}
	}
	return testResult{
		Method:   http.MethodHead,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:      err,
		FuncName: "StatObject",

		Node:    n.nodes[o.NodeIdx].endpointURL,
		Latency: time.Since(start),
		data:    oi,
	}
}

func (n *nodeState) delete(ctx context.Context, o delOpts) (res testResult) {
	start := time.Now()
	node := n.nodes[o.NodeIdx]

	res = testResult{
		Method:   "DLET",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "RemoveObject",
		Node:     n.nodes[o.NodeIdx].endpointURL,
	}
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	opts := o.RemoveObjectOptions
	err := node.client.RemoveObject(ctx, o.Bucket, o.Object, opts)
	return testResult{
		Method:   http.MethodDelete,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "RemoveObject",
		Err:      err,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		Latency:  time.Since(start),
	}
}

func (n *nodeState) cleanup() {
	if n.numFailed > 0 {
		return
	}
	bucket := n.cliCtx.String("bucket")
	var clnt *minio.Client
	for _, node := range n.nodes {
		if n.hc.isOffline(node.endpointURL) {
			continue
		}
		clnt = node.client
		break
	}
	if clnt == nil {
		return
	}
	for _, pfx := range n.Prefixes {
		err := clnt.RemoveObject(context.Background(), bucket, pfx, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		if err != nil {
			continue
		}
	}
}

func (n *nodeState) list(ctx context.Context, numEntries int, o listOpts) (res testResult) {
	start := time.Now()
	path := fmt.Sprintf("%s/%s", o.Bucket, o.Prefix)
	res = testResult{
		Method:   "LIST",
		Path:     path,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		FuncName: "ListObjects",
	}
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		return
	}
	doneCh := make(chan struct{})
	defer close(doneCh)
	saw := 0
	for objCh := range node.client.ListObjects(ctx, o.Bucket, minio.ListObjectsOptions{
		Prefix:       o.Prefix,
		Recursive:    true,
		WithVersions: true,
	}) {
		if objCh.Err != nil {
			res.Err = objCh.Err
			res.Latency = time.Since(start)
			return
		}
		saw++
	}
	if saw != numEntries {
		res.Err = fmt.Errorf("mismatch in number of versions: expected %d , saw %d for %s", numEntries, saw, path)
	}
	return testResult{
		Method:   "LIST",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Prefix),
		FuncName: "ListObjects",
		Err:      nil,
		Node:     n.nodes[o.NodeIdx].endpointURL,
		Latency:  time.Since(start),
	}
}

func toObjectInfo(o minio.UploadInfo) minio.ObjectInfo {
	return minio.ObjectInfo{
		Key:            o.Key,
		ETag:           o.ETag,
		Size:           o.Size,
		LastModified:   o.LastModified,
		VersionID:      o.VersionID,
		ChecksumCRC32:  o.ChecksumCRC32,
		ChecksumCRC32C: o.ChecksumCRC32C,
		ChecksumSHA1:   o.ChecksumSHA1,
		ChecksumSHA256: o.ChecksumSHA256,
	}
}

type putOpts struct {
	minio.PutObjectOptions
	Bucket  string
	Object  string
	Size    int64
	NodeIdx int
}

type getOpts struct {
	minio.GetObjectOptions
	Bucket  string
	Object  string
	Size    int64
	NodeIdx int
	ObjInfo minio.ObjectInfo
}

type statOpts getOpts

type delOpts struct {
	minio.RemoveObjectOptions
	Bucket  string
	Object  string
	NodeIdx int
}
type listOpts struct {
	minio.ListObjectsOptions
	Bucket  string
	Prefix  string
	NodeIdx int
}

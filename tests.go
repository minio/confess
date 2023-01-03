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
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

type Op struct {
	Type      string
	ObjInfo   minio.ObjectInfo
	Key       string
	VersionID string
	Prefix    string
	NodeIdx   int // node at which to run operation
}

type OpSequence struct {
	Ops      []Op
	RetryCnt int
}

const maxRetry = 5

type testResult struct {
	Method   string           `json:"method"`
	FuncName string           `json:"funcName"`
	Path     string           `json:"path"`
	Node     *url.URL         `json:"node"`
	Err      error            `json:"err,omitempty"`
	Latency  time.Duration    `json:"duration"`
	Offline  bool             `json:"offline"`
	data     minio.ObjectInfo `json:"-"`
}

func (r *testResult) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return fmt.Sprintf("%s: %s %s %s %s", r.Node, r.Path, r.Method, r.FuncName, errMsg)
}

var (
	errInvalidOpSeq = fmt.Errorf("invalid op sequence")
	errNodeOffline  = fmt.Errorf("node is offline")
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
func (n *nodeState) generateOpSequence() (seq OpSequence) {
	pfx := n.getRandomPfx()
	object := fmt.Sprintf("%s/%s", pfx, shortuuid.New())
	seq.Ops = append(seq.Ops, Op{Type: http.MethodPut, Key: object})
	for i := 0; i < 5; i++ {
		idx := rand.Intn(3)
		var op Op
		op.Key = object
		op.Prefix = pfx
		switch idx {
		case 0:
			op.Type = http.MethodGet
		case 1:
			op.Type = http.MethodHead
		case 2:
			op.Type = "LIST"
		}
		seq.Ops = append(seq.Ops, op)
	}
	seq.Ops = append(seq.Ops, Op{Type: http.MethodDelete, Key: object})
	return seq
}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			close(n.testCh)
			return
		default:
			seq := n.generateOpSequence()
			n.queueTest(ctx, seq)
		}
	}
}

// validate if op sequence has PUT and DELETE as first and last ops.
func validateOpSequence(seq OpSequence) error {
	if len(seq.Ops) < 2 {
		return errInvalidOpSeq
	}
	if seq.Ops[0].Type != http.MethodPut {
		return errInvalidOpSeq
	}
	if seq.Ops[len(seq.Ops)-1].Type != http.MethodDelete {
		return errInvalidOpSeq
	}
	return nil
}

func (n *nodeState) runOpSeq(ctx context.Context, seq OpSequence) {
	var oi minio.ObjectInfo
	if err := validateOpSequence(seq); err != nil {
		console.Errorln(err)
		return
	}
	for _, op := range seq.Ops {
		if op.Type == http.MethodPut {
			op.NodeIdx = rand.Intn(len(n.nodes))
			res := n.runTest(ctx, op.NodeIdx, op)
			if res.Err != nil { // discard all other ops in this sequence
				seq.RetryCnt += 1
				if seq.RetryCnt <= maxRetry {
					go n.queueTest(ctx, seq)
				} else {
					select {
					case n.resCh <- res:
					case <-ctx.Done():
						return
					}
				}
				return
			}
			oi = res.data
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
			continue
		}
		if op.Type == http.MethodDelete {
			op.NodeIdx = rand.Intn(len(n.nodes))
			res := n.runTest(ctx, op.NodeIdx, op)
			select {
			case n.resCh <- res:
			case <-ctx.Done():
				return
			}
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int, op Op) {
				defer wg.Done()
				op.NodeIdx = i
				op.ObjInfo.VersionID = oi.VersionID
				op.ObjInfo.ETag = oi.ETag
				op.ObjInfo.Size = oi.Size
				res2 := n.runTest(ctx, op.NodeIdx, op)
				select {
				case n.resCh <- res2:
				case <-ctx.Done():
					return
				}
			}(i, op)
		}
		wg.Wait()
	}
}

func (n *nodeState) runTest(ctx context.Context, idx int, op Op) (res testResult) {
	bucket := n.cliCtx.String("bucket")
	node := n.nodes[idx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		res.Err = errNodeOffline
		res.Node = node.endpointURL
		return
	}
	defer func() {
		if res.Err == nil || !n.hc.isOffline(node.endpointURL) {
			return
		}
		res.Offline = true
		res.Err = errNodeOffline
		res.Node = node.endpointURL
	}()
	switch op.Type {
	case http.MethodPut:
		select {
		case <-ctx.Done():
			return
		default:
			res = n.put(ctx, putOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				Size:    4 * humanize.KiByte,
			})
			return
		}
	case http.MethodGet:
		select {
		default:
			res = n.get(ctx, getOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				ObjInfo: op.ObjInfo,
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
				ObjInfo: op.ObjInfo,
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
			res = n.list(ctx, listOpts{
				Bucket:  bucket,
				Prefix:  op.Prefix,
				NodeIdx: idx,
			})
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
	oi, err := node.client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
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
	opts := minio.StatObjectOptions{}
	opts.SetMatchETag(o.ObjInfo.ETag)
	oi, err := node.client.StatObject(ctx, o.Bucket, o.Object, opts)
	if err == nil {
		if oi.ETag != o.ObjInfo.ETag ||
			oi.VersionID != o.ObjInfo.VersionID ||
			oi.Size != o.ObjInfo.Size {
			err = fmt.Errorf("metadata mismatch: %s, %s, %s,%s, %d, %d", oi.ETag, o.ObjInfo.ETag, oi.VersionID, o.ObjInfo.VersionID, oi.Size, o.Size)
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

func (n *nodeState) list(ctx context.Context, o listOpts) (res testResult) {
	start := time.Now()
	res = testResult{
		Method:   "LIST",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Prefix),
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
	Bucket       string
	Object       string
	Size         int64
	UserMetadata map[string]string
	NodeIdx      int
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

package ops

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/confess/data"
	"github.com/minio/confess/node"
	"github.com/minio/minio-go/v7"
)

var (
	ErrNodeOffline = fmt.Errorf("node is offline")
)

const (
	MultipartType = "MULTIPART"
	ListType      = "LIST"
)

type Op struct {
	minio.ObjectInfo
	Type          string
	TestName      string
	Prefix        string
	ExpectedCount int // for listing
	Opts          Opts
	// Retry         RetryInfo
}

func (o *Op) SetAddlOpts(bucket string, nodeIdx int, objInfo minio.ObjectInfo) Opts {
	switch o.Opts.(type) {
	case GetOpts:
		opts := o.Opts.(GetOpts)
		opts.Bucket = bucket
		opts.NodeIdx = nodeIdx
		opts.Object = objInfo.Key
		opts.VersionID = objInfo.VersionID
		opts.TestName = o.TestName
		return opts
	case StatOpts:
		opts := o.Opts.(StatOpts)
		opts.Bucket = bucket
		opts.NodeIdx = nodeIdx
		opts.Object = objInfo.Key
		opts.VersionID = objInfo.VersionID
		opts.TestName = o.TestName
		return opts
	case PutOpts:
		opts := o.Opts.(PutOpts)
		opts.Bucket = bucket
		opts.NodeIdx = nodeIdx
		opts.Object = objInfo.Key
		opts.TestName = o.TestName
		return opts
	case ListOpts:
		opts := o.Opts.(ListOpts)
		opts.Bucket = bucket
		opts.NodeIdx = nodeIdx
		opts.Prefix = objInfo.Key
		opts.Object = objInfo.Key
		opts.TestName = o.TestName
		return opts
	case DelOpts:
		opts := o.Opts.(DelOpts)
		opts.Bucket = bucket
		opts.NodeIdx = nodeIdx
		opts.Object = objInfo.Key
		opts.VersionID = objInfo.VersionID
		opts.TestName = o.TestName
		return opts
	}
	return o.Opts
}

type Result struct {
	TestName     string           `json:"testName"`
	Method       string           `json:"method"`
	FuncName     string           `json:"funcName"`
	Path         string           `json:"path"`
	Node         *url.URL         `json:"node"`
	Err          error            `json:"err,omitempty"`
	Latency      time.Duration    `json:"duration"`
	Offline      bool             `json:"offline"`
	RetryRequest bool             `json:"retry"`
	Data         minio.ObjectInfo `json:"-"`
	StartTime    time.Time        `json:"startTime"`
	EndTime      time.Time        `json:"endTime"`
	// test details
	AbortTest bool `json:"abortTest"`
}

func (r *Result) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	return fmt.Sprintf("%s %s %s: %s %s %s %s %s", r.StartTime.Format(time.RFC3339Nano), r.EndTime.Format(time.RFC3339Nano), r.Node, r.TestName, r.Path, r.Method, r.FuncName, errMsg)
}

type Opts interface{}
type BaseOpts struct {
	Opts
	Bucket   string
	Object   string
	Size     int64
	NodeIdx  int
	TestName string
}
type PutOpts struct {
	BaseOpts
	minio.PutObjectOptions
}

type GetOpts struct {
	BaseOpts
	minio.GetObjectOptions
	ObjInfo   minio.ObjectInfo
	TestGetFn func(ctx context.Context, bucket, object string, client *minio.Client, opts minio.GetObjectOptions) (oi minio.ObjectInfo, err error)
}

type StatOpts struct {
	BaseOpts
	minio.StatObjectOptions
	ObjInfo    minio.ObjectInfo
	TestStatFn func(ctx context.Context, o StatOpts, node *node.Node) Result
}

type DelOpts struct {
	BaseOpts
	minio.RemoveObjectOptions
	TestRemoveFn func(ctx context.Context, bucket, object string, client *minio.Client, opts minio.RemoveObjectOptions) error
}
type ListOpts struct {
	BaseOpts
	minio.ListObjectsOptions
	TestListFn func(ctx context.Context, bucket, prefix string, expectedCount int, client *minio.Client, opts minio.ListObjectsOptions) error
}

func Put(ctx context.Context, o PutOpts, node *node.Node) (res Result) {
	reader := data.Reader(o.Size)
	defer reader.Close()
	res = Result{
		Method:   http.MethodPut,
		FuncName: "PutObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     node.EndpointURL,
	}
	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	start := time.Now()

	oi, err := node.Client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), o.PutObjectOptions)
	return Result{
		Method:    http.MethodPut,
		Path:      fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName:  "PutObject",
		Err:       err,
		AbortTest: err != nil,
		Node:      node.EndpointURL,
		Latency:   time.Since(start),
		Data:      toObjectInfo(oi),
	}
}

func MultipartPut(ctx context.Context, o PutOpts, nodes []*node.Node) (res Result) {
	reader := data.Reader(o.Size)
	defer reader.Close()
	node := nodes[o.NodeIdx]

	res = Result{
		Method:   http.MethodPost,
		FuncName: "NewMultipartUpload",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     node.EndpointURL,
	}

	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	var uploadedParts []minio.CompletePart
	c := minio.Core{Client: node.Client}
	start := time.Now()

	uploadID, err := c.NewMultipartUpload(context.Background(), o.Bucket, o.Object, o.PutObjectOptions)
	if err != nil {
		res.Err = err
		return
	}

	defer func() {
		if err != nil {
			c.AbortMultipartUpload(ctx, o.Bucket, o.Object, uploadID)
			res.FuncName = "AbortMultipartUpload"
			res.Method = http.MethodPost
			res.AbortTest = true
			return
		}
	}()
	mpartSize := o.Size
	partSize := int64(humanize.MiByte * 5)
	res.FuncName = "PutObjectPart"

	for i, node := range nodes {
		clnt := minio.Core{Client: node.Client}
		if node.Client.IsOffline() {
			continue
		}
		var (
			pInfo minio.ObjectPart
		)
		if i == (len(nodes)-1) && mpartSize > 0 {
			partSize = int64(mpartSize)
		}
		reader := data.Reader(partSize)
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
		mpartSize -= int64(partSize)

		uploadedParts = append(uploadedParts, minio.CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	var etag string
	etag, err = c.CompleteMultipartUpload(ctx, o.Bucket, o.Object, uploadID, uploadedParts, minio.PutObjectOptions{})

	return Result{
		Method:   http.MethodPut,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "CompleteMultipartUpload",
		Err:      err,
		Node:     node.EndpointURL,
		Latency:  time.Since(start),
		Data: minio.ObjectInfo{
			Key:          o.Object,
			ETag:         etag,
			Size:         o.Size,
			UserMetadata: o.UserMetadata,
		},
	}
}

func Get(ctx context.Context, o GetOpts, node *node.Node) (res Result, obj *minio.Object) {
	res = Result{
		Method:   http.MethodGet,
		FuncName: "GetObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:     node.EndpointURL,
	}
	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	start := time.Now()
	var (
		err error
	)
	obj, err = node.Client.GetObject(ctx, o.Bucket, o.Object, o.GetObjectOptions)
	res = Result{
		Method:   http.MethodGet,
		FuncName: "GetObject",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:      err,
		Node:     node.EndpointURL,
		Latency:  time.Since(start),
	}
	if err != nil {
		return
	} else {
		oi, err := obj.Stat()
		if err != nil {
			res.Err = err
		} else {
			res.Data = oi
		}
	}
	return
}

func Stat(ctx context.Context, o StatOpts, node *node.Node) (res Result, oi minio.ObjectInfo) {
	res = Result{
		Method: http.MethodHead,
		Path:   fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Node:   node.EndpointURL,
	}
	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	start := time.Now()
	oi, err := node.Client.StatObject(ctx, o.Bucket, o.Object, o.StatObjectOptions)
	return Result{
		Method:    http.MethodHead,
		Path:      fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:       err,
		FuncName:  "StatObject",
		StartTime: start,
		EndTime:   time.Now().UTC(),
		Node:      node.EndpointURL,
		Latency:   time.Since(start),
		Data:      oi,
	}, oi
}

func Delete(ctx context.Context, o DelOpts, node *node.Node) (res Result) {
	res = Result{
		Method:   "DLET",
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName: "RemoveObject",
		Node:     node.EndpointURL,
	}
	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	start := time.Now().UTC()

	opts := o.RemoveObjectOptions
	err := node.Client.RemoveObject(ctx, o.Bucket, o.Object, opts)
	return Result{
		Method:    http.MethodDelete,
		Path:      fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		FuncName:  "RemoveObject",
		Err:       err,
		Node:      node.EndpointURL,
		Latency:   time.Since(start),
		StartTime: start,
		EndTime:   time.Now().UTC(),
		TestName:  o.TestName,
	}
}

// TBD -> ignore NodeIdx and send correct node as arg.,
func List(ctx context.Context, numEntries int, o ListOpts, node *node.Node) (res Result, objCh <-chan minio.ObjectInfo) {
	path := fmt.Sprintf("%s/%s", o.Bucket, o.Prefix)
	res = Result{
		Method:   ListType,
		Path:     path,
		Node:     node.EndpointURL,
		FuncName: "ListObjects",
	}
	if node.Client.IsOffline() {
		res.Offline = true
		res.Err = ErrNodeOffline
		return
	}
	start := time.Now()
	objCh = node.Client.ListObjects(ctx, o.Bucket, o.ListObjectsOptions)

	return Result{
		Method:   ListType,
		Path:     fmt.Sprintf("%s/%s", o.Bucket, o.Prefix),
		FuncName: "ListObjects",
		Node:     node.EndpointURL,
		Latency:  time.Since(start),
	}, objCh
}

func Cleanup(bucket string, prefixes []string, nodes []*node.Node) {
	var clnt *minio.Client
	for _, node := range nodes {
		if node.Client.IsOffline() {
			continue
		}
		clnt = node.Client
		break
	}
	if clnt == nil {
		return
	}
	for _, pfx := range prefixes {
		err := clnt.RemoveObject(context.Background(), bucket, pfx, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		if err != nil {
			continue
		}
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

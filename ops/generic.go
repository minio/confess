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

package ops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/minio/confess/node"
	"github.com/minio/minio-go/v7"
)

// CheckGetConsistency - base GetObject call that passes ETag match or other headers to minio-go client.
func CheckGetConsistency(ctx context.Context, bucket, object string, client *minio.Client, opts minio.GetObjectOptions) (oi minio.ObjectInfo, err error) {
	obj, gerr := client.GetObject(ctx, bucket, object, opts)
	if gerr != nil {
		return oi, gerr
	}
	defer obj.Close()
	if _, err = io.Copy(ioutil.Discard, obj); err == nil {
		oi, err = obj.Stat()
	}
	return oi, err
}

// CheckStatConsistency - base StatObject call that passes ETag match or other headers in opts to minio-go client.
// ObjectInfo from PUT operation compared with stat output.
func CheckStatConsistency(ctx context.Context, o StatOpts, node *node.Node) (res Result) {
	res, oi := Stat(ctx, o, node)
	var err = res.Err
	if err == nil {
		other := o.ObjInfo
		// compare ETag, size and version id if available
		if oi.ETag != other.ETag ||
			(oi.VersionID != other.VersionID && other.VersionID != "") ||
			oi.Size != other.Size {
			err = fmt.Errorf("metadata mismatch: ETag(expected: %s, actual:%s),Version (expected: %s,actual: %s), Size:(expected: %d, actual:%d)", other.ETag, oi.ETag, other.VersionID, oi.VersionID, other.Size, oi.Size)
		}
		if err == nil && len(other.UserMetadata) > 0 {
			if !reflect.DeepEqual(other.UserMetadata, oi.UserMetadata) {
				err = fmt.Errorf("metadata mismatch of user defined metadata")
			}
		}
	}
	res.Data = oi
	res.Err = err
	res.Node = node.EndpointURL
	return res
}

// check if get readable..
func TestGet(ctx context.Context, o GetOpts, node *node.Node) (res Result) {
	if o.TestGetFn != nil {
		oi, err := o.TestGetFn(ctx, o.Bucket, o.Object, node.Client, o.GetObjectOptions)
		res.Data = oi
		res.Err = err
		return res
	}
	// check if object is readable
	res, obj := Get(ctx, o, node)
	if res.Err != nil {
		return
	}
	defer obj.Close()
	_, err := io.Copy(ioutil.Discard, obj)
	res.Err = err
	offline, retryable := IsRetryableError(res.Err, node)
	res.Offline = offline
	res.RetryRequest = retryable
	res.Node = node.EndpointURL

	return res
}

func TestStat(ctx context.Context, o StatOpts, node *node.Node) (res Result) {
	testFn := CheckStatConsistency
	if o.TestStatFn != nil {
		testFn = o.TestStatFn
	}
	res = testFn(ctx, o, node)
	offline, retryable := IsRetryableError(res.Err, node)
	res.Offline = offline
	res.RetryRequest = retryable
	res.Node = node.EndpointURL

	return
}
func TestList(ctx context.Context, numEntries int, o ListOpts, node *node.Node) (res Result) {
	var err error
	if o.TestListFn != nil { // write some custom test
		err = o.TestListFn(ctx, o.Bucket, o.Prefix, numEntries, node.Client, o.ListObjectsOptions)
	} else { // default List consistency check
		err = CheckListConsistency(ctx, o.Bucket, o.Prefix, numEntries, node.Client, o.ListObjectsOptions)
	}
	res.Err = err
	offline, retryable := IsRetryableError(res.Err, node)
	res.Offline = offline
	res.RetryRequest = retryable
	res.Node = node.EndpointURL

	return
}

func TestDelete(ctx context.Context, o DelOpts, node *node.Node) (res Result) {
	var err error
	if o.TestRemoveFn != nil {
		err = o.TestRemoveFn(ctx, o.Bucket, o.Object, node.Client, o.RemoveObjectOptions)
		res.Err = err
	} else {
		res = Delete(ctx, o, node)
	}
	offline, retryable := IsRetryableError(res.Err, node)
	res.Offline = offline
	res.RetryRequest = retryable
	res.Node = node.EndpointURL

	return
}
func CheckListConsistency(ctx context.Context, bucket, prefix string, expectedCount int, client *minio.Client, opts minio.ListObjectsOptions) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	saw := 0
	var err error
	for objCh := range client.ListObjects(ctx, bucket, opts) {
		if objCh.Err != nil {
			err = objCh.Err
			break
		}
		saw++
	}

	if saw != expectedCount && !errors.Is(err, ErrNodeOffline) && !errors.Is(err, context.Canceled) {
		err = fmt.Errorf("mismatch in number of versions: expected %d , saw %d for %s", expectedCount, saw, fmt.Sprintf("%s/%s", bucket, prefix))
	}
	return err
}
func IsRetryableError(err error, node *node.Node) (offline bool, retry bool) {
	if errors.Is(err, context.Canceled) {
		return false, false
	}
	errResp := minio.ToErrorResponse(err)
	if minio.IsNetworkOrHostDown(err, false) ||
		node.Client.IsOffline() ||
		errResp.StatusCode == http.StatusServiceUnavailable {
		return true, true
	}
	switch {
	case errResp == minio.ErrorResponse{}:
		return false, true
	case errResp.Code == "InternalError":
		return false, true
	}
	return false, false
}
func Run(ctx context.Context, bucket string, idx int, op Op, nodes []*node.Node, opts Opts) (res Result) {
	node := nodes[idx]

	startTime := time.Now().UTC()

	if node.Client.IsOffline() {
		res = Result{
			TestName:  op.TestName,
			Offline:   true,
			Err:       ErrNodeOffline,
			Node:      node.EndpointURL,
			StartTime: startTime,
			EndTime:   time.Now().UTC(),
		}
		return
	}
	defer func() {
		res.TestName = op.TestName
		res.StartTime = startTime
		res.EndTime = time.Now().UTC()
		res.Node = node.EndpointURL
		if res.Err == nil {
			return
		}
		offline, retryable := IsRetryableError(res.Err, node)
		if offline {
			res.Err = ErrNodeOffline
			res.RetryRequest = true
			res.Node = node.EndpointURL
			return
		}
		res.RetryRequest = retryable
	}()
	switch op.Type {
	case http.MethodPut:
		select {
		case <-ctx.Done():
			return
		default:
			popts := opts.(PutOpts)
			res = Put(ctx, popts, node)
			return
		}
	case http.MethodGet:
		select {
		default:
			gopts := opts.(GetOpts)
			res = TestGet(ctx, gopts, node)
			return
		case <-ctx.Done():
			return
		}
	case http.MethodHead:
		select {
		default:
			sopts := opts.(StatOpts)
			res = TestStat(ctx, sopts, node)
			return
		case <-ctx.Done():
			return
		}
	case http.MethodDelete:
		select {
		default:
			dopts := opts.(DelOpts)
			res = TestDelete(ctx, dopts, node)
			return
		case <-ctx.Done():
			return
		}
	case ListType:
		select {
		default:
			lopts := opts.(ListOpts)
			res = TestList(ctx, op.ExpectedCount, lopts, node)
			return
		case <-ctx.Done():
			return
		}
	case MultipartType:
		select {
		default:
			popts := opts.(PutOpts)
			res = MultipartPut(ctx, popts, nodes)
			return
		case <-ctx.Done():
			return
		}
	default:
	}

	return res
}

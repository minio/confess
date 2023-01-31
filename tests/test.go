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
	"math/rand"
	"net/url"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/confess/node"
	"github.com/minio/confess/ops"
	"github.com/minio/minio-go/v7"
)

// Status of test
type StatusType string

const (
	Started StatusType = "Started"
	Ended   StatusType = "Ended"
	Failed  StatusType = "Failed"
)

type Tester interface {
	Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retry RetryInfo)
	Concurrency() int
}
type RetryInfo struct {
	OK           bool // retries GET|HEAD|LIST operation
	Bucket       string
	ObjInfo      minio.ObjectInfo
	Err          error
	TestName     string
	LastOpResult ops.Result
}
type Test struct {
	Name       string
	Concurrent int
	NodeSlc    node.NodeSlc
	IsRetry    bool
	Retry      RetryInfo
}
type Result struct {
	TestName  string        `json:"testName"`
	Status    StatusType    `json:"status"`
	Node      *url.URL      `json:"node"`
	Err       error         `json:"err,omitempty"`
	Failures  int           `json:"failures,omitempty"`
	Latency   time.Duration `json:"duration"`
	StartTime time.Time     `json:"startTime"`
	EndTime   time.Time     `json:"endTime"`
	AbortTest bool          `json:"abortTest"`
	IsRetry   bool
	Retry     RetryInfo
}

func (r *Result) String() string {
	var errMsg string
	if r.Err != nil {
		errMsg = r.Err.Error()
	}
	lastRes := r.Retry.LastOpResult
	return fmt.Sprintf("%s %s %s: %s %s %s %s %s ::%t:%t", r.StartTime.Format(time.RFC3339Nano), r.EndTime.Format(time.RFC3339Nano), lastRes.Node, r.TestName, lastRes.Path, lastRes.Method, lastRes.FuncName, errMsg, lastRes.RetryRequest, r.AbortTest)
}
func (t *Test) genObjName() string {
	pfx := t.getRandomPfx()
	return fmt.Sprintf("%s/%s", pfx, shortuuid.New())
}
func (t *Test) getRandomPfx() string {
	idx := rand.Intn(len(t.NodeSlc.Prefixes))
	return t.NodeSlc.Prefixes[idx]
}

func (t *Test) Concurrency() int {
	conc := 1
	if t.Concurrent > conc {
		conc = t.Concurrent
	}
	return conc
}

func toRetryInfo(bucket, testName string, res ops.Result) RetryInfo {
	r := RetryInfo{
		OK:           true,
		Bucket:       bucket,
		Err:          res.Err,
		TestName:     testName,
		ObjInfo:      cloneOI(res.Data),
		LastOpResult: res,
	}
	return r
}

func (t *Test) markTestStartFn(ctx context.Context, startTime time.Time, resCh chan interface{}) {
	select {
	case resCh <- Result{
		TestName:  t.Name,
		StartTime: startTime,
		Status:    Started,
	}:
	case <-ctx.Done():
		return
	}
}
func (t *Test) markTestStatusFn(ctx context.Context, startedAt time.Time, lastErr ops.Result, resCh chan interface{}) {
	st := Ended
	if lastErr.Err != nil {
		st = Failed
	}

	select {
	case resCh <- Result{
		TestName:  t.Name,
		Err:       lastErr.Err,
		Latency:   time.Since(startedAt),
		StartTime: startedAt,
		EndTime:   time.Now(),
		Status:    st,
		IsRetry:   lastErr.RetryRequest,
		AbortTest: errors.Is(lastErr.Err, context.Canceled) || lastErr.AbortTest,
		Retry:     toRetryInfo(t.NodeSlc.Bucket, t.Name, lastErr),
	}:
	case <-ctx.Done():
		return
	}
}

func LoadRegistry() *Registry {
	r := NewRegistry()
	r.Register("PutGetCheck", PutGetCheck())
	r.Register("PutGetCheck2", PutGetCheck2())
	r.Register("PutStatCheck", PutStatCheck())
	r.Register("PutStatCheck2", PutStatCheck2())
	r.Register("PutListCheck1", newPutListCheckTest1())
	r.Register("PutListCheck2", newPutListCheckTest2())

	//	r.Register("PutGetCheck3", &PutGetCheck3Test{}) // write custom test
	return r
}

// cloneMSS will clone a map[string]string.
// If input is nil an empty map is returned, not nil.
func cloneMSS(v map[string]string) map[string]string {
	r := make(map[string]string, len(v))
	for k, v := range v {
		r[k] = v
	}
	return r
}
func cloneOI(o1 minio.ObjectInfo) minio.ObjectInfo {
	o2 := o1
	o2.UserMetadata = cloneMSS(o1.UserMetadata)
	o2.UserTags = cloneMSS(o1.UserTags)
	for k, v := range o1.Metadata {
		o2.Metadata[k] = v
	}
	return o2
}

func getMultipartObjSize(numNodes int) int64 {
	if numNodes == 1 {
		return humanize.MiByte * 5
	}
	return int64((numNodes-1)*humanize.MiByte*5 + 1)
}

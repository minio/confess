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

	"github.com/minio/confess/node"
	"github.com/minio/minio-go/v7"
)

// PutGetCheck creates an object (version), then tests if version is consistent across all
// available nodes
func PutGetCheck() *GenericTest {
	t := &GenericTest{
		Test: Test{
			Name:       "PutGetCheck",
			Concurrent: 1000,
		},
	}
	t.SetupOpFn = t.setupPutOp(false, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	// this function sets up options needed for the test.Write custom function if you like.
	t.TestOpFns = []TestOpFn{t.setupGetOp()}
	// this function sets up options needed for the teardown - performs versioned delete.
	t.CleanupOpFn = t.setupDelOp()
	return t
}

// PutGetCheck2 creates a multipart object - one part per node, then tests if version is consistent across all
// available nodes
func PutGetCheck2() *GenericTest {
	t := &GenericTest{
		Test: Test{
			Name:       "PutGetCheck2",
			Concurrent: 1000,
		},
	}
	t.SetupOpFn = t.setupPutOp(true, minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Customkey": "extra  spaces  in   value",
			"Key2":      "Confess"},
	})
	t.TestOpFns = []TestOpFn{t.setupGetOp()}
	t.CleanupOpFn = t.setupDelOp()
	return t
}

// optionally write your own test
type PutGetCheck3Test struct {
	Test
}

func (t *PutGetCheck3Test) Run(ctx context.Context, nodeSlc node.NodeSlc, resCh chan interface{}, retryInfo RetryInfo) {
	// needs a setup phase to create object(s)
	// mark test started with markTestStartFn
	// run one or more operations [ send results of operation to resCh]
	// decide if test is a pass or needs retry | to be aborted
	// mark test status with markTestStatusFn
	// perform cleanup
}

func (t PutGetCheck3Test) Concurrency() int {
	return 1 // run this test x times concurrently
}

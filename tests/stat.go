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
	t.TestOpFn = t.setupStatOp()
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
	t.TestOpFn = t.setupStatOp()
	// this function sets up options needed for the teardown - performs versioned delete.
	t.CleanupOpFn = t.setupDelOp()
	return t
}

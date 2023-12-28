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

	"github.com/minio/pkg/v2/sync/errgroup"
)

var errTestNotInitialized = errors.New("the test is not initialized")

// Test interface defines the test.
type Test interface {
	Name() string
	Init(ctx context.Context, config Config, stats *Stats) error
	Setup(ctx context.Context) error
	Run(ctx context.Context) error
	TearDown(ctx context.Context) error
}

func runFn(ctx context.Context, threadCount, concurrency int, fn func(ctx context.Context, index int) error) error {
	g := errgroup.WithNErrs(threadCount).WithConcurrency(concurrency)
	ctx, _ = g.WithCancelOnError(ctx)
	for i := 0; i < threadCount; i++ {
		g.Go(
			func(index int) func() error {
				return func() error {
					return fn(ctx, index)
				}
			}(i),
			i,
		)
	}
	return g.WaitErr()
}

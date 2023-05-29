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

package utils

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/minio/minio-go/v7"
)

var ErrAllTargetsOffline = errors.New("all the targets are offline")

const maxAttempts = 3

// GetRandomClient fetches a random client from the provided list of clients.
func GetRandomClient(clients []*minio.Client) (*minio.Client, error) {
	return getRandomClient(clients, 0)
}

func getRandomClient(clients []*minio.Client, attempt int) (*minio.Client, error) {
	visitedIds := make(map[int]struct{})
	idx := rand.Intn(len(clients))
	visitedIds[idx] = struct{}{}

	for clients[idx].IsOffline() {
		if len(visitedIds) == len(clients) {
			if attempt == maxAttempts {
				return nil, ErrAllTargetsOffline
			}
			attempt++
			time.Sleep(2 * time.Second)
			return getRandomClient(clients, attempt)
		}
		newIdx := rand.Intn(len(clients))
		if _, ok := visitedIds[newIdx]; ok {
			continue
		}
		idx = newIdx
		visitedIds[idx] = struct{}{}
	}

	return clients[idx], nil
}

// IsContextError returns true if the error relates to context cancel or deadline exceeded.
func IsContextError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

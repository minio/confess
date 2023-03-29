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

package stats

import (
	"sync"
	"sync/atomic"
)

type Stats struct {
	TotalCount   int
	SuccessCount int
	sync.Mutex
}

func (stats *Stats) incrementStats(success bool) {
	stats.Lock()
	defer stats.Unlock()

	stats.TotalCount++
	if success {
		stats.SuccessCount++
	}
}

type APIStats struct {
	Puts    Stats
	Lists   Stats
	Deletes Stats
	Heads   Stats
	Total   int64
	Success int64
}

func (a *APIStats) IncrementStats(apiStats *Stats, success bool) {
	atomic.AddInt64(&a.Total, 1)
	if success {
		atomic.AddInt64(&a.Success, 1)
	}
	apiStats.incrementStats(success)
}

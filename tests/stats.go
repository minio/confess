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
	"fmt"
	"sync/atomic"
)

// Stat denotes the total and success stat info.
type Stat struct {
	Total  atomic.Int64
	Failed atomic.Int64
}

func (stat *Stat) increment(success bool) {
	stat.Total.Add(1)
	if !success {
		stat.Failed.Add(1)
	}
}

func (stat *Stat) incrementTotal() {
	stat.Total.Add(1)
}

func (stat *Stat) incrementFailed() {
	stat.Failed.Add(1)
}

// Info returns the stat info.
func (stats *Stat) info() (total, failed int64) {
	return stats.Total.Load(), stats.Failed.Load()
}

// Stats denotes the statistical information on the API requests made.
type Stats struct {
	APIStat  *Stat
	TestStat *Stat
}

// NewStats creates an instance of Stats.
func NewStats() *Stats {
	return &Stats{
		APIStat:  &Stat{},
		TestStat: &Stat{},
	}
}

// APIInfo returns the API stats.
func (stats *Stats) APIInfo() (total, failed int64) {
	return stats.APIStat.info()
}

// TestInfo returns the Test stats.
func (stats *Stats) TestInfo() (total, failed int64) {
	return stats.TestStat.info()
}

func (stats *Stats) String() string {
	totalAPIs, failedAPIs := stats.APIInfo()
	_, failedTests := stats.TestInfo()
	return fmt.Sprintf(
		"Total APIs: %d\nFailed APIs: %d\nFailed Tests: %d",
		totalAPIs,
		failedAPIs,
		failedTests,
	)
}

func (stats *Stats) updateAPIStat(success bool) {
	stats.APIStat.increment(success)
}

func (stats *Stats) incrementTotalTests() {
	stats.TestStat.incrementTotal()
}

func (stats *Stats) incrementFailedTests() {
	stats.TestStat.incrementFailed()
}

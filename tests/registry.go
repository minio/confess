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
	"math/rand"
	"sync"
)

type Registry struct {
	sync.RWMutex
	Tests []Tester
	tIdx  map[string]int
}

func NewRegistry() *Registry {
	return &Registry{
		Tests: make([]Tester, 0, 10),
		tIdx:  make(map[string]int),
	}
}

func (r *Registry) Register(name string, t Tester) {
	r.Lock()
	defer r.Unlock()
	r.Tests = append(r.Tests, t)
	r.tIdx[name] = len(r.Tests) - 1
}

func (r *Registry) GetRandomTest() Tester {
	idx := rand.Intn(len(r.Tests))
	return r.Tests[idx]
}

func (r *Registry) GetTestByName(name string) Tester {
	r.RLock()
	defer r.RUnlock()
	idx := r.tIdx[name]
	return r.Tests[idx]
}

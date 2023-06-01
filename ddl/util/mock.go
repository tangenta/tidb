// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import "sync"

var MockGlobalStateEntry = &MockGlobalState{
	currentOwner: make(map[string]string),
}

type MockGlobalState struct {
	mu           sync.Mutex
	currentOwner map[string]string
}

func (m *MockGlobalState) Type(tp string) *MockGlobalStateSelector {
	return &MockGlobalStateSelector{m: m, tp: tp}
}

type MockGlobalStateSelector struct {
	m  *MockGlobalState
	tp string
}

func (t *MockGlobalStateSelector) GetOwner() string {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	return t.m.currentOwner[t.tp]
}

func (t *MockGlobalStateSelector) SetOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	if t.m.currentOwner[t.tp] == "" {
		t.m.currentOwner[t.tp] = owner
		return true
	}
	return false
}

func (t *MockGlobalStateSelector) UnsetOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	if t.m.currentOwner[t.tp] == owner {
		t.m.currentOwner[t.tp] = ""
		return true
	}
	return false
}

func (t *MockGlobalStateSelector) IsOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	return t.m.currentOwner[t.tp] == owner
}

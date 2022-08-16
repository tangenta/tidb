// Copyright 2022 PingCAP, Inc.
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

package lightning

import (
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/mathutil"
)

type MemRoot interface {
	Consume(size int64)
	Release(size int64)
	TryConsume(size int64) error
	ConsumeWithTag(tag string, size int64)
	ReleaseWithTag(tag string)

	SetMaxMemoryQuota(quota int64)
	MaxMemoryQuota() int64
	CurrentUsage() int64
	CurrentUsageWithTag(tag string) int64
	RefreshConsumption()
	// WorkerDegree adjust worker count according the available memory.
	// return 0 means there is no enough memory for one lightning worker.
	// TODO: split this function into two functions:
	// 1. Calculate the worker degree.
	// 2. Update the MemRoot.
	WorkerDegree(workerCnt int, engineKey string, jobID int64) int
}

const (
	_kb       = 1024
	_mb       = 1024 * _kb
	_gb       = 1024 * _mb
	_tb       = 1024 * _gb
	_pb       = 1024 * _tb
	flushSize = 8 * _mb
)

// Used to mark the object size did not store in map
const allocFailed int64 = 0

var (
	// StructSizeBackendCtx is the size of BackendContext.
	StructSizeBackendCtx int64
	// StructSizeEngineInfo is the size of EngineInfo.
	StructSizeEngineInfo int64
	// StructSizeWorkerCtx is the size of WorkerContext.
	StructSizeWorkerCtx int64
)

func init() {
	StructSizeBackendCtx = int64(unsafe.Sizeof(BackendContext{}))
	StructSizeEngineInfo = int64(unsafe.Sizeof(engineInfo{}))
	StructSizeWorkerCtx = int64(unsafe.Sizeof(WorkerContext{}))
}

type MemRootImpl struct {
	maxLimit      int64
	currUsage     int64
	structSize    map[string]int64
	backendCtxMgr *BackendCtxManager
	mu            sync.RWMutex
}

func NewMemRootImpl(maxQuota int64, bcCtxMgr *BackendCtxManager) *MemRootImpl {
	return &MemRootImpl{
		maxLimit:      mathutil.Max(maxQuota, flushSize),
		currUsage:     0,
		structSize:    make(map[string]int64, 10),
		backendCtxMgr: bcCtxMgr,
	}
}

func (m *MemRootImpl) SetMaxMemoryQuota(maxQuota int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxLimit = mathutil.Max(maxQuota, flushSize)
}

func (m *MemRootImpl) MaxMemoryQuota() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.maxLimit
}

func (m *MemRootImpl) CurrentUsage() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currUsage
}

func (m *MemRootImpl) CurrentUsageWithTag(tag string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.structSize[tag]
}

func (m *MemRootImpl) Consume(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage += size
}

func (m *MemRootImpl) Release(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage -= size
}

func (m *MemRootImpl) ConsumeWithTag(tag string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage += size
	m.structSize[tag] = size
}

func (m *MemRootImpl) TryConsume(size int64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.currUsage+size > m.maxLimit {
		return errors.New(LitErrOutMaxMem)
	}
	return nil
}

func (m *MemRootImpl) ReleaseWithTag(tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage -= m.structSize[tag]
	delete(m.structSize, tag)
}

func (m *MemRootImpl) RefreshConsumption() {
	m.backendCtxMgr.UpdateMemoryUsage()
}

func (m *MemRootImpl) WorkerDegree(workerCnt int, engineKey string, jobID int64) int {
	var enSize int64
	var currWorkerNum int
	m.mu.Lock()
	defer m.mu.Unlock()
	bc, exist := m.backendCtxMgr.Load(jobID)
	if !exist {
		return 0
	}

	_, exist = m.structSize[engineKey]
	if !exist {
		enSize = int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	} else {
		en, exist1 := bc.EngMgr.Load(engineKey)
		if !exist1 {
			return 0
		}
		currWorkerNum = en.writerCount
	}
	if currWorkerNum+workerCnt > bc.cfg.TikvImporter.RangeConcurrency {
		workerCnt = bc.cfg.TikvImporter.RangeConcurrency - currWorkerNum
		if workerCnt == 0 {
			return workerCnt
		}
	}

	size := int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize)

	// If only one worker's memory init requirement still bigger than mem limitation.
	if enSize+size+m.currUsage > m.maxLimit {
		return int(allocFailed)
	}

	for enSize+size*int64(workerCnt)+m.currUsage > m.maxLimit && workerCnt > 1 {
		workerCnt /= 2
	}

	m.currUsage += size * int64(workerCnt)

	if !exist {
		m.structSize[engineKey] = size * int64(workerCnt)
	} else {
		m.structSize[engineKey] += size * int64(workerCnt)
	}
	return workerCnt
}

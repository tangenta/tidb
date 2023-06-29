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

package sharedisk

import (
	"bytes"
	"container/heap"
	"context"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type kvPair struct {
	key        []byte
	value      []byte
	fileOffset int
}

type kvPairHeap []*kvPair

func (h kvPairHeap) Len() int {
	return len(h)
}

func (h kvPairHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h kvPairHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *kvPairHeap) Push(x interface{}) {
	*h = append(*h, x.(*kvPair))
}

func (h *kvPairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MergeIter struct {
	startKey       []byte
	endKey         []byte
	dataFilePaths  []string
	dataFileReader []*DataFileReader
	exStorage      storage.ExternalStorage
	kvHeap         kvPairHeap
	currKV         *kvPair

	firstKey []byte

	err error
}

func NewMergeIter(ctx context.Context, startKey, endKey []byte, paths []string, exStorage storage.ExternalStorage) (*MergeIter, error) {
	it := &MergeIter{
		startKey:      startKey,
		endKey:        endKey,
		dataFilePaths: paths,
	}
	it.dataFileReader = make([]*DataFileReader, 0, len(paths))
	it.kvHeap = make([]*kvPair, 0, len(paths))
	for i, path := range paths {
		rd := DataFileReader{ctx: ctx, name: path, exStorage: exStorage}
		rd.readBuffer = make([]byte, 4096)
		it.dataFileReader = append(it.dataFileReader, &rd)
		k, v, err := rd.GetNextKV()
		if err != nil {
			return nil, err
		}
		if len(k) == 0 {
			continue
		}
		pair := kvPair{key: k, value: v, fileOffset: i}
		it.kvHeap.Push(&pair)
	}
	heap.Init(&it.kvHeap)
	return it, nil
}

func (i *MergeIter) Seek(key []byte) bool {
	// Don't support.
	return false
}

func (i *MergeIter) Error() error {
	return i.err
}

func (i *MergeIter) First() bool {
	// Don't support.
	return false
}

func (i *MergeIter) Last() bool {
	// Don't support.
	return false
}

func (i *MergeIter) Valid() bool {
	return i.currKV != nil
}

func (i *MergeIter) Next() bool {
	if i.kvHeap.Len() == 0 {
		return false
	}
	i.currKV = heap.Pop(&i.kvHeap).(*kvPair)
	k, v, err := i.dataFileReader[i.currKV.fileOffset].GetNextKV()
	if err != nil {
		i.err = err
		return false
	}
	if len(k) > 0 {
		heap.Push(&i.kvHeap, &kvPair{k, v, i.currKV.fileOffset})
	}

	return true
}

func (i *MergeIter) Key() []byte {
	return i.currKV.key
}

func (i *MergeIter) Value() []byte {
	return i.currKV.value
}

func (i *MergeIter) Close() []byte {
	return i.firstKey
}

func (i *MergeIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

type prop struct {
	p          RangeProperty
	fileOffset int
}

type propHeap []*prop

func (h propHeap) Len() int {
	return len(h)
}

func (h propHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].p.Key, h[j].p.Key) < 0
}

func (h propHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *propHeap) Push(x interface{}) {
	*h = append(*h, x.(*prop))
}

func (h *propHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MergePropIter struct {
	startKey       []byte
	endKey         []byte
	statFilePaths  []string
	statFileReader []*statFileReader
	exStorage      storage.ExternalStorage
	propHeap       propHeap
	currProp       *prop

	firstKey []byte

	err error
}

func NewMergePropIter(ctx context.Context, startKey, endKey []byte, paths []string, exStorage storage.ExternalStorage) (*MergePropIter, error) {
	it := &MergePropIter{
		startKey:      startKey,
		endKey:        endKey,
		statFilePaths: paths,
	}
	it.propHeap = make([]*prop, 0, len(paths))
	for i, path := range paths {
		rd := statFileReader{ctx: ctx, name: path, exStorage: exStorage}
		rd.readBuffer = make([]byte, 4096)
		it.statFileReader = append(it.statFileReader, &rd)
		p, err := rd.GetNextProp()
		if err != nil {
			return nil, err
		}
		if p == nil {
			continue
		}
		pair := prop{p: *p, fileOffset: i}
		it.propHeap.Push(&pair)
	}
	heap.Init(&it.propHeap)
	return it, nil
}

func (i *MergePropIter) Error() error {
	return i.err
}

func (i *MergePropIter) Valid() bool {
	return i.currProp != nil
}

func (i *MergePropIter) Next() bool {
	if i.propHeap.Len() == 0 {
		return false
	}
	i.currProp = heap.Pop(&i.propHeap).(*prop)
	p, err := i.statFileReader[i.currProp.fileOffset].GetNextProp()
	if err != nil {
		i.err = err
		return false
	}
	if p != nil {
		heap.Push(&i.propHeap, &prop{*p, i.currProp.fileOffset})
	}

	return true
}

func (i *MergePropIter) prop() *RangeProperty {
	return &i.currProp.p
}

func (i *MergePropIter) Close() error {
	return nil
}

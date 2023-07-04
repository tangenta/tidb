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
	"context"
	"strings"

	"github.com/pingcap/tidb/kv"
)

type RangeSplitter struct {
	maxSize   uint64
	maxKeys   uint64
	maxWays   uint64
	propIter  *MergePropIter
	dataFiles []string
	exhausted bool
}

func NewRangeSplitter(maxSize, maxKeys, maxWays uint64, propIter *MergePropIter, dataFiles []string) *RangeSplitter {
	return &RangeSplitter{
		maxSize:   maxSize,
		maxKeys:   maxKeys,
		maxWays:   maxWays,
		propIter:  propIter,
		dataFiles: dataFiles,
	}
}

func (r *RangeSplitter) FirstStartKey(ctx context.Context) (kv.Key, error) {
	offsets := make([]uint64, len(r.dataFiles))
	iter, err := NewMergeIter(ctx, r.dataFiles, offsets, r.propIter.statFileReader[0].exStorage)
	if err != nil {
		return nil, err
	}
	if iter.Next() {
		return iter.Key(), nil
	}
	return nil, nil
}

func (r *RangeSplitter) SplitOne(ctx context.Context) (kv.Key, []string, []string, error) {
	if r.exhausted {
		return nil, nil, nil, nil
	}
	var curSize, curKeys uint64
	curWays := make(map[int]struct{})
	dataFiles := make([]string, 0, 16)
	statsFiles := make([]string, 0, 16)
	//var curKey kv.Key
	//var curFileIdx int
	var lastFileIdx int
	var lastOffset uint64
	for r.propIter.Next() {
		if err := r.propIter.Error(); err != nil {
			return nil, nil, nil, err
		}
		//if curKey.Cmp(r.propIter.currProp.p.Key) > 0 {
		//	logutil.BgLogger().Info("unexpected descending key",
		//		zap.String("prev", hex.EncodeToString(curKey)),
		//		zap.Int("prevFileIdx", curFileIdx),
		//		zap.String("curr", hex.EncodeToString(r.propIter.currProp.p.Key)))
		//}
		//curKey = kv.Key(r.propIter.currProp.p.Key).Clone()
		//curFileIdx = r.propIter.currProp.fileIdx
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curKeys += prop.rangeOffsets.Keys
		fileIdx := r.propIter.currProp.fileOffset
		lastFileIdx = fileIdx
		lastOffset = prop.offset
		if _, found := curWays[fileIdx]; !found {
			curWays[fileIdx] = struct{}{}
			statFile := r.propIter.statFilePaths[fileIdx]
			statsFiles = append(statsFiles, statFile)
			dataFiles = append(dataFiles, strings.Replace(statFile, "_stat", "", 1))
		}
		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(curWays)) >= r.maxWays {
			return prop.Key, dataFiles, statsFiles, nil
		}
	}
	if err := r.propIter.Error(); err != nil {
		return nil, nil, nil, err
	}
	maxKeyFile := []string{r.dataFiles[lastFileIdx]}
	startOffset := []uint64{lastOffset}
	iter, err := NewMergeIter(ctx, maxKeyFile, startOffset, r.propIter.statFileReader[0].exStorage)
	if err != nil {
		return nil, nil, nil, err
	}
	var maxKey kv.Key
	for iter.Next() {
		if err := iter.Error(); err != nil {
			return nil, nil, nil, err
		}
		maxKey = iter.Key()
	}
	if err := iter.Error(); err != nil {
		return nil, nil, nil, err
	}
	r.exhausted = true
	return maxKey, dataFiles, statsFiles, nil
}

func (r *RangeSplitter) FileCount() int {
	return len(r.propIter.statFileReader)
}

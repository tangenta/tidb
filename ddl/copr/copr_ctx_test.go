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

package copr

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNewCopContextSingleIndex(t *testing.T) {
	var mockColInfos []*model.ColumnInfo
	for i := 0; i < 5; i++ {
		mockColInfos = append(mockColInfos, &model.ColumnInfo{
			ID:        int64(i),
			Name:      model.NewCIStr(fmt.Sprintf("c%d", i)),
			FieldType: *types.NewFieldType(1),
			State:     model.StatePublic,
		})
	}
	findColByName := func(name string) *model.ColumnInfo {
		for _, info := range mockColInfos {
			if info.Name.L == name {
				return info
			}
		}
		return nil
	}
	var mockIdxInfos []*model.IndexInfo
	for i, testIdxColNames := range [][]string{
		{"c1"},
		{"c1", "c3"},
		{"c5", "c1"},
	} {
		var idxCols []*model.IndexColumn
		for _, cn := range testIdxColNames {
			idxCols = append(idxCols, &model.IndexColumn{
				Name:   model.NewCIStr(cn),
				Offset: findColByName(cn).Offset,
			})
		}
		mockIdxInfos = append(mockIdxInfos, &model.IndexInfo{
			ID:      int64(i),
			Name:    model.NewCIStr(fmt.Sprintf("i%d", i)),
			Columns: idxCols,
			State:   model.StatePublic,
		})
	}
	mockTableInfo := &model.TableInfo{
		Name:       model.NewCIStr("t"),
		Columns:    mockColInfos,
		Indices:    mockIdxInfos,
		PKIsHandle: false,
	}
	copCtx, err := NewCopContextSingleIndex(mockTableInfo, mockIdxInfos[0], mock.NewContext(), "")
	require.NoError(t, err)
	base := copCtx.GetBase()
	require.Equal(t, "t", base.TableInfo.Name.L)
}

func TestResolveIndicesForHandle(t *testing.T) {
	type args struct {
		cols      []*expression.Column
		handleIDs []int64
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{
			name: "Basic 1",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{2},
			},
			want: []int{1},
		},
		{
			name: "Basic 2",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{3, 2, 1},
			},
			want: []int{2, 1, 0},
		},
		{
			name: "Basic 3",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{1, 3},
			},
			want: []int{0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveIndicesForHandle(tt.args.cols, tt.args.handleIDs)
			require.Equal(t, got, tt.want)
		})
	}
}

func TestCollectVirtualColumnOffsetsAndTypes(t *testing.T) {
	tests := []struct {
		name    string
		cols    []*expression.Column
		offsets []int
		fieldTp []int
	}{
		{
			name: "Basic 1",
			cols: []*expression.Column{
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(1)},
				{VirtualExpr: nil},
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(2)},
			},
			offsets: []int{0, 2},
			fieldTp: []int{1, 2},
		},
		{
			name: "Basic 2",
			cols: []*expression.Column{
				{VirtualExpr: nil},
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(1)},
				{VirtualExpr: nil},
			},
			offsets: []int{1},
			fieldTp: []int{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffsets, gotFt := collectVirtualColumnOffsetsAndTypes(tt.cols)
			require.Equal(t, gotOffsets, tt.offsets)
			require.Equal(t, len(gotFt), len(tt.fieldTp))
			for i, ft := range gotFt {
				require.Equal(t, int(ft.GetType()), tt.fieldTp[i])
			}
		})
	}
}

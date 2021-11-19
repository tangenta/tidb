// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap/tidb/util/codec"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSplitIndex{})

type testSplitIndex struct {
}

func (s *testSplitIndex) SetUpSuite(c *C) {
}

func (s *testSplitIndex) TearDownSuite(c *C) {
}

func (s *testSplitIndex) TestCodecKeyInvalid(c *C) {
	keys := []string{"t_130752_r_9321509774043722664", "t_130752_r_9358682202155580546", "t_130752_r_9359868658259904585", "t_130752_r_9361288803635854510", "t_130752_r_9362990634012400793", "t_130752_r_9364413420191284354", "t_130752_r_9367714904132105128", "t_130752_r_9398624655553128719", "t_130752_r_9404417132231192315", "t_130752_r_9483010785809464450", "t_130752_r_9483585652866721041", "t_130752_r_9484102789610063945", "t_130752_r_9484926692095918084", "t_130752_r_9485982185118466052", "t_130752_r_9486620604928720900", "t_130752_r_9488159971778526382", "t_130752_r_9489565412265609289", "t_130752_r_9491026073476333340", "t_130752_r_9491823576919063258", "t_130752_r_9549818208361790170", "t_130752_r_9557229308718392593", "t_130752_r_9569594311214476501", "t_130752_r_9576111628287051536", "t_130752_r_9650523992085978265", "t_130752_r_9830295053173128322", "t_130752_r_9841401997837781065", "t_130752_r_9871917479298006812", "t_130752_r_9892133546619879164", "t_130752_r_9940996337167400964", "t_130752_r_10021175904592264322", "t_130752_r_10043814489490516239", "t_130752_r_10069366964558249430", "t_130752_r_10158554384598943817", "t_130752_r_10184154875286306556", "t_130752_r_10199685556091682588", "t_130752_r_10220462954267792773", "t_130752_r_10256724409375377481", "t_130752_r_10413550813239262678", "t_130752_r_10469332834269055049", "t_130752_r_10647246981891672137", "t_130752_r_10753103653495294025", "t_130752_r_10853089582485129289", "t_130752_r_11031593831189703925", "t_130752_r_11143172539569132933", "t_130752_r_11302759585451600143", "t_130752_r_11447977915333593161", "t_130752_r_11491527945328427012", "t_130752_r_11726945500431154350", "t_130752_r_11894938261074080502", "t_130752_r_12056284324406657028", "t_130752_r_12216770079327897673", "t_130752_r_12321361160225523472", "t_130752_r_12450740607781928964", "t_130752_r_12615423528048955891", "t_130752_r_12832983338123296772", "t_130752_r_12997583464475672022", "t_130752_r_13165516514369404700", "t_130752_r_13335129727787697936", "t_130752_r_13499411423885128438", "t_130752_r_13660091223245897468", "t_130752_r_13808093260987055574", "t_130752_r_14028154948960354308", "t_130752_r_14192580017525505494", "t_130752_r_14356484851359987512", "t_130752_r_14517422469944633615", "t_130752_r_14637511762670271958", "t_130752_r_14811267468085413961", "t_130752_r_14953224497255628246", "t_130752_r_15121211582786584022", "t_130752_r_15256130872479041609", "t_130752_r_15482569607654271247", "t_130752_r_15652010850008431746", "t_130752_r_15737308618743793737", "t_130752_r_15897906440518486089", "t_130752_r_16106676401425401929", "t_130752_r_16311942217114896457", "t_130752_r_16504282909476353010", "t_130752_r_16665521792273598537", "t_130752_r_16769466936006605954", "t_130752_r_16934574946728864015", "t_130752_r_17066055491602925641", "t_130752_r_17293218180935837967", "t_130752_r_17454870244190712566", "t_130752_r_17619805520005233794", "t_130752_r_17780813893964142937", "t_130752_r_17866847415844915273", "t_130752_r_18037120696093966108", "t_130752_r_18251587511800088649", "t_130752_r_18418831008214826454", "t_130752_r_147801147796280709", "t_130752_r_311498990060737011", "t_130752_r_437540299347445833", "t_130752_r_605622931275827273", "t_130752_r_773254549328559234", "t_130752_r_934099248932732374", "t_130752_r_1105627561440664729", "t_130752_r_1299430052157292548", "t_130752_r_1387912123527364612", "t_130752_r_1615173231924559318", "t_130752_r_1782591629191382800", "t_130752_r_1943393726882742276", "t_130752_r_2117062228725366788", "t_130752_r_2290366229014838402", "t_130752_r_2451281251115433988", "t_130752_r_2684149135631763529", "t_130752_r_2848026373499355140", "t_130752_r_2939404656701917257", "t_130752_r_3106422162930976841", "t_130752_r_3267759483687222742", "t_130752_r_3400859447416684548", "t_130752_r_3491641708983732297", "t_130752_r_3690620094268950601", "t_130752_r_3871256832233717206", "t_130752_r_4039116066233252994", "t_130752_r_4191427677439445065", "t_130752_r_4358856856093182025", "t_130752_r_4519740558484700290", "t_130752_r_4753161939815914649", "t_130752_r_4920349105689691920", "t_130752_r_5087470653457464494", "t_130752_r_5251882124273436745", "t_130752_r_5433577428808948383", "t_130752_r_5671295182924430806", "t_130752_r_5832026341578885193", "t_130752_r_5993464720509880393", "t_130752_r_6104204802807774678", "t_130752_r_6269464474786037764", "t_130752_r_6484704733035746693", "t_130752_r_6645582425380323332", "t_130752_r_6798977237922709508", "t_130752_r_6966399823464185302", "t_130752_r_7070978829487428997", "t_130752_r_7238592080047622217", "t_130752_r_7403238706286116310", "t_130752_r_7567685250560179670", "t_130752_r_7728713575937787977", "t_130752_r_7889413907960817935", "t_130752_r_8091873628313536585", "t_130752_r_8318511305552742473", "t_130752_r_8479593852271804703", "t_130752_r_8640379838605657872", "t_130752_r_8815194857943809494", "t_130752_r_9038391595798860275", "t_130752_r_9200028114322150649"}
	tableID := int64(130752)
	for _, k := range keys {
		cmtf := Commentf("%v", k)
		keyPart := k[len("t_130752_r_"):]
		buf := make([]byte, 0, 11)
		buf = append(buf, 't')
		buf = codec.EncodeInt(buf, tableID)
		buf = append(buf, '_', 'r')
		rowID, err := strconv.ParseUint(keyPart, 10, 64)
		c.Assert(err, IsNil, cmtf)
		buf = codec.EncodeInt(buf, int64(rowID))
		decoder := regionKeyDecoder{
			physicalTableID:      tableID,
			tablePrefix:          tablecodec.GenTablePrefix(tableID),
			recordPrefix:         tablecodec.GenTableRecordPrefix(tableID),
			indexPrefix:          []byte("random"),
			indexID:              0,
			hasUnsignedIntHandle: true,
		}
		// Make sure the key is constructed correctly.
		str := decoder.decodeRegionKey(buf)
		c.Assert(k, Equals, str, cmtf)
		// Try to reproduce dead loop.
		_, err = tablecodec.DecodeRowKey(buf)
		c.Assert(err, IsNil, cmtf)
	}
}

func (s *testSplitIndex) TestLongestCommonPrefixLen(c *C) {
	cases := []struct {
		s1 string
		s2 string
		l  int
	}{
		{"", "", 0},
		{"", "a", 0},
		{"a", "", 0},
		{"a", "a", 1},
		{"ab", "a", 1},
		{"a", "ab", 1},
		{"b", "ab", 0},
		{"ba", "ab", 0},
	}

	for _, ca := range cases {
		re := longestCommonPrefixLen([]byte(ca.s1), []byte(ca.s2))
		c.Assert(re, Equals, ca.l)
	}
}

func (s *testSplitIndex) TestgetStepValue(c *C) {
	cases := []struct {
		lower []byte
		upper []byte
		l     int
		v     uint64
	}{
		{[]byte{}, []byte{}, 0, math.MaxUint64},
		{[]byte{0}, []byte{128}, 0, binary.BigEndian.Uint64([]byte{128, 255, 255, 255, 255, 255, 255, 255})},
		{[]byte{'a'}, []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255, 255, 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255 - 'b', 255 - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l := longestCommonPrefixLen(ca.lower, ca.upper)
		c.Assert(l, Equals, ca.l)
		v0 := getStepValue(ca.lower[l:], ca.upper[l:], 1)
		c.Assert(v0, Equals, ca.v)
	}
}

func (s *testSplitIndex) TestSplitIndex(c *C) {
	tbInfo := &model.TableInfo{
		Name: model.NewCIStr("t1"),
		ID:   rand.Int63(),
		Columns: []*model.ColumnInfo{
			{
				Name:         model.NewCIStr("c0"),
				ID:           1,
				Offset:       1,
				DefaultValue: 0,
				State:        model.StatePublic,
				FieldType:    *types.NewFieldType(mysql.TypeLong),
			},
		},
	}
	idxCols := []*model.IndexColumn{{Name: tbInfo.Columns[0].Name, Offset: 0, Length: types.UnspecifiedLength}}
	idxInfo := &model.IndexInfo{
		ID:      2,
		Name:    model.NewCIStr("idx1"),
		Table:   model.NewCIStr("t1"),
		Columns: idxCols,
		State:   model.StatePublic,
	}
	firstIdxInfo0 := idxInfo.Clone()
	firstIdxInfo0.ID = 1
	firstIdxInfo0.Name = model.NewCIStr("idx")
	tbInfo.Indices = []*model.IndexInfo{firstIdxInfo0, idxInfo}

	// Test for int index.
	// range is 0 ~ 100, and split into 10 region.
	// So 10 regions range is like below, left close right open interval:
	// region1: [-inf ~ 10)
	// region2: [10 ~ 20)
	// region3: [20 ~ 30)
	// region4: [30 ~ 40)
	// region5: [40 ~ 50)
	// region6: [50 ~ 60)
	// region7: [60 ~ 70)
	// region8: [70 ~ 80)
	// region9: [80 ~ 90)
	// region10: [90 ~ +inf)
	ctx := mock.NewContext()
	e := &SplitIndexRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		indexInfo:    idxInfo,
		lower:        []types.Datum{types.NewDatum(0)},
		upper:        []types.Datum{types.NewDatum(100)},
		num:          10,
	}
	valueList, err := e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases := []struct {
		value        int
		lessEqualIdx int
	}{
		{-1, 0},
		{0, 0},
		{1, 0},
		{10, 1},
		{11, 1},
		{20, 2},
		{21, 2},
		{31, 3},
		{41, 4},
		{51, 5},
		{61, 6},
		{71, 7},
		{81, 8},
		{91, 9},
		{100, 9},
		{1000, 9},
	}

	index := tables.NewIndex(tbInfo.ID, tbInfo, idxInfo)
	for _, ca := range cases {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, kv.IntHandle(math.MinInt64), nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, kv.IntHandle(math.MaxInt64), nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
	// Test for varchar index.
	// range is a ~ z, and split into 26 region.
	// So 26 regions range is like below:
	// region1: [-inf ~ b)
	// region2: [b ~ c)
	// .
	// .
	// .
	// region26: [y ~ +inf)
	e.lower = []types.Datum{types.NewDatum("a")}
	e.upper = []types.Datum{types.NewDatum("z")}
	e.num = 26
	// change index column type to varchar
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeVarchar)

	valueList, err = e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases2 := []struct {
		value        string
		lessEqualIdx int
	}{
		{"", 0},
		{"a", 0},
		{"abcde", 0},
		{"b", 1},
		{"bzzzz", 1},
		{"c", 2},
		{"czzzz", 2},
		{"z", 25},
		{"zabcd", 25},
	}

	for _, ca := range cases2 {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, kv.IntHandle(math.MinInt64), nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, kv.IntHandle(math.MaxInt64), nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}

	// Test for timestamp index.
	// range is 2010-01-01 00:00:00 ~ 2020-01-01 00:00:00, and split into 10 region.
	// So 10 regions range is like below:
	// region1: [-inf					~ 2011-01-01 00:00:00)
	// region2: [2011-01-01 00:00:00 	~ 2012-01-01 00:00:00)
	// .
	// .
	// .
	// region10: [2019-01-01 00:00:00 	~ +inf)
	lowerTime := types.NewTime(types.FromDate(2010, 1, 1, 0, 0, 0, 0), mysql.TypeTimestamp, types.DefaultFsp)
	upperTime := types.NewTime(types.FromDate(2020, 1, 1, 0, 0, 0, 0), mysql.TypeTimestamp, types.DefaultFsp)
	e.lower = []types.Datum{types.NewDatum(lowerTime)}
	e.upper = []types.Datum{types.NewDatum(upperTime)}
	e.num = 10

	// change index column type to timestamp
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeTimestamp)

	valueList, err = e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases3 := []struct {
		value        types.CoreTime
		lessEqualIdx int
	}{
		{types.FromDate(2009, 11, 20, 12, 50, 59, 0), 0},
		{types.FromDate(2010, 1, 1, 0, 0, 0, 0), 0},
		{types.FromDate(2011, 12, 31, 23, 59, 59, 0), 1},
		{types.FromDate(2011, 2, 1, 0, 0, 0, 0), 1},
		{types.FromDate(2012, 3, 1, 0, 0, 0, 0), 2},
		{types.FromDate(2013, 4, 1, 0, 0, 0, 0), 3},
		{types.FromDate(2014, 5, 1, 0, 0, 0, 0), 4},
		{types.FromDate(2015, 6, 1, 0, 0, 0, 0), 5},
		{types.FromDate(2016, 8, 1, 0, 0, 0, 0), 6},
		{types.FromDate(2017, 9, 1, 0, 0, 0, 0), 7},
		{types.FromDate(2018, 10, 1, 0, 0, 0, 0), 8},
		{types.FromDate(2019, 11, 1, 0, 0, 0, 0), 9},
		{types.FromDate(2020, 12, 1, 0, 0, 0, 0), 9},
		{types.FromDate(2030, 12, 1, 0, 0, 0, 0), 9},
	}

	for _, ca := range cases3 {
		value := types.NewTime(ca.value, mysql.TypeTimestamp, types.DefaultFsp)
		// test for min int64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, kv.IntHandle(math.MinInt64), nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, kv.IntHandle(math.MaxInt64), nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func (s *testSplitIndex) TestSplitTable(c *C) {
	tbInfo := &model.TableInfo{
		Name: model.NewCIStr("t1"),
		ID:   rand.Int63(),
		Columns: []*model.ColumnInfo{
			{
				Name:         model.NewCIStr("c0"),
				ID:           1,
				Offset:       1,
				DefaultValue: 0,
				State:        model.StatePublic,
				FieldType:    *types.NewFieldType(mysql.TypeLong),
			},
		},
	}
	defer func(originValue int64) {
		minRegionStepValue = originValue
	}(minRegionStepValue)
	minRegionStepValue = 10
	// range is 0 ~ 100, and split into 10 region.
	// So 10 regions range is like below:
	// region1: [-inf ~ 10)
	// region2: [10 ~ 20)
	// region3: [20 ~ 30)
	// region4: [30 ~ 40)
	// region5: [40 ~ 50)
	// region6: [50 ~ 60)
	// region7: [60 ~ 70)
	// region8: [70 ~ 80)
	// region9: [80 ~ 90 )
	// region10: [90 ~ +inf)
	ctx := mock.NewContext()
	e := &SplitTableRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		handleCols:   core.NewIntHandleCols(&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}),
		lower:        []types.Datum{types.NewDatum(0)},
		upper:        []types.Datum{types.NewDatum(100)},
		num:          10,
	}
	valueList, err := e.getSplitTableKeys()
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num-1)

	cases := []struct {
		value        int
		lessEqualIdx int
	}{
		{-1, -1},
		{0, -1},
		{1, -1},
		{10, 0},
		{11, 0},
		{20, 1},
		{21, 1},
		{31, 2},
		{41, 3},
		{51, 4},
		{61, 5},
		{71, 6},
		{81, 7},
		{91, 8},
		{100, 8},
		{1000, 8},
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	for _, ca := range cases {
		// test for minInt64 handle
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(ca.value))
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, key)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func (s *testSplitIndex) TestClusterIndexSplitTable(c *C) {
	tbInfo := &model.TableInfo{
		Name:                model.NewCIStr("t"),
		ID:                  1,
		IsCommonHandle:      true,
		CommonHandleVersion: 1,
		Indices: []*model.IndexInfo{
			{
				ID:      1,
				Primary: true,
				State:   model.StatePublic,
				Columns: []*model.IndexColumn{
					{Offset: 1},
					{Offset: 2},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{
				Name:      model.NewCIStr("c0"),
				ID:        1,
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeDouble),
			},
			{
				Name:      model.NewCIStr("c1"),
				ID:        2,
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			{
				Name:      model.NewCIStr("c2"),
				ID:        3,
				Offset:    2,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
		},
	}
	defer func(originValue int64) {
		minRegionStepValue = originValue
	}(minRegionStepValue)
	minRegionStepValue = 3
	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	e := &SplitTableRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		handleCols:   buildHandleColsForSplit(sc, tbInfo),
		lower:        types.MakeDatums(1, 0),
		upper:        types.MakeDatums(1, 100),
		num:          10,
	}
	valueList, err := e.getSplitTableKeys()
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num-1)

	cases := []struct {
		value        []types.Datum
		lessEqualIdx int
	}{
		// For lower-bound and upper-bound, because 0 and 100 are padding with 7 zeros,
		// the split points are not (i * 10) but approximation.
		{types.MakeDatums(1, -1), -1},
		{types.MakeDatums(1, 0), -1},
		{types.MakeDatums(1, 10), -1},
		{types.MakeDatums(1, 11), 0},
		{types.MakeDatums(1, 20), 0},
		{types.MakeDatums(1, 21), 1},

		{types.MakeDatums(1, 31), 2},
		{types.MakeDatums(1, 41), 3},
		{types.MakeDatums(1, 51), 4},
		{types.MakeDatums(1, 61), 5},
		{types.MakeDatums(1, 71), 6},
		{types.MakeDatums(1, 81), 7},
		{types.MakeDatums(1, 91), 8},
		{types.MakeDatums(1, 100), 8},
		{types.MakeDatums(1, 101), 8},
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	for _, ca := range cases {
		h, err := e.handleCols.BuildHandleByDatums(ca.value)
		c.Assert(err, IsNil)
		key := tablecodec.EncodeRecordKey(recordPrefix, h)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, key)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func searchLessEqualIdx(valueList [][]byte, value []byte) int {
	idx := -1
	for i, v := range valueList {
		if bytes.Compare(value, v) >= 0 {
			idx = i
			continue
		}
		break
	}
	return idx
}

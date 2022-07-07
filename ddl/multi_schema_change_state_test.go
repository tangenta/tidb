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

package ddl_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type currentColumn struct {
	name      string
	tp        string
	valInit   string
	valInsert string
	valUpdate string
	descTp    string
	invisible bool
}

type currentTable struct {
	columns []*currentColumn
	indexes string
	rn      *rand.Rand
}

type modification struct {
	alterSpec string
	updateCol func(*currentColumn)
}

func descTp(newDescTp string) func(*currentColumn) {
	return func(column *currentColumn) {
		column.descTp = newDescTp
	}
}

func descTpVal(newDescTp string, insert, update string) func(*currentColumn) {
	return func(column *currentColumn) {
		column.descTp = newDescTp
		column.valInsert = insert
		column.valUpdate = update
	}
}

type modifications map[string][]modification

func TestMultiSchemaChangeState(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	seed := time.Now().UnixNano()
	logutil.BgLogger().Warn("[test] TestMultiSchemaChangeState", zap.Int64("current seed", seed))

	tbl := currentTable{
		columns: []*currentColumn{
			{"c_1", "int", "100", "101", "102", "int(11)", false},
			{"c_2", "char(20)", "'123'", "'12'", "'1'", "char(20)", false},
			{"c_pos_1", "int", "100", "101", "102", "int(11)", false},
			{"c_idx_visible", "int", "100", "101", "102", "int(11)", false},
			{"c_3", "decimal(5, 3)", "1.111", "2.1", "3.1", "decimal(5,3)", false},
			{"c_drop_1", "time", "'08:00:00'", "'09:00:00'", "'10:00:00'", "time", false},
			{"c_4", "datetime", "'2020-01-01 08:00:00'", "'2020-01-01 09:00:00'", "'2020-01-01 10:00:00'", "datetime", false},
			{"c_drop_idx", "char(10)", "'qwer'", "'wer'", "'er'", "char(10)", false},
			{"c_5", "time", "'08:00:00'", "'09:00:00'", "'10:00:00'", "time", false},
			{"c_6", "double", "3.123", "4.123", "5.123", "double", false},
			{"c_drop_2", "int", "1", "2", "3", "int(11)", false},
			{"c_pos_2", "char(10)", "'fdsa'", "'asdf'", "'dddd'", "char(10)", false},
			{"c_add_1", "", "", "", "", "", true},
			{"c_add_2", "", "", "", "", "", true},
			{"c_add_idx_1", "int", "100", "101", "102", "int(11)", false},
			{"c_add_idx_2", "char(20)", "'zxcv'", "'zxcvb'", "'zxc'", "char(20)", false},
		},
		indexes: "index idx_1(c_1), index idx_2(c_2), index idx_drop(c_drop_idx), index idx_3(c_drop_1), " +
			"index idx_4(c_4), index idx_5(c_pos_1, c_pos_2), index idx_visible(c_idx_visible)",
		rn: rand.New(rand.NewSource(seed)),
	}

	modifications := modifications{
		"c_1": {
			{"modify column c_1 bigint", descTp("bigint(20)")},
			{"modify column c_1 tinyint", descTp("tinyint(4)")},
			{"modify column c_1 char(255)", descTp("char(255)")},
			{"alter column c_1 set default 100", descTp("int(11)")},
			{"modify column c_1 tinyint first", descTp("tinyint(4)")},
			{"modify column c_1 int after c_pos_1", descTp("int(11)")},
			{"modify column c_1 int after c_pos_2", descTp("int(11)")},
		},
		"c_2": {
			{"modify column c_2 char(100)", descTp("char(100)")},
			{"modify column c_2 char(5)", descTp("char(5)")},
			{"alter column c_2 set default 'char_def'", descTp("char(20)")},
			{"modify column c_2 tinyint first", descTp("tinyint(4)")},
			{"modify column c_2 int after c_pos_1", descTp("int(11)")},
			{"modify column c_2 int after c_pos_2", descTp("int(11)")},
		},
		"c_3": {
			{"modify column c_3 decimal(10, 2)", descTp("decimal(10,2)")},
			{"modify column c_3 decimal(10, 0)", descTp("decimal(10,0)")},
			{"modify column c_3 varchar(255)", descTp("varchar(255)")},
			{"alter column c_3 set default 1.1", descTp("decimal(5,3)")},
		},
		"c_drop_idx": {
			{"drop index idx_drop", descTp("char(10)")},
			{"drop column c_drop_idx", descTp("shouldn't exist")},
		},
		"c_4": {
			{"modify column c_4 datetime first", descTp("datetime")},
			{"modify column c_4 varchar(255) after c_pos_2", descTp("varchar(255)")},
		},
		"c_5": {
			{"modify column c_5 varchar(255) first", descTpVal("varchar(255)", "'c_5_insert'", "'c_5_update'")},
		},
		"c_6": {
			{"modify column c_6 int", descTpVal("int(11)", "1", "2")},
		},
		"c_drop_1": {
			{"drop column c_drop_1", nil},
		},
		"c_drop_2": {
			{"drop column c_drop_2", nil},
		},
		"c_add_1": {
			{"add column c_add_1 bigint", descTpVal("bigint(20)", "10000", "10001")},
			{"add column c_add_1 char(20)", descTpVal("char(20)", "'c_add_1'", "'c_add_1_1'")},
			{"add column c_add_1 char(20) after c_pos_1", descTpVal("char(20)", "'c_add_1_2'", "'c_add_1_3'")},
			{"add column c_add_1 char(20) first", descTpVal("char(20)", "'c_add_1_4'", "'c_add_1_5'")},
			{"add column c_add_1 int as (c_pos_1 + 1)", descTpVal("int(11)", "10002", "10003")},
		},
		"c_add_2": {
			{"add column c_add_2 bigint", descTpVal("bigint(20)", "10000", "10001")},
			{"add column c_add_2 char(20)", descTpVal("char(20)", "'c_add_2'", "'c_add_2_1'")},
			{"add column c_add_2 char(20) after c_pos_1", descTpVal("char(20)", "'c_add_2_2'", "'c_add_2_3'")},
			{"add column c_add_2 char(20) first", descTpVal("char(20)", "'c_add_2_4'", "'c_add_2_5'")},
			{"add column c_add_2 int as (c_pos_1 + 1)", descTpVal("int(11)", "10002", "10003")},
		},
		"c_add_idx_1": {
			{"add index i_add_1(c_add_idx_1)", nil},
			{"add index i_add_1((c_add_idx_1 + 1))", nil},
			{"add primary key(c_add_idx_1)", nil},
		},
		"c_add_idx_2": {
			{"add index i_add_2(c_add_idx_2)", nil},
			{"add index i_add_2(c_add_idx_1, c_add_idx_2(2))", nil},
		},
		"c_idx_visible": {
			{"alter index idx_visible invisible", nil},
		},
	}

	tk.MustExec(tbl.createTable())
	tk.MustExec(tbl.insertInitialData())

	multiSchemaChangeSQL, applyChange := tbl.alterTable(modifications)

	var lastStates subStates
	var checkErr error
	changeNoticeable := false
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(&ddl.TestDDLCallback{Do: dom, OnJobUpdatedExported: func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if newStates, updated := updateSubSchemaStates(job, lastStates); updated {
			lastStates = newStates
		} else {
			return
		}
		if !changeNoticeable && !job.MultiSchemaInfo.Revertible {
			changeNoticeable = true
			applyChange()
		}
		colNames := getColNamesFromDescribe(t, tk1, true)
		insertSQL := tbl.insert(colNames)
		_, checkErr = tk1.Exec(insertSQL)
		assert.NoError(t, checkErr)
		if checkErr != nil {
			return
		}
		updateSQL := tbl.update(colNames)
		_, checkErr = tk1.Exec(updateSQL)
		assert.NoError(t, checkErr)
		if checkErr != nil {
			return
		}
		deleteSQL := tbl.delete(colNames)
		_, checkErr = tk1.Exec(deleteSQL)
		assert.NoError(t, checkErr)
		if checkErr != nil {
			return
		}
		checkErr = assertHasOnlyOneRow(tk1)
		assert.NoError(t, checkErr)
	}})
	tk.MustExec(multiSchemaChangeSQL)
	require.NoError(t, checkErr)
	dom.DDL().SetHook(originHook)

	tk.MustExec("admin check table t;")
	newColNames := getColNamesFromDescribe(t, tk, false)
	newColTps := getColDescTpsFromDescribe(t, tk)
	require.Equal(t, len(newColNames), len(newColTps))
	for i := range newColNames {
		require.Equal(t, newColTps[i], tbl.findColumn(newColNames[i]).descTp)
	}
	require.NoError(t, assertHasOnlyOneRow(tk))
}

func (t *currentTable) createTable() string {
	var colDefs []string
	for _, c := range t.columns {
		if !c.invisible {
			colDefs = append(colDefs, fmt.Sprintf("%s %s", c.name, c.tp))
		}
	}
	return fmt.Sprintf("create table t (%s, %s);", strings.Join(colDefs, ", "), t.indexes)
}

func (t *currentTable) insertInitialData() string {
	var colVals []string
	for _, c := range t.columns {
		if !c.invisible {
			colVals = append(colVals, c.valInit)
		}
	}
	return fmt.Sprintf("insert into t values (%s);", strings.Join(colVals, ", "))
}

func (t *currentTable) alterTable(possibleMods modifications) (string, func()) {
	var specs []string
	cols := make([]*currentColumn, 0, len(t.columns))
	fns := make([]func(*currentColumn), 0, len(t.columns))
	for _, c := range t.columns {
		m, ok := possibleMods[c.name]
		if !ok {
			continue
		}
		spec := m[t.rn.Intn(len(m))]
		cols = append(cols, c)
		fns = append(fns, spec.updateCol)
		specs = append(specs, spec.alterSpec)
	}
	t.rn.Shuffle(len(specs), func(i, j int) {
		specs[i], specs[j] = specs[j], specs[i]
	})
	return fmt.Sprintf("alter table t %s;", strings.Join(specs, ", ")), func() {
		for i, c := range cols {
			if fns[i] != nil {
				fns[i](c)
			}
		}
	}
}

func (t *currentTable) insert(colNames []string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("insert into t (%s) values (", strings.Join(colNames, ", ")))
	for i, c := range colNames {
		sb.WriteString(t.findColumn(c).valInsert)
		if i != len(colNames)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(");")
	return sb.String()
}

func (t *currentTable) update(colNames []string) string {
	var sb strings.Builder
	sb.WriteString("update t set ")
	for i, c := range colNames {
		sb.WriteString(c)
		sb.WriteString(" = ")
		sb.WriteString(t.findColumn(c).valUpdate)
		if i != len(colNames)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(" where ")
	whereCol := colNames[t.rn.Intn(len(colNames))]
	sb.WriteString(whereCol)
	sb.WriteString(" = ")
	sb.WriteString(t.findColumn(whereCol).valInsert)
	sb.WriteString(";")
	return sb.String()
}

func (t *currentTable) delete(colNames []string) string {
	var sb strings.Builder
	sb.WriteString("delete from t where ")
	whereCol := colNames[t.rn.Intn(len(colNames))]
	sb.WriteString(whereCol)
	sb.WriteString(" = ")
	sb.WriteString(t.findColumn(whereCol).valUpdate)
	sb.WriteString(";")
	return sb.String()
}

func (t *currentTable) findColumn(name string) *currentColumn {
	for _, c := range t.columns {
		if c.name == name {
			return c
		}
	}
	return nil
}

func updateSubSchemaStates(job *model.Job, lastSchemaState subStates) (subStates, bool) {
	if job.MultiSchemaInfo == nil {
		return lastSchemaState, false
	}
	if len(lastSchemaState) != len(job.MultiSchemaInfo.SubJobs) {
		lastSchemaState = make(subStates, len(job.MultiSchemaInfo.SubJobs))
	}
	updated := false
	for i, subJob := range job.MultiSchemaInfo.SubJobs {
		if subJob.SchemaState != lastSchemaState[i] {
			lastSchemaState[i] = subJob.SchemaState
			updated = true
		}
	}
	return lastSchemaState, updated
}

func getColNamesFromDescribe(t *testing.T, tk *testkit.TestKit, filtering bool) []string {
	rs, err := tk.Exec("describe t;")
	require.NoError(t, err)
	rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	colNames := make([]string, 0, len(rows))
	for _, row := range rows {
		if filtering && strings.Contains(strings.ToLower(row[5]), "generated") {
			continue
		}
		colNames = append(colNames, row[0])
	}
	return colNames
}

func getColDescTpsFromDescribe(t *testing.T, tk *testkit.TestKit) []string {
	rs, err := tk.Exec("describe t;")
	require.NoError(t, err)
	rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	tps := make([]string, 0, len(rows))
	for _, row := range rows {
		tps = append(tps, row[1])
	}
	return tps
}

func assertHasOnlyOneRow(tk *testkit.TestKit) error {
	rs, err := tk.Exec("select * from t;")
	if err != nil {
		return err
	}
	rows, err := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("expected 1 row, got %v", rows)
	}
	return nil
}

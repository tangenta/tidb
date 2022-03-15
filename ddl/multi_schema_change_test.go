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
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/admin"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add multiple columns in multiple specs.
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column b int default 2, add column c int default 3;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	// Test add multiple columns in one spec.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add column (d int default 4, e int default 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4 5"))

	// Test referencing previous column in multi-schema change is not supported.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t add column b int after a, add column c int after b", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int after b, add column b int", errno.ErrBadField)

	// Test add multiple columns with different position.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec(`alter table t
        add column d int default 4 first,
        add column e int default 5 after b,
        add column f int default 6 after b;`)
	tk.MustQuery("select * from t;").Check(testkit.Rows("4 1 2 6 5 3"))

	// Test [if not exists] for adding columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column b int default 2, add column if not exists a int;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1060 Duplicate column name 'a'"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))

	// Test add columns with same name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t add column b int default 2, add column b int default 3", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddColumnsCancelled(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	var checkErr error
	var once bool
	hook := func(job *model.Job) {
		if once || job.MultiSchemaInfo.SubJobs[1].SchemaState != model.StateWriteReorganization {
			return
		}
		once = true
		checkErr = kv.RunInNewTxn(context.Background(), store, false,
			func(ctx context.Context, txn kv.Transaction) error {
				errs, err := admin.CancelJobs(txn, []int64{job.ID})
				if errs[0] != nil {
					return errs[0]
				}
				return err
			})
	}
	dom.DDL().SetHook(&ddl.TestDDLCallback{Do: dom, OnJobUpdatedExported: hook})
	require.NoError(t, checkErr)
	sql := "alter table t add column b int default 2, add column c int default 3, add column d int default 4;"
	tk.MustGetErrCode(sql, errno.ErrCancelledDDLJob)
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func TestMultiSchemaChangeDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// Test drop all columns
	tk.MustExec("create table t (a int, b int);")
	tk.MustGetErrCode("alter table t drop column a, drop column b;", errno.ErrCantRemoveAllFields)

	// Test drop multiple columns in multiple specs
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, d int, e int);")
	tk.MustExec("insert into t values (1, 2, 3, 4, 5);")
	tk.MustExec("alter table t drop column a, drop column d, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 5"))

	// Test drop same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t drop column a, drop column a", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// [a, b] -> [+c, -a, +d, -b] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column c int default 3, drop column a, add column d int default 4, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	// [a, b] -> [-a, -b, +c, +d] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop column a, drop column b, add column c int default 3, add column d int default 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	// [a, b] -> [+c after a, +d first, -a, -b] -> [d, c]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	// Note that MariaDB does not support this: Unknown column 'a' in 't'.
	// Since TiDB's implementation is snapshot + reasonable cascading, this is supported.
	tk.MustExec("alter table t add column c int default 3 after a, add column d int default 4 first, drop column a, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("4 3"))
}

func TestMultiSchemaRenameColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add and rename to same column name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t rename column b to c, add column c int", errno.ErrUnsupportedDDLOperation)

	// Test drop and rename with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t drop column b, rename column b to c", errno.ErrUnsupportedDDLOperation)

	// Test add index and rename with same column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2, index t(a, b));")
	tk.MustExec("insert into t values ();")
	tk.MustGetErrCode("alter table t rename column b to c, add index t1(a, b)", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeAddIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add multiple indexes with same column.
	/*
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int, c int)")
		tk.MustExec("alter table t add index t(a, b), add index t1(a)")
		tk.MustExec("alter table t add index t2(a), add index t3(a, b)")
	*/

	// Test add multiple indexes with same name.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), add index t(b)", errno.ErrUnsupportedDDLOperation)

	// Test add indexes with drop column.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustGetErrCode("alter table t add index t(a), drop column a", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add index t(a, b), drop column a", errno.ErrUnsupportedDDLOperation)
}

func TestMultiSchemaChangeDropIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test drop same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t drop index t, drop index t", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, c1 int, c2 int, primary key(id), key i1(c1), key i2(c2));")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t drop index i1, drop index i2;")
	tk.MustGetErrCode("select * from t use index(i1);", errno.ErrKeyDoesNotExist)
	tk.MustGetErrCode("select * from t use index(i2);", errno.ErrKeyDoesNotExist)

	// Test drop index with drop column.
	/*
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int default 1, b int default 2, c int default 3, index t(a))")
		tk.MustExec("insert into t values ();")
		tk.MustExec("alter table t drop index t, drop column a")
		tk.MustGetErrCode("select * from t force index(t)", errno.ErrKeyDoesNotExist)
	*/
}

func TestMultiSchemaChangeAddDropIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add and drop same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t drop index t, add index t(b)", errno.ErrDupKeyName)

	// Test add and drop same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t add index t1(b), drop index t1", errno.ErrCantDropFieldOrKey)
}

func TestMultiSchemaRenameIndexes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test drop and rename same index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t drop index t, rename index t to t1", errno.ErrUnsupportedDDLOperation)

	// Test add and rename to same index name.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index t(a))")
	tk.MustGetErrCode("alter table t add index t1(b), rename index t to t1", errno.ErrUnsupportedDDLOperation)
}

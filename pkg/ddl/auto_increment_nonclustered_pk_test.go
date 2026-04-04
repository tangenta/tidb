// Copyright 2026 PingCAP, Inc.
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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("insert into t values (10), (20), (30)")

	tk.MustExec("alter table t " +
		"add column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered")

	// Existing rows should be backfilled with distinct, non-null ids.
	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("3 3 0"))

	// AUTO_INCREMENT should keep working after the DDL.
	tk.MustExec("insert into t(v) values (40)")
	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("4 4 0"))

	// PK should be nonclustered and the new column should be AUTO_INCREMENT.
	tk.MustQuery("show create table t").CheckContain("AUTO_INCREMENT")
	tk.MustQuery("show create table t").CheckContain("NONCLUSTERED")
}

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyDMLDuringReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("insert into t values (10), (20), (30)")

	// Insert a few rows after the auto_increment column is in WriteReorganization state.
	// These rows won't be covered by the backfill snapshot and must be filled by DML itself.
	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onAddColumnStateWriteReorg", func() {
		once.Do(func() {
			tk.MustExec("insert into t(v) values (40), (50)")
		})
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("alter table t " +
			"add column id bigint not null auto_increment, " +
			"add primary key (id) nonclustered")
	}()
	wg.Wait()

	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("5 5 0"))
	tk.MustQuery("select count(distinct id), sum(id=0), sum(id is null) from t where v in (40, 50)").
		Check(testkit.Rows("2 0 0"))
}

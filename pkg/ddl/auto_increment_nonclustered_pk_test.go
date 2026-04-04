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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
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

func TestAlterTableModifyNonclusteredAutoIncrementPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id bigint not null, v int)")
	tk.MustExec("insert into t values (10, 10), (20, 20), (30, 30)")

	tk.MustExec("alter table t " +
		"modify column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered")

	// Existing rows keep their ids.
	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("3 3 0"))
	tk.MustQuery("select sum(id in (10, 20, 30)) from t").Check(testkit.Rows("3"))

	// AUTO_INCREMENT should start after max(id).
	tk.MustExec("insert into t(v) values (40)")
	tk.MustQuery("select id > 30 from t where v=40").Check(testkit.Rows("1"))

	// PK should be nonclustered and the column should be AUTO_INCREMENT.
	tk.MustQuery("show create table t").CheckContain("AUTO_INCREMENT")
	tk.MustQuery("show create table t").CheckContain("NONCLUSTERED")
}

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyPartitionedGlobal(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set tidb_enable_global_index=true")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int) partition by hash(v) partitions 4")
	tk.MustExec("insert into t values (10), (20), (30), (40), (50), (60)")

	tk.MustExec("alter table t " +
		"add column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered global")

	tk.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id=0) from t").
		Check(testkit.Rows("6 6 0 0"))

	// AUTO_INCREMENT should keep working after the DDL across partitions.
	tk.MustExec("insert into t(v) values (70), (80), (90)")
	tk.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id=0) from t").
		Check(testkit.Rows("9 9 0 0"))

	tk.MustQuery("show create table t").CheckContain("GLOBAL")
	tk.MustQuery("show create table t").CheckContain("NONCLUSTERED")
	tk.MustQuery("show create table t").CheckContain("AUTO_INCREMENT")
}

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyAutoIDCache1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int) AUTO_ID_CACHE=1")
	tk.MustExec("insert into t values (10), (20), (30)")

	tk.MustExec("alter table t " +
		"add column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered")

	// Existing rows should be backfilled with distinct, non-null, non-zero ids.
	tk.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id=0) from t").
		Check(testkit.Rows("3 3 0 0"))

	// The table should expose a dedicated AUTO_INCREMENT allocator when AUTO_ID_CACHE=1.
	tk.MustQuery("show table t next_row_id").CheckContain("AUTO_INCREMENT")

	// Post-DDL insert should allocate new AUTO_INCREMENT values.
	tk.MustExec("insert into t(v) values (40)")
	tk.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id=0) from t").
		Check(testkit.Rows("4 4 0 0"))
}

func TestAlterTableModifyNonclusteredAutoIncrementPrimaryKeyAutoIDCache1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id bigint not null, v int) AUTO_ID_CACHE=1")
	tk.MustExec("insert into t values (10, 10), (20, 20), (30, 30)")

	tk.MustExec("alter table t " +
		"modify column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered")

	// Existing rows keep their ids.
	tk.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id=0) from t").
		Check(testkit.Rows("3 3 0 0"))
	tk.MustQuery("select sum(id in (10, 20, 30)) from t").Check(testkit.Rows("3"))

	// AUTO_INCREMENT should start after max(id).
	tk.MustExec("insert into t(v) values (40)")
	tk.MustQuery("select id > 30 from t where v=40").Check(testkit.Rows("1"))

	tk.MustQuery("show table t next_row_id").CheckContain("AUTO_INCREMENT")
}

func TestCancelAlterTableAddNonclusteredAutoIncrementPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tkCancel := testkit.NewTestKit(t, store)
	tkCancel.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 8")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	// Make the DDL take long enough for cancel to reliably kick in.
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}

	// Slow down the backfill workers so the cancel request has time to land.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockBackfillSlow", "return")

	var cancelled atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *model.Job) {
		if cancelled.Load() {
			return
		}
		if job.Type != model.ActionMultiSchemaChange || job.MultiSchemaInfo == nil {
			return
		}
		if !strings.Contains(job.Query, "add column id") {
			return
		}
		// Cancel the job while the AddColumn backfill is running.
		foundRunningBackfill := false
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			if sub == nil {
				continue
			}
			if sub.Type != model.ActionAddColumn {
				continue
			}
			if sub.SchemaState != model.StateWriteReorganization {
				continue
			}
			if sub.ReorgStage != model.ReorgStageAddAutoIncrementColumnBackfill {
				continue
			}
			foundRunningBackfill = true
			break
		}
		if !foundRunningBackfill {
			return
		}

		rs := tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
		if len(rs.Rows()) > 0 && strings.Contains(rs.Rows()[0][1].(string), "success") {
			cancelled.Store(true)
		}
	})

	tk.MustGetErrCode("alter table t "+
		"add column id bigint not null auto_increment, "+
		"add primary key (id) nonclustered", errno.ErrCancelledDDLJob)

	// DDL should rollback cleanly: the new column and PK should not be visible.
	tk.MustQuery("show create table t").CheckNotContain("`id`")
	tk.MustQuery("show create table t").CheckNotContain("PRIMARY KEY")
}

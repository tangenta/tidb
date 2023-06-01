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

package ingest_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func injectMockBackendMgr(t *testing.T, store kv.Storage) (restore func()) {
	tk := testkit.NewTestKit(t, store)
	oldLitBackendMgr := ingest.LitBackCtxMgr
	oldInitialized := ingest.LitInitialized

	ingest.LitBackCtxMgr = ingest.NewMockBackendCtxMgr(func() sessionctx.Context {
		tk.MustExec("rollback;")
		tk.MustExec("begin;")
		return tk.Session()
	})
	ingest.LitInitialized = true

	return func() {
		ingest.LitBackCtxMgr = oldLitBackendMgr
		ingest.LitInitialized = oldInitialized
	}
}

func TestAddIndexIngestGeneratedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	assertLastNDDLUseIngest := func(n int) {
		tk.MustExec("admin check table t;")
		rows := tk.MustQuery(fmt.Sprintf("admin show ddl jobs %d;", n)).Rows()
		require.Len(t, rows, n)
		for i := 0; i < n; i++ {
			//nolint: forcetypeassert
			jobTp := rows[i][3].(string)
			require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
		}
	}
	tk.MustExec("create table t (a int, b int, c int as (b+10), d int as (b+c), primary key (a) clustered);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(c, a);")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("alter table t add index idx3(d);")
	tk.MustExec("alter table t add index idx4(d, c);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 11 12", "2 2 12 14", "3 3 13 16"))
	assertLastNDDLUseIngest(5)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c char(10) as (concat(b, 'x')), d int, e char(20) as (c));")
	tk.MustExec("insert into t (a, b, d) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(a, c);")
	tk.MustExec("alter table t add index idx2(c(7));")
	tk.MustExec("alter table t add index idx3(e(5));")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1x 1 1x", "2 2 2x 2 2x", "3 3 3x 3 3x"))
	assertLastNDDLUseIngest(4)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c tinyint, d int as (a + c), e bigint as (d - a), primary key(b, a) clustered);")
	tk.MustExec("insert into t (a, b, c) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(d);")
	tk.MustExec("alter table t add index idx1(b(2), d);")
	tk.MustExec("alter table t add index idx2(d, c);")
	tk.MustExec("alter table t add index idx3(e);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1 2 1", "2 2 2 4 2", "3 3 3 6 3"))
	assertLastNDDLUseIngest(4)
}

func TestIngestError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 4; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockCopSenderError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockCopSenderError"))
	tk.MustExec("admin check table t;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp := rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := 0; i < 4; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockLocalWriterError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockLocalWriterError"))
	tk.MustExec("admin check table t;")
	rows = tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp = rows[0][3].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestAddIndexIngestPanic(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	// Mock panic on coprocessor request sender.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockCopSenderPanic", "return(true)"))
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
	tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockCopSenderPanic"))

	// Mock panic on local engine writer.
	tk.MustExec("drop table t;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockLocalWriterPanic", "return"))
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
	tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockLocalWriterPanic"))
}

func TestAddIndexIngestCancel(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	defHook := dom.DDL().GetHook()
	customHook := newTestCallBack(t, dom)
	cancelled := false
	customHook.OnJobRunBeforeExported = func(job *model.Job) {
		if cancelled {
			return
		}
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			idx := testutil.FindIdxInfo(dom, "test", "t", "idx")
			if idx == nil {
				return
			}
			if idx.BackfillState == model.BackfillStateRunning {
				tk2 := testkit.NewTestKit(t, store)
				rs, err := tk2.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
				assert.NoError(t, err)
				assert.NoError(t, rs.Close())
				cancelled = true
			}
		}
	}
	dom.DDL().SetHook(customHook)
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrCancelledDDLJob)
	require.True(t, cancelled)
	dom.DDL().SetHook(defHook)
	ok, err := ingest.LitBackCtxMgr.CheckAvailable()
	require.NoError(t, err)
	require.True(t, ok)
}

type testCallback struct {
	ddl.Callback
	OnJobRunBeforeExported func(job *model.Job)
}

func newTestCallBack(t *testing.T, dom *domain.Domain) *testCallback {
	defHookFactory, err := ddl.GetCustomizedHook("default_hook")
	require.NoError(t, err)
	return &testCallback{
		Callback: defHookFactory(dom),
	}
}

func (c *testCallback) OnJobRunBefore(job *model.Job) {
	if c.OnJobRunBeforeExported != nil {
		c.OnJobRunBeforeExported(job)
	}
}

func TestIngestPartitionRowCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	tk.MustExec(`create table t (a int, b int, c int as (b+10), d int as (b+c),
		primary key (a) clustered) partition by range (a) (
    		partition p0 values less than (1),
    		partition p1 values less than (2),
    		partition p2 values less than MAXVALUE);`)
	tk.MustExec("insert into t (a, b) values (0, 0), (1, 1), (2, 2);")
	tk.MustExec("alter table t add index idx(d);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	//nolint: forcetypeassert
	rowCount := rows[0][7].(string)
	require.Equal(t, "3", rowCount)
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestClientError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer injectMockBackendMgr(t, store)()

	tk.MustExec("CREATE TABLE t1 (f1 json);")
	tk.MustExec(`insert into t1(f1) values (cast("null" as json));`)
	tk.MustGetErrCode("create index i1 on t1((cast(f1 as unsigned array)));", errno.ErrInvalidJSONValueForFuncIndex)
}

func TestAddIndexIngestRecoverPartition(t *testing.T) {
	store, dom1, dom2 := testkit.CreateMockStoreAnd2Domain(t)
	defer injectMockBackendMgr(t, store)()
	originHook := dom1.DDL().GetHook()
	defer dom1.DDL().SetHook(originHook)
	originHook2 := dom2.DDL().GetHook()
	defer dom2.DDL().SetHook(originHook2)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 4;")
	tk.MustExec("insert into t values (2, 3), (3, 3), (5, 5);")
	tk2 := testkit.NewTestKit(t, store)

	err := dom2.DDL().OwnerManager().ResignOwner(context.Background())
	require.NoError(t, err)
	count := 2 // The second partition.
	fn := func(job *model.Job) {
		if !t.Failed() && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			sql := fmt.Sprintf("select start_key, end_key, physical_id, reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
			rs := tk2.MustQuery(sql).Rows()
			if len(rs) == 0 {
				return
			}
			startStr, endStr := rs[0][0].(string), rs[0][1].(string)
			start, end := hex.EncodeToString([]byte(startStr)), hex.EncodeToString([]byte(endStr))
			pid := rs[0][2].(string)
			rgMetaStr := rs[0][3].(string)
			var reorgMeta ingest.JobReorgMeta
			err = json.Unmarshal([]byte(rgMetaStr), &reorgMeta)
			assert.NoError(t, err)
			cp := reorgMeta.Checkpoint
			logutil.BgLogger().Info("reorg meta",
				zap.String("start", start), zap.String("end", end), zap.String("pid", pid),
				zap.String("cpStart", hex.EncodeToString(cp.StartKey)),
				zap.String("cpEnd", hex.EncodeToString(cp.EndKey)), zap.Int64("cpPid", cp.PhysicalID))
			count--
			if count == 0 {
				// During adding index, the TiDB owner is changed.
				dom1.DDL().OwnerManager().Cancel()
			}
		}
	}
	hook := &callback.TestDDLCallback{Do: dom1}
	hook.OnJobUpdatedExported.Store(&fn)
	dom1.DDL().SetHook(hook)
	dom2.DDL().SetHook(hook)
	tk.MustExec("alter table t add index idx(b);")
}

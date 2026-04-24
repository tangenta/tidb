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

package ddltest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

type resignHookResult struct {
	dmlErr    error
	resignErr error
}

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKeyConcurrentDMLAcrossOwnerTransfer(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t, realtikvtest.WithAllocPort(true))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 4")

	tkCheck := testkit.NewTestKit(t, store)
	tkCheck.MustExec("use test")
	tkDML := testkit.NewTestKit(t, store)
	tkDML.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	for i := range 400 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustQuery("split table t between (0) and (4000) regions 4").Check(testkit.Rows("3 1"))

	var (
		beforeIngestTriggered atomic.Bool
		beforeIngestOnce      sync.Once
	)
	hookResultCh := make(chan resignHookResult, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", func() {
		beforeIngestOnce.Do(func() {
			beforeIngestTriggered.Store(true)
			hookResultCh <- resignHookResult{
				dmlErr:    tkDML.ExecToErr("insert into t(v) values (1001), (1002), (1003)"),
				resignErr: dom.DDL().OwnerManager().ResignOwner(context.Background()),
			}
		})
	})

	require.NoError(t, tk.ExecToErr("alter table t add column id bigint not null auto_increment, add primary key (id) nonclustered"))
	require.True(t, beforeIngestTriggered.Load())
	hookResult := <-hookResultCh
	require.NoError(t, hookResult.dmlErr)
	require.NoError(t, hookResult.resignErr)

	tkCheck.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id = 0) from t").
		Check(testkit.Rows("403 403 0 0"))
	tkCheck.MustQuery("select count(*), count(distinct id), sum(id is null), sum(id = 0) from t where v in (1001, 1002, 1003)").
		Check(testkit.Rows("3 3 0 0"))
	tkCheck.MustExec("admin check table t")
}

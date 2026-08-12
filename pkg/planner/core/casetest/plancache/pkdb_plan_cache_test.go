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

package plancache

import (
	"context"
	"fmt"
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPKDBInvalidZeroTemporalParamBypassesInstancePlanCache verifies that
// invalid-zero executions bypass the shared cache without disturbing valid plans.
func TestPKDBInvalidZeroTemporalParamBypassesInstancePlanCache(t *testing.T) {
	ctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyEnableInstancePlanCache{}, true)
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExecWithContext(ctx, "use test")
	tk1.MustExecWithContext(ctx, "set @@sql_mode = ''")
	tk1.MustExecWithContext(ctx, "create table t_time_instance_cache (id int primary key, tm time unique)")
	tk1.MustExecWithContext(ctx, "insert into t_time_instance_cache values (1, '19:10:06')")
	tk1.MustExecWithContext(ctx, "prepare time_comparison from 'select id from t_time_instance_cache where tm = ?'")
	tk1.MustExecWithContext(ctx, "set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk1.MustQueryWithContext(ctx, "execute time_comparison using @p").Check(testkit.Rows("1"))
	require.False(t, tk1.Session().GetSessionVars().FoundInPlanCache)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExecWithContext(ctx, "use test")
	tk2.MustExecWithContext(ctx, "set @@sql_mode = ''")
	tk2.MustExecWithContext(ctx, "prepare time_comparison from 'select id from t_time_instance_cache where tm = ?'")
	tk2.MustExecWithContext(ctx, "set @p = cast('1970-00-01 19:10:06' as datetime)")
	tk2.MustQueryWithContext(ctx, "execute time_comparison using @p").Check(testkit.Rows())
	require.False(t, tk2.Session().GetSessionVars().FoundInPlanCache)
	tk2.MustQueryWithContext(ctx, "execute time_comparison using @p").Check(testkit.Rows())
	require.False(t, tk2.Session().GetSessionVars().FoundInPlanCache)

	tk2.MustExecWithContext(ctx, "set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk2.MustQueryWithContext(ctx, "execute time_comparison using @p").Check(testkit.Rows("1"))
	require.True(t, tk2.Session().GetSessionVars().FoundInPlanCache)
}

// TestTimeColumnTemporalParameterPlanCache verifies TIME-column comparison
// semantics and range rebuilding for valid temporal parameters in the session cache.
func TestTimeColumnTemporalParameterPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache=1")
	tk.MustExec("create table t_time_cache (id int primary key, trx_time time(0), key(trx_time))")
	tk.MustExec("insert into t_time_cache values (1, '19:10:06'), (2, '18:00:00'), (3, '20:00:00'), (4, '00:00:00')")
	checkRange := func(expected string) {
		processInfo := tk.Session().ShowProcess()
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*sessmgr.ProcessInfo{processInfo}})
		tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", processInfo.ID)).MultiCheckContain(
			[]string{"IndexRangeScan", expected})
	}

	tk.MustExec("prepare time_eq from 'select /*+ use_index(t_time_cache, trx_time) */ id from t_time_cache where trx_time = ? order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:05.600000' as datetime(6))")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	checkRange("range:[19:10:06,19:10:06]")
	tk.MustExec("set @p = cast('2030-12-31 18:00:00.400000' as datetime(6))")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	checkRange("range:[18:00:00,18:00:00]")
	tk.MustExec("set @p = cast('1970-01-01' as date)")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("4"))
	checkRange("range:[00:00:00,00:00:00]")

	tk.MustExec("prepare time_null_safe_eq from 'select id from t_time_cache where trx_time <=> ? order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_null_safe_eq using @p").Check(testkit.Rows("1"))
	tk.MustExec("set @p = cast('2030-12-31 18:00:00' as datetime)")
	tk.MustQuery("execute time_null_safe_eq using @p").Check(testkit.Rows("2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec("prepare time_reversed from 'select id from t_time_cache where ? <= trx_time order by id'")
	tk.MustExec("set @p = cast('1970-01-01 19:10:05.600000' as datetime(6))")
	tk.MustQuery("execute time_reversed using @p").Check(testkit.Rows("1", "3"))

	tk.MustExec("prepare time_explicit_cast from 'select id from t_time_cache where trx_time = cast(? as datetime) order by id'")
	tk.MustExec("set @p = cast('1000-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_explicit_cast using @p").Check(testkit.Rows())
	tk.MustExec("set @p = cast('9999-12-31 18:00:00' as datetime)")
	tk.MustQuery("execute time_explicit_cast using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare time_timestamp_expression from 'select id from t_time_cache where trx_time = timestamp(?) order by id'")
	tk.MustExec("set @p = cast('1000-01-01 19:10:06' as datetime)")
	tk.MustQuery("execute time_timestamp_expression using @p").Check(testkit.Rows())

	tk.MustExec("set tidb_enable_prepared_plan_cache=0")
	tk.MustExec("set @p = cast('2040-06-01 20:00:00' as datetime)")
	tk.MustQuery("execute time_eq using @p").Check(testkit.Rows("3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

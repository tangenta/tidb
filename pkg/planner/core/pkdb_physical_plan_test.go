package core_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestSubstituteUDV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), added_time datetime)")
	tk.MustExec("set @@session.tidb_enable_udv_substitute=1")

	tk.MustExec("set @a = 1")
	plan := tk.MustQuery("explain select * from t where a = 1").Rows()
	tk.MustQuery("explain select * from t where a = @a").Check(plan)

	tk.MustExec("set @b = aaa")
	plan = tk.MustQuery("explain select * from t where b = 'aaa'").Rows()
	tk.MustQuery("explain select * from t where b = @b").Check(plan)

	tk.MustExec("SET @startTime = '2021-04-07 16:00:00';SET @endTime = '2021-04-08 16:00:00';")
	plan = tk.MustQuery("explain select count(1) AS total, 0 AS now from t where added_time >= '2021-04-07 16:00:00' and added_time < '2021-04-08 16:00:00';").Rows()
	tk.MustQuery("explain select count(1) AS total, 0 AS now from t where added_time >= @startTime and added_time < @endTime;").Check(plan)
}

func TestBuiltInGetIntVarSigType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable()})
	sctx := tk.Session().(sessionctx.Context)
	p := parser.New()

	testCases := map[types.EvalType][]string{
		types.ETInt: {
			"1",
			"0",
			"-1",
			"-9223372036854775808",
			"18446744073709551615",
			"true",
			"false",
			"cast(1 as signed)",
			"cast(1 as unsigned)",
			"cast(-1 as signed)",
			"cast(-1 as unsigned)",
			"x'41'+0",
			"cast(x'41' as unsigned)",
			"cast(x'41' as signed)",
			"b'1000001'+0",
			"cast(b'1000001' as unsigned)",
			"cast(b'1000001' as signed)",
		},
		types.ETString: {
			"x'41'",
			"b'1000001'",
			"a",
			"tidb",
			"'true'",
			"'false'",
			"'1'",
			"'0'",
			"'-1'",
			"'烫烫烫'",
			"'烫烫烫0xFF'",
			"''",
			"concat('6', 'six')",
			"'!@#$%^&*()_+-*/'",
			"'[]{};:,./<>?'",
			"'，。、；：？！￥……（）—《》'",
		},
		types.ETDecimal: {
			"1.1",
			"0.0",
			"-1.1",
			"cast(1.1 as decimal)",
			"cast(1.1 as decimal(1))",
			"cast(-1.1 as decimal(2))",
			"cast(1.1 as decimal(10, 2))",
			"cast(-1.1 as decimal(10, 2))",
			"cast(1.1 as decimal(10, 0))",
			"cast(-1.1 as decimal(10, 0))",
		},
		types.ETReal: {
			"1e-1",
			"1e0",
			"1e1",
			"cast(1.234 as float)",
			"cast(-1.234 as float)",
			"cast(1.234 as float(2))",
			"cast(-1.234 as float(5))",
			"cast(1.234 as float(5,2))",
			"cast(-1.234 as float(5,4))",
			"cast(-1.234e-1 as float(5,2))",
			"cast(-1.234e-0 as float(5,4))",
			"cast(-1.234e-1 as float(5,0))",
			"cast(1.234 as double)",
			"cast(-1.234 as double)",
			"cast(-1.234e-1 as double)",
			"cast(-1.234e-0 as double)",
		},
		types.ETDatetime: {
			"cast('2021-04-07 16:00:00' as datetime)",
			"cast('2021-04-07 16:00:00' as datetime(0))",
			"cast('2021-04-07 16:00:00' as datetime(6))",
			"cast('2021-04-07' as datetime)",
			"cast('2021-04-07' as date)",
		},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/clauseSubstituteAbleForUDV", "return(true)")
	tk.Session().GetSessionVars().EnableUDVSubstitute = true
	ctx1 := mock.NewContext()
	for evalType, cases := range testCases {
		for _, testCase := range cases {
			sql := fmt.Sprintf("set @a = %s", testCase)
			tk.MustExec(sql)
			// build constant
			stmt, err := p.ParseOneStmt("select @a", "", "")
			require.NoError(t, err)
			sctx.GetSessionVars().ReplaceAbleUserDefVars = map[string]struct{}{"a": {}}
			nodeW := resolve.NewNodeW(stmt)
			plan, err1 := core.BuildLogicalPlanForTest(ctx, sctx, nodeW, is)
			require.NoError(t, err1)
			constant := plan.(base.LogicalPlan).(*logicalop.LogicalProjection).Exprs[0].(*expression.Constant)
			require.NotNil(t, constant)
			constRetType := constant.GetType(sctx.GetExprCtx().GetEvalCtx())
			require.Equal(t, constRetType.EvalType(), evalType)
			// build scalarFunc
			tp, ok := tk.Session().GetSessionVars().GetUserVarType("a")
			require.True(t, ok)
			expr, err2 := expression.NewFunction(sctx.GetExprCtx(), ast.GetVar, tp, expression.DatumToConstant(types.NewStringDatum("a"), mysql.TypeString, 0))
			require.NoError(t, err2)
			require.Equal(t, expr.(*expression.ScalarFunction).FuncName.L, ast.GetVar)
			getFuncRetType := expr.GetType(ctx1)
			require.Equal(t, getFuncRetType.EvalType(), evalType)
			// two result type must be equal
			require.Equal(t, constRetType, getFuncRetType)
		}
	}
}

func TestSPParamSubstituteInJoinOnClause(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, key idx_a(a))")
	tk.MustExec("create table t2(a int, key idx_a(a))")

	sctx := tk.Session().(sessionctx.Context)
	sctx.GetSessionVars().SetInCallProcedure()
	procCtx := variable.NewProcedureContext(variable.BLOCKLABEL)
	procCtx.Vars = append(procCtx.Vars, variable.NewProcedureVars("pa", types.NewFieldType(mysql.TypeLong)))
	require.NoError(t, sctx.GetSessionVars().SetProcedureContext(procCtx))
	require.NoError(t, core.UpdateVariableVar("pa", types.NewDatum(1), sctx.GetSessionVars()))

	sql := "explain format='brief' select * from t1 join t2 on t2.a = pa where t1.a = pa"
	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 0")
	require.Contains(t, fmt.Sprint(tk.MustQuery(sql).Rows()), "getprocedurevar")

	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 1")
	require.NotContains(t, fmt.Sprint(tk.MustQuery(sql).Rows()), "getprocedurevar")
}

func TestSPParamSubstituteInHavingClause(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key idx_a(a))")
	tk.MustExec("insert into t values (1, 10), (2, 20)")

	sctx := tk.Session().(sessionctx.Context)
	sctx.GetSessionVars().SetInCallProcedure()
	procCtx := variable.NewProcedureContext(variable.BLOCKLABEL)
	procCtx.Vars = append(procCtx.Vars, variable.NewProcedureVars("pa", types.NewFieldType(mysql.TypeLong)))
	require.NoError(t, sctx.GetSessionVars().SetProcedureContext(procCtx))
	require.NoError(t, core.UpdateVariableVar("pa", types.NewDatum(1), sctx.GetSessionVars()))

	sql := "explain format='brief' select a, sum(b) from t group by a having a = pa and sum(b) > pa"
	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 0")
	require.Contains(t, fmt.Sprint(tk.MustQuery(sql).Rows()), "getprocedurevar")

	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 1")
	require.NotContains(t, fmt.Sprint(tk.MustQuery(sql).Rows()), "getprocedurevar")

	tk.MustQuery("select a as pa, sum(b) from t group by a having pa = 2").Check(testkit.Rows("2 20"))
}

func TestReplaceAbleUserDefVarsIfEmpty(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.Nil(t, tk.Session().GetSessionVars().ReplaceAbleUserDefVars)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 1")

	tk.MustExec("set @a = 1")
	tk.MustQuery("select * from t where a = @a")
	tk.MustQuery("select * from t ")
	require.Nil(t, tk.Session().GetSessionVars().ReplaceAbleUserDefVars)

	tk.MustQuery("select * from t where a = @a")
	tk.MustExec("set @@session.tidb_enable_udv_substitute = 0")
	require.Nil(t, tk.Session().GetSessionVars().ReplaceAbleUserDefVars)
}

func TestUDVSubstituteResult(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("set @@session.tidb_enable_udv_substitute = 1")
	tk.MustExec("set @@session.tidb_enable_sp_param_substitute = 1")
	tk.InProcedure()

	testCases := []struct {
		paramType  string
		filter     string
		paramValue string
	}{
		// integer
		{"boolean", "pa = false", "false"},
		{"tinyint", "pa = -1", "-1"},
		{"smallint", "pa = -1", "-1"},
		{"mediumint", "pa = -1", "-1"},
		{"int", "pa = -1", "-1"},
		{"bigint", "pa = -1", "-1"},
		// decimal
		//{"float", "pa = 1.2", "1.2"},
		{"double", "pa = 1.2", "1.2"},
		{"decimal(5,3)", "pa = 1.200", "1.2"},
		// date and time
		{"date", "pa = '2023-01-01'", "'2023-01-01'"},
		{"time", "pa = '00:00:00'", "'00:00:00'"},
		{"datetime", "pa = '2023-01-01 00:00:00'", "'2023-01-01 00:00:00'"},
		{"timestamp", "pa = '2023-01-01 00:00:00'", "'2023-01-01 00:00:00'"},
		{"year", "pa = 2023", "2023"},
		// string
		{"char(10)", "pa = 'abc烫！、,;'", "'abc烫！、,;'"},
		{"varchar(10)", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"text", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"tinytext", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"mediumtext", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"longtext", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"binary(15)", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"varbinary(15)", "pa = 'abc烫！、,;7'", "'abc烫！、,;7'"},
		{"enum('a', 'b', 'c')", "pa = 'a'", "'a'"},
		{"set('a', 'b', 'c')", "pa = 'a'", "'a'"},
	}

	// general sql
	for _, testCase := range testCases {
		tk.MustExec("create table t(a " + testCase.paramType + ")")
		tk.MustExec("insert into t values(" + testCase.paramValue + ")")
		tk.MustExec("set @a = " + testCase.paramValue)
		tk.MustQuery("select 1 from t where a = @a").Check(testkit.Rows("1"))
		tk.MustExec("drop table t")
	}
	// stored procedure param
	for _, testCase := range testCases {
		tk.MustExec("create procedure sp(pa " + testCase.paramType + ") begin select 1 where " + testCase.filter + "; end")
		tk.MustExec("call sp(" + testCase.paramValue + ")")
		tk.Res[0].Check(testkit.Rows("1"))
		tk.ClearProcedureRes()
		tk.MustExec("set @a = " + testCase.paramValue)
		tk.MustExec("call sp(@a)")
		tk.Res[0].Check(testkit.Rows("1"))
		tk.ClearProcedureRes()
		tk.MustExec("drop procedure sp")
	}
	// stored procedure local variable
	for _, testCase := range testCases {
		tk.MustExec("create procedure sp() begin declare pa " + testCase.paramType + "; set pa = " +
			testCase.paramValue + "; select 1 where " + testCase.filter + "; end")
		tk.MustExec("call sp()")
		tk.Res[0].Check(testkit.Rows("1"))
		tk.ClearProcedureRes()
		tk.MustExec("drop procedure sp")
	}
	// stored procedure udv
	for _, testCase := range testCases {
		tk.MustExec("create table t(a " + testCase.paramType + ")")
		tk.MustExec("insert into t values(" + testCase.paramValue + ")")
		tk.MustExec("set @a = " + testCase.paramValue)
		tk.MustExec("create procedure sp() begin select 1 from t where a = @a; end")
		tk.MustExec("call sp()")
		tk.Res[0].Check(testkit.Rows("1"))
		tk.ClearProcedureRes()
		tk.MustExec("drop procedure sp")
		tk.MustExec("drop table t")
	}
}

package analyzetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeTableWithVarcharShardIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")

	tk.MustExec(`create table t_varchar_shard (
		id int primary key,
		a varchar(32) collate utf8mb4_general_ci,
		unique key uk ((tidb_shard(a)), a)
	)`)
	tk.MustExec(`insert into t_varchar_shard values (1, 'FT00001')`)
	tk.MustExec("analyze table t_varchar_shard")

	h := dom.StatsHandle()
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_varchar_shard"))
	require.NoError(t, err)

	tblStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats)
	require.True(t, tblStats.IsAnalyzed())
	require.True(t, tblStats.GetIdx(tbl.Meta().Indices[0].ID).IsAnalyzed())
}

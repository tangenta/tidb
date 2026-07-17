package model

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestIsVersionedTiDBShardExprNode(t *testing.T) {
	expr, err := parseGeneratedExpr("tidb_shard(`a`, 1)")
	require.NoError(t, err)

	baseColName, ok := IsVersionedTiDBShardExprNode(expr)
	require.True(t, ok)
	require.Equal(t, ast.NewCIStr("a"), baseColName)

	expr, err = parseGeneratedExpr("tidb_shard(`a`)")
	require.NoError(t, err)
	baseColName, ok = IsVersionedTiDBShardExprNode(expr)
	require.False(t, ok)
	require.Equal(t, ast.CIStr{}, baseColName)
}

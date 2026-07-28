// Copyright 2026 PingCAP, Inc.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestPKDBCutoverReplacePrimaryKeyByID(t *testing.T) {
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("old_col")},
			{ID: 2, Name: ast.NewCIStr("new_col")},
		},
		Indices: []*model.IndexInfo{
			{
				ID: 10, Name: ast.NewCIStr(mysql.PrimaryKeyName), Primary: true,
				State: model.StateWriteOnly, Columns: []*model.IndexColumn{{Name: ast.NewCIStr("old_col"), Offset: 0}},
			},
			{ID: 11, Name: ast.NewCIStr(model.GenRemovingObjName(mysql.PrimaryKeyName)), State: model.StateNone},
			{
				ID: 20, Name: ast.NewCIStr("candidate_without_reserved_prefix"), Primary: true,
				State: model.StateWriteReorganization, Columns: []*model.IndexColumn{{Name: ast.NewCIStr("new_col"), Offset: 1}},
			},
		},
	}
	newPrimaryKey := tblInfo.FindIndexByID(20)

	require.NoError(t, pkdbCutoverReplacePrimaryKey(tblInfo, newPrimaryKey, 10))
	require.Equal(t, int64(20), tblInfo.Indices[0].ID)
	require.Equal(t, "primary", newPrimaryKey.Name.L)
	require.Equal(t, model.StatePublic, newPrimaryKey.State)
	require.True(t, mysql.HasPriKeyFlag(tblInfo.Columns[1].GetFlag()))
	require.False(t, mysql.HasPriKeyFlag(tblInfo.Columns[0].GetFlag()))

	oldPrimaryKey := tblInfo.FindIndexByID(10)
	require.Equal(t, model.StateWriteOnly, oldPrimaryKey.State)
	require.Equal(t, model.GenRemovingObjName(mysql.PrimaryKeyName+"_1"), oldPrimaryKey.Name.O)
	require.NotEqual(t, tblInfo.FindIndexByID(11).Name.L, oldPrimaryKey.Name.L)
	require.NoError(t, pkdbCutoverReplacePrimaryKey(tblInfo, newPrimaryKey, 10))

	oldPrimaryKey.Name = ast.NewCIStr("arbitrary_old_primary_key_name")
	require.ErrorContains(t, pkdbCutoverReplacePrimaryKey(tblInfo, newPrimaryKey, 10),
		"inconsistent completed primary key cutover")
}

func TestPKDBFindIndexToDropUsesNonzeroIDExclusively(t *testing.T) {
	primaryKey := &model.IndexInfo{ID: 10, Name: ast.NewCIStr(mysql.PrimaryKeyName), Primary: true}
	tblInfo := &model.TableInfo{Indices: []*model.IndexInfo{primaryKey}}

	require.Nil(t, pkdbFindIndexToDrop(tblInfo, &model.IndexArg{
		IndexID:   999,
		IndexName: ast.NewCIStr(mysql.PrimaryKeyName),
	}))
	require.Same(t, primaryKey, pkdbFindIndexToDrop(tblInfo, &model.IndexArg{
		IndexName: ast.NewCIStr(mysql.PrimaryKeyName),
	}))
}

func TestPKDBFindDropIndexPrimarySpec(t *testing.T) {
	specs := []*ast.AlterTableSpec{
		{Tp: ast.AlterTableDropColumn},
		{Tp: ast.AlterTableDropIndex, Name: mysql.PrimaryKeyName},
	}

	require.Equal(t, 1, pkdbFindDropPrimaryKeySpec(specs))
}

func TestDropColumnValidationPolicyForPrimaryKeyReplacement(t *testing.T) {
	columnName := ast.NewCIStr("old_col")
	oldPrimaryKey := &model.IndexInfo{
		ID:      10,
		Name:    ast.NewCIStr(mysql.PrimaryKeyName),
		Primary: true,
		State:   model.StateWriteOnly,
		Columns: []*model.IndexColumn{
			{Name: columnName, Offset: 0},
			{Name: ast.NewCIStr("shared_col"), Offset: 1},
		},
	}
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: columnName, Offset: 0},
			{ID: 2, Name: ast.NewCIStr("shared_col"), Offset: 1},
			{ID: 3, Name: ast.NewCIStr("other_col"), Offset: 2},
		},
		Indices: []*model.IndexInfo{oldPrimaryKey},
	}
	allowedPolicy := dropColumnValidationPolicy{allowedReplacementOldPrimaryKeyID: oldPrimaryKey.ID}

	require.Error(t, isDroppableColumn(tblInfo, columnName, dropColumnValidationPolicy{}))
	require.Error(t, isDroppableColumn(tblInfo, columnName, dropColumnValidationPolicy{
		allowedReplacementOldPrimaryKeyID: 999,
	}))
	require.NoError(t, isDroppableColumn(tblInfo, columnName, allowedPolicy))

	oldPrimaryKey.State = model.StatePublic
	require.Error(t, isDroppableColumn(tblInfo, columnName, allowedPolicy))
	oldPrimaryKey.State = model.StateWriteOnly
	tblInfo.Indices = append(tblInfo.Indices, &model.IndexInfo{
		ID:    20,
		Name:  ast.NewCIStr("idx_unrelated"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: columnName, Offset: 0},
			{Name: ast.NewCIStr("other_col"), Offset: 2},
		},
	})
	require.Error(t, isDroppableColumn(tblInfo, columnName, allowedPolicy))
}

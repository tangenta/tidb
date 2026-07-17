package core

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func tryRewriteTriggerPseudoColumn(er *expressionRewriter, v *ast.ColumnName) bool {
	if !v.IsTriggerPseudoRecord() || er.planCtx == nil {
		return false
	}
	trigCtx := er.planCtx.builder.ctx.GetSessionVars().StmtCtx.TriggerCtx
	if !trigCtx.InTrigger {
		return false
	}
	colIdx := -1
	var retType *types.FieldType
	for i, col := range trigCtx.TableInfo.Columns {
		if col.Name.L == v.Name.L {
			colIdx = i
			retType = &col.FieldType
			break
		}
	}
	if colIdx == -1 {
		er.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[er.clause()])
		return true
	}
	var d types.Datum
	switch v.Table.L {
	case "new":
		if len(trigCtx.NewData) == 0 {
			er.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("NEW", "")
			return true
		}
		if colIdx >= len(trigCtx.NewData) {
			er.err = plannererrors.ErrInternal.GenWithStack("trigger pseudo record NEW index out of range: colIdx=%d, newLen=%d", colIdx, len(trigCtx.NewData))
			return true
		}
		d = trigCtx.NewData[colIdx]
	case "old":
		if len(trigCtx.OldData) == 0 {
			er.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs("OLD", "")
			return true
		}
		if colIdx >= len(trigCtx.OldData) {
			er.err = plannererrors.ErrInternal.GenWithStack("trigger pseudo record OLD index out of range: colIdx=%d, oldLen=%d", colIdx, len(trigCtx.OldData))
			return true
		}
		d = trigCtx.OldData[colIdx]
	default:
		er.err = plannererrors.ErrInternal.GenWithStack("unknown trigger pseudo record: %s", v.Table.O)
		return true
	}
	expr := er.assembleConstant(d, retType)
	er.ctxStackAppend(expr, types.EmptyName)
	return true
}

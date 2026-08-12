package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type timeComparisonOperand struct {
	expr expression.Expression
	node ast.ExprNode
	name *types.FieldName
}

// castTemporalConstantForTimeColumnComparison casts an eligible DATE,
// DATETIME, or TIMESTAMP constant to the direct TIME column's type and FSP.
func (er *expressionRewriter) castTemporalConstantForTimeColumnComparison(
	lhs, rhs timeComparisonOperand,
) (expression.Expression, expression.Expression) {
	operands := [2]timeComparisonOperand{lhs, rhs}
	evalCtx := er.sctx.GetEvalCtx()
	for constantIdx := range operands {
		constant := operands[constantIdx]
		constantType := constant.expr.GetType(evalCtx).GetType()
		if constantType != mysql.TypeDate && constantType != mysql.TypeDatetime && constantType != mysql.TypeTimestamp {
			continue
		}
		columnIdx := 1 - constantIdx
		columnType, ok := er.directTimeColumnType(evalCtx, operands[columnIdx])
		if !ok || !er.isEligibleTimeComparisonConstant(evalCtx, constant) {
			continue
		}
		operands[constantIdx].expr = expression.BuildCastFunction(er.sctx, constant.expr, columnType.Clone())
		return operands[0].expr, operands[1].expr
	}
	return operands[0].expr, operands[1].expr
}

func (er *expressionRewriter) directTimeColumnType(
	ctx expression.EvalContext,
	operand timeComparisonOperand,
) (*types.FieldType, bool) {
	if er.planCtx == nil {
		return nil, false
	}
	// Projection-like inputs may expose derived or view expressions as columns.
	if _, ok := er.planCtx.plan.(*logicalop.DataSource); !ok {
		return nil, false
	}
	if _, ok := unwrapParentheses(operand.node).(*ast.ColumnNameExpr); !ok {
		return nil, false
	}
	column, ok := operand.expr.(*expression.Column)
	if !ok || operand.name == nil || operand.name.OrigTblName.L == "" || column.VirtualExpr != nil {
		return nil, false
	}
	tp := column.GetType(ctx)
	return tp, tp.GetType() == mysql.TypeDuration
}

func (er *expressionRewriter) isEligibleTimeComparisonConstant(ctx expression.EvalContext, operand timeComparisonOperand) bool {
	node := unwrapParentheses(operand.node)
	// A bare marker is typed from the field. An expression around a marker,
	// including an explicit cast, keeps its own type in MySQL.
	_, isParamMarker := node.(ast.ParamMarkerExpr)
	if isParamMarker {
		if er.planCtx != nil && er.planCtx.builder != nil && er.planCtx.builder.ctx.GetSessionVars().GetCallProcedure() {
			return false
		}
	} else {
		if operand.expr.ConstLevel() != expression.ConstStrict {
			return false
		}
		checker := timeComparisonConstantChecker{ctx: er.sctx}
		node.Accept(&checker)
		if checker.dynamic {
			return false
		}
	}
	// Keep incomplete dates on the generic comparison path. Converting them to
	// TIME would discard the invalid date parts and could create a false match.
	value, isNull, err := operand.expr.EvalTime(ctx, chunk.Row{})
	return err == nil && !isNull && !value.InvalidZero()
}

func unwrapParentheses(node ast.ExprNode) ast.ExprNode {
	for {
		parenthesized, ok := node.(*ast.ParenthesesExpr)
		if !ok {
			return node
		}
		node = parenthesized.Expr
	}
}

type timeComparisonConstantChecker struct {
	ctx     expression.BuildContext
	dynamic bool
}

func (c *timeComparisonConstantChecker) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case ast.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.VariableExpr, *ast.SubqueryExpr:
		c.dynamic = true
	case *ast.FuncCallExpr:
		c.dynamic = c.dynamic ||
			isUserDefinedStoredFunction(x) ||
			expression.IsDeferredFunctions(c.ctx, x.FnName.L)
	}
	return node, c.dynamic
}

func (c *timeComparisonConstantChecker) Leave(node ast.Node) (ast.Node, bool) {
	return node, !c.dynamic
}

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

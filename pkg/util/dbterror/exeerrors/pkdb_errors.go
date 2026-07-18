package exeerrors

import (
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// PKDB executor errors.
var (
	ErrCTASDistTask            = dbterror.ClassExecutor.NewStd(mysql.ErrCTASDistTask)
	ErrUdfExists               = dbterror.ClassExecutor.NewStd(mysql.ErrUdfExists)
	ErrSpAlreadyExists         = dbterror.ClassExecutor.NewStd(mysql.ErrSpAlreadyExists)
	ErrSpDoesNotExist          = dbterror.ClassExecutor.NewStd(mysql.ErrSpDoesNotExist)
	ErrSpNoreturnend           = dbterror.ClassExecutor.NewStd(mysql.ErrSpNoreturnend)
	ErrSpDupParam              = dbterror.ClassExecutor.NewStd(mysql.ErrSpDupParam)
	ErrSpNotVarArg             = dbterror.ClassOptimizer.NewStd(mysql.ErrSpNotVarArg)
	ErrOperandColumns          = dbterror.ClassOptimizer.NewStd(mysql.ErrOperandColumns)
	ErrSpVarcondAfterCurshndlr = dbterror.ClassOptimizer.NewStd(mysql.ErrSpVarcondAfterCurshndlr)
	ErrSpDupVar                = dbterror.ClassOptimizer.NewStd(mysql.ErrSpDupVar)
	ErrSpDupCurs               = dbterror.ClassOptimizer.NewStd(mysql.ErrSpDupCurs)
	ErrSpCursorMismatch        = dbterror.ClassOptimizer.NewStd(mysql.ErrSpCursorMismatch)
	ErrSpUndeclaredVar         = dbterror.ClassOptimizer.NewStd(mysql.ErrSpUndeclaredVar)
	ErrSpCursorNotOpen         = dbterror.ClassOptimizer.NewStd(mysql.ErrSpCursorNotOpen)
	ErrSpFetchNoData           = dbterror.ClassOptimizer.NewStd(mysql.ErrSpFetchNoData)
	ErrSpWrongNoOfFetchArgs    = dbterror.ClassOptimizer.NewStd(mysql.ErrSpWrongNoOfFetchArgs)
	ErrSpCursorAlreadyOpen     = dbterror.ClassOptimizer.NewStd(mysql.ErrSpCursorAlreadyOpen)
	ErrSpDupHandler            = dbterror.ClassOptimizer.NewStd(mysql.ErrSpDupHandler)
	ErrSpCursorAfterHandler    = dbterror.ClassOptimizer.NewStd(mysql.ErrSpCursorAfterHandler)
	ErrSpBadSQLstate           = dbterror.ClassOptimizer.NewStd(mysql.ErrSpBadSQLstate)
	ErrSpNoRetset              = dbterror.ClassOptimizer.NewStd(mysql.ErrSpNoRetset)
	ErrSpecificAccessDenied    = dbterror.ClassExecutor.NewStd(mysql.ErrSpecificAccessDenied)
	ErrNoSuchUser              = dbterror.ClassOptimizer.NewStd(mysql.ErrNoSuchUser)
	ErrSpRecursionLimit        = dbterror.ClassExecutor.NewStd(mysql.ErrSpRecursionLimit)
	ErrSpWrongNoOfArgs         = dbterror.ClassExecutor.NewStd(mysql.ErrSpWrongNoOfArgs)
	ErrTooLongRoutineComment   = dbterror.ClassExecutor.NewStd(mysql.ErrTooLongRoutineComment)
	ErrBadField                = dbterror.ClassExecutor.NewStd(mysql.ErrBadField)
	ErrCondItemTooLong         = dbterror.ClassExecutor.NewStd(mysql.ErrCondItemTooLong)
	WarnCondItemTruncated      = dbterror.ClassExecutor.NewStd(mysql.WarnCondItemTruncated)
	ErrWrongValueForVar        = dbterror.ClassExecutor.NewStd(mysql.ErrWrongValueForVar)

	ErrGetStackedDaWithoutActiveHandler = dbterror.ClassExecutor.NewStd(mysql.ErrGetStackedDaWithoutActiveHandler)
	ErrDaInvalidConditionNumber         = dbterror.ClassExecutor.NewStd(mysql.ErrDaInvalidConditionNumber)
	ErrCantUpdateUsedTableInSfOrTrg     = dbterror.ClassExecutor.NewStd(mysql.ErrCantUpdateUsedTableInSfOrTrg)
	ErrProcedureDisabled                = dbterror.ClassExecutor.NewStd(mysql.ErrProcedureDisabled)
)

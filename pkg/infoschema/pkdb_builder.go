package infoschema

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func (b *Builder) reloadRoutines() error {
	if b.infoSchema.routineMap == nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
	}

	// The sys session factory is optional (for example, in some cross keyspace
	// loading cases). Leave the routine cache empty in such cases.
	if b.factory == nil {
		return nil
	}

	res, err := b.factory()
	if err != nil {
		return errors.Trace(err)
	}
	defer res.Close()
	sctx, ok := res.(sessionctx.Context)
	if !ok {
		return errors.Errorf("unexpected sys session type %T", res)
	}
	sessIS := sctx.GetLatestInfoSchema()
	if sessIS == nil {
		// During bootstrap/domain initialization the session may not have any usable infoschema yet.
		// Skip routine loading in such cases; it will be loaded on later infoschema reloads.
		return nil
	}
	if ext, ok := sessIS.(*SessionExtendedInfoSchema); ok && ext.InfoSchema == nil {
		// During bootstrap/domain initialization the session may not have any usable infoschema yet.
		// Skip routine loading in such cases; it will be loaded on later infoschema reloads.
		return nil
	}

	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sql, args := BuildRoutineMetadataSQL(RoutineMetadataFilter{})
	rs, err := exec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
		return nil
	}
	if rs == nil {
		return nil
	}
	chunkRows, err := sqlexec.DrainRecordSetAndClose(ctx, rs, 1024)
	if err != nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
		return nil
	}

	newRoutineMap := make(map[string]map[string]*model.ProcedureInfo)
	for _, row := range chunkRows {
		if row.Len() != 18 {
			continue
		}
		procInfo, err := DecodeRoutineMetadataRow(row)
		if err != nil {
			return err
		}

		var retType *types.FieldType
		if procInfo.Type == "FUNCTION" {
			retType = getStoredFuncRetType(procInfo.DefinitionUTF8, procInfo.SQLMode)
		}
		procInfo.RetType = retType
		procInfo.State = model.StatePublic
		routines, ok := newRoutineMap[procInfo.Schema.L]
		if !ok {
			routines = make(map[string]*model.ProcedureInfo)
			newRoutineMap[procInfo.Schema.L] = routines
		}
		routines[routineKey(procInfo.Type, procInfo.Name.L)] = procInfo
	}

	b.infoSchema.routineMap = newRoutineMap
	return nil
}

func getStoredFuncRetType(defUTF8, sqlModeStr string) *types.FieldType {
	sqlMode, err := mysql.GetSQLMode(sqlModeStr)
	if err != nil {
		logutil.BgLogger().Error("failed to parse SQL mode from string",
			zap.String("sql_mode", sqlModeStr),
			zap.Error(err))
		return nil
	}

	p := parser.GetParser()
	defer parser.DestroyParser(p)
	p.SetSQLMode(sqlMode)
	createFnSQL := "create function p() " + defUTF8
	stmt, err := p.ParseOneStmt(createFnSQL, "", "")
	if err != nil {
		logutil.BgLogger().Error("failed to parse create function statement",
			zap.Error(err))
		return nil
	}
	createFn, ok := stmt.(*ast.CreateProcedureInfo)
	if !ok || createFn.FunctionInfo.RetType == nil {
		logutil.BgLogger().Error("failed to parse create function ret type",
			zap.String("statementType", fmt.Sprintf("%T", stmt)),
			zap.Bool("retTypeIsNil", createFn.FunctionInfo.RetType == nil),
		)
		return nil
	}
	return createFn.FunctionInfo.RetType
}

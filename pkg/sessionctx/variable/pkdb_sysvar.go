package variable

import (
	"context"
	"math"
	"time"

	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

var pkdbSysVars = []*SysVar{
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableLoginHistory, Value: BoolToOnOff(vardef.DefTiDBEnableLoginHistory), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			vardef.EnableLoginHistory.Store(TiDBOptOn(val))
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableLoginHistory.Load()), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBLoginHistoryRetainDuration, Value: vardef.DefTiDBLoginHistoryRetainDuration.String(),
		Type: vardef.TypeDuration, MinValue: int64(time.Second), MaxValue: math.MaxUint64,
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.LoginHistoryRetainDuration.Load().String(), nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			vardef.LoginHistoryRetainDuration.Store(d)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBCreateFromSelectUsingImport, Value: BoolToOnOff(vardef.DefTiDBCreateFromSelectUsingImport), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.CreateFromSelectUsingImport = TiDBOptOn(val)
			return nil
		},
		IsHintUpdatableVerified: true,
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.PKDBEnableWhitelist, Value: BoolToOnOff(vardef.DefPKDBEnableWhitelist), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			vardef.EnableWhitelist.Store(TiDBOptOn(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableWhitelist.Load()), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.PKDBEnableEAL, Value: BoolToOnOff(vardef.DefPKDBEnableEAL), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			vardef.EnableEAL.Store(TiDBOptOn(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableEAL.Load()), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.PKDBExtraDataType, Value: BoolToOnOff(vardef.DefPKDBExtraDataType), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			enabled := TiDBOptOn(val)
			parsertypes.EnableExtraDataType.Store(enabled)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(parsertypes.EnableExtraDataType.Load()), nil
		},
	},
}

func init() {
	defaultSysVars = append(defaultSysVars, pkdbSysVars...)
}

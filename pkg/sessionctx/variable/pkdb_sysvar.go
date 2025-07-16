package variable

import (
	"context"
	"math"
	"time"

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
}

func init() {
	defaultSysVars = append(defaultSysVars, pkdbSysVars...)
}

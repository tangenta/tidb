//go:build fusion
// +build fusion

package variable

import (
	"context"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// FFI-dependent system variables
var ffiSysVars = []*SysVar{
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBXEnableTiKVLocalCall, Value: BoolToOnOff(vardef.DefTiDBXEnableLocalRPCOpt), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.EnableTiKVLocalCall.Load() {
				tikvrpc.EnableTiKVLocalCall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt", zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.EnableTiKVLocalCall.Load()), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBXEnableScheduleLeaderRule, Value: BoolToOnOff(vardef.DefTiDBXEnableScheduleLeaderRule), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			v := TiDBOptOn(s)
			if v != vardef.EnableScheduleLeaderRule.Load() {
				vardef.EnableScheduleLeaderRule.Store(v)
				if vardef.EnableScheduleLeaderRuleFn != nil {
					vardef.EnableScheduleLeaderRuleFn(v)
				}
			}
			return nil
		}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableScheduleLeaderRule.Load()), nil
		}},
}

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}

//go:build fusion
// +build fusion

package variable

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// FFI-dependent system variables
var ffiSysVars = []*SysVar{
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBXEnableTiKVLocalCall, Value: BoolToOnOff(vardef.DefTiDBXEnableLocalRPCOpt), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.EnableTiKVLocalCall.Load() {
				tikvrpc.EnableTiKVLocalCall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt",
					zap.String("variable", vardef.TiDBXEnableTiKVLocalCall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.EnableTiKVLocalCall.Load()), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBXEnableScheduleLeaderRule, Value: BoolToOnOff(vardef.DefTiDBXEnableScheduleLeaderRule), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			v := TiDBOptOn(s)
			if v != vardef.EnableScheduleLeaderRule.Load() {
				vardef.EnableScheduleLeaderRule.Store(v)
				if vardef.EnableScheduleLeaderRuleFn != nil {
					vardef.EnableScheduleLeaderRuleFn(v)
				}
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableScheduleLeaderRule.Load()), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBXEnablePDLocalCall, Value: BoolToOnOff(vardef.DefTiDBXEnableLocalRPCOpt), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if PDLocalCallVar == nil {
				return errors.Errorf("%s is not initialized. Please check if this is fusion mode", vardef.TiDBXEnablePDLocalCall)
			}
			if TiDBOptOn(s) != (*PDLocalCallVar).Load() {
				(*PDLocalCallVar).Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt",
					zap.String("variable", vardef.TiDBXEnablePDLocalCall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(context.Context, *SessionVars) (string, error) {
			if PDLocalCallVar == nil {
				return "", errors.Errorf("%s is not initialized. Please check if this is fusion mode", vardef.TiDBXEnablePDLocalCall)
			}
			return BoolToOnOff((*PDLocalCallVar).Load()), nil
		}},
}

// PDLocalCallVar will be set by the upper package tidbx-server to point to pd-server's
// EnableIPC. This is to break the dependency cycle between tidbx-server and
// pd-server.
var PDLocalCallVar *atomic.Bool

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}

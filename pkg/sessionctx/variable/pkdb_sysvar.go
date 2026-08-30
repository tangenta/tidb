package variable

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

var pkdbSysVars = []*SysVar{
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSlowLogEscapeNewline, Value: BoolToOnOff(vardef.DefTiDBSlowLogEscapeNewline), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, val string) error {
			vardef.SlowLogEscapeNewline.Store(TiDBOptOn(val))
			return nil
		},
		GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(vardef.SlowLogEscapeNewline.Load()), nil
		},
	},
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
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBImportIntoPrecheckConcurrency, Value: strconv.Itoa(vardef.DefTiDBImportIntoPrecheckConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableSPPlanCache, Value: BoolToOnOff(vardef.DefTiDBEnableSPPlanCache), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableSPPlanCache = TiDBOptOn(val)
			return nil
		},
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
	{Scope: vardef.ScopeGlobal, Name: vardef.PKDBCommentByteLengthLimit, Value: strconv.FormatInt(vardef.DefPKDBCommentByteLengthLimit, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			vardef.CommentByteLengthLimit.Store(TidbOptInt64(val, vardef.DefPKDBCommentByteLengthLimit))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.CommentByteLengthLimit.Load(), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.PKDBSequenceCacheEpoch, Value: strconv.FormatUint(vardef.DefPKDBSequenceCacheEpoch, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			epoch, err := strconv.ParseUint(normalizedValue, 10, 32)
			if err != nil {
				return normalizedValue, err
			}
			current := vardef.CurrentSequenceCacheEpoch()
			if epoch != vardef.DefPKDBSequenceCacheEpoch && epoch < current {
				return normalizedValue, fmt.Errorf("%s cannot be decreased from %d to %d except reset to %d", vardef.PKDBSequenceCacheEpoch, current, epoch, vardef.DefPKDBSequenceCacheEpoch)
			}
			return normalizedValue, nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			epoch, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return err
			}
			vardef.SetSequenceCacheEpoch(epoch)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatUint(vardef.CurrentSequenceCacheEpoch(), 10), nil
		},
	},
}

func init() {
	defaultSysVars = append(defaultSysVars, pkdbSysVars...)
}

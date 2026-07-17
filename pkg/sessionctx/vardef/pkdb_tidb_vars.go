package vardef

import (
	"time"

	"go.uber.org/atomic"
)

const (
	// TiDBEnableLoginHistory indicates whether tidb need enable the login-history.
	TiDBEnableLoginHistory = "tidb_enable_login_history"
	// TiDBLoginHistoryRetainDuration indicates the duration of retaining the record in mysql.log_history.
	TiDBLoginHistoryRetainDuration = "tidb_login_history_retain_duration"
	// TiDBEnableProcedure if enable store procedure
	TiDBEnableProcedure = "tidb_enable_procedure"
	// TiDBEnableSPPlanCache indicates whether to enable the stored-routine hidden prepared plan-cache path
	// and the related compatibility behavior added for it.
	TiDBEnableSPPlanCache = "tidb_enable_sp_plan_cache"
	// TiDBEnableProcedureAstCache indicates whether tidb need enable or disable ast cache.
	TiDBEnableProcedureAstCache = "tidb_enable_sp_ast_cache"
	//TiDBProcedureLastErrorSQL procedure last hander SQL warning/error.
	TiDBProcedureLastErrorSQL = "sp_last_error_sql"
	// TiDBEnableDutySeparationMode indicates if enable the mode of duty separation.
	TiDBEnableDutySeparationMode = "tidb_enable_duty_separation_mode"

	// TiDBCreateFromSelectUsingImport indicates whether to use import into to create table as select.
	TiDBCreateFromSelectUsingImport = "tidb_create_from_select_using_import"

	// fusion mode related variables

	// TiDBXEnableScheduleLeaderRule indicates whether to enable region leader in one store.
	TiDBXEnableScheduleLeaderRule = "tidbx_enable_schedule_leader_rule"
	// TiDBXEnableTiKVLocalCall indicates whether to enable TiKV local calls.
	TiDBXEnableTiKVLocalCall = "tidbx_enable_tikv_local_call"
	// TiDBXEnablePDLocalCall indicates whether to use Inter-Process Call for PD.
	TiDBXEnablePDLocalCall = "tidbx_enable_pd_local_call"
	// PKDBEnableWhitelist indicates whether to enable the whitelist feature.
	PKDBEnableWhitelist = "pkdb_whitelist"
	// PKDBEnableEAL indicates whether to enable the EAL feature.
	PKDBEnableEAL = "pkdb_eal"
	// PKDBExtraDataType indicates whether to enable extra data types.
	PKDBExtraDataType = "pkdb_extra_data_type"
	// PKDBCommentByteLengthLimit controls the byte-length limit override for table and column comments.
	// Value 0 means keeping the built-in MySQL-compatible baseline.
	PKDBCommentByteLengthLimit = "pkdb_comment_byte_length_limit"
	// TiDBEnableLBAC is used to enable or disable LBAC enforcement on TiDB.
	TiDBEnableLBAC = "pkdb_lbac"
)

// Default TiDB system variable values.
const (
	DefTiDBEnableLoginHistory          = false
	DefTiDBLoginHistoryRetainDuration  = time.Hour * 24 * 90 // default 90 days.
	DefStoredProgramCacheSize          = 256
	DefTiDBEnableProcedure             = false
	DefTiDBEnableSPPlanCache           = false
	DefTiDBEnableDutySeparationMode    = false
	DefTiDBEnableUDVSubstitute         = false
	DefTiDBEnableSPParamSubstitute     = false
	DefTiDBCreateFromSelectUsingImport = false
	DefLowerCaseTableNames             = "2"

	// fusion mode related variables

	DefTiDBXEnableLocalRPCOpt        = false
	DefTiDBXEnableScheduleLeaderRule = false
	DefPKDBEnableWhitelist           = false
	DefPKDBEnableEAL                 = false
	DefPKDBExtraDataType             = false
	DefTiDBEnableLBAC                = false
	DefPKDBCommentByteLengthLimit    = int64(0)
)

// UnspecifiedServerID indicates the unspecified server id.
const UnspecifiedServerID = 0

// Process global variables.
var (
	EnableScheduleLeaderRule                = atomic.NewBool(DefTiDBXEnableScheduleLeaderRule)
	EnableScheduleLeaderRuleFn func(v bool) = nil
	EnableWhitelist                         = atomic.NewBool(DefPKDBEnableWhitelist)
	EnableEAL                               = atomic.NewBool(DefPKDBEnableEAL)
	EnableLBAC                              = atomic.NewBool(DefTiDBEnableLBAC)
	CommentByteLengthLimit                  = atomic.NewInt64(DefPKDBCommentByteLengthLimit)
)

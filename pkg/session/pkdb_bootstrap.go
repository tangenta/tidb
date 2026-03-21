package session

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/extension/enterprise/whitelist"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
)

const (
	// init Enterprise Edition version.
	eeversion1 = 1
	// eeversion2 add Max_user_connections into mysql.user.
	eeversion2 = 2
	// eeversion3 add mysql.login_history
	eeversion3 = 3
	// eeversions4 add the table INFORMATION_SCHEMA.routines
	eeversion4 = 4
	// eeversion5 add label security tables.
	eeversion5 = 5
	// eeversion6 add the table mysql.procs_priv
	eeversion6 = 6
	// eeversion7 add the table mysql.audit_log_filters and mysql.audit_log_filter_rules
	eeversion7 = 7
	// eeversion8 add the table mysql.whitelist
	eeversion8 = 8
	// eeversion9 removes the value of `Column_priv` in both `mysql.tables_priv` and `mysql.columns_priv`
	eeversion9 = 9
	// eeversion10 alters table mysql.login_history MODIFY column Time DATETIME(6).
	eeversion10 = 10
	// eeversion11 alters table mysql.login_history renaming column `host` to `server_host` and adding a new column `user_host`
	eeversion11 = 11
	// eeversion12 solves the problem that the `mysql.login_history` table is created by open-source TiDB
	eeversion12 = 12
	// eeversion13 adds the table mysql.func for UDFs.
	eeversion13 = 13
	// eeversion14 reserve for LBAC implementation.
	eeversion14 = 14
	// eeversion15 adds the support for lower_case_table_names in EE.
	eeversion15 = 15
)

const (
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB Enterprise Edition server which bootstrapped the store.
	tidbEnterpriseEditionServerVersionVar = "tidb_enterprise_edition_server_version"
)

// currentEEBootstrapVersion is defined as a variable, so we can modify its value for testing.
// Please make sure this is the largest version.
var currentEEBootstrapVersion int64 = eeversion15

var bootstrapEEVersion = []func(sessionapi.Session, int64){
	upgradeEEToVer2,
	upgradeEEToVer3,
	upgradeToEEVer4,
	upgradeToEEVer5,
	upgradeToEEVer6,
	upgradeToEEVer7,
	upgradeToEEVer8,
	upgradeToEEVer9,
	upgradeToEEVer10,
	upgradeToEEVer11,
	upgradeToEEVer12,
	upgradeToEEVer13,
	upgradeToEEVer14,
	upgradeToEEVer15,
}

func doPkdbDDLWorks(s sessionapi.Session) {
	// Create route table mysql.routines
	mustExecute(s, CreateRouteTable)
	// Create routine privilege table mysql.procs_priv
	mustExecute(s, CreateProcsPriv)
	// Create label security tables
	mustExecute(s, CreateLSPolicies)
	mustExecute(s, CreateLSTables)
	mustExecute(s, CreateLSUsers)
	mustExecute(s, CreateLSElements)
	// Create login_history table
	mustExecute(s, CreateLoginHistory)
	// Create whitelist table
	mustExecute(s, whitelist.CreateWhitelistTableSQL)
	// Create audit tables
	mustExecute(s, audit.CreateFilterTableSQL)
	mustExecute(s, audit.CreateFilterRuleTableSQL)
	// Create mysql.func for loadable UDFs.
	mustExecute(s, CreateFuncTable)
}

func doPkdbDMLWorks(s sessionapi.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Enterprise Edition bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbEnterpriseEditionServerVersionVar, currentEEBootstrapVersion,
	)
}

func getBootstrapEEVersion(s sessionapi.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, tidbEnterpriseEditionServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

func updateEEBootstrapVer(s sessionapi.Session) {
	// Update bootstrap eeversion.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Enterprise Edition bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbEnterpriseEditionServerVersionVar, currentEEBootstrapVersion, currentEEBootstrapVersion,
	)
}

// PKDB system table definitions.
const (
	// CreateLoginHistory is a table about login history in mysql.
	CreateLoginHistory = `CREATE TABLE  IF NOT EXISTS mysql.login_history (
		Time datetime(6) NOT NULL,
		Server_host char(255)  NOT NULL DEFAULT '',
		User char(32)  NOT NULL DEFAULT '',
		User_host char(255) NOT NULL DEFAULT '',
		DB char(64)  NOT NULL DEFAULT '',
		Connection_id BIGINT(21) UNSIGNED NOT NULL DEFAULT 0,
		Result char(16)  NOT NULL DEFAULT '',
		Client_host char(255)  NOT NULL DEFAULT '',
		Detail text,
		INDEX idx_user(User, User_host, Result, Time),
		INDEX idx_time(Time)
		); `

	// CreateRouteTable is a table save routines info.
	// TODO: make as hidden table and query through the view.
	CreateRouteTable = `CREATE TABLE IF NOT EXISTS mysql.routines (
		route_schema varchar(64) NOT NULL,
		name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		type enum('FUNCTION','PROCEDURE') COLLATE utf8mb4_bin NOT NULL,
		definition longblob,
		definition_utf8 longtext COLLATE utf8mb4_bin,
		parameter_str blob,
		is_deterministic tinyint(1) NOT NULL,
		sql_data_access enum('CONTAINS SQL','NO SQL','READS SQL DATA','MODIFIES SQL DATA') COLLATE utf8mb4_bin NOT NULL,
		security_type enum('DEFAULT','INVOKER','DEFINER') COLLATE utf8mb4_bin NOT NULL,
		definer varchar(288) COLLATE utf8mb4_bin NOT NULL,
		sql_mode set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','POSTGRESQL','ORACLE','MSSQL','DB2','MAXDB','NO_KEY_OPTIONS','NO_TABLE_OPTIONS','NO_FIELD_OPTIONS','MYSQL323','MYSQL40','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','INVALID_DATES','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NO_AUTO_CREATE_USER','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL') COLLATE utf8mb4_bin NOT NULL,
		character_set_client varchar(100) NOT NULL,
		connection_collation varchar(100) NOT NULL,
		schema_collation varchar(100) NOT NULL,
		created timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		last_altered timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		comment text COLLATE utf8mb4_bin NOT NULL,
		options mediumtext COLLATE utf8mb4_bin,
		external_language varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT 'SQL',
		PRIMARY KEY (route_schema, name, type)
		) ;`

	// CreateProcsPriv is a table saving routines privilege.
	CreateProcsPriv = `CREATE TABLE IF NOT EXISTS mysql.procs_priv (
		Host char(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
		Db char(64) NOT NULL DEFAULT '',
		User char(32) NOT NULL DEFAULT '',
		Routine_name char(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
		Routine_type enum('FUNCTION','PROCEDURE') NOT NULL,
		Grantor varchar(288) NOT NULL DEFAULT '',
		Proc_priv set('Execute','Alter Routine','Grant') COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
		Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (Host,User,Db,Routine_name,Routine_type) /*T![clustered_index] CLUSTERED */,
		KEY Grantor (Grantor)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='Procedure privileges'`

	// CreateFuncTable stores loadable user-defined functions (UDFs).
	CreateFuncTable = `CREATE TABLE IF NOT EXISTS mysql.func (
		name char(64) NOT NULL DEFAULT '',
		ret tinyint NOT NULL DEFAULT '0',
		dl char(128) NOT NULL DEFAULT '',
		type enum('function','aggregate') NOT NULL,
		PRIMARY KEY (name)
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='User defined functions'`

	// CreateLSPolicies is used to create table tidb_ls_policies.
	CreateLSPolicies = `CREATE TABLE IF NOT EXISTS mysql.tidb_ls_policies(
		policy_name varchar(64) NOT NULL,
		label_column varchar(64) NOT NULL,
		PRIMARY KEY(policy_name)
	);`

	// CreateLSTables is used to create table tidb_ls_tables.
	CreateLSTables = `CREATE TABLE IF NOT EXISTS mysql.tidb_ls_tables(
		policy_name varchar(64) NOT NULL,
		schema_name varchar(64) NOT NULL COLLATE utf8mb4_general_ci,
		table_name varchar(64) NOT NULL COLLATE utf8mb4_general_ci,
		table_options enum('READ_CONTROL','WRITE_CONTROL'),
		PRIMARY KEY(policy_name),
		UNIQUE KEY(schema_name, table_name)
	) CHARSET=utf8mb4;`

	// CreateLSUsers is used to create table tidb_ls_users.
	CreateLSUsers = `CREATE TABLE IF NOT EXISTS mysql.tidb_ls_users(
		policy_name varchar(64) NOT NULL,
		user_name varchar(64) NOT NULL,
		label_value varchar(64) NOT NULL,
		PRIMARY KEY(policy_name, user_name)
	);`

	// CreateLSElements is used to create mysql.tidb_ls_elements.
	CreateLSElements = `CREATE TABLE IF NOT EXISTS mysql.tidb_ls_elements (
		policy_name varchar(64) NOT NULL,
		element_type enum('level','compartment','group') NOT NULL,
		element_id int,
		element_name varchar(64),
		element_comment varchar(128),
		Primary key(policy_name, element_type, element_name),
		Unique key(policy_name, element_type, element_id)
	);`
)

func upgradeEEToVer2(s sessionapi.Session, ver int64) {
	if ver >= eeversion2 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Max_user_connections` SMALLINT UNSIGNED DEFAULT 0 AFTER `Password_lifetime`")
}

func upgradeEEToVer3(s sessionapi.Session, ver int64) {
	if ver >= eeversion3 {
		return
	}
	doReentrantDDL(s, CreateLoginHistory)
}

func upgradeToEEVer4(s sessionapi.Session, ver int64) {
	if ver >= eeversion4 {
		return
	}
	doReentrantDDL(s, CreateRouteTable)
}

func upgradeToEEVer5(s sessionapi.Session, ver int64) {
	if ver >= eeversion5 {
		return
	}
	doReentrantDDL(s, CreateLSPolicies)
	doReentrantDDL(s, CreateLSTables)
	doReentrantDDL(s, CreateLSUsers)
	doReentrantDDL(s, CreateLSElements)
}

func upgradeToEEVer6(s sessionapi.Session, ver int64) {
	if ver >= eeversion6 {
		return
	}
	doReentrantDDL(s, CreateProcsPriv)
}

func upgradeToEEVer7(s sessionapi.Session, ver int64) {
	if ver >= eeversion7 {
		return
	}
	doReentrantDDL(s, audit.CreateFilterTableSQL)
	doReentrantDDL(s, audit.CreateFilterRuleTableSQL)
}

func upgradeToEEVer8(s sessionapi.Session, ver int64) {
	if ver >= eeversion8 {
		return
	}
	doReentrantDDL(s, whitelist.CreateWhitelistTableSQL)
}

func upgradeToEEVer9(s sessionapi.Session, ver int64) {
	if ver >= eeversion9 {
		return
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.tables_priv SET Column_priv=''")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.columns_priv SET Column_priv=''")
}

func upgradeToEEVer10(s sessionapi.Session, ver int64) {
	if ver >= eeversion10 {
		return
	}

	mustExecute(s, "ALTER TABLE mysql.login_history MODIFY COLUMN Time DATETIME(6) NOT NULL")
	mustExecute(s, "ALTER TABLE mysql.login_history ADD INDEX IF NOT EXISTS idx_time(Time)")
	if ver >= eeversion6 {
		mustExecute(s, "ALTER TABLE mysql.login_history REMOVE TTL")
	}
}

func upgradeToEEVer11(s sessionapi.Session, ver int64) {
	if ver >= eeversion11 {
		return
	}

	mustExecute(s, "ALTER TABLE `mysql`.`login_history` CHANGE COLUMN IF EXISTS Host Server_host char(255) NOT NULL DEFAULT ''")
	mustExecute(s, "ALTER TABLE `mysql`.`login_history` ADD COLUMN IF NOT EXISTS `User_host` char(255) NOT NULL DEFAULT '' AFTER `User`")
	mustExecute(s, "ALTER TABLE `mysql`.`login_history` DROP INDEX IF EXISTS idx_session_id")
	mustExecute(s, "ALTER TABLE `mysql`.`login_history` DROP INDEX IF EXISTS idx_user")
	mustExecute(s, "ALTER TABLE `mysql`.`login_history` ADD INDEX IF NOT EXISTS idx_user(User, User_host, Result, Time)")
}

func upgradeToEEVer12(s sessionapi.Session, ver int64) {
	if ver >= eeversion12 {
		return
	}
	doReentrantDDL(s, "ALTER TABLE `mysql`.`user` MODIFY COLUMN Max_user_connections INT UNSIGNED NOT NULL DEFAULT 0")
}

func writeLowerCaseTableNamesParameter(s sessionapi.Session, val int) {
	comment := "lower_case_table_names value. Do not edit it."
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, lowerCaseTableNames, val, comment, val,
	)
}

func upgradeToEEVer13(s sessionapi.Session, ver int64) {
	if ver >= eeversion13 {
		return
	}
	doReentrantDDL(s, CreateFuncTable)
}

func upgradeToEEVer14(s sessionapi.Session, ver int64) {
	if ver >= eeversion14 {
		return
	}
	// reserve for LBAC implementation.
}

func upgradeToEEVer15(s sessionapi.Session, ver int64) {
	if ver >= eeversion15 {
		return
	}
	// Forbid updating lower_case_table_names for existing clusters.
	writeLowerCaseTableNamesParameter(s, 2)
}

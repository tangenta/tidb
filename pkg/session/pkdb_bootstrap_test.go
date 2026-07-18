package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestOSSCreatedMaxUserConnections(t *testing.T) {
	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	// get the table structure of mysql.user for a fresh-bootstrapped store
	se := CreateSessionAndSetID(t, store)
	res := MustExecToRecodeSet(t, se, "SHOW CREATE TABLE mysql.user")
	chk := res.NewChunk(nil)
	err := res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row := chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	userTable := row.GetString(1)
	require.Contains(t, userTable, "`Max_user_connections` int(10) unsigned NOT NULL DEFAULT '0'")

	// prepare environment at v7.1.8-5.2. where Max_user_connections is SMALLINT
	oldUserTable := `CREATE TABLE IF NOT EXISTS mysql.user (
		Host					CHAR(255),
		User					CHAR(32),
		authentication_string	TEXT,
		plugin					CHAR(64),
		Select_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Repl_slave_priv	    	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Repl_client_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Shutdown_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Reload_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		FILE_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Config_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_Tablespace_Priv  ENUM('N','Y') NOT NULL DEFAULT 'N',
		Password_reuse_history  smallint unsigned DEFAULT NULL,
		Password_reuse_time     smallint unsigned DEFAULT NULL,
		User_attributes			json,
		Token_issuer			VARCHAR(255),
		Password_expired		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Password_last_changed	TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
		Password_lifetime		SMALLINT UNSIGNED DEFAULT NULL,
		Max_user_connections 	SMALLINT UNSIGNED DEFAULT 0,
		PRIMARY KEY (Host, User) /*T![clustered_index] NONCLUSTERED */);`
	MustExec(t, se, "DROP TABLE IF EXISTS mysql.user")
	MustExec(t, se, oldUserTable)

	// v7.1.8-5.2 is version220 and eeversion11
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(220)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	RevertVersionAndVariables(t, se, 220)
	txn, err = store.Begin()
	require.NoError(t, err)
	m = meta.NewMutator(txn)
	err = m.FinishBootstrapEE(eeversion11)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se, fmt.Sprintf(
		"update mysql.tidb set variable_value='%d' where variable_name='tidb_enterprise_edition_server_version'",
		eeversion11,
	))

	// Upgrade to latest version.
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()
	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := getBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	// get the table structure of mysql.user for the upgraded store
	se = CreateSessionAndSetID(t, store)
	res = MustExecToRecodeSet(t, se, "SHOW CREATE TABLE mysql.user")
	chk = res.NewChunk(nil)
	err = res.Next(ctx, chk)
	require.NoError(t, err)
	require.Equal(t, 1, chk.NumRows())
	row = chk.GetRow(0)
	require.Equal(t, 2, row.Len())
	userTable2 := row.GetString(1)
	require.Equal(t, userTable, userTable2)
}

func TestUpgradeCleansStaleRoutineMetadata(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "set global tidb_enable_procedure = on")
	MustExec(t, se, "drop database if exists Foo")
	MustExec(t, se, "drop database if exists foo")
	MustExec(t, se, "create database foo")
	MustExec(t, se, "use foo")
	MustExec(t, se, "create procedure p() begin select 1; end;")
	MustExec(t, se, `insert into mysql.routines
		select
			'Foo', name, type, definition, definition_utf8, parameter_str,
			is_deterministic, sql_data_access, security_type, definer, sql_mode,
			character_set_client, connection_collation, schema_collation,
			created, last_altered, comment, options, external_language
		from mysql.routines
		where route_schema = 'foo' and name = 'p' and type = 'PROCEDURE'`)
	MustExec(t, se, "commit")
	rows := MustExecToRecodeSet(t, se, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk := rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 2, chk.NumRows())
	require.Equal(t, "Foo", chk.GetRow(0).GetString(0))
	require.Equal(t, "foo", chk.GetRow(1).GetString(0))

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrapEE(eeversion15)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se, fmt.Sprintf(
		"update mysql.tidb set variable_value='%d' where variable_name='tidb_enterprise_edition_server_version'",
		eeversion15,
	))

	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := getBootstrapEEVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentEEBootstrapVersion, ver)
	rows = MustExecToRecodeSet(t, seCurVer, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk = rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "foo", chk.GetRow(0).GetString(0))
}

func TestUpgradeKeepsLiveRoutineMetadataWithoutExactCaseMatch(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "set global tidb_enable_procedure = on")
	MustExec(t, se, "drop database if exists Foo")
	MustExec(t, se, "drop database if exists foo")
	MustExec(t, se, "create database Foo")
	MustExec(t, se, "create procedure Foo.p() begin select 1; end;")
	MustExec(t, se, `update mysql.routines
		set route_schema = 'foo'
		where route_schema = 'Foo' and name = 'p' and type = 'PROCEDURE'`)
	MustExec(t, se, "commit")

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrapEE(eeversion15)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se, fmt.Sprintf(
		"update mysql.tidb set variable_value='%d' where variable_name='tidb_enterprise_edition_server_version'",
		eeversion15,
	))

	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	rows := MustExecToRecodeSet(t, seCurVer, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk := rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "foo", chk.GetRow(0).GetString(0))
}

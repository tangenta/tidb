package ddl_test

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/stretchr/testify/require"
)

func TestModifyNonclusteredPrimaryKeyColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	t.Run("single column primary key", func(t *testing.T) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`create table t (
			id decimal(20,0) not null,
			value varchar(10),
			primary key (id) nonclustered
		)`)
		tk.MustExec("insert into t values (-2, 'negative'), (0, 'zero'), (7, 'positive')")

		tk.MustExec("alter table t modify column id bigint")

		tk.MustQuery("select id, value from t use index(primary) order by id").Check(testkit.Rows(
			"-2 negative",
			"0 zero",
			"7 positive",
		))
		tk.MustExec("admin check table t")
	})

	t.Run("composite primary key with concurrent writes", func(t *testing.T) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`create table t (
			a decimal(20,0) not null,
			b decimal(20,0) not null,
			value varchar(10),
			primary key (a, b) nonclustered
		)`)
		tk.MustExec("insert into t values (1, 1, 'delete'), (1, 2, 'keep'), (2, 1, 'update')")
		tbl := external.GetTableByName(t, tk, "test", "t")

		var once sync.Once
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
			if job.TableID != tbl.Meta().ID || job.Type != model.ActionModifyColumn || job.SchemaState != model.StateWriteReorganization {
				return
			}
			once.Do(func() {
				tk2 := testkit.NewTestKit(t, store)
				tk2.MustExec("use test")
				tk2.MustExec("insert into t values (3, 1, 'insert')")
				tk2.MustExec("update t set a = 20, value = 'updated' where a = 2 and b = 1")
				tk2.MustExec("delete from t where a = 1 and b = 1")
			})
		})

		tk.MustExec("alter table t modify column a bigint")

		tk.MustQuery("select a, b, value from t use index(primary) order by a, b").Check(testkit.Rows(
			"1 2 keep",
			"3 1 insert",
			"20 1 updated",
		))
		tk.MustExec("admin check table t")

		// Reorganizing a non-leading primary key column should preserve the composite key as well.
		tk.MustExec("alter table t modify column b bigint")
		tk.MustQuery(`select key_name, seq_in_index, column_name, clustered
			from information_schema.tidb_indexes
			where table_schema = 'test' and table_name = 't'
			order by seq_in_index`).Check(testkit.Rows(
			"PRIMARY 1 a NO",
			"PRIMARY 2 b NO",
		))
		tk.MustExec("admin check table t")
	})

	t.Run("composite primary key conversion collision", func(t *testing.T) {
		tk.MustExec("drop table if exists t")
		tk.MustExec(`create table t (
			a int not null,
			b decimal(4,1) not null,
			value varchar(10),
			primary key (a, b) nonclustered
		)`)
		tk.MustExec("insert into t values (1, 1.1, 'first'), (1, 1.2, 'second')")

		err := tk.ExecToErr("alter table t modify column b bigint")
		require.ErrorContains(t, err, "Duplicate entry")

		tk.MustQuery("select a, b, value from t use index(primary) order by a, b").Check(testkit.Rows(
			"1 1.1 first",
			"1 1.2 second",
		))
		tk.MustExec("admin check table t")
	})
}

func TestModifyColumnCharsetFromUTF8MB4ToUTF8(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists eteams")
	tk.MustExec("create database eteams default character set utf8")
	tk.MustExec("use eteams")

	tk.MustExec(`create table tmp_test_utf8mb4_to_utf8(
		id bigint(20) not null,
		content longtext collate utf8mb4_unicode_ci,
		primary key (id) using btree
	)`)
	tk.MustExec("alter table tmp_test_utf8mb4_to_utf8 modify column content longtext")
	col := external.GetModifyColumn(t, tk, "eteams", "tmp_test_utf8mb4_to_utf8", "content", false)
	require.Equal(t, charset.CharsetUTF8, col.GetCharset())
	require.Equal(t, charset.CollationUTF8, col.GetCollate())
}

func TestAlterTableConvertCharsetFromUTF8MB4ToUTF8Rejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_convert_charset(
		id int primary key,
		c varchar(20) character set utf8mb4 collate utf8mb4_bin,
		unique key uk(c)
	) character set utf8mb4 collate utf8mb4_bin`)
	tk.MustExec("insert into t_convert_charset values (1, x'f09f8c80'), (2, 'text')")

	tk.MustGetErrCode("alter table t_convert_charset convert to character set utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)

	tbl := external.GetTableByName(t, tk, "test", "t_convert_charset")
	require.Equal(t, charset.CharsetUTF8MB4, tbl.Meta().Charset)
	require.Equal(t, charset.CollationUTF8MB4, tbl.Meta().Collate)
	col := external.GetModifyColumn(t, tk, "test", "t_convert_charset", "c", false)
	require.Equal(t, charset.CharsetUTF8MB4, col.GetCharset())
	require.Equal(t, charset.CollationUTF8MB4, col.GetCollate())
	tk.MustQuery("select id, hex(c) from t_convert_charset order by id").Check(testkit.Rows("1 F09F8C80", "2 74657874"))
	tk.MustExec("admin check table t_convert_charset")
}

func TestModifyColumnCharsetFromUTF8MB4ToUTF8RejectsInvalidData(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_invalid(id int primary key, c varchar(20) charset utf8mb4) charset utf8")
	tk.MustExec("insert into t_invalid values (1, x'f09f8c80')")
	tk.MustGetErrCode("alter table t_invalid modify column c varchar(20)", errno.ErrTruncatedWrongValueForField)

	col := external.GetModifyColumn(t, tk, "test", "t_invalid", "c", false)
	require.Equal(t, charset.CharsetUTF8MB4, col.GetCharset())
	tk.MustQuery("select hex(c) from t_invalid").Check(testkit.Rows("F09F8C80"))
	tk.MustExec("admin check table t_invalid")
}

func TestModifyColumnCharsetFromUTF8MB4ToUTF8WithIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_index(id int primary key, c varchar(20) charset utf8mb4 collate utf8mb4_unicode_ci, key idx(c)) charset utf8")
	tk.MustExec("insert into t_index values (1, 'abc'), (2, '中文')")
	tk.MustExec("alter table t_index modify column c varchar(20)")

	tk.MustQuery("select * from t_index order by id").Check(testkit.Rows("1 abc", "2 中文"))
	col := external.GetModifyColumn(t, tk, "test", "t_index", "c", false)
	require.Equal(t, charset.CharsetUTF8, col.GetCharset())
	tk.MustExec("admin check table t_index")
}

func TestModifyColumnCollationWithIndex(t *testing.T) {
	oldNewCollationEnabled := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(oldNewCollationEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists modify_collation")
	tk.MustExec("create database modify_collation default character set utf8")
	tk.MustExec("use modify_collation")

	tk.MustExec(`create table tmp_test_unsupported_modifying_collation(
		id bigint(20) not null,
		user_name varchar(100) character set utf8mb4 collate utf8mb4_general_ci default '' not null,
		account_name varchar(50) character set utf8mb4 collate utf8mb4_general_ci default null,
		primary key (id),
		key uk_ehu_user_name (user_name)
	)`)
	tk.MustExec("insert into tmp_test_unsupported_modifying_collation values (1, 'alice', 'a'), (2, 'ALICE', 'b')")
	tk.MustExec("alter table tmp_test_unsupported_modifying_collation modify user_name varchar(100) null")

	col := external.GetModifyColumn(t, tk, "modify_collation", "tmp_test_unsupported_modifying_collation", "user_name", false)
	require.Equal(t, charset.CharsetUTF8, col.GetCharset())
	require.Equal(t, charset.CollationUTF8, col.GetCollate())
	require.False(t, mysql.HasNotNullFlag(col.GetFlag()))
	tk.MustQuery("select id, user_name, account_name from tmp_test_unsupported_modifying_collation order by id").Check(testkit.Rows("1 alice a", "2 ALICE b"))
	tk.MustExec("admin check table tmp_test_unsupported_modifying_collation")
}

func TestModifyColumnCollationWithUniqueIndexDuplicate(t *testing.T) {
	oldNewCollationEnabled := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(oldNewCollationEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_dup_collate(id int primary key, c varchar(20) charset utf8mb4 collate utf8mb4_bin, unique key uk(c)) charset utf8mb4 collate utf8mb4_general_ci")
	tk.MustExec("insert into t_dup_collate values (1, 'a'), (2, 'A')")
	tk.MustGetErrCode("alter table t_dup_collate modify column c varchar(20)", errno.ErrDupEntry)

	col := external.GetModifyColumn(t, tk, "test", "t_dup_collate", "c", false)
	require.Equal(t, charset.CharsetUTF8MB4, col.GetCharset())
	require.Equal(t, charset.CollationUTF8MB4, col.GetCollate())
	tk.MustExec("admin check table t_dup_collate")
}

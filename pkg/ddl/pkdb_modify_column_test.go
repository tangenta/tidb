package ddl

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestAlterTableAddNonclusteredAutoIncrementPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	tk.MustExec("insert into t values (10), (20), (30)")

	tk.MustExec("alter table t " +
		"add column id bigint not null auto_increment, " +
		"add primary key (id) nonclustered")

	// Existing rows should be backfilled with distinct, non-null ids.
	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("3 3 0"))

	// AUTO_INCREMENT should keep working after the DDL.
	tk.MustExec("insert into t(v) values (40)")
	tk.MustQuery("select count(*), count(distinct id), sum(id is null) from t").
		Check(testkit.Rows("4 4 0"))

	// PK should be nonclustered and the new column should be AUTO_INCREMENT.
	tk.MustQuery("show create table t").CheckContain("AUTO_INCREMENT")
	tk.MustQuery("show create table t").CheckContain("NONCLUSTERED")
}

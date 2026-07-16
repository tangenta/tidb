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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestLoadableAndStoredFunctionInSameSelect(t *testing.T) {
	restore := expression.SetLoadUDFHookForTest(expression.MockLoadUDFForTest)
	t.Cleanup(restore)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("set global tidb_enable_procedure = ON")
	tk.MustExec("use test")
	tk.MustExec("drop function if exists myfunc_int")
	tk.MustExec("drop function if exists f1")
	tk.MustExec("create function myfunc_int returns integer soname 'udf1.so'")
	tk.MustExec("create function f1() returns int return 1")

	tk.MustQuery("select myfunc_int(1)").Check(testkit.Rows("2"))
	tk.MustQuery("select f1()").Check(testkit.Rows("1"))
	tk.MustQuery("select myfunc_int(1), f1()").Check(testkit.Rows("2 1"))
	tk.MustQuery("select f1(), myfunc_int(1)").Check(testkit.Rows("1 2"))
	tk.MustQuery("select myfunc_int(1), test.f1()").Check(testkit.Rows("2 1"))
}

// Copyright 2021 PingCAP, Inc.
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
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDDLStatementsBackFill(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	needReorg := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *model.Job) {
		if job.SchemaState == model.StateWriteReorganization {
			needReorg = true
		}
	})
	tk.MustExec("create table t (a int, b char(65));")
	tk.MustExec("insert into t values (1, '123');")
	testCases := []struct {
		ddlSQL            string
		expectedNeedReorg bool
	}{
		{"alter table t modify column a bigint;", false},
		{"alter table t modify column b char(255);", false},
		{"alter table t modify column a varchar(100);", true},
		{"create table t1 (a int, b int);", false},
		{"alter table t1 add index idx_a(a);", true},
		{"alter table t1 add primary key(b) nonclustered;", true},
		{"alter table t1 drop primary key;", false},
	}
	for _, tc := range testCases {
		needReorg = false
		tk.MustExec(tc.ddlSQL)
		require.Equal(t, tc.expectedNeedReorg, needReorg, tc)
	}
}

func TestTruncateTableDuringAllocID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")

	tk.MustExec("create table t (id bigint unsigned primary key auto_increment, c int);")

	beforeJobTxnCommit := newSyncChan("beforeJobTxnCommit")
	beforeAllocIDTxnCommit := newSyncChan("beforeAllocIDTxnCommit")
	afterJobTxnCommit := newSyncChan("afterJobTxnCommit")
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
		if job.Type == model.ActionTruncateTable {
			beforeJobTxnCommit.send("ddl job txn [2]")        // 2. before truncate table job txn commit.
			beforeAllocIDTxnCommit.receive("ddl job txn [5]") // 5. wait for ID allocation txn.
		}
	})
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/afterJobTxnCommit", func(job *model.Job) {
		afterJobTxnCommit.send("ddl job txn [6]") // 6. after truncate table job txn commit.
	})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		beforeJobTxnCommit.receive("insert stmt [3]") // 3. insert a row into table.
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/meta/autoid/duringAlloc4Unsigned", func() {
			beforeAllocIDTxnCommit.send("alloc internal txn [4]") // 4. before commiting ID allocation txn.
			afterJobTxnCommit.receive("alloc internal txn [7]")   // 7. wait for truncate table job txn commit finished.
		})
		tk2.MustExec("insert into t (c) values (1);")
	}()
	tk.MustExec("truncate table t;") // 1. start truncate table job.

	wg.Wait()
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

type syncChan struct {
	ch   chan struct{}
	name string
	sent bool
	rcvd bool
}

func newSyncChan(name string) *syncChan {
	return &syncChan{
		ch:   make(chan struct{}),
		name: name,
	}
}

func (s *syncChan) receive(as string) {
	if s.rcvd {
		return
	}
	logutil.BgLogger().Info("before receive", zap.String("name", s.name), zap.String("as", as))
	<-s.ch
	s.rcvd = true
	logutil.BgLogger().Info("after receive", zap.String("name", s.name), zap.String("as", as))
}

func (s *syncChan) send(from string) {
	if s.sent {
		return
	}
	logutil.BgLogger().Info("before send", zap.String("name", s.name), zap.String("from", from))
	close(s.ch)
	s.sent = true
	logutil.BgLogger().Info("after send", zap.String("name", s.name), zap.String("from", from))
}

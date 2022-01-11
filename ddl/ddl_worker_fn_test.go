// Copyright 2022 PingCAP, Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestParallelDDL(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	db1, db2 := model.NewCIStr("test_parallel_ddl_1"), model.NewCIStr("test_parallel_ddl_2")
	tb1, tb2, tb3 := model.NewCIStr("t1"), model.NewCIStr("t2"), model.NewCIStr("t3")
	tk.MustExec("use test;")
	tk.MustExec("create database test_parallel_ddl_1;")
	tk.MustExec("use test_parallel_ddl_1;")
	tk.MustExec("create table t1 (c1 int, c2 int);")
	tk.MustExec("insert into t1 values (1, 1), (2, 2);")
	tk.MustExec("create table t2 (c1 int primary key clustered, c2 int, c3 int);")
	tk.MustExec("insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3);")

	tk.MustExec("create database test_parallel_ddl_2;")
	tk.MustExec("use test_parallel_ddl_2;")
	tk.MustExec("create table t3 (c1 int, c2 int, c3 int, c4 int);")
	tk.MustExec("insert into t3 values (11, 22, 33, 44);")

	dom := domain.GetDomain(tk.Session())
	d := dom.DDL()
	once := sync.Once{}
	blocker := make(chan struct{})
	d.SetHook(&ddl.TestDDLCallback{
		Do: dom,
		OnJobRunBeforeExported: func(job *model.Job) {
			once.Do(func() {
				<-blocker
			})
		},
	})
	infoScm := dom.InfoSchema()
	dbInfo1, ok := infoScm.SchemaByName(db1)
	require.True(t, ok)
	dbInfo2, ok := infoScm.SchemaByName(db2)
	require.True(t, ok)
	tbInfo1, err := infoScm.TableByName(db1, tb1)
	require.NoError(t, err)
	tbInfo2, err := infoScm.TableByName(db1, tb2)
	require.NoError(t, err)
	tbInfo3, err := infoScm.TableByName(db2, tb3)
	require.NoError(t, err)
	tblInfo1, tblInfo2, tblInfo3 := tbInfo1.Meta(), tbInfo2.Meta(), tbInfo3.Meta()
	/*
		prepare jobs:
		/	job no.	/	database no.	/	table no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add column	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop column	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop column	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schema  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	job1 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx1", "c1")
	require.NoError(t, ddl.SendJobToEnqueue(d, job1))
	job2 := buildCreateColumnJob(dbInfo1, tblInfo1, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, nil)
	require.NoError(t, ddl.SendJobToEnqueue(d, job2))
	job3 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx2", "c3")
	require.NoError(t, ddl.SendJobToEnqueue(d, job3))
	job4 := buildDropColumnJob(dbInfo1, tblInfo2, "c3")
	require.NoError(t, ddl.SendJobToEnqueue(d, job4))
	job5 := buildDropIdxJob(dbInfo1, tblInfo1, "db1_idx1")
	require.NoError(t, ddl.SendJobToEnqueue(d, job5))
	job6 := buildCreateIdxJob(dbInfo1, tblInfo2, false, "db2_idx1", "c2")
	require.NoError(t, ddl.SendJobToEnqueue(d, job6))
	job7 := buildDropColumnJob(dbInfo2, tblInfo3, "c4")
	require.NoError(t, ddl.SendJobToEnqueue(d, job7))
	job8 := buildRebaseAutoIDJobJob(dbInfo2, tblInfo3, 1024)
	require.NoError(t, ddl.SendJobToEnqueue(d, job8))
	job9 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx3", "c2")
	require.NoError(t, ddl.SendJobToEnqueue(d, job9))
	job10 := buildDropSchemaJob(dbInfo2)
	require.NoError(t, ddl.SendJobToEnqueue(d, job10))
	job11 := buildCreateIdxJob(dbInfo2, tblInfo3, false, "db3_idx1", "c2")
	require.NoError(t, ddl.SendJobToEnqueue(d, job11))

	blocker <- struct{}{} // unblock the workers

	// check results.
	var checkErr error
	isChecked := false
	for !isChecked {
		err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			lastJob, err := m.GetHistoryDDLJob(job11.ID)
			require.NoError(t, err)
			// all jobs are finished.
			if lastJob != nil {
				finishedJobs, err := m.GetAllHistoryDDLJobs()
				require.NoError(t, err)
				// get the last 12 jobs completed.
				finishedJobs = finishedJobs[len(finishedJobs)-11:]
				// check some jobs are ordered because of the dependence.
				require.Equal(t, finishedJobs[0].ID, job1.ID, finishedJobs)
				require.Equal(t, finishedJobs[1].ID, job2.ID, finishedJobs)
				require.Equal(t, finishedJobs[2].ID, job3.ID, finishedJobs)
				require.Equal(t, finishedJobs[4].ID, job5.ID, finishedJobs)
				require.Equal(t, finishedJobs[10].ID, job11.ID, finishedJobs)
				// check the jobs are ordered in the backfill-job queue or general-job queue.
				backfillJobID := int64(0)
				generalJobID := int64(0)
				for _, job := range finishedJobs {
					// check jobs' order.
					if ddl.MayNeedReorg(job) {
						require.Greater(t, job.ID, backfillJobID)
						backfillJobID = job.ID
					} else {
						require.Greater(t, job.ID, generalJobID)
						generalJobID = job.ID
					}
					// check jobs' state.
					if job.ID == lastJob.ID {
						require.Equal(t, model.JobStateCancelled, job.State, job)
					} else {
						require.Equal(t, model.JobStateSynced, job.State, job)
					}
				}
				isChecked = true
			}
			return nil
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, checkErr)
}

func buildCreateIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexPartSpecification{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}
}

func buildCreateColumnJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string,
	pos *ast.ColumnPosition, defaultValue interface{}) *model.Job {
	col := &model.ColumnInfo{
		Name:               model.NewCIStr(colName),
		Offset:             len(tblInfo.Columns),
		DefaultValue:       defaultValue,
		OriginDefaultValue: defaultValue,
	}
	tblInfo.MaxColumnID++
	col.ID = tblInfo.MaxColumnID
	col.FieldType = *types.NewFieldType(mysql.TypeLong)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, pos, 0},
	}
	return job
}

func buildDropColumnJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string) *model.Job {
	return &model.Job{
		SchemaID:        dbInfo.ID,
		TableID:         tblInfo.ID,
		Type:            model.ActionDropColumn,
		BinlogInfo:      &model.HistoryInfo{},
		MultiSchemaInfo: &model.MultiSchemaInfo{},
		Args:            []interface{}{model.NewCIStr(colName)},
	}
}

func buildDropIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	tp := model.ActionDropIndex
	if indexName == "primary" {
		tp = model.ActionDropPrimaryKey
	}
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       tp,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(indexName)},
	}
}

func buildRebaseAutoIDJobJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, newBaseID int64) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionRebaseAutoID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newBaseID},
	}
}

func buildDropSchemaJob(dbInfo *model.DBInfo) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
}

// Copyright 2023-2023 PingCAP, Inc.

package gcworker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func setEnableLogHistoryStatus(enable bool) {
	vardef.EnableLoginHistory.Store(enable)
}

func setLoginHistoryRetainDuration(dur time.Duration) {
	vardef.LoginHistoryRetainDuration.Store(dur)
}

func TestTickGCSysTable(t *testing.T) {
	// create mock store
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().InRestrictedSQL = false

	// create gcworker
	gw, err := gcworker.NewGCWorker(store, nil)
	require.NoError(t, err)

	// query from mysql.login_history and get 0 rows.
	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 0)

	// insert 2 records.
	cur := time.Now()
	loginTimes := []time.Time{
		cur.Add(-(vardef.LoginHistoryRetainDuration.Load() + time.Second)),
		cur,
	}
	for _, loginTime := range loginTimes {
		sql := fmt.Sprintf(
			`insert into mysql.LOGIN_HISTORY VALUES("%s", "10.244.4.0", "root", "127.0.0.100", "mydb", 7602485189326930323, 'success', "127.0.0.100", "")`,
			loginTime.Format("2006-01-02 15:04:05.999999"),
		)
		tk.MustExec(sql)
	}

	// check 2 records in mysql.login_history.
	rows = tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 2)

	// the records do not GC because the EnableLoginHistory is disabled.
	err = gw.TickGCSysTable(context.TODO())
	require.NoError(t, err)
	rows = tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 2)

	// enable the EnableLoginHistory and gc one records.
	setEnableLogHistoryStatus(true)
	err = gw.TickGCSysTable(context.TODO())
	require.NoError(t, err)
	tk.MustQuery("SELECT * FROM mysql.login_history").Check(testkit.Rows(
		loginTimes[1].Format("2006-01-02 15:04:05.999999") + " 10.244.4.0 root 127.0.0.100 mydb 7602485189326930323 success 127.0.0.100 ",
	))
}

type mockPDClient struct {
	pd.Client
}

func (m mockPDClient) GetGCInternalController(uint32) pdgc.InternalController {
	return nil
}

func TestTickGCSysTableAfterInterval(t *testing.T) {
	// create mock store
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().InRestrictedSQL = false

	// set enable logHistory and set retain duration 0s.
	setEnableLogHistoryStatus(true)
	setLoginHistoryRetainDuration(0)

	// create gcworker
	gw, err := gcworker.NewGCWorker(store, mockPDClient{})
	require.NoError(t, err)

	// TickGCSysTable firstly.
	err = gw.TickGCSysTable(context.TODO())
	require.NoError(t, err)

	// insert 2 records.
	cur := time.Now()
	loginTimes := []time.Time{
		cur.Add(-(vardef.LoginHistoryRetainDuration.Load() + time.Second)),
		cur.Add(-time.Minute),
	}
	for _, loginTime := range loginTimes {
		sql := fmt.Sprintf(
			`insert into mysql.LOGIN_HISTORY VALUES("%s", "10.244.4.0", "root", "127.0.0.100", "mydb", 7602485189326930323, 'success', "127.0.0.100", "")`,
			loginTime.Format("2006-01-02 15:04:05.999999"),
		)
		tk.MustExec(sql)
	}

	// gc nothing because no more than 10 minutes interval have passed since the last GC.
	err = gw.TickGCSysTable(context.TODO())
	require.NoError(t, err)
	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 2)

	// gc all of records.
	gw.UpdateLastGCSystem(cur.Add(-(time.Minute*10 + time.Second)))
	err = gw.TickGCSysTable(context.TODO())
	require.NoError(t, err)
	rows = tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 0)
}

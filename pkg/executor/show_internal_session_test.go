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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestReleaseSysSessionRestoresSessionVars(t *testing.T) {
	do := domain.NewDomain(nil, time.Millisecond, 0, 0, pools.Factory(func() (pools.Resource, error) {
		return newMockSysSession(), nil
	}))

	caller := mock.NewContext()
	caller.BindDomain(do)
	baseExecutor := exec.NewBaseExecutor(caller, nil, 0)

	sysCtx, err := baseExecutor.GetSysSession()
	require.NoError(t, err)
	originalVars := getSystemSessionVarValues(t, sysCtx)

	require.NoError(t, sysCtx.GetSessionVars().SetSystemVar(variable.SQLModeVar, "ANSI_QUOTES"))
	require.NoError(t, sysCtx.GetSessionVars().SetSystemVar(variable.CharacterSetClient, "latin1"))
	require.NoError(t, sysCtx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, "latin1"))
	require.NoError(t, sysCtx.GetSessionVars().SetSystemVar(variable.CollationConnection, "latin1_bin"))
	require.NotEqual(t, originalVars, getSystemSessionVarValues(t, sysCtx))

	baseExecutor.ReleaseSysSession(context.Background(), sysCtx)

	reusedSysCtx, err := baseExecutor.GetSysSession()
	require.NoError(t, err)
	defer baseExecutor.ReleaseSysSession(context.Background(), reusedSysCtx)
	require.Same(t, sysCtx, reusedSysCtx)
	require.Equal(t, originalVars, getSystemSessionVarValues(t, reusedSysCtx))
}

type mockSysSession struct {
	*mock.Context
}

func newMockSysSession() *mockSysSession {
	sctx := &mockSysSession{Context: mock.NewContext()}
	for name, value := range map[string]string{
		variable.SQLModeVar:             mysql.DefaultSQLMode,
		variable.CharacterSetClient:     "utf8mb4",
		variable.CharacterSetConnection: "utf8mb4",
		variable.CollationConnection:    "utf8mb4_bin",
	} {
		if err := sctx.GetSessionVars().SetSystemVar(name, value); err != nil {
			panic(err)
		}
	}
	return sctx
}

func (s *mockSysSession) ExecuteInternal(_ context.Context, sql string, _ ...any) (sqlexec.RecordSet, error) {
	if sql == "rollback" {
		return nil, nil
	}
	return nil, errors.New("unsupported internal SQL")
}

func (s *mockSysSession) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *mockSysSession) Close() {}

func getSystemSessionVarValues(t *testing.T, sctx sessionctx.Context) map[string]string {
	t.Helper()

	values := make(map[string]string, 4)
	for _, name := range []string{
		variable.SQLModeVar,
		variable.CharacterSetClient,
		variable.CharacterSetConnection,
		variable.CollationConnection,
	} {
		value, ok := sctx.GetSessionVars().GetSystemVar(name)
		require.True(t, ok, name)
		values[name] = value
	}
	return values
}

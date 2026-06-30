// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeKeyspacePDClient struct {
	keyspaces []*keyspacepb.KeyspaceMeta
	closed    bool
	calls     []getAllKeyspacesCall
}

type getAllKeyspacesCall struct {
	startID uint32
	limit   uint32
}

func (c *fakeKeyspacePDClient) GetAllKeyspaces(_ context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error) {
	c.calls = append(c.calls, getAllKeyspacesCall{startID: startID, limit: limit})
	if limit == 0 {
		return c.keyspaces, nil
	}
	result := make([]*keyspacepb.KeyspaceMeta, 0, limit)
	for _, keyspaceMeta := range c.keyspaces {
		if keyspaceMeta == nil || keyspaceMeta.GetId() < startID {
			continue
		}
		result = append(result, keyspaceMeta)
		if len(result) >= int(limit) {
			break
		}
	}
	return result, nil
}

func (c *fakeKeyspacePDClient) Close() {
	c.closed = true
}

func TestAutoDetectKeyspaceName(t *testing.T) {
	originNewKeyspacePDClient := newKeyspacePDClient
	defer func() {
		newKeyspacePDClient = originNewKeyspacePDClient
	}()

	var pdClient *fakeKeyspacePDClient
	newKeyspacePDClient = func(
		ctx context.Context,
		callerComponent caller.Component,
		pdAddrs []string,
		security pd.SecurityOption,
		opts ...opt.ClientOption,
	) (keyspacePDClient, error) {
		require.Equal(t, []string{"127.0.0.1:2379"}, pdAddrs)
		require.NotEmpty(t, opts)
		pdClient = &fakeKeyspacePDClient{
			keyspaces: []*keyspacepb.KeyspaceMeta{
				{
					Id:    1,
					Name:  "ultra_default",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
		}
		return pdClient, nil
	}

	cfg := &task.Config{PD: []string{"127.0.0.1:2379"}}
	err := autoDetectKeyspaceName(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, "ultra_default", cfg.KeyspaceName)
	require.NotNil(t, pdClient)
	require.Equal(t, []getAllKeyspacesCall{
		{startID: constants.DefaultKeyspaceID, limit: autoDetectKeyspaceBatchSize},
	}, pdClient.calls)
	require.True(t, pdClient.closed)
}

func TestAutoDetectKeyspaceNamePaginatesKeyspaces(t *testing.T) {
	disabledKeyspaces := make([]*keyspacepb.KeyspaceMeta, 0, autoDetectKeyspaceBatchSize)
	disabledKeyspaces = append(disabledKeyspaces, &keyspacepb.KeyspaceMeta{
		Id:    constants.DefaultKeyspaceID,
		Name:  constants.DefaultKeyspaceName,
		State: keyspacepb.KeyspaceState_ENABLED,
	})
	for id := uint32(1); id < autoDetectKeyspaceBatchSize; id++ {
		disabledKeyspaces = append(disabledKeyspaces, &keyspacepb.KeyspaceMeta{
			Id:    id,
			Name:  "disabled",
			State: keyspacepb.KeyspaceState_DISABLED,
		})
	}

	pdClient := &fakeKeyspacePDClient{
		keyspaces: append(disabledKeyspaces, &keyspacepb.KeyspaceMeta{
			Id:    autoDetectKeyspaceBatchSize,
			Name:  "a",
			State: keyspacepb.KeyspaceState_ENABLED,
		}),
	}
	keyspaceName, err := detectOnlyUserKeyspace(context.Background(), pdClient)
	require.NoError(t, err)
	require.Equal(t, "a", keyspaceName)
	require.Equal(t, []getAllKeyspacesCall{
		{startID: constants.DefaultKeyspaceID, limit: autoDetectKeyspaceBatchSize},
		{startID: autoDetectKeyspaceBatchSize, limit: autoDetectKeyspaceBatchSize},
	}, pdClient.calls)
}

func TestAutoDetectKeyspaceNameStopsAfterSecondUserKeyspace(t *testing.T) {
	keyspaces := []*keyspacepb.KeyspaceMeta{
		{
			Id:    constants.DefaultKeyspaceID,
			Name:  constants.DefaultKeyspaceName,
			State: keyspacepb.KeyspaceState_ENABLED,
		},
		{
			Id:    1,
			Name:  "a",
			State: keyspacepb.KeyspaceState_ENABLED,
		},
	}
	for id := uint32(2); id < autoDetectKeyspaceBatchSize; id++ {
		keyspaces = append(keyspaces, &keyspacepb.KeyspaceMeta{
			Id:    id,
			Name:  "disabled",
			State: keyspacepb.KeyspaceState_DISABLED,
		})
	}
	keyspaces = append(keyspaces,
		&keyspacepb.KeyspaceMeta{
			Id:    autoDetectKeyspaceBatchSize,
			Name:  "b",
			State: keyspacepb.KeyspaceState_ENABLED,
		},
		&keyspacepb.KeyspaceMeta{
			Id:    autoDetectKeyspaceBatchSize + 1,
			Name:  "c",
			State: keyspacepb.KeyspaceState_ENABLED,
		},
	)

	pdClient := &fakeKeyspacePDClient{keyspaces: keyspaces}
	keyspaceName, err := detectOnlyUserKeyspace(context.Background(), pdClient)
	require.EqualError(t, err, "multiple keyspaces found in PD: a, b, please specify --keyspace-name explicitly")
	require.Empty(t, keyspaceName)
	require.Equal(t, []getAllKeyspacesCall{
		{startID: constants.DefaultKeyspaceID, limit: autoDetectKeyspaceBatchSize},
		{startID: autoDetectKeyspaceBatchSize, limit: autoDetectKeyspaceBatchSize},
	}, pdClient.calls)
}

func TestAutoDetectKeyspaceNameSkipsExplicitKeyspace(t *testing.T) {
	originNewKeyspacePDClient := newKeyspacePDClient
	defer func() {
		newKeyspacePDClient = originNewKeyspacePDClient
	}()

	newKeyspacePDClient = func(
		context.Context,
		caller.Component,
		[]string,
		pd.SecurityOption,
		...opt.ClientOption,
	) (keyspacePDClient, error) {
		require.Fail(t, "PD client should not be created when keyspace name is explicit")
		return nil, nil
	}

	cfg := &task.Config{KeyspaceName: "a"}
	err := autoDetectKeyspaceName(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, "a", cfg.KeyspaceName)
}

func TestIsKeyspaceAutoDetectUnsupportedErr(t *testing.T) {
	require.True(t, isKeyspaceAutoDetectUnsupportedErr(status.Error(codes.Unimplemented, "not supported")))
	require.True(t, isKeyspaceAutoDetectUnsupportedErr(stderrors.New("unknown service keyspacepb.Keyspace")))
	require.False(t, isKeyspaceAutoDetectUnsupportedErr(nil))
}

func TestDetectOnlyUserKeyspace(t *testing.T) {
	tests := []struct {
		name          string
		keyspaces     []*keyspacepb.KeyspaceMeta
		expected      string
		expectedError string
	}{
		{
			name:     "no keyspaces",
			expected: "",
		},
		{
			name: "built-in keyspaces only",
			keyspaces: []*keyspacepb.KeyspaceMeta{
				{
					Id:    constants.DefaultKeyspaceID,
					Name:  constants.DefaultKeyspaceName,
					State: keyspacepb.KeyspaceState_ENABLED,
				},
				{
					Id:    1,
					Name:  systemKeyspaceName,
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
			expected: "",
		},
		{
			name: "single user keyspace",
			keyspaces: []*keyspacepb.KeyspaceMeta{
				{
					Id:    constants.DefaultKeyspaceID,
					Name:  constants.DefaultKeyspaceName,
					State: keyspacepb.KeyspaceState_ENABLED,
				},
				{
					Id:    1,
					Name:  "ultra_default",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
			expected: "ultra_default",
		},
		{
			name: "ignore non-enabled keyspaces",
			keyspaces: []*keyspacepb.KeyspaceMeta{
				{
					Id:    1,
					Name:  "disabled",
					State: keyspacepb.KeyspaceState_DISABLED,
				},
				{
					Id:    2,
					Name:  "archived",
					State: keyspacepb.KeyspaceState_ARCHIVED,
				},
				{
					Id:    3,
					Name:  "active",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
			expected: "active",
		},
		{
			name: "multiple user keyspaces",
			keyspaces: []*keyspacepb.KeyspaceMeta{
				{
					Id:    1,
					Name:  "a",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
				{
					Id:    2,
					Name:  "ultra_default",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
			expectedError: "multiple keyspaces found in PD: a, ultra_default, please specify --keyspace-name explicitly",
		},
		{
			name: "nil meta ignored",
			keyspaces: []*keyspacepb.KeyspaceMeta{
				nil,
				{
					Id:    1,
					Name:  "a",
					State: keyspacepb.KeyspaceState_ENABLED,
				},
			},
			expected: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := detectOnlyUserKeyspace(context.Background(), &fakeKeyspacePDClient{keyspaces: tt.keyspaces})
			if tt.expectedError != "" {
				require.EqualError(t, err, tt.expectedError)
				require.Empty(t, actual)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

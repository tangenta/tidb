// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type storageWithCodec struct {
	kv.Storage
	codec tikv.Codec
}

func (s *storageWithCodec) GetCodec() tikv.Codec {
	return s.codec
}

type clientMgrWithCodec struct {
	*conn.Mgr
	storage *storageWithCodec
}

func newClientMgrWithCodec(mgr *conn.Mgr, codec tikv.Codec) *clientMgrWithCodec {
	return &clientMgrWithCodec{
		Mgr:     mgr,
		storage: &storageWithCodec{codec: codec},
	}
}

func (m *clientMgrWithCodec) GetStorage() kv.Storage {
	return m.storage
}

func TestOnBackupResponseDecodesKeyspaceLock(t *testing.T) {
	s := createBackupSuite(t)
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 101})
	require.NoError(t, err)
	s.mockMgr.storage.codec = codec

	key := []byte("key")
	primary := []byte("primary")
	secondary := []byte("secondary")
	lockInfo := &kvrpcpb.LockInfo{
		Key:            codec.EncodeKey(key),
		PrimaryLock:    codec.EncodeKey(primary),
		Secondaries:    [][]byte{codec.EncodeKey(secondary)},
		LockVersion:    1,
		LockTtl:        1,
		LockType:       kvrpcpb.Op_Put,
		UseAsyncCommit: true,
	}
	response := &backup.ResponseAndStore{
		Resp: &backuppb.BackupResponse{
			Error: &backuppb.Error{
				Detail: &backuppb.Error_KvError{
					KvError: &kvrpcpb.KeyError{Locked: lockInfo},
				},
			},
		},
	}

	lock, err := s.backupClient.OnBackupResponse(
		context.Background(), response, utils.NewErrorContext("test", 1), nil)
	require.NoError(t, err)
	require.Equal(t, key, lock.Key)
	require.Equal(t, primary, lock.Primary)
	require.Equal(t, [][]byte{secondary}, lockInfo.Secondaries)
}

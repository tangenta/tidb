// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/tikv"
)

// decodeBackupLockInfo converts the physical keys returned directly by the
// Backup RPC into the logical keys expected by client-go's lock resolver.
func decodeBackupLockInfo(codec tikv.Codec, lock *kvrpcpb.LockInfo) error {
	key, err := codec.DecodeKey(lock.GetKey())
	if err != nil {
		return errors.Annotate(err, "decode backup lock key")
	}
	primary, err := codec.DecodeKey(lock.GetPrimaryLock())
	if err != nil {
		return errors.Annotate(err, "decode backup lock primary")
	}
	var secondaries [][]byte
	if lock.Secondaries != nil {
		secondaries = make([][]byte, len(lock.Secondaries))
	}
	for i, secondary := range lock.GetSecondaries() {
		secondaries[i], err = codec.DecodeKey(secondary)
		if err != nil {
			return errors.Annotatef(err, "decode backup lock secondary %d", i)
		}
	}

	lock.Key = key
	lock.PrimaryLock = primary
	lock.Secondaries = secondaries
	return nil
}

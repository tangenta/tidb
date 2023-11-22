// Copyright 2023 PingCAP, Inc.
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

package meta

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
)

var mDistributedLock = []byte("DistributedLock")

const (
	lockRefreshInterval = 1 * time.Minute
	lockAcquireInterval = 5 * time.Second
)

// LockDistributed locks the distributed lock.
func (m *Meta) LockDistributed(key, val []byte) error {
	val, err := m.txn.Get(mDistributedLock)
	if err != nil {
		return err
	}
	return errors.Trace(m.txn.Set(mDistributedLock, val))
}

func (m *Meta) UnlockDistributed(key, val []byte) error {
	return errors.Trace(m.txn.Clear(mDistributedLock))
}

type DistLock struct {
	StartTS    uint64 `json:"start_ts"`
	InstanceID string `json:"instance_id"`
}

type DistLockManager struct {
	store kv.Storage
}

func (m *DistLockManager) tryAcquireLock(ctx context.Context, key string) error {
	tick := time.NewTicker(lockAcquireInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-tick.C:
			err := kv.RunInNewTxn(ctx, m.store, true, func(ctx context.Context, txn kv.Transaction) error {

			})
			if err != nil {
				return err
			}
		}
	}
}

func acquireLock(key string) (bool, error) {
	lock := &DistLock{
		StartTS:    oracle.GetTimestamp(),
		InstanceID: instanceID,
	}
	val, err := json.Marshal(lock)
	if err != nil {
		return false, errors.Trace(err)
	}
	err = meta.LockDistributed([]byte(key), val)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

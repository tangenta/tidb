// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

const serviceSafePointCleanupTimeout = 10 * time.Second

// gcProtectionKeeper owns an acknowledged registration and its renewal worker.
// done is closed only after renewal has stopped and deletion has finished.
type gcProtectionKeeper struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// stop waits for deletion; cancellation and waiting on a closed channel are
// safe to repeat when both Dump and Close release the same keeper.
func (k *gcProtectionKeeper) stop() {
	if k == nil {
		return
	}
	k.cancel()
	<-k.done
}

// startGCProtection reuses the constructor's keeper or starts a fresh one for
// a subsequent Dump. The handle is published only after PD acknowledges it.
func (d *Dumper) startGCProtection() error {
	d.gcProtectionMu.Lock()
	defer d.gcProtectionMu.Unlock()
	if err := d.tctx.Err(); err != nil {
		return errors.Trace(err)
	}
	if d.gcProtection != nil {
		return nil
	}
	snapshotTS, err := parseSnapshotToTSO(d.tctx, d.dbHandle, d.conf.Snapshot)
	if err != nil {
		return err
	}
	if d.tidbUseKeyspaceGC {
		d.gcProtection, err = startKeyspaceGCBarrierKeeper(d.tctx, d.tidbPDClientForGC, d.tidbKeyspaceID, defaultDumpGCSafePointTTL, snapshotTS)
	} else {
		d.gcProtection, err = startServiceSafePointKeeper(d.tctx, d.tidbPDClientForGC, defaultDumpGCSafePointTTL, snapshotTS)
	}
	return err
}

// stopGCProtection releases the current registration without canceling the
// export context, which still owns the SQL connections and the Dumper itself.
func (d *Dumper) stopGCProtection() {
	d.gcProtectionMu.Lock()
	defer d.gcProtectionMu.Unlock()
	d.gcProtection.stop()
	d.gcProtection = nil
}

// startGCProtectionKeeper confirms the first registration before returning.
// Renewal retries preserve the master policy; failed startup owns no deletion.
// ttl must be positive, and the RPC callbacks must honor context cancellation.
func startGCProtectionKeeper(
	tctx *tcontext.Context,
	ttl int64,
	snapshotTS uint64,
	update func(ctx context.Context, protectTS uint64, retryCnt int) error,
	cleanup func(ctx context.Context) error,
) (*gcProtectionKeeper, error) {
	keeperCtx, cancel := tctx.WithCancel()
	// Protect the snapshot without underflow when no snapshot TSO is available.
	protectTS := snapshotTS
	if protectTS > 0 {
		protectTS--
	}
	if err := keeperCtx.Err(); err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	if err := update(keeperCtx, protectTS, 0); err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	removeRegistration := func() {
		// Cancellation must stop renewal before deletion, but must not cancel the
		// bounded cleanup RPC itself. Preserve any context values needed by PD.
		ctx, cancelCleanup := context.WithTimeout(context.WithoutCancel(tctx), serviceSafePointCleanupTimeout)
		defer cancelCleanup()
		if err := cleanup(ctx); err != nil {
			tctx.L().Warn("failed to remove GC protection; registration will expire at its TTL", zap.Error(err))
		}
	}
	if err := keeperCtx.Err(); err != nil {
		cancel()
		removeRegistration()
		return nil, errors.Trace(err)
	}

	k := &gcProtectionKeeper{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(k.done)
		defer cancel()
		defer removeRegistration()
		// The first update already succeeded. Renew after half the TTL instead
		// of issuing another registration immediately from the goroutine.
		tick := time.NewTicker(time.Duration(ttl) * time.Second / 2)
		defer tick.Stop()
		for {
			select {
			case <-keeperCtx.Done():
				return
			case <-tick.C:
			}
			for retryCnt := range 11 {
				if keeperCtx.Err() != nil {
					return
				}
				if err := update(keeperCtx, protectTS, retryCnt); err == nil {
					break
				}
				select {
				case <-keeperCtx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
	return k, nil
}

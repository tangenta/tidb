// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// dumplingGCProtectionClient implements only the PD operations owned by a dump.
// Unexpected calls fail through the nil embedded interface.
type dumplingGCProtectionClient struct {
	pd.Client
	update   func(context.Context, string, int64, uint64) (uint64, error)
	keyspace *dumplingGCProtectionStatesClient
}

// UpdateServiceGCSafePoint forwards global registration and deletion to assertions.
func (c *dumplingGCProtectionClient) UpdateServiceGCSafePoint(ctx context.Context, id string, ttl int64, ts uint64) (uint64, error) {
	return c.update(ctx, id, ttl, ts)
}

// GetGCStatesClient returns the fake client for the requested keyspace.
func (c *dumplingGCProtectionClient) GetGCStatesClient(uint32) pdgc.GCStatesClient {
	return c.keyspace
}

// dumplingGCProtectionStatesClient isolates the modern keyspace barrier API.
type dumplingGCProtectionStatesClient struct {
	pdgc.GCStatesClient
	set    func(context.Context, string, uint64, time.Duration) (*pdgc.GCBarrierInfo, error)
	delete func(context.Context, string) (*pdgc.GCBarrierInfo, error)
}

// SetGCBarrier forwards initial registration and renewal to assertions.
func (c *dumplingGCProtectionStatesClient) SetGCBarrier(ctx context.Context, id string, ts uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	return c.set(ctx, id, ts, ttl)
}

// DeleteGCBarrier forwards acknowledged barrier deletion to assertions.
func (c *dumplingGCProtectionStatesClient) DeleteGCBarrier(ctx context.Context, id string) (*pdgc.GCBarrierInfo, error) {
	return c.delete(ctx, id)
}

// newGCProtectionDumper supplies a numeric snapshot requiring no SQL lookup.
func newGCProtectionDumper(t *testing.T, client pd.Client, keyspace bool) *Dumper {
	conf := defaultConfigForTest(t)
	conf.Snapshot = "100"
	tctx, cancel := tcontext.Background().WithCancel()
	t.Cleanup(cancel)
	return &Dumper{
		tctx:              tctx,
		cancelCtx:         cancel,
		conf:              conf,
		metrics:           newMetrics(conf.PromFactory, conf.Labels),
		tidbPDClientForGC: client,
		tidbUseKeyspaceGC: keyspace,
		tidbKeyspaceID:    42,
	}
}

func TestDumplingGCProtectionStartupError(t *testing.T) {
	// Fail the first RPC in both GC modes and through both entry points. The
	// original PD error must return before SQL/export work, without a deletion.
	for _, keyspace := range []bool{false, true} {
		for _, entry := range []string{"initialization", "Dump"} {
			t.Run(gcProtectionTestName(keyspace)+"/"+entry, func(t *testing.T) {
				registerErr := errors.New("registration failed")
				calls := 0
				client := &dumplingGCProtectionClient{
					update: func(_ context.Context, _ string, ttl int64, _ uint64) (uint64, error) {
						calls++
						require.Positive(t, ttl)
						return 0, registerErr
					},
					keyspace: &dumplingGCProtectionStatesClient{
						set: func(context.Context, string, uint64, time.Duration) (*pdgc.GCBarrierInfo, error) {
							calls++
							return nil, registerErr
						},
					},
				}
				d := newGCProtectionDumper(t, client, keyspace)
				var err error
				if entry == "Dump" {
					err = d.Dump()
				} else {
					err = tidbStartGCSavepointUpdateService(d)
				}
				require.ErrorIs(t, err, registerErr)
				require.Equal(t, 1, calls)
				require.Nil(t, d.gcProtection)
				require.NoError(t, d.Close())
				require.Equal(t, 1, calls)
			})
		}
	}
}

func gcProtectionTestName(keyspace bool) string {
	if keyspace {
		return "keyspace barrier"
	}
	return "global safepoint"
}

func TestDumplingGCProtectionCleanup(t *testing.T) {
	// Register in each mode, optionally cancel the parent, and close the Dumper.
	// Cleanup must use a live bounded context and finish before Close returns,
	// including when deletion fails; repeated Close must not delete twice.
	for _, keyspace := range []bool{false, true} {
		for _, cancelDump := range []bool{false, true} {
			for _, cleanupErr := range []error{nil, errors.New("PD unavailable")} {
				name := gcProtectionTestName(keyspace)
				if cancelDump {
					name += "/canceled"
				}
				if cleanupErr != nil {
					name += "/deletion failure"
				}
				t.Run(name, func(t *testing.T) {
					var registrationCtx context.Context
					var registrationID string
					registrations, deletions := 0, 0
					assertCleanup := func(ctx context.Context, id string) {
						deletions++
						require.ErrorIs(t, registrationCtx.Err(), context.Canceled)
						require.NoError(t, ctx.Err())
						deadline, ok := ctx.Deadline()
						require.True(t, ok)
						require.WithinDuration(t, time.Now().Add(serviceSafePointCleanupTimeout), deadline, time.Second)
						require.Equal(t, registrationID, id)
					}
					client := &dumplingGCProtectionClient{
						update: func(ctx context.Context, id string, ttl int64, ts uint64) (uint64, error) {
							if ttl == 0 {
								assertCleanup(ctx, id)
								require.Zero(t, ts)
								return 0, cleanupErr
							}
							registrations++
							registrationCtx, registrationID = ctx, id
							require.Equal(t, uint64(99), ts)
							return ts, nil
						},
						keyspace: &dumplingGCProtectionStatesClient{
							set: func(ctx context.Context, id string, ts uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
								registrations++
								registrationCtx, registrationID = ctx, id
								require.Equal(t, uint64(99), ts)
								require.Equal(t, time.Duration(defaultDumpGCSafePointTTL)*time.Second, ttl)
								return &pdgc.GCBarrierInfo{}, nil
							},
							delete: func(ctx context.Context, id string) (*pdgc.GCBarrierInfo, error) {
								assertCleanup(ctx, id)
								return nil, cleanupErr
							},
						},
					}
					d := newGCProtectionDumper(t, client, keyspace)
					require.NoError(t, tidbStartGCSavepointUpdateService(d))
					require.Equal(t, 1, registrations)
					if cancelDump {
						d.cancelCtx()
					}
					require.NoError(t, d.Close())
					require.Equal(t, 1, deletions)
					require.Nil(t, d.gcProtection)
					require.NoError(t, d.Close())
					require.Equal(t, 1, deletions)
				})
			}
		}
	}
}

func TestDumplingGCProtectionDumpErrorCleanup(t *testing.T) {
	// Reuse the constructor's registration for the first Dump, then restart for
	// a second Dump. A closed SQL pool fails both exports; each must finish its
	// own deletion and preserve the SQL error without canceling the parent.
	for _, keyspace := range []bool{false, true} {
		t.Run(gcProtectionTestName(keyspace), func(t *testing.T) {
			client := newMockPDClientForGC()
			d := newGCProtectionDumper(t, client, keyspace)
			d.conf.Consistency = ConsistencyTypeNone
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			mock.ExpectClose()
			require.NoError(t, db.Close())
			d.dbHandle = db
			require.NoError(t, tidbStartGCSavepointUpdateService(d))
			for finishedDumps := 1; finishedDumps <= 2; finishedDumps++ {
				require.ErrorContains(t, d.Dump(), "sql: database is closed")
				require.Nil(t, d.gcProtection)
				require.NoError(t, d.tctx.Err())
				if keyspace {
					require.Equal(t, finishedDumps, client.gcStatesClient.setCalls)
					require.Equal(t, finishedDumps, client.gcStatesClient.delCalls)
				} else {
					require.Equal(t, 2*finishedDumps, client.updateSafePointCalls)
					require.Zero(t, client.lastSafePointTTL)
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestDumplingGCProtectionCloseDuringSnapshotConversion retries a failed dump
// with a timestamp snapshot and closes it during a slow TSO conversion query.
// Close must cancel that query before its delay expires, without registering
// or deleting another GC protection record in either GC mode.
func TestDumplingGCProtectionCloseDuringSnapshotConversion(t *testing.T) {
	for _, keyspace := range []bool{false, true} {
		t.Run(gcProtectionTestName(keyspace), func(t *testing.T) {
			queryStarted := make(chan struct{})
			conversions := 0
			matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
				err := sqlmock.QueryMatcherRegexp.Match(expected, actual)
				if err == nil && actual == "SELECT unix_timestamp(?)" {
					conversions++
					if conversions == 2 {
						close(queryStarted)
					}
				}
				return err
			})
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
			require.NoError(t, err)
			client := newMockPDClientForGC()
			d := newGCProtectionDumper(t, client, keyspace)
			d.dbHandle = db
			d.conf.Snapshot = "2026-10-12 18:00:00"
			d.conf.Consistency = ConsistencyTypeSnapshot
			d.conf.ServerInfo.ServerType = version.ServerTypeTiDB
			t.Cleanup(func() { require.NoError(t, d.Close()) })

			// Register once and release that keeper when the first export fails.
			mock.ExpectQuery("SELECT unix_timestamp").WithArgs(d.conf.Snapshot).
				WillReturnRows(sqlmock.NewRows([]string{"unix_timestamp"}).AddRow(1791799200))
			firstDumpErr := errors.New("snapshot transaction failed")
			mock.ExpectExec("START TRANSACTION").WillReturnError(firstDumpErr)
			mock.ExpectExec("START TRANSACTION").WillReturnError(firstDumpErr)
			require.NoError(t, tidbStartGCSavepointUpdateService(d))
			require.ErrorIs(t, d.Dump(), firstDumpErr)
			require.Nil(t, d.gcProtection)
			require.NoError(t, d.tctx.Err())

			// Observe the retry's query before Close cancels the export context.
			mock.ExpectQuery("SELECT unix_timestamp").WithArgs(d.conf.Snapshot).
				WillDelayFor(10 * time.Second).
				WillReturnRows(sqlmock.NewRows([]string{"unix_timestamp"}).AddRow(1791799200))
			mock.ExpectClose()
			dumpResult := make(chan error, 1)
			go func() { dumpResult <- d.Dump() }()
			select {
			case <-queryStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("retry did not start timestamp conversion")
			}
			closeResult := make(chan error, 1)
			go func() { closeResult <- d.Close() }()
			select {
			case err := <-closeResult:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel timestamp conversion")
			}
			require.ErrorIs(t, <-dumpResult, sqlmock.ErrCancelled)
			require.Nil(t, d.gcProtection)
			if keyspace {
				require.Equal(t, 1, client.gcStatesClient.setCalls)
				require.Equal(t, 1, client.gcStatesClient.delCalls)
			} else {
				require.Equal(t, 2, client.updateSafePointCalls)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDumplingGCProtectionCloseDuringStartup(t *testing.T) {
	// Block the initial registration until Close cancels it. Close must unblock
	// startup, wait for ownership resolution, and issue no deletion for failure.
	started := make(chan struct{})
	client := &dumplingGCProtectionClient{
		update: func(ctx context.Context, _ string, ttl int64, _ uint64) (uint64, error) {
			require.Positive(t, ttl)
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}
	d := newGCProtectionDumper(t, client, false)
	result := make(chan error, 1)
	go func() { result <- tidbStartGCSavepointUpdateService(d) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not start")
	}
	require.NoError(t, d.Close())
	require.ErrorIs(t, <-result, context.Canceled)
	require.Nil(t, d.gcProtection)
}

func TestDumplingGCProtectionCancellationAfterAcknowledgement(t *testing.T) {
	// PD acknowledges registration just as the export is canceled. The startup
	// path must synchronously delete that registration and return cancellation,
	// even though no background worker was started or published to the Dumper.
	deletions := 0
	var cancelDump context.CancelFunc
	client := &dumplingGCProtectionClient{
		update: func(ctx context.Context, _ string, ttl int64, ts uint64) (uint64, error) {
			if ttl == 0 {
				deletions++
				require.NoError(t, ctx.Err())
				return 0, nil
			}
			cancelDump()
			return ts, nil
		},
	}
	d := newGCProtectionDumper(t, client, false)
	cancelDump = d.cancelCtx
	require.ErrorIs(t, tidbStartGCSavepointUpdateService(d), context.Canceled)
	require.Equal(t, 1, deletions)
	require.Nil(t, d.gcProtection)
	require.NoError(t, d.Close())
	require.Equal(t, 1, deletions)
}

func TestDumplingGCProtectionWithoutPD(t *testing.T) {
	// MySQL has no PD registration. Initialization and repeated cleanup/Close
	// must remain safe and must leave the keeper absent.
	d := newGCProtectionDumper(t, nil, false)
	require.NoError(t, tidbStartGCSavepointUpdateService(d))
	d.stopGCProtection()
	require.NoError(t, d.Close())
	require.NoError(t, d.Close())
	require.Nil(t, d.gcProtection)
}

// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/task"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	autoDetectKeyspaceTimeout   = 10 * time.Second
	autoDetectKeyspaceBatchSize = 1024
	systemKeyspaceName          = "SYSTEM"
)

type keyspacePDClient interface {
	GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error)
	Close()
}

var newKeyspacePDClient = func(
	ctx context.Context,
	callerComponent caller.Component,
	pdAddrs []string,
	security pd.SecurityOption,
	opts ...opt.ClientOption,
) (keyspacePDClient, error) {
	return pd.NewClientWithContext(ctx, callerComponent, pdAddrs, security, opts...)
}

func autoDetectKeyspaceName(ctx context.Context, cfg *task.Config) error {
	if cfg.KeyspaceName != "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	securityOpt := pd.SecurityOption{}
	if cfg.TLS.IsEnabled() {
		securityOpt = cfg.TLS.ToPDSecurityOption()
	}

	pdCli, err := newKeyspacePDClient(
		ctx,
		caller.Component("br-keyspace-detector"),
		cfg.PD,
		securityOpt,
		opt.WithCustomTimeoutOption(autoDetectKeyspaceTimeout),
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer pdCli.Close()

	keyspaceName, err := detectOnlyUserKeyspace(ctx, pdCli)
	if err != nil {
		if isKeyspaceAutoDetectUnsupportedErr(err) {
			log.Info("PD does not support keyspace list API, continue with empty keyspace name", zap.Error(err))
			return nil
		}
		return errors.Trace(err)
	}

	if keyspaceName == "" {
		log.Info("no user keyspace detected from PD, continue with empty keyspace name")
		return nil
	}

	cfg.KeyspaceName = keyspaceName
	log.Info("auto detected keyspace name from PD", zap.String("keyspaceName", keyspaceName))
	return nil
}

func detectOnlyUserKeyspace(ctx context.Context, pdCli keyspacePDClient) (string, error) {
	var keyspaceName string
	for startID := constants.DefaultKeyspaceID; ; {
		keyspaces, err := pdCli.GetAllKeyspaces(ctx, startID, autoDetectKeyspaceBatchSize)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(keyspaces) == 0 {
			return keyspaceName, nil
		}

		for _, keyspaceMeta := range keyspaces {
			if !isAutoDetectUserKeyspace(keyspaceMeta) {
				continue
			}
			if keyspaceName != "" {
				return "", errors.Errorf("multiple keyspaces found in PD: %s, %s, please specify --keyspace-name explicitly",
					keyspaceName, keyspaceMeta.GetName())
			}
			keyspaceName = keyspaceMeta.GetName()
		}

		lastID, ok := maxKeyspaceIDInBatch(keyspaces)
		if !ok || len(keyspaces) < autoDetectKeyspaceBatchSize || lastID >= constants.MaxKeyspaceID {
			return keyspaceName, nil
		}
		startID = lastID + 1
	}
}

func isAutoDetectUserKeyspace(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	// DEFAULT and SYSTEM are PD's built-in keyspaces. When no explicit BR keyspace is
	// configured, keeping an empty keyspace name preserves the existing
	// dedicated-cluster behavior. Only user-created keyspaces, such as the
	// ones pre-allocated by TiUP playground/cluster, can be auto-selected.
	return keyspaceMeta != nil &&
		keyspaceMeta.GetState() == keyspacepb.KeyspaceState_ENABLED &&
		keyspaceMeta.GetName() != constants.DefaultKeyspaceName &&
		keyspaceMeta.GetName() != systemKeyspaceName
}

func maxKeyspaceIDInBatch(keyspaces []*keyspacepb.KeyspaceMeta) (uint32, bool) {
	var maxID uint32
	found := false
	for _, keyspaceMeta := range keyspaces {
		if keyspaceMeta == nil {
			continue
		}
		if !found || keyspaceMeta.GetId() > maxID {
			maxID = keyspaceMeta.GetId()
			found = true
		}
	}
	return maxID, found
}

func isKeyspaceAutoDetectUnsupportedErr(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(errors.Cause(err)) == codes.Unimplemented {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "Unimplemented") ||
		strings.Contains(errMsg, "unknown service keyspacepb.Keyspace")
}

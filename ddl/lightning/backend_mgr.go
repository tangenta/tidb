package lightning

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type BackendCtxManager struct {
	ResourceManager[BackendContext]
	MemRoot MemRoot
}

func (m *BackendCtxManager) init(memRoot MemRoot) {
	m.ResourceManager.init()
	m.MemRoot = memRoot
}

func (m *BackendCtxManager) Register(ctx context.Context, unique bool, jobID int64, _ mysql.SQLMode) error {
	// Firstly, get backend context from backend cache.
	_, exist := m.Load(jobID)
	// If bc not exist, build a new backend for reorg task, otherwise reuse exist backend
	// to continue the task.
	if !exist {
		m.MemRoot.RefreshConsumption()
		err := m.MemRoot.TryConsume(StructSizeBackendCtx)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("backend key", jobID),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)))
			return err
		}
		cfg, err := generateLightningConfig(m.MemRoot, jobID, unique)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("backend key", jobID),
				zap.String("Generate config for lightning error:", err.Error()))
			return err
		}
		bd, err := createLocalBackend(ctx, cfg, glueLit{})
		if err != nil {
			logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Int64("backend key", jobID),
				zap.String("Error", err.Error()), zap.Stack("stack trace"))
			return err
		}

		// Init important variables
		sysVars := obtainImportantVariables()

		m.Store(jobID, newBackendContext(ctx, jobID, &bd, cfg, sysVars, m.MemRoot))

		// Count memory usage.
		m.MemRoot.Consume(StructSizeBackendCtx)
		logutil.BgLogger().Info(LitInfoCreateBackend, zap.Int64("backend key", jobID),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)),
			zap.String("Unique Index:", strconv.FormatBool(unique)))
	}
	return nil
}

func (m *BackendCtxManager) Load(jobID int64) (*BackendContext, bool) {
	key := GenBackendContextKey(jobID)
	return m.ResourceManager.Load(key)
}

func (m *BackendCtxManager) Store(jobID int64, bc *BackendContext) {
	key := GenBackendContextKey(jobID)
	m.ResourceManager.Store(key, bc)
}

func (m *BackendCtxManager) Drop(jobID int64) {
	key := GenBackendContextKey(jobID)
	m.ResourceManager.Drop(key)
}

func createLocalBackend(ctx context.Context, cfg *config.Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Error(err))
		return backend.Backend{}, err
	}

	errorMgr := errormanager.New(nil, cfg, log.Logger{Logger: logutil.BgLogger()})
	return local.NewLocalBackend(ctx, tls, cfg, glue, int(GlobalEnv.limit), errorMgr)
}

func newBackendContext(ctx context.Context, jobID int64, be *backend.Backend,
	cfg *config.Config, vars map[string]string, memRoot MemRoot) *BackendContext {
	key := GenBackendContextKey(jobID)
	bc := &BackendContext{
		key:     key,
		backend: be,
		ctx:     ctx,
		cfg:     cfg,
		sysVars: vars,
	}
	bc.EngMgr.init(memRoot)
	return bc
}

func (m *BackendCtxManager) Unregister(jobID int64) {
	bc, exist := m.Load(jobID)
	if !exist {
		logutil.BgLogger().Error(LitErrGetBackendFail, zap.Int64("backend key", jobID))
	}
	bc.EngMgr.UnregisterAll()
	m.Drop(jobID)
	logutil.BgLogger().Info(LitInfoCloseBackend, zap.Int64("backend key", jobID),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)))
}

func (m *BackendCtxManager) CheckDiskQuota(quota int64) int64 {
	var totalDiskUsed int64
	for _, bc := range m.ResourceManager.item {
		_, _, bcDiskUsed, _ := bc.backend.CheckDiskQuota(GlobalEnv.diskQuota)
		totalDiskUsed += bcDiskUsed
	}
	return totalDiskUsed
}

func (m *BackendCtxManager) UpdateMemoryUsage() {
	for _, bc := range m.ResourceManager.item {
		curSize := bc.backend.TotalMemoryConsume()
		m.MemRoot.ReleaseWithTag(bc.key)
		m.MemRoot.ConsumeWithTag(bc.key, curSize)
	}
}

// GenBackendContextKey generate a backend key from job id for a DDL job.
func GenBackendContextKey(jobID int64) string {
	return strconv.FormatInt(jobID, 10)
}

type glueLit struct{}

// OwnsSQLExecutor Implement interface OwnsSQLExecutor.
func (g glueLit) OwnsSQLExecutor() bool {
	return false
}

// GetSQLExecutor Implement interface GetSQLExecutor.
func (g glueLit) GetSQLExecutor() glue.SQLExecutor {
	return nil
}

// GetDB Implement interface GetDB.
func (g glueLit) GetDB() (*sql.DB, error) {
	return nil, nil
}

// GetParser Implement interface GetParser.
func (g glueLit) GetParser() *parser.Parser {
	return nil
}

// GetTables Implement interface GetTables.
func (g glueLit) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

// GetSession Implement interface GetSession.
func (g glueLit) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}

// OpenCheckpointsDB Implement interface OpenCheckpointsDB.
func (g glueLit) OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently.
func (g glueLit) Record(string, uint64) {
}

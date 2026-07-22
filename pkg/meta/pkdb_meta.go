package meta

import (
	"strconv"

	"github.com/pingcap/errors"
)

var (
	mBootstrapEEKey = []byte("BootstrapEEKey")
)

// FinishBootstrapEE finishes bootstrap.
func (m *Mutator) FinishBootstrapEE(version int64) error {
	err := m.txn.Set(mBootstrapEEKey, []byte(strconv.FormatInt(version, 10)))
	return errors.Trace(err)
}

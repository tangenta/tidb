package export

import "github.com/pingcap/errors"

const (
	flagExportTiDBRowID          = "export-tidb-rowid"
)

// ParseExportTiDBRowIDMode parses export-tidb-rowid string to ExportTiDBRowIDMode
func ParseExportTiDBRowIDMode(mode string) (ExportTiDBRowIDMode, error) {
	switch mode {
	case "", "off":
		return ExportTiDBRowIDModeOff, nil
	case "int-pk-auto-inc":
		return ExportTiDBRowIDModeIntPKAutoInc, nil
	default:
		return ExportTiDBRowIDModeOff, errors.Errorf("unknown export-tidb-rowid value %q (supported: off, int-pk-auto-inc)", mode)
	}
}

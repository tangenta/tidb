package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExportTiDBRowIDMode(t *testing.T) {
	mode, err := ParseExportTiDBRowIDMode("")
	require.NoError(t, err)
	require.Equal(t, ExportTiDBRowIDModeOff, mode)

	mode, err = ParseExportTiDBRowIDMode("off")
	require.NoError(t, err)
	require.Equal(t, ExportTiDBRowIDModeOff, mode)

	mode, err = ParseExportTiDBRowIDMode("int-pk-auto-inc")
	require.NoError(t, err)
	require.Equal(t, ExportTiDBRowIDModeIntPKAutoInc, mode)

	_, err = ParseExportTiDBRowIDMode("not-a-mode")
	require.Error(t, err)
}

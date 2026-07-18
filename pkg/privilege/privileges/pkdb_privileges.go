package privileges

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var dutySeparationPrivTables = map[string]struct{}{
	strings.ToLower(mysql.UserTable):        {},
	strings.ToLower(mysql.DBTable):          {},
	strings.ToLower(mysql.TablePrivTable):   {},
	strings.ToLower(mysql.ColumnPrivTable):  {},
	strings.ToLower(mysql.GlobalPrivTable):  {},
	"global_grants":                         {},
	strings.ToLower(mysql.RoleEdgeTable):    {},
	strings.ToLower(mysql.DefaultRoleTable): {},
	strings.ToLower(mysql.ProcsPriv):        {},
}

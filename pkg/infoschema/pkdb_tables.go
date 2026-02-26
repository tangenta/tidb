package infoschema

import "github.com/pingcap/tidb/pkg/meta/autoid"

func init() {
	tableIDMap[TableUserLoginHistory] = autoid.ReservedTablesBaseID
	tableIDMap[ClusterTableAuditLog] = autoid.ReservedTablesBaseID + 1
	tableIDMap[TableAuditLog] = autoid.ReservedTablesBaseID + 2
	tableIDMap[TableRegions] = autoid.ReservedTablesBaseID + 3

	init2()
}

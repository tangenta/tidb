package errno

import "github.com/pingcap/tidb/pkg/parser/mysql"

func init() {
	MySQLErrName[ErrForeignKeyWithCreateAsSelect] = mysql.Message("Foreign key creation is not allowed with CREATE TABLE as SELECT and CREATE TABLE with START TRANSACTION statement.", nil)
}

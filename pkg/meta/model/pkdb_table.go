package model

import "github.com/pingcap/tidb/pkg/parser/ast"

var (
	lowerCaseTableNames int32 = 2
	// NameAsID returns the name according to the lower_case_table_names setting.
	NameAsID = ciStrLower
	// NameEqual checks whether two table names are equal according to the lower_case_table_names setting.
	NameEqual = ciStrEqLower
)

func ciStrLower(s ast.CIStr) string {
	return s.L
}

func ciStrOrigin(s ast.CIStr) string {
	return s.O
}

func ciStrEqLower(a, b ast.CIStr) bool {
	return a.L == b.L
}

func ciStrEqOrigin(a, b ast.CIStr) bool {
	return a.O == b.O
}

// SetLowerCaseTableNamesOnBootstrap initialize the lowerCaseTableNames value on bootstrap.
func SetLowerCaseTableNamesOnBootstrap(val int) {
	lowerCaseTableNames = int32(val)
	switch val {
	case 0:
		NameAsID = ciStrOrigin
		NameEqual = ciStrEqOrigin
	case 1, 2:
		NameAsID = ciStrLower
		NameEqual = ciStrEqLower
	}
}

// GetLowerCaseTableNames loads the lowerCaseTableNames value.
func GetLowerCaseTableNames() int {
	return int(lowerCaseTableNames)
}

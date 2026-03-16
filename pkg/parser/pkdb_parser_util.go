package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

type createFunctionPrefix struct {
	orReplace     bool
	viewAlgorithm ast.ViewAlgorithm
	definer       *auth.UserIdentity
	ifNotExists   bool
}

// Copyright 2026 PingCAP, Inc.

package model

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

// ProcedureInfo provides meta data describing a stored procedure/function.
// It mirrors the mysql.routines table schema, with additional state fields for DDL operations.
type ProcedureInfo struct {
	Schema ast.CIStr `json:"schema"`
	Name   ast.CIStr `json:"name"`
	Type   string    `json:"type"` // "PROCEDURE" or "FUNCTION"

	Definition     string `json:"definition"`
	DefinitionUTF8 string `json:"definition_utf8"`
	ParameterStr   string `json:"parameter_str"`

	IsDeterministic int64  `json:"is_deterministic"`
	SQLDataAccess   string `json:"sql_data_access"`
	SecurityType    string `json:"security_type"`
	Definer         string `json:"definer"`
	SQLMode         string `json:"sql_mode"`

	CharacterSetClient  string `json:"character_set_client"`
	CollationConnection string `json:"collation_connection"`
	SchemaCollation     string `json:"schema_collation"`

	Created     types.Time `json:"created"`
	LastAltered types.Time `json:"last_altered"`

	Comment          string  `json:"comment"`
	Options          *string `json:"options,omitempty"`
	ExternalLanguage string  `json:"external_language"`

	State SchemaState `json:"state"`
}

// LoadableFunctionInfo contains metadata for creating a loadable function.
type LoadableFunctionInfo struct {
	Name       ast.CIStr      `json:"name"`
	ReturnType types.EvalType `json:"return_type"`
	SoName     string         `json:"so_name"`
}

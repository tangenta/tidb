// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestAnnotateSchemaInfoError(t *testing.T) {
	originalErr := infoschema.ErrTableNotExists.FastGenByArgs("test", "missing")
	result := annotateSchemaInfoError(originalErr, domain.ErrInfoSchemaChanged)

	// Keep the schema-change error code so callers retain the retry behavior, while
	// exposing the original DDL error that caused the schema check to run.
	require.True(t, terror.ErrorEqual(result, domain.ErrInfoSchemaChanged))
	require.Contains(t, result.Error(), "original DDL error: "+originalErr.Error())

	// Avoid adding a redundant annotation when the DDL already returned the
	// schema-change error.
	result = annotateSchemaInfoError(domain.ErrInfoSchemaChanged, domain.ErrInfoSchemaChanged)
	require.Equal(t, domain.ErrInfoSchemaChanged.Error(), result.Error())

	require.Same(t, originalErr, annotateSchemaInfoError(originalErr, nil))
}

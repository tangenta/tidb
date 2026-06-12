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

package stmtctx

// RegisterUDFCleanup registers a statement-scoped cleanup callback for loadable
// functions (UDF). It is concurrency-safe.
func (sc *StatementContext) RegisterUDFCleanup(cleanup func()) {
	if cleanup == nil {
		return
	}
	sc.UserFuncCtx.Lock()
	sc.UserFuncCtx.loadableFuncCleanups = append(sc.UserFuncCtx.loadableFuncCleanups, cleanup)
	sc.UserFuncCtx.Unlock()
}

// UDFCleanup calls cleanup functions registered by RegisterUDFCleanup.
func (sc *StatementContext) UDFCleanup() {
	for _, f := range sc.UserFuncCtx.loadableFuncCleanups {
		f()
	}
	sc.UserFuncCtx.loadableFuncCleanups = nil
}

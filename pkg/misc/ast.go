/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package misc

import "github.com/cectc/dbpack/third_party/parser/ast"

// GetStmtLabel generates a label for given statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		return "AnalyzeTable"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.ChangeStmt:
		return "Change"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateViewStmt:
		return "CreateView"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropTableStmt:
		if x.IsView {
			return "DropView"
		}
		return "DropTable"
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			return "DescTable"
		}
		if x.Analyze {
			return "ExplainAnalyzeSQL"
		}
		return "ExplainSQL"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "Rollback"
	case *ast.SelectStmt:
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateTableStmt:
		return "TruncateTable"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.GrantStmt:
		return "Grant"
	case *ast.RevokeStmt:
		return "Revoke"
	case *ast.DeallocateStmt:
		return "Deallocate"
	case *ast.ExecuteStmt:
		return "Execute"
	case *ast.PrepareStmt:
		return "Prepare"
	case *ast.UseStmt:
		return "Use"
	case *ast.CreateBindingStmt:
		return "CreateBinding"
	case *ast.IndexAdviseStmt:
		return "IndexAdvise"
	case *ast.DropBindingStmt:
		return "DropBinding"
	case *ast.TraceStmt:
		return "Trace"
	case *ast.ShutdownStmt:
		return "Shutdown"
	}
	return "Unrecognized"
}

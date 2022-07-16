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
package tracing

const (
	// service
	HTTPProxyService = "http_proxy_service"

	// global transcation span name.
	GlobalTransactionBegin    = "global_transaction_begin"
	GlobalTransactionEnd      = "global_transaction_end"
	GlobalTransactionCommit   = "global_transaction_commit"
	GlobalTransactionRollback = "global_transaction_rollback"

	// branch transaction.
	BranchTransactionRegister = "branch_transaction_register"
	BranchTransactionEnd      = "branch_transaction_end"

	// mysql command
	MySQLComQuery = "mysql_com_query"

	// executor
	ExecutorFetchBeforeImage = "executor_fetch_before_image"
	ExecutorFetchAfterImage  = "executor_fetch_after_image"

	// db execute
	DBExecSQL          = "db_exec_sql"
	DBExecStmt         = "db_exec_stmt"
	DBQuery            = "db_query"
	DBExecFieldList    = "db_exec_field_list"
	DBUse              = "db_use"
	DBTransactionBegin = "db_transaction_begin"

	// single db
	SDBExecComQuery = "sdb_exec_com_query"

	// read write splitting
	RWExecComQuery = "rw_exec_com_query"
	RWExecComStmt  = "rw_exec_com_stmt"

	// sharding
	ShardExecComQuery = "shard_exec_com_query"
	ShardExecComStmt  = "shard_exec_com_stmt"
)

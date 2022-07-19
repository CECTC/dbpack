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
	TraceParentHeader = "traceparent"

	// service
	HTTPProxyService = "http_proxy_service"

	// distributed transaction filter
	DTHttpFilterPreHandle   = "dt_http_filter_pre_handle"
	DTHttpFilterPostHandle  = "dt_http_filter_post_handle"
	DTMysqlFilterPreHandle  = "dt_mysql_filter_pre_handle"
	DTMysqlFilterPostHandle = "dt_mysql_filter_post_handle"

	// global transcation span name.
	GlobalTransactionBegin    = "global_transaction_begin"
	GlobalTransactionEnd      = "global_transaction_end"
	GlobalTransactionCommit   = "global_transaction_commit"
	GlobalTransactionRollback = "global_transaction_rollback"

	// branch transaction.
	BranchTransactionRegister = "branch_transaction_register"
	BranchTransactionEnd      = "branch_transaction_end"

	// executor
	ExecutorFetchBeforeImage = "executor_fetch_before_image"
	ExecutorFetchAfterImage  = "executor_fetch_after_image"
	Executable               = "executable"

	// mysql command
	MySQLListenerComQuery       = "mysql_listener_com_query"
	MySQLListenerComStmtExecute = "mysql_listener_com_stmt_execute"

	// single db
	SDBComQuery       = "sdb_com_query"
	SDBComStmtExecute = "sdb_com_stmt_execute"

	// read write splitting
	RWSComQuery       = "rws_com_query"
	RWSComStmtExecute = "rws_com_stmt_execute"

	// sharding
	SHDComQuery       = "shd_com_query"
	SHDComStmtExecute = "shd_com_stmt_execute"

	// db
	DBUse                   = "db_use"
	DBQuery                 = "db_query"
	DBExecSQL               = "db_exec_sql"
	DBExecStmt              = "db_exec_stmt"
	DBExecFieldList         = "db_exec_field_list"
	DBLocalTransactionBegin = "db_local_transaction_begin"

	// tx
	TxQuery    = "tx_query"
	TxExecSQL  = "tx_exec_sql"
	TxExecStmt = "tx_exec_stmt"
	TxCommit   = "db_local_transaction_commit"
	TxRollback = "db_local_transaction_rollback"

	// conn
	ConnQuery       = "conn_com_query"
	ConnStmtExecute = "conn_com_stmt_exec"
)

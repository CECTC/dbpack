%{
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

// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"math"
	"strconv"

	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

%}

%union {
	offset  int
	ident   string
	number  uint64
	hint    *ast.TableOptimizerHint
	hints []*ast.TableOptimizerHint
	table 	ast.HintTable
	modelIdents []model.CIStr
}

%token	<number>

	/*yy:token "%d" */
	hintIntLit "a 64-bit unsigned integer"

%token	<ident>

	/*yy:token "%c" */
	hintIdentifier

	/*yy:token "@%c" */
	hintSingleAtIdentifier "identifier with single leading at"

	/*yy:token "'%c'" */
	hintStringLit

	/* MySQL 8.0 hint names */
	hintJoinFixedOrder      "JOIN_FIXED_ORDER"
	hintJoinOrder           "JOIN_ORDER"
	hintJoinPrefix          "JOIN_PREFIX"
	hintJoinSuffix          "JOIN_SUFFIX"
	hintBKA                 "BKA"
	hintNoBKA               "NO_BKA"
	hintBNL                 "BNL"
	hintNoBNL               "NO_BNL"
	hintHashJoin            "HASH_JOIN"
	hintNoHashJoin          "NO_HASH_JOIN"
	hintMerge               "MERGE"
	hintNoMerge             "NO_MERGE"
	hintIndexMerge          "INDEX_MERGE"
	hintNoIndexMerge        "NO_INDEX_MERGE"
	hintMRR                 "MRR"
	hintNoMRR               "NO_MRR"
	hintNoICP               "NO_ICP"
	hintNoRangeOptimization "NO_RANGE_OPTIMIZATION"
	hintSkipScan            "SKIP_SCAN"
	hintNoSkipScan          "NO_SKIP_SCAN"
	hintSemijoin            "SEMIJOIN"
	hintNoSemijoin          "NO_SEMIJOIN"
	hintMaxExecutionTime    "MAX_EXECUTION_TIME"
	hintSetVar              "SET_VAR"
	hintResourceGroup       "RESOURCE_GROUP"
	hintQBName              "QB_NAME"

	/* DBPack hint names */
	hintXID         "XID"
	hintGlobalLock  "GLOBALLOCK"
	hintUseDB       "USEDB"
	hintTraceParent "TRACEPARENT"

	/* Other keywords */
	hintPartition       "PARTITION"
	hintDupsWeedOut     "DUPSWEEDOUT"
	hintFirstMatch      "FIRSTMATCH"
	hintLooseScan       "LOOSESCAN"
	hintMaterialization "MATERIALIZATION"

%type	<ident>
	Identifier                             "identifier (including keywords)"
	QueryBlockOpt                          "Query block identifier optional"
	JoinOrderOptimizerHintName
	UnsupportedTableLevelOptimizerHintName
	SupportedTableLevelOptimizerHintName
	UnsupportedIndexLevelOptimizerHintName
	SubqueryOptimizerHintName
	NullaryHintName                        "name of hints which take no input"
	SubqueryStrategy
	Value                                  "the value in the SET_VAR() hint"

%type	<number>
	CommaOpt    "optional ','"

%type	<hints>
	OptimizerHintList           "optimizer hint list"

%type	<hint>
	TableOptimizerHintOpt   "optimizer hint"
	HintTableList           "table list in optimizer hint"
	HintTableListOpt        "optional table list in optimizer hint"
	HintIndexList           "table name with index list in optimizer hint"
	IndexNameList           "index list in optimizer hint"
	IndexNameListOpt        "optional index list in optimizer hint"
	SubqueryStrategies      "subquery strategies"
	SubqueryStrategiesOpt   "optional subquery strategies"

%type	<table>
	HintTable "Table in optimizer hint"

%type	<modelIdents>
	PartitionList    "partition name list in optimizer hint"
	PartitionListOpt "optional partition name list in optimizer hint"


%start	Start

%%

Start:
	OptimizerHintList
	{
		parser.result = $1
	}

OptimizerHintList:
	TableOptimizerHintOpt
	{
		if $1 != nil {
			$$ = []*ast.TableOptimizerHint{$1}
		}
	}
|	OptimizerHintList CommaOpt TableOptimizerHintOpt
	{
		if $3 != nil {
			$$ = append($1, $3)
		} else {
			$$ = $1
		}
	}

TableOptimizerHintOpt:
	"JOIN_FIXED_ORDER" '(' QueryBlockOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	JoinOrderOptimizerHintName '(' HintTableList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	UnsupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		h := $3
		h.HintName = model.NewCIStr($1)
		$$ = h
	}
|	UnsupportedIndexLevelOptimizerHintName '(' HintIndexList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SubqueryOptimizerHintName '(' QueryBlockOpt SubqueryStrategiesOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	"MAX_EXECUTION_TIME" '(' QueryBlockOpt hintIntLit ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
			HintData: $4,
		}
	}
|	"SET_VAR" '(' Identifier '=' Value ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			HintData: ast.HintSetVar{
				VarName: $3,
				Value:   $5,
			},
		}
	}
|	"RESOURCE_GROUP" '(' Identifier ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	"QB_NAME" '(' Identifier ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
		}
	}
|   "XID" '(' Value ')'
    {
    	$$ = &ast.TableOptimizerHint{
            HintName: model.NewCIStr($1),
            HintData: model.NewCIStr($3),
        }
    }
|   "GLOBALLOCK" '('')'
    {
    	$$ = &ast.TableOptimizerHint{
            HintName: model.NewCIStr($1),
        }
    }
|   "USEDB" '(' Value ')'
    {
    	$$ = &ast.TableOptimizerHint{
            HintName: model.NewCIStr($1),
            HintData: model.NewCIStr($3),
        }
    }
|   "TRACEPARENT" '(' Value ')'
    {
    	$$ = &ast.TableOptimizerHint{
            HintName: model.NewCIStr($1),
            HintData: model.NewCIStr($3),
        }
    }
|	NullaryHintName '(' QueryBlockOpt ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
		}
	}

QueryBlockOpt:
	/* empty */
	{
		$$ = ""
	}
|	hintSingleAtIdentifier

CommaOpt:
	/*empty*/
	{}
|	','
	{}

PartitionListOpt:
	/* empty */
	{
		$$ = nil
	}
|	"PARTITION" '(' PartitionList ')'
	{
		$$ = $3
	}

PartitionList:
	Identifier
	{
		$$ = []model.CIStr{model.NewCIStr($1)}
	}
|	PartitionList CommaOpt Identifier
	{
		$$ = append($1, model.NewCIStr($3))
	}

/**
 * HintTableListOpt:
 *
 *	[@query_block_name] [tbl_name [, tbl_name] ...]
 *	[tbl_name@query_block_name [, tbl_name@query_block_name] ...]
 *
 */
HintTableListOpt:
	HintTableList
|	QueryBlockOpt
	{
		$$ = &ast.TableOptimizerHint{
			QBName: model.NewCIStr($1),
		}
	}

HintTableList:
	QueryBlockOpt HintTable
	{
		$$ = &ast.TableOptimizerHint{
			Tables: []ast.HintTable{$2},
			QBName: model.NewCIStr($1),
		}
	}
|	HintTableList ',' HintTable
	{
		h := $1
		h.Tables = append(h.Tables, $3)
		$$ = h
	}

HintTable:
	Identifier QueryBlockOpt PartitionListOpt
	{
		$$ = ast.HintTable{
			TableName:     model.NewCIStr($1),
			QBName:        model.NewCIStr($2),
			PartitionList: $3,
		}
	}
|	Identifier '.' Identifier QueryBlockOpt PartitionListOpt
	{
		$$ = ast.HintTable{
			DBName:        model.NewCIStr($1),
			TableName:     model.NewCIStr($3),
			QBName:        model.NewCIStr($4),
			PartitionList: $5,
		}
	}

/**
 * HintIndexList:
 *
 *	[@query_block_name] tbl_name [index_name [, index_name] ...]
 *	tbl_name@query_block_name [index_name [, index_name] ...]
 */
HintIndexList:
	QueryBlockOpt HintTable CommaOpt IndexNameListOpt
	{
		h := $4
		h.Tables = []ast.HintTable{$2}
		h.QBName = model.NewCIStr($1)
		$$ = h
	}

IndexNameListOpt:
	/* empty */
	{
		$$ = &ast.TableOptimizerHint{}
	}
|	IndexNameList

IndexNameList:
	Identifier
	{
		$$ = &ast.TableOptimizerHint{
			Indexes: []model.CIStr{model.NewCIStr($1)},
		}
	}
|	IndexNameList ',' Identifier
	{
		h := $1
		h.Indexes = append(h.Indexes, model.NewCIStr($3))
		$$ = h
	}

/**
 * Miscellaneous rules
 */
SubqueryStrategiesOpt:
	/* empty */
	{}
|	SubqueryStrategies

SubqueryStrategies:
	SubqueryStrategy
	{}
|	SubqueryStrategies ',' SubqueryStrategy

Value:
	hintStringLit
|	Identifier
|	hintIntLit
	{
		$$ = strconv.FormatUint($1, 10)
	}

JoinOrderOptimizerHintName:
	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"

UnsupportedTableLevelOptimizerHintName:
	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
/* HASH_JOIN is supported by TiDB */
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"

SupportedTableLevelOptimizerHintName:
	"HASH_JOIN"

UnsupportedIndexLevelOptimizerHintName:
	"INDEX_MERGE"
/* NO_INDEX_MERGE is currently a nullary hint in TiDB */
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"

SubqueryOptimizerHintName:
	"SEMIJOIN"
|	"NO_SEMIJOIN"

SubqueryStrategy:
	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"

NullaryHintName:
	"NO_INDEX_MERGE"

Identifier:
	hintIdentifier
/* MySQL 8.0 hint names */
|	"JOIN_FIXED_ORDER"
|	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"
|	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
|	"HASH_JOIN"
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"
|	"INDEX_MERGE"
|	"NO_INDEX_MERGE"
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"
|	"SEMIJOIN"
|	"NO_SEMIJOIN"
|	"MAX_EXECUTION_TIME"
|	"SET_VAR"
|	"RESOURCE_GROUP"
|	"QB_NAME"
/* DBPack hint names */
|   "XID"
|   "GLOBALLOCK"
|   "USEDB"
|   "TRACEPARENT"
/* other keywords */
|	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"
%%

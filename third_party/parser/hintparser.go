// Code generated by goyacc - DO NOT EDIT.

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
	__yyfmt__ "fmt"
	"strconv"

	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

type yyhintSymType struct {
	yys         int
	offset      int
	ident       string
	number      uint64
	hint        *ast.TableOptimizerHint
	hints       []*ast.TableOptimizerHint
	table       ast.HintTable
	modelIdents []model.CIStr
}

type yyhintXError struct {
	state, xsym int
}

const (
	yyhintDefault           = 57385
	yyhintEofCode           = 57344
	yyhintErrCode           = 57345
	hintBKA                 = 57354
	hintBNL                 = 57356
	hintDupsWeedOut         = 57381
	hintFirstMatch          = 57382
	hintGlobalLock          = 57377
	hintHashJoin            = 57358
	hintIdentifier          = 57347
	hintIndexMerge          = 57362
	hintIntLit              = 57346
	hintJoinFixedOrder      = 57350
	hintJoinOrder           = 57351
	hintJoinPrefix          = 57352
	hintJoinSuffix          = 57353
	hintLooseScan           = 57383
	hintMRR                 = 57364
	hintMaterialization     = 57384
	hintMaxExecutionTime    = 57372
	hintMerge               = 57360
	hintNoBKA               = 57355
	hintNoBNL               = 57357
	hintNoHashJoin          = 57359
	hintNoICP               = 57366
	hintNoIndexMerge        = 57363
	hintNoMRR               = 57365
	hintNoMerge             = 57361
	hintNoRangeOptimization = 57367
	hintNoSemijoin          = 57371
	hintNoSkipScan          = 57369
	hintPartition           = 57380
	hintQBName              = 57375
	hintResourceGroup       = 57374
	hintSemijoin            = 57370
	hintSetVar              = 57373
	hintSingleAtIdentifier  = 57348
	hintSkipScan            = 57368
	hintStringLit           = 57349
	hintTraceID             = 57379
	hintUseDB               = 57378
	hintXID                 = 57376

	yyhintMaxDepth = 200
	yyhintTabOfs   = -105
)

var (
	yyhintPrec = map[int]int{}

	yyhintXLAT = map[int]int{
		41:    0,  // ')' (88x)
		57354: 1,  // hintBKA (88x)
		57356: 2,  // hintBNL (88x)
		57377: 3,  // hintGlobalLock (88x)
		57358: 4,  // hintHashJoin (88x)
		57362: 5,  // hintIndexMerge (88x)
		57350: 6,  // hintJoinFixedOrder (88x)
		57351: 7,  // hintJoinOrder (88x)
		57352: 8,  // hintJoinPrefix (88x)
		57353: 9,  // hintJoinSuffix (88x)
		57372: 10, // hintMaxExecutionTime (88x)
		57360: 11, // hintMerge (88x)
		57364: 12, // hintMRR (88x)
		57355: 13, // hintNoBKA (88x)
		57357: 14, // hintNoBNL (88x)
		57359: 15, // hintNoHashJoin (88x)
		57366: 16, // hintNoICP (88x)
		57363: 17, // hintNoIndexMerge (88x)
		57361: 18, // hintNoMerge (88x)
		57365: 19, // hintNoMRR (88x)
		57367: 20, // hintNoRangeOptimization (88x)
		57371: 21, // hintNoSemijoin (88x)
		57369: 22, // hintNoSkipScan (88x)
		57375: 23, // hintQBName (88x)
		57374: 24, // hintResourceGroup (88x)
		57370: 25, // hintSemijoin (88x)
		57373: 26, // hintSetVar (88x)
		57368: 27, // hintSkipScan (88x)
		57379: 28, // hintTraceID (88x)
		57378: 29, // hintUseDB (88x)
		57376: 30, // hintXID (88x)
		44:    31, // ',' (79x)
		57381: 32, // hintDupsWeedOut (71x)
		57382: 33, // hintFirstMatch (71x)
		57383: 34, // hintLooseScan (71x)
		57384: 35, // hintMaterialization (71x)
		57347: 36, // hintIdentifier (68x)
		57348: 37, // hintSingleAtIdentifier (45x)
		57380: 38, // hintPartition (40x)
		40:    39, // '(' (37x)
		46:    40, // '.' (36x)
		61:    41, // '=' (36x)
		57344: 42, // $end (19x)
		57391: 43, // Identifier (16x)
		57399: 44, // QueryBlockOpt (10x)
		57346: 45, // hintIntLit (7x)
		57349: 46, // hintStringLit (4x)
		57388: 47, // HintTable (4x)
		57409: 48, // Value (4x)
		57386: 49, // CommaOpt (3x)
		57389: 50, // HintTableList (3x)
		57390: 51, // HintTableListOpt (2x)
		57394: 52, // JoinOrderOptimizerHintName (2x)
		57395: 53, // NullaryHintName (2x)
		57398: 54, // PartitionListOpt (2x)
		57401: 55, // SubqueryOptimizerHintName (2x)
		57404: 56, // SubqueryStrategy (2x)
		57405: 57, // SupportedTableLevelOptimizerHintName (2x)
		57406: 58, // TableOptimizerHintOpt (2x)
		57407: 59, // UnsupportedIndexLevelOptimizerHintName (2x)
		57408: 60, // UnsupportedTableLevelOptimizerHintName (2x)
		57387: 61, // HintIndexList (1x)
		57392: 62, // IndexNameList (1x)
		57393: 63, // IndexNameListOpt (1x)
		57396: 64, // OptimizerHintList (1x)
		57397: 65, // PartitionList (1x)
		57400: 66, // Start (1x)
		57402: 67, // SubqueryStrategies (1x)
		57403: 68, // SubqueryStrategiesOpt (1x)
		57385: 69, // $default (0x)
		57345: 70, // error (0x)
	}

	yyhintSymNames = []string{
		"')'",
		"hintBKA",
		"hintBNL",
		"hintGlobalLock",
		"hintHashJoin",
		"hintIndexMerge",
		"hintJoinFixedOrder",
		"hintJoinOrder",
		"hintJoinPrefix",
		"hintJoinSuffix",
		"hintMaxExecutionTime",
		"hintMerge",
		"hintMRR",
		"hintNoBKA",
		"hintNoBNL",
		"hintNoHashJoin",
		"hintNoICP",
		"hintNoIndexMerge",
		"hintNoMerge",
		"hintNoMRR",
		"hintNoRangeOptimization",
		"hintNoSemijoin",
		"hintNoSkipScan",
		"hintQBName",
		"hintResourceGroup",
		"hintSemijoin",
		"hintSetVar",
		"hintSkipScan",
		"hintTraceID",
		"hintUseDB",
		"hintXID",
		"','",
		"hintDupsWeedOut",
		"hintFirstMatch",
		"hintLooseScan",
		"hintMaterialization",
		"hintIdentifier",
		"hintSingleAtIdentifier",
		"hintPartition",
		"'('",
		"'.'",
		"'='",
		"$end",
		"Identifier",
		"QueryBlockOpt",
		"hintIntLit",
		"hintStringLit",
		"HintTable",
		"Value",
		"CommaOpt",
		"HintTableList",
		"HintTableListOpt",
		"JoinOrderOptimizerHintName",
		"NullaryHintName",
		"PartitionListOpt",
		"SubqueryOptimizerHintName",
		"SubqueryStrategy",
		"SupportedTableLevelOptimizerHintName",
		"TableOptimizerHintOpt",
		"UnsupportedIndexLevelOptimizerHintName",
		"UnsupportedTableLevelOptimizerHintName",
		"HintIndexList",
		"IndexNameList",
		"IndexNameListOpt",
		"OptimizerHintList",
		"PartitionList",
		"Start",
		"SubqueryStrategies",
		"SubqueryStrategiesOpt",
		"$default",
		"error",
	}

	yyhintTokenLiteralStrings = map[int]string{
		57354: "BKA",
		57356: "BNL",
		57377: "GLOBALLOCK",
		57358: "HASH_JOIN",
		57362: "INDEX_MERGE",
		57350: "JOIN_FIXED_ORDER",
		57351: "JOIN_ORDER",
		57352: "JOIN_PREFIX",
		57353: "JOIN_SUFFIX",
		57372: "MAX_EXECUTION_TIME",
		57360: "MERGE",
		57364: "MRR",
		57355: "NO_BKA",
		57357: "NO_BNL",
		57359: "NO_HASH_JOIN",
		57366: "NO_ICP",
		57363: "NO_INDEX_MERGE",
		57361: "NO_MERGE",
		57365: "NO_MRR",
		57367: "NO_RANGE_OPTIMIZATION",
		57371: "NO_SEMIJOIN",
		57369: "NO_SKIP_SCAN",
		57375: "QB_NAME",
		57374: "RESOURCE_GROUP",
		57370: "SEMIJOIN",
		57373: "SET_VAR",
		57368: "SKIP_SCAN",
		57379: "TRACEID",
		57378: "USEDB",
		57376: "XID",
		57381: "DUPSWEEDOUT",
		57382: "FIRSTMATCH",
		57383: "LOOSESCAN",
		57384: "MATERIALIZATION",
		57348: "identifier with single leading at",
		57380: "PARTITION",
		57346: "a 64-bit unsigned integer",
	}

	yyhintReductions = map[int]struct{ xsym, components int }{
		0:   {0, 1},
		1:   {66, 1},
		2:   {64, 1},
		3:   {64, 3},
		4:   {58, 4},
		5:   {58, 4},
		6:   {58, 4},
		7:   {58, 4},
		8:   {58, 4},
		9:   {58, 5},
		10:  {58, 5},
		11:  {58, 6},
		12:  {58, 4},
		13:  {58, 4},
		14:  {58, 4},
		15:  {58, 3},
		16:  {58, 4},
		17:  {58, 4},
		18:  {58, 4},
		19:  {44, 0},
		20:  {44, 1},
		21:  {49, 0},
		22:  {49, 1},
		23:  {54, 0},
		24:  {54, 4},
		25:  {65, 1},
		26:  {65, 3},
		27:  {51, 1},
		28:  {51, 1},
		29:  {50, 2},
		30:  {50, 3},
		31:  {47, 3},
		32:  {47, 5},
		33:  {61, 4},
		34:  {63, 0},
		35:  {63, 1},
		36:  {62, 1},
		37:  {62, 3},
		38:  {68, 0},
		39:  {68, 1},
		40:  {67, 1},
		41:  {67, 3},
		42:  {48, 1},
		43:  {48, 1},
		44:  {48, 1},
		45:  {52, 1},
		46:  {52, 1},
		47:  {52, 1},
		48:  {60, 1},
		49:  {60, 1},
		50:  {60, 1},
		51:  {60, 1},
		52:  {60, 1},
		53:  {60, 1},
		54:  {60, 1},
		55:  {57, 1},
		56:  {59, 1},
		57:  {59, 1},
		58:  {59, 1},
		59:  {59, 1},
		60:  {59, 1},
		61:  {59, 1},
		62:  {59, 1},
		63:  {55, 1},
		64:  {55, 1},
		65:  {56, 1},
		66:  {56, 1},
		67:  {56, 1},
		68:  {56, 1},
		69:  {53, 1},
		70:  {43, 1},
		71:  {43, 1},
		72:  {43, 1},
		73:  {43, 1},
		74:  {43, 1},
		75:  {43, 1},
		76:  {43, 1},
		77:  {43, 1},
		78:  {43, 1},
		79:  {43, 1},
		80:  {43, 1},
		81:  {43, 1},
		82:  {43, 1},
		83:  {43, 1},
		84:  {43, 1},
		85:  {43, 1},
		86:  {43, 1},
		87:  {43, 1},
		88:  {43, 1},
		89:  {43, 1},
		90:  {43, 1},
		91:  {43, 1},
		92:  {43, 1},
		93:  {43, 1},
		94:  {43, 1},
		95:  {43, 1},
		96:  {43, 1},
		97:  {43, 1},
		98:  {43, 1},
		99:  {43, 1},
		100: {43, 1},
		101: {43, 1},
		102: {43, 1},
		103: {43, 1},
		104: {43, 1},
	}

	yyhintXErrors = map[yyhintXError]string{}

	yyhintParseTab = [166][]uint16{
		// 0
		{1: 127, 129, 120, 134, 135, 109, 124, 125, 126, 115, 132, 136, 128, 130, 131, 138, 144, 133, 137, 139, 143, 141, 118, 117, 142, 116, 140, 122, 121, 119, 52: 110, 123, 55: 114, 57: 112, 108, 113, 111, 64: 107, 66: 106},
		{42: 105},
		{1: 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 230, 42: 104, 49: 269},
		{1: 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 42: 103},
		{39: 266},
		// 5
		{39: 262},
		{39: 259},
		{39: 251},
		{39: 225},
		{39: 213},
		// 10
		{39: 209},
		{39: 204},
		{39: 201},
		{39: 198},
		{39: 195},
		// 15
		{39: 193},
		{39: 190},
		{39: 149},
		{39: 145},
		{39: 60},
		// 20
		{39: 59},
		{39: 58},
		{39: 57},
		{39: 56},
		{39: 55},
		// 25
		{39: 54},
		{39: 53},
		{39: 52},
		{39: 51},
		{39: 50},
		// 30
		{39: 49},
		{39: 48},
		{39: 47},
		{39: 46},
		{39: 45},
		// 35
		{39: 44},
		{39: 43},
		{39: 42},
		{39: 41},
		{39: 36},
		// 40
		{86, 37: 147, 44: 146},
		{148},
		{85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 38: 85, 45: 85},
		{1: 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 87, 42: 87},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 152, 45: 153, 151, 48: 150},
		// 45
		{189},
		{63},
		{62},
		{61},
		{35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 40: 35, 35},
		// 50
		{34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 40: 34, 34},
		{33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 40: 33, 33},
		{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 40: 32, 32},
		{31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 40: 31, 31},
		{30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 40: 30, 30},
		// 55
		{29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 29, 40: 29, 29},
		{28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 40: 28, 28},
		{27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 40: 27, 27},
		{26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 40: 26, 26},
		{25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 40: 25, 25},
		// 60
		{24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 40: 24, 24},
		{23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 40: 23, 23},
		{22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 40: 22, 22},
		{21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 40: 21, 21},
		{20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40: 20, 20},
		// 65
		{19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 40: 19, 19},
		{18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 40: 18, 18},
		{17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 40: 17, 17},
		{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 40: 16, 16},
		{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 40: 15, 15},
		// 70
		{14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 40: 14, 14},
		{13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 40: 13, 13},
		{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 40: 12, 12},
		{11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 40: 11, 11},
		{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 40: 10, 10},
		// 75
		{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 40: 9, 9},
		{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 40: 8, 8},
		{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 40: 7, 7},
		{6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 40: 6, 6},
		{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 40: 5, 5},
		// 80
		{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 40: 4, 4},
		{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 40: 3, 3},
		{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 40: 2, 2},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 40: 1, 1},
		{1: 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 42: 88},
		// 85
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 152, 45: 153, 151, 48: 191},
		{192},
		{1: 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 89, 42: 89},
		{194},
		{1: 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 42: 90},
		// 90
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 152, 45: 153, 151, 48: 196},
		{197},
		{1: 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 42: 91},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 199},
		{200},
		// 95
		{1: 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 42: 92},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 202},
		{203},
		{1: 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 42: 93},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 205},
		// 100
		{41: 206},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 152, 45: 153, 151, 48: 207},
		{208},
		{1: 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 94, 42: 94},
		{37: 147, 44: 210, 86},
		// 105
		{45: 211},
		{212},
		{1: 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 95, 42: 95},
		{86, 32: 86, 86, 86, 86, 37: 147, 44: 214},
		{67, 32: 218, 219, 220, 221, 56: 217, 67: 216, 215},
		// 110
		{224},
		{66, 31: 222},
		{65, 31: 65},
		{40, 31: 40},
		{39, 31: 39},
		// 115
		{38, 31: 38},
		{37, 31: 37},
		{32: 218, 219, 220, 221, 56: 223},
		{64, 31: 64},
		{1: 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 42: 96},
		// 120
		{1: 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 32: 86, 86, 86, 86, 86, 147, 44: 227, 61: 226},
		{250},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 228, 47: 229},
		{86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 147, 86, 40: 238, 44: 237},
		{84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 230, 84, 84, 84, 84, 84, 49: 231},
		// 125
		{83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 32: 83, 83, 83, 83, 83},
		{71, 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 234, 62: 233, 232},
		{72},
		{70, 31: 235},
		{69, 31: 69},
		// 130
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 236},
		{68, 31: 68},
		{82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 38: 241, 54: 249},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 239},
		{86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 147, 86, 44: 240},
		// 135
		{82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 38: 241, 54: 242},
		{39: 243},
		{73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73, 73},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 245, 65: 244},
		{246, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 84, 230, 84, 84, 84, 84, 84, 49: 247},
		// 140
		{80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80, 80},
		{81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 248},
		{79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79, 79},
		{74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74, 74},
		// 145
		{1: 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 42: 97},
		{86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 32: 86, 86, 86, 86, 86, 147, 44: 254, 50: 253, 252},
		{258},
		{78, 31: 256},
		{77, 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 228, 47: 255},
		// 150
		{76, 31: 76},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 228, 47: 257},
		{75, 31: 75},
		{1: 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 42: 98},
		{86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 32: 86, 86, 86, 86, 86, 147, 44: 254, 50: 253, 260},
		// 155
		{261},
		{1: 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 42: 99},
		{1: 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 86, 32: 86, 86, 86, 86, 86, 147, 44: 264, 50: 263},
		{265, 31: 256},
		{1: 159, 161, 182, 163, 167, 155, 156, 157, 158, 177, 165, 169, 160, 162, 164, 171, 168, 166, 170, 172, 176, 174, 180, 179, 175, 178, 173, 184, 183, 181, 32: 185, 186, 187, 188, 154, 43: 228, 47: 255},
		// 160
		{1: 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 42: 100},
		{86, 37: 147, 44: 267},
		{268},
		{1: 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 42: 101},
		{1: 127, 129, 120, 134, 135, 109, 124, 125, 126, 115, 132, 136, 128, 130, 131, 138, 144, 133, 137, 139, 143, 141, 118, 117, 142, 116, 140, 122, 121, 119, 52: 110, 123, 55: 114, 57: 112, 270, 113, 111},
		// 165
		{1: 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 102, 42: 102},
	}
)

var yyhintDebug = 0

type yyhintLexer interface {
	Lex(lval *yyhintSymType) int
	Errorf(format string, a ...interface{}) error
	AppendError(err error)
	Errors() (warns []error, errs []error)
}

type yyhintLexerEx interface {
	yyhintLexer
	Reduced(rule, state int, lval *yyhintSymType) bool
}

func yyhintSymName(c int) (s string) {
	x, ok := yyhintXLAT[c]
	if ok {
		return yyhintSymNames[x]
	}

	if c < 0x7f {
		return __yyfmt__.Sprintf("%q", c)
	}

	return __yyfmt__.Sprintf("%d", c)
}

func yyhintlex1(yylex yyhintLexer, lval *yyhintSymType) (n int) {
	n = yylex.Lex(lval)
	if n <= 0 {
		n = yyhintEofCode
	}
	if yyhintDebug >= 3 {
		__yyfmt__.Printf("\nlex %s(%#x %d), lval: %+v\n", yyhintSymName(n), n, n, lval)
	}
	return n
}

func yyhintParse(yylex yyhintLexer, parser *hintParser) int {
	const yyError = 70

	yyEx, _ := yylex.(yyhintLexerEx)
	var yyn int
	var yylval yyhintSymType
	var yyVAL yyhintSymType
	yyS := make([]yyhintSymType, 200)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yyerrok := func() {
		if yyhintDebug >= 2 {
			__yyfmt__.Printf("yyerrok()\n")
		}
		Errflag = 0
	}
	_ = yyerrok
	yystate := 0
	yychar := -1
	var yyxchar int
	var yyshift int
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yyhintSymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	if yychar < 0 {
		yylval.yys = yystate
		yychar = yyhintlex1(yylex, &yylval)
		var ok bool
		if yyxchar, ok = yyhintXLAT[yychar]; !ok {
			yyxchar = len(yyhintSymNames) // > tab width
		}
	}
	if yyhintDebug >= 4 {
		var a []int
		for _, v := range yyS[:yyp+1] {
			a = append(a, v.yys)
		}
		__yyfmt__.Printf("state stack %v\n", a)
	}
	row := yyhintParseTab[yystate]
	yyn = 0
	if yyxchar < len(row) {
		if yyn = int(row[yyxchar]); yyn != 0 {
			yyn += yyhintTabOfs
		}
	}
	switch {
	case yyn > 0: // shift
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		yyshift = yyn
		if yyhintDebug >= 2 {
			__yyfmt__.Printf("shift, and goto state %d\n", yystate)
		}
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	case yyn < 0: // reduce
	case yystate == 1: // accept
		if yyhintDebug >= 2 {
			__yyfmt__.Println("accept")
		}
		goto ret0
	}

	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			if yyhintDebug >= 1 {
				__yyfmt__.Printf("no action for %s in state %d\n", yyhintSymName(yychar), yystate)
			}
			msg, ok := yyhintXErrors[yyhintXError{yystate, yyxchar}]
			if !ok {
				msg, ok = yyhintXErrors[yyhintXError{yystate, -1}]
			}
			if !ok && yyshift != 0 {
				msg, ok = yyhintXErrors[yyhintXError{yyshift, yyxchar}]
			}
			if !ok {
				msg, ok = yyhintXErrors[yyhintXError{yyshift, -1}]
			}
			if yychar > 0 {
				ls := yyhintTokenLiteralStrings[yychar]
				if ls == "" {
					ls = yyhintSymName(yychar)
				}
				if ls != "" {
					switch {
					case msg == "":
						msg = __yyfmt__.Sprintf("unexpected %s", ls)
					default:
						msg = __yyfmt__.Sprintf("unexpected %s, %s", ls, msg)
					}
				}
			}
			if msg == "" {
				msg = "syntax error"
			}
			yylex.AppendError(__yyfmt__.Errorf(msg))
			Nerrs++
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				row := yyhintParseTab[yyS[yyp].yys]
				if yyError < len(row) {
					yyn = int(row[yyError]) + yyhintTabOfs
					if yyn > 0 { // hit
						if yyhintDebug >= 2 {
							__yyfmt__.Printf("error recovery found error shift in state %d\n", yyS[yyp].yys)
						}
						yystate = yyn /* simulate a shift of "error" */
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyhintDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			if yyhintDebug >= 2 {
				__yyfmt__.Printf("error recovery failed\n")
			}
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyhintDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyhintSymName(yychar))
			}
			if yychar == yyhintEofCode {
				goto ret1
			}

			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	r := -yyn
	x0 := yyhintReductions[r]
	x, n := x0.xsym, x0.components
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= n
	if yyp+1 >= len(yyS) {
		nyys := make([]yyhintSymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	exState := yystate
	yystate = int(yyhintParseTab[yyS[yyp].yys][x]) + yyhintTabOfs
	/* reduction by production r */
	if yyhintDebug >= 2 {
		__yyfmt__.Printf("reduce using rule %v (%s), and goto state %d\n", r, yyhintSymNames[x], yystate)
	}

	switch r {
	case 1:
		{
			parser.result = yyS[yypt-0].hints
		}
	case 2:
		{
			if yyS[yypt-0].hint != nil {
				yyVAL.hints = []*ast.TableOptimizerHint{yyS[yypt-0].hint}
			}
		}
	case 3:
		{
			if yyS[yypt-0].hint != nil {
				yyVAL.hints = append(yyS[yypt-2].hints, yyS[yypt-0].hint)
			} else {
				yyVAL.hints = yyS[yypt-2].hints
			}
		}
	case 4:
		{
			parser.warnUnsupportedHint(yyS[yypt-3].ident)
			yyVAL.hint = nil
		}
	case 5:
		{
			parser.warnUnsupportedHint(yyS[yypt-3].ident)
			yyVAL.hint = nil
		}
	case 6:
		{
			parser.warnUnsupportedHint(yyS[yypt-3].ident)
			yyVAL.hint = nil
		}
	case 7:
		{
			h := yyS[yypt-1].hint
			h.HintName = model.NewCIStr(yyS[yypt-3].ident)
			yyVAL.hint = h
		}
	case 8:
		{
			parser.warnUnsupportedHint(yyS[yypt-3].ident)
			yyVAL.hint = nil
		}
	case 9:
		{
			parser.warnUnsupportedHint(yyS[yypt-4].ident)
			yyVAL.hint = nil
		}
	case 10:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-4].ident),
				QBName:   model.NewCIStr(yyS[yypt-2].ident),
				HintData: yyS[yypt-1].number,
			}
		}
	case 11:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-5].ident),
				HintData: ast.HintSetVar{
					VarName: yyS[yypt-3].ident,
					Value:   yyS[yypt-1].ident,
				},
			}
		}
	case 12:
		{
			parser.warnUnsupportedHint(yyS[yypt-3].ident)
			yyVAL.hint = nil
		}
	case 13:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-3].ident),
				QBName:   model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 14:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-3].ident),
				HintData: model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 15:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-2].ident),
			}
		}
	case 16:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-3].ident),
				HintData: model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 17:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-3].ident),
				HintData: model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 18:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				HintName: model.NewCIStr(yyS[yypt-3].ident),
				QBName:   model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 19:
		{
			yyVAL.ident = ""
		}
	case 23:
		{
			yyVAL.modelIdents = nil
		}
	case 24:
		{
			yyVAL.modelIdents = yyS[yypt-1].modelIdents
		}
	case 25:
		{
			yyVAL.modelIdents = []model.CIStr{model.NewCIStr(yyS[yypt-0].ident)}
		}
	case 26:
		{
			yyVAL.modelIdents = append(yyS[yypt-2].modelIdents, model.NewCIStr(yyS[yypt-0].ident))
		}
	case 28:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				QBName: model.NewCIStr(yyS[yypt-0].ident),
			}
		}
	case 29:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				Tables: []ast.HintTable{yyS[yypt-0].table},
				QBName: model.NewCIStr(yyS[yypt-1].ident),
			}
		}
	case 30:
		{
			h := yyS[yypt-2].hint
			h.Tables = append(h.Tables, yyS[yypt-0].table)
			yyVAL.hint = h
		}
	case 31:
		{
			yyVAL.table = ast.HintTable{
				TableName:     model.NewCIStr(yyS[yypt-2].ident),
				QBName:        model.NewCIStr(yyS[yypt-1].ident),
				PartitionList: yyS[yypt-0].modelIdents,
			}
		}
	case 32:
		{
			yyVAL.table = ast.HintTable{
				DBName:        model.NewCIStr(yyS[yypt-4].ident),
				TableName:     model.NewCIStr(yyS[yypt-2].ident),
				QBName:        model.NewCIStr(yyS[yypt-1].ident),
				PartitionList: yyS[yypt-0].modelIdents,
			}
		}
	case 33:
		{
			h := yyS[yypt-0].hint
			h.Tables = []ast.HintTable{yyS[yypt-2].table}
			h.QBName = model.NewCIStr(yyS[yypt-3].ident)
			yyVAL.hint = h
		}
	case 34:
		{
			yyVAL.hint = &ast.TableOptimizerHint{}
		}
	case 36:
		{
			yyVAL.hint = &ast.TableOptimizerHint{
				Indexes: []model.CIStr{model.NewCIStr(yyS[yypt-0].ident)},
			}
		}
	case 37:
		{
			h := yyS[yypt-2].hint
			h.Indexes = append(h.Indexes, model.NewCIStr(yyS[yypt-0].ident))
			yyVAL.hint = h
		}
	case 44:
		{
			yyVAL.ident = strconv.FormatUint(yyS[yypt-0].number, 10)
		}

	}

	if yyEx != nil && yyEx.Reduced(r, exState, &yyVAL) {
		return -1
	}
	goto yystack /* stack new state and value */
}

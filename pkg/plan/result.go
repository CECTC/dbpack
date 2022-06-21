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

package plan

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type ResultWithErr struct {
	Database string
	Result   proto.Result
	Warning  uint16
	Error    error
}

type ResultWithErrs []*ResultWithErr

func (r ResultWithErrs) Len() int { return len(r) }

func (r ResultWithErrs) Less(i, j int) bool {
	if r[i].Database < r[j].Database {
		return true
	}
	return false
}

func (r ResultWithErrs) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type OrderField struct {
	asc             bool
	fieldValueIndex int
	value           interface{}
}

type OrderByCell struct {
	orderField []*OrderField
	next       bool
	row        proto.Row
}

type OrderByCells []*OrderByCell

func (c OrderByCells) Len() int { return len(c) }

func (c OrderByCells) Less(i, j int) bool {
	var (
		index = 0
		res   int
	)
	for index < len(c[i].orderField) {
		isAsc := c[i].orderField[index].asc
		if isAsc {
			res = compare(c[j].orderField[index].value, c[i].orderField[index].value)
		} else {
			res = compare(c[i].orderField[index].value, c[j].orderField[index].value)
		}
		if res != 0 {
			return res > 0
		}
		index++
	}
	return res > 0
}

func compare(val1, val2 interface{}) int {
	if val1 == nil && val2 == nil {
		return 0
	} else if val1 == nil {
		return -1
	} else if val2 == nil {
		return 1
	}
	switch v1 := val1.(type) {
	case int64:
		v2 := val2.(int64)
		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		}
		return 1
	case uint64:
		v2 := val2.(uint64)
		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		}
		return 1
	case float32:
		v2 := val2.(float32)
		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		}
		return 1
	case float64:
		v2 := val2.(float64)
		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		}
		return 1
	case string:
		v2 := val2.(string)
		if v1 < v2 {
			return -1
		} else if v1 == v2 {
			return 0
		}
		return 1
	case []uint8:
		v2 := val2.([]uint8)
		if string(v1) < string(v2) {
			return -1
		} else if string(v1) == string(v2) {
			return 0
		}
		return 1
	case time.Time:
		v2 := val2.(time.Time)
		if v1.Before(v2) {
			return -1
		} else if v1.Equal(v2) {
			return 0
		}
		return 1
	default:
		log.Panicf("unsupported value type, val1: %s, val2: %s", val1, val2)
	}
	return 0
}

func (c OrderByCells) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func mergeResult(ctx context.Context, results []*ResultWithErr, orderBy *ast.OrderByClause, limit *Limit) (*mysql.MergeResult, uint16) {
	if orderBy == nil && limit == nil {
		return mergeResultWithOutOrderByAndLimit(ctx, results)
	}
	if orderBy != nil && limit != nil {
		return mergeResultWithOrderByAndLimit(ctx, results, orderBy, limit)
	}
	if orderBy != nil {
		return mergeResultWithOrderBy(ctx, results, orderBy)
	}
	if limit != nil {
		log.Panic("unsupported limit without order by")
	}
	return nil, 0
}

func mergeResultWithOutOrderByAndLimit(ctx context.Context, results []*ResultWithErr) (*mysql.MergeResult, uint16) {
	var (
		fields      []*mysql.Field
		warning     uint16 = 0
		commandType        = proto.CommandType(ctx)
		rows               = make([]proto.Row, 0)
		// Record whether mysql.Result has been traversed
		endResult = make([]bool, len(results))
	)
	for _, rlt := range results {
		warning += rlt.Warning
	}
	for {
		pop := 0
		for i, rlt := range results {
			if endResult[i] {
				continue
			}
			result := rlt.Result.(*mysql.Result)
			row, err := result.Rows.Next()
			if err != nil {
				endResult[i] = true
				continue
			}
			if commandType == constant.ComQuery {
				binaryRow := &mysql.TextRow{Row: row}
				rows = append(rows, binaryRow)
			} else {
				binaryRow := &mysql.BinaryRow{Row: row}
				rows = append(rows, binaryRow)
			}
			pop += 1
		}
		if pop == 0 {
			break
		}
	}
	fields = results[0].Result.(*mysql.Result).Fields
	result := &mysql.MergeResult{
		Fields:       fields,
		AffectedRows: 0,
		InsertId:     0,
		Rows:         rows,
	}
	return result, warning
}

func mergeResultWithOrderByAndLimit(ctx context.Context, results []*ResultWithErr, orderBy *ast.OrderByClause, limit *Limit) (*mysql.MergeResult, uint16) {
	var (
		fields        []*mysql.Field
		orderByFields []*OrderField
		warning       uint16 = 0
		offset        int64
		count         int64
		rowCount      int64
		commandType   = proto.CommandType(ctx)
		rows          = make([]proto.Row, 0)
		cells         = make([]*OrderByCell, len(results))
		endResult     = make([]bool, len(results))
	)
	fields = results[0].Result.(*mysql.Result).Fields
	orderByFields = castOrderByItemsToOrderField(orderBy, fields)
	offset = limit.Offset
	count = limit.Count
	rowCount = 0
	for {
		pop := 0
		for i, rlt := range results {
			if cells[i] != nil && !cells[i].next {
				continue
			}
			if (cells[i] == nil || cells[i].next) && !endResult[i] {
				result := rlt.Result.(*mysql.Result)
				row, err := result.Rows.Next()
				if err != nil {
					endResult[i] = true
					continue
				}
				pop += 1
				orderFields := copyOrderFields(orderByFields)
				if commandType == constant.ComQuery {
					textRow := &mysql.TextRow{Row: row}
					values, err := textRow.Decode()
					if err != nil {
						log.Panic(err)
					}
					for _, of := range orderFields {
						of.value = values[of.fieldValueIndex].Val
					}

					cells[i] = &OrderByCell{
						orderField: orderFields,
						next:       false,
						row:        textRow,
					}
				} else {
					binaryRow := &mysql.BinaryRow{Row: row}
					values, err := binaryRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					for _, of := range orderFields {
						of.value = values[of.fieldValueIndex].Val
					}

					cells[i] = &OrderByCell{
						orderField: orderFields,
						next:       false,
						row:        binaryRow,
					}
				}
			}
		}
		if pop == 0 {
			break
		}
		cell := compareOrderByCells(cells)
		rowCount += 1
		if rowCount > offset {
			rows = append(rows, cell.row)
			if int64(len(rows)) == count {
				break
			}
		}
	}

	if int64(len(rows)) != count {
		leftCount := countOrderByCells(cells)
		for leftCount > 0 {
			cell := compareOrderByCells(cells)
			rows = append(rows, cell.row)
			leftCount = countOrderByCells(cells)
		}
	}

	for _, rlt := range results {
		warning += rlt.Warning
	}
	result := &mysql.MergeResult{
		Fields:       fields,
		AffectedRows: 0,
		InsertId:     0,
		Rows:         rows,
	}
	return result, warning
}

func mergeResultWithOrderBy(ctx context.Context, results []*ResultWithErr, orderBy *ast.OrderByClause) (*mysql.MergeResult, uint16) {
	var (
		fields        []*mysql.Field
		orderByFields []*OrderField
		warning       uint16 = 0
		commandType          = proto.CommandType(ctx)
		// result rows
		rows = make([]proto.Row, 0)
		// OrderBy compare
		cells = make([]*OrderByCell, len(results))
		// Record whether mysql.Result has been traversed
		endResult = make([]bool, len(results))
	)
	fields = results[0].Result.(*mysql.Result).Fields
	orderByFields = castOrderByItemsToOrderField(orderBy, fields)
	for {
		pop := 0
		for i, rlt := range results {
			if cells[i] != nil && !cells[i].next {
				continue
			}
			if (cells[i] == nil || cells[i].next) && !endResult[i] {
				result := rlt.Result.(*mysql.Result)
				row, err := result.Rows.Next()
				if err != nil {
					endResult[i] = true
					continue
				}
				pop += 1
				orderFields := copyOrderFields(orderByFields)
				if commandType == constant.ComQuery {
					textRow := &mysql.TextRow{Row: row}
					values, err := textRow.Decode()
					if err != nil {
						log.Panic(err)
					}
					for _, of := range orderFields {
						of.value = values[of.fieldValueIndex].Val
					}

					cells[i] = &OrderByCell{
						orderField: orderFields,
						next:       false,
						row:        textRow,
					}
				} else {
					binaryRow := &mysql.BinaryRow{Row: row}
					values, err := binaryRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					for _, of := range orderFields {
						of.value = values[of.fieldValueIndex].Val
					}

					cells[i] = &OrderByCell{
						orderField: orderFields,
						next:       false,
						row:        binaryRow,
					}
				}
			}
		}
		if pop == 0 {
			break
		}
		cell := compareOrderByCells(cells)
		rows = append(rows, cell.row)
	}

	count := countOrderByCells(cells)
	for count > 0 {
		cell := compareOrderByCells(cells)
		rows = append(rows, cell.row)
		count = countOrderByCells(cells)
	}

	for _, rlt := range results {
		warning += rlt.Warning
	}
	result := &mysql.MergeResult{
		Fields:       fields,
		AffectedRows: 0,
		InsertId:     0,
		Rows:         rows,
	}
	return result, warning
}

func countOrderByCells(cells []*OrderByCell) int {
	count := 0
	for _, cell := range cells {
		if !cell.next {
			count++
		}
	}
	return count
}

func compareOrderByCells(cells []*OrderByCell) *OrderByCell {
	cellSlice := make([]*OrderByCell, 0)
	for _, cell := range cells {
		if !cell.next {
			cellSlice = append(cellSlice, cell)
		}
	}
	sort.Sort(OrderByCells(cellSlice))
	cellSlice[0].next = true
	return cellSlice[0]
}

func castOrderByItemsToOrderField(orderBy *ast.OrderByClause, fields []*mysql.Field) []*OrderField {
	var (
		sb     strings.Builder
		result []*OrderField
	)
	for _, item := range orderBy.Items {
		sb.Reset()
		restoreCtx := format.NewRestoreCtx(format.RestoreKeyWordUppercase, &sb)
		if err := item.Expr.Restore(restoreCtx); err != nil {
			log.Fatal(err)
		}
		orderByField := sb.String()
		orderByIndex := getOrderByFieldIndex(orderByField, fields)
		result = append(result, &OrderField{asc: !item.Desc, fieldValueIndex: orderByIndex})
	}
	return result
}

func copyOrderFields(fields []*OrderField) []*OrderField {
	var result []*OrderField
	for _, field := range fields {
		result = append(result, &OrderField{asc: field.asc, fieldValueIndex: field.fieldValueIndex})
	}
	return result
}

func getOrderByFieldIndex(orderByField string, fields []*mysql.Field) int {
	for i, field := range fields {
		if strings.EqualFold(orderByField, field.Name) {
			return i
		}
	}
	return 0
}

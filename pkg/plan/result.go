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

type OrderByCell struct {
	Index int
	Val   interface{}
	Next  bool
	Row   proto.Row
}

type OrderByCells []*OrderByCell

func (c OrderByCells) Len() int { return len(c) }

func (c OrderByCells) Less(i, j int) bool {
	switch val1 := c[i].Val.(type) {
	case int64:
		val2 := c[j].Val.(int64)
		if val1 < val2 {
			return true
		}
	case float32:
		val2 := c[j].Val.(float32)
		if val1 < val2 {
			return true
		}
	case float64:
		val2 := c[j].Val.(float64)
		if val1 < val2 {
			return true
		}
	case string:
		val2 := c[j].Val.(string)
		if val1 < val2 {
			return true
		}
	case []uint8:
		val2 := c[j].Val.([]uint8)
		if string(val1) < string(val2) {
			return true
		}
	case time.Time:
		val2 := c[j].Val.(time.Time)
		if val1.Before(val2) {
			return true
		}
	}
	return false
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
		log.Fatal("unsupported limit without order by")
	}
	return nil, 0
}

func mergeResultWithOutOrderByAndLimit(ctx context.Context, results []*ResultWithErr) (*mysql.MergeResult, uint16) {
	var (
		fields      []*mysql.Field
		warning     uint16 = 0
		commandType        = proto.CommandType(ctx)
		rows               = make([]proto.Row, 0)
	)
	for _, rlt := range results {
		result := rlt.Result.(*mysql.Result)
		for {
			row, err := result.Rows.Next()
			if err != nil {
				break
			}
			if commandType == constant.ComQuery {
				binaryRow := &mysql.TextRow{Row: row}
				rows = append(rows, binaryRow)
			} else {
				binaryRow := &mysql.BinaryRow{Row: row}
				rows = append(rows, binaryRow)
			}
		}
		warning += rlt.Warning
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
		sb           strings.Builder
		fields       []*mysql.Field
		orderByField string
		orderByIndex int
		warning      uint16 = 0
		desc         bool
		offset       int64
		count        int64
		rowCount     int64
		commandType  = proto.CommandType(ctx)
		rows         = make([]proto.Row, 0)
		cells        = make([]*OrderByCell, len(results))
		endResult    = make([]bool, len(results))
	)
	if len(orderBy.Items) > 0 {
		// todo built-in lightweight sql engine sorting
	}
	fields = results[0].Result.(*mysql.Result).Fields
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := orderBy.Items[0].Expr.Restore(restoreCtx); err != nil {
		log.Fatal(err)
	}
	orderByField = sb.String()
	orderByIndex = getOrderByFieldIndex(orderByField, fields)
	desc = orderBy.Items[0].Desc
	offset = limit.Offset
	count = limit.Count
	rowCount = 0
	for {
		pop := 0
		for i, rlt := range results {
			if cells[i] != nil && !cells[i].Next {
				pop += 1
			}
			if (cells[i] == nil || cells[i].Next) && !endResult[i] {
				result := rlt.Result.(*mysql.Result)
				row, err := result.Rows.Next()
				if err != nil {
					endResult[i] = true
					continue
				}
				pop += 1
				if commandType == constant.ComQuery {
					textRow := &mysql.TextRow{Row: row}
					values, err := textRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					value := values[orderByIndex]
					cells[i] = &OrderByCell{
						Index: i,
						Val:   value.Val,
						Next:  false,
						Row:   textRow,
					}
				} else {
					binaryRow := &mysql.BinaryRow{Row: row}
					values, err := binaryRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					value := values[orderByIndex]
					cells[i] = &OrderByCell{
						Index: i,
						Val:   value.Val,
						Next:  false,
						Row:   binaryRow,
					}
				}
			}
		}
		if pop == 0 {
			break
		}
		cell := compareOrderByCells(cells, desc)
		rowCount += 1
		cells[cell.Index].Next = true
		if rowCount > offset {
			rows = append(rows, cell.Row)
			if int64(len(rows)) == count {
				break
			}
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
		sb           strings.Builder
		fields       []*mysql.Field
		orderByField string
		orderByIndex int
		warning      uint16 = 0
		desc         bool
		commandType  = proto.CommandType(ctx)
		rows         = make([]proto.Row, 0)
		cells        = make([]*OrderByCell, len(results))
		endResult    = make([]bool, len(results))
	)
	if len(orderBy.Items) > 0 {
		// todo built-in lightweight sql engine sorting
	}
	fields = results[0].Result.(*mysql.Result).Fields
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := orderBy.Items[0].Expr.Restore(restoreCtx); err != nil {
		log.Fatal(err)
	}
	orderByField = sb.String()
	orderByIndex = getOrderByFieldIndex(orderByField, fields)
	desc = orderBy.Items[0].Desc
	for {
		pop := 0
		for i, rlt := range results {
			if cells[i] != nil && !cells[i].Next {
				pop += 1
			}
			if (cells[i] == nil || cells[i].Next) && !endResult[i] {
				result := rlt.Result.(*mysql.Result)
				row, err := result.Rows.Next()
				if err != nil {
					endResult[i] = true
					continue
				}
				pop += 1
				if commandType == constant.ComQuery {
					textRow := &mysql.TextRow{Row: row}
					values, err := textRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					value := values[orderByIndex]
					cells[i] = &OrderByCell{
						Index: i,
						Val:   value.Val,
						Next:  false,
						Row:   textRow,
					}
				} else {
					binaryRow := &mysql.BinaryRow{Row: row}
					values, err := binaryRow.Decode()
					if err != nil {
						log.Fatal(err)
					}
					value := values[orderByIndex]
					cells[i] = &OrderByCell{
						Index: i,
						Val:   value.Val,
						Next:  false,
						Row:   binaryRow,
					}
				}
			}
		}
		if pop == 0 {
			break
		}
		cell := compareOrderByCells(cells, desc)
		rows = append(rows, cell.Row)
		cells[cell.Index].Next = true
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

func compareOrderByCells(cells []*OrderByCell, desc bool) *OrderByCell {
	cellSlice := make([]*OrderByCell, 0)
	for _, cell := range cells {
		if !cell.Next {
			cellSlice = append(cellSlice, cell)
		}
	}
	sort.Sort(OrderByCells(cellSlice))
	if desc {
		return cellSlice[len(cellSlice)-1]
	}
	return cellSlice[0]
}

func getOrderByFieldIndex(orderByField string, fields []*mysql.Field) int {
	for i, field := range fields {
		if strings.EqualFold(orderByField, field.Name) {
			return i
		}
	}
	return 0
}

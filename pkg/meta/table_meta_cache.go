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

package meta

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
)

var ExpireTime = 15 * time.Minute

var tableMetaCache = &MysqlTableMetaCache{
	tableMetaCache: cache.New(ExpireTime, 10*ExpireTime),
}

func GetTableMetaCache() *MysqlTableMetaCache {
	return tableMetaCache
}

type MysqlTableMetaCache struct {
	tableMetaCache *cache.Cache
}

func (cache *MysqlTableMetaCache) GetTableMeta(ctx context.Context, db proto.DB, tableName string) (schema.TableMeta, error) {
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return schema.TableMeta{}, errors.New("TableMeta cannot be fetched without schemaName")
	}
	if tableName == "" {
		return schema.TableMeta{}, errors.New("TableMeta cannot be fetched without tableName")
	}
	cacheKey := cache.GetCacheKey(schemaName, tableName)
	tMeta, found := cache.tableMetaCache.Get(cacheKey)
	if found {
		meta := tMeta.(schema.TableMeta)
		return meta, nil
	} else {
		meta, err := cache.FetchSchema(ctx, db, tableName)
		if err != nil {
			return schema.TableMeta{}, errors.WithStack(err)
		}
		cache.tableMetaCache.Set(cacheKey, meta, ExpireTime)
		return meta, nil
	}
}

func (cache *MysqlTableMetaCache) Refresh(db proto.DB) error {
	for k, v := range cache.tableMetaCache.Items() {
		meta := v.Object.(schema.TableMeta)
		key := cache.GetCacheKey(meta.SchemaName, meta.TableName)
		if k == key {
			tMeta, err := cache.FetchSchema(
				proto.WithSchema(context.Background(), meta.SchemaName), db, meta.TableName)
			if err != nil {
				return errors.WithStack(err)
			}
			if !cmp.Equal(tMeta, meta) {
				cache.tableMetaCache.Set(key, tMeta, ExpireTime)
				log.Info("table meta change was found, update table meta cache automatically.")
			}
		}
	}
	return nil
}

func (cache *MysqlTableMetaCache) GetCacheKey(dbName, tableName string) string {
	var defaultTableName string
	tableNameWithCatalog := strings.Split(strings.ReplaceAll(tableName, "`", ""), ".")
	if len(tableNameWithCatalog) > 1 {
		defaultTableName = tableNameWithCatalog[1]
	} else {
		defaultTableName = tableNameWithCatalog[0]
	}
	return fmt.Sprintf("%s.%s", dbName, defaultTableName)
}

func (cache *MysqlTableMetaCache) FetchSchema(ctx context.Context, db proto.DB, tableName string) (schema.TableMeta, error) {
	schemaName := proto.Schema(ctx)
	tm := schema.TableMeta{
		SchemaName: schemaName,
		TableName:  tableName,
		AllColumns: make(map[string]schema.ColumnMeta),
		AllIndexes: make(map[string]schema.IndexMeta),
	}
	columnMetas, err := GetColumns(ctx, db, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any columns in the table: %s", tableName)
	}
	columns := make([]string, 0)
	for _, column := range columnMetas {
		tm.AllColumns[column.ColumnName] = column
		columns = append(columns, column.ColumnName)
	}
	tm.Columns = columns
	indexes, err := GetIndexes(ctx, db, tableName)
	if err != nil {
		return schema.TableMeta{}, errors.Wrapf(err, "Could not found any index in the table: %s", tableName)
	}
	for _, index := range indexes {
		col := tm.AllColumns[index.ColumnName]
		idx, ok := tm.AllIndexes[index.IndexName]
		if ok {
			idx.Values = append(idx.Values, col)
		} else {
			index.Values = append(index.Values, col)
			tm.AllIndexes[index.IndexName] = index
		}
	}
	if len(tm.AllIndexes) == 0 {
		return schema.TableMeta{}, errors.Errorf("Could not found any index in the table: %s", tableName)
	}

	return tm, nil
}

func GetColumns(ctx context.Context, db proto.DB, tableName string) ([]schema.ColumnMeta, error) {
	var (
		schemaName = proto.Schema(ctx)
		tn         = escape(tableName, "`")
	)
	//`TABLE_CATALOG`,	`TABLE_SCHEMA`,	`TABLE_NAME`,	`COLUMN_NAME`,	`ORDINAL_POSITION`,	`COLUMN_DEFAULT`,
	//`IS_NULLABLE`, `DATA_TYPE`,	`CHARACTER_MAXIMUM_LENGTH`,	`CHARACTER_OCTET_LENGTH`,	`NUMERIC_PRECISION`,
	//`NUMERIC_SCALE`, `DATETIME_PRECISION`, `CHARACTER_SET_NAME`,	`COLLATION_NAME`,	`COLUMN_TYPE`,	`COLUMN_KEY',
	//`EXTRA`,	`PRIVILEGES`, `COLUMN_COMMENT`, `GENERATION_EXPRESSION`, `SRS_ID`
	s := "SELECT `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, " +
		"`NUMERIC_PRECISION`, `NUMERIC_SCALE`, `IS_NULLABLE`, `COLUMN_COMMENT`, `COLUMN_DEFAULT`, `CHARACTER_OCTET_LENGTH`, " +
		"`ORDINAL_POSITION`, `COLUMN_KEY`, `EXTRA`  FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	// should use new context, otherwise, some filters will be executed repeatedly.
	dataTable, _, err := db.ExecuteSql(context.Background(), s, schemaName, tn)
	if err != nil {
		return nil, err
	}

	dt := dataTable.(*mysql.Result)
	result := make([]schema.ColumnMeta, 0)
	for {
		row, err := dt.Rows.Next()
		if err != nil {
			break
		}

		binaryRow := mysql.BinaryRow{Row: row}
		values, err := binaryRow.Decode()
		if err != nil {
			break
		}

		col := schema.ColumnMeta{}

		if values[0] != nil {
			col.TableCat = fmt.Sprintf("%s", values[0].Val)
		}
		if values[1] != nil {
			col.TableSchemeName = fmt.Sprintf("%s", values[1].Val)
		}
		if values[2] != nil {
			col.TableName = fmt.Sprintf("%s", values[2].Val)
		}
		if values[3] != nil {
			col.ColumnName = strings.Trim(fmt.Sprintf("%s", values[3].Val), "` ")
		}
		if values[4] != nil {
			col.DataTypeName = fmt.Sprintf("%s", values[4].Val)
			col.DataType = constant.GetSqlDataType(col.DataTypeName)
		}
		if values[5] != nil {
			col.ColumnSize = values[5].Val.(int64)
		}
		if values[6] != nil {
			col.DecimalDigits = values[6].Val.(int64)
		}
		if values[7] != nil {
			col.NumPrecRadix = values[7].Val.(int64)
		}
		if values[8] != nil {
			col.IsNullable = fmt.Sprintf("%s", values[8].Val)
			if strings.ToLower(col.IsNullable) == "yes" {
				col.Nullable = 1
			} else {
				col.Nullable = 0
			}
		}
		if values[9] != nil {
			col.Remarks = fmt.Sprintf("%s", values[9].Val)
		}
		if values[10] != nil {
			col.ColumnDef = fmt.Sprintf("%s", values[10].Val)
		}
		col.SqlDataType = 0
		col.SqlDatetimeSub = 0
		if values[11] != nil {
			col.CharOctetLength = values[11].Val.(int64)
		}
		if values[12] != nil {
			col.OrdinalPosition = values[12].Val.(int64)
		}
		if values[14] != nil {
			col.IsAutoIncrement = fmt.Sprintf("%s", values[14].Val)
		}

		result = append(result, col)
	}

	return result, nil
}

func GetIndexes(ctx context.Context, db proto.DB, tableName string) ([]schema.IndexMeta, error) {
	var (
		schemaName = proto.Schema(ctx)
		tn         = escape(tableName, "`")
	)
	//`TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_SCHEMA`, `INDEX_NAME`, `SEQ_IN_INDEX`,
	//`COLUMN_NAME`, `COLLATION`, `CARDINALITY`, `SUB_PART`, `PACKED`, `NULLABLE`, `INDEX_TYPE`, `COMMENT`,
	//`INDEX_COMMENT`, `IS_VISIBLE`, `EXPRESSION`
	s := "SELECT `INDEX_NAME`, `COLUMN_NAME`, `NON_UNIQUE`, `INDEX_TYPE`, `SEQ_IN_INDEX`, `COLLATION`, `CARDINALITY` " +
		"FROM `INFORMATION_SCHEMA`.`STATISTICS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	// should use new context, otherwise, some filters will be executed repeatedly.
	dataTable, _, err := db.ExecuteSql(context.Background(), s, schemaName, tn)
	if err != nil {
		return nil, err
	}

	dt := dataTable.(*mysql.Result)
	result := make([]schema.IndexMeta, 0)
	for {
		row, err := dt.Rows.Next()
		if err != nil {
			break
		}

		binaryRow := mysql.BinaryRow{Row: row}
		values, err := binaryRow.Decode()
		if err != nil {
			break
		}

		index := schema.IndexMeta{
			Values: make([]schema.ColumnMeta, 0),
		}

		if values[0] != nil {
			index.IndexName = fmt.Sprintf("%s", values[0].Val)
		}
		if values[1] != nil {
			index.ColumnName = fmt.Sprintf("%s", values[1].Val)
		}
		if values[2] != nil {
			nonUnique := fmt.Sprintf("%s", values[2].Val)
			if "yes" == strings.ToLower(nonUnique) || nonUnique == "1" {
				index.NonUnique = true
			}
		}
		if values[4] != nil {
			ordinalPosition := values[4].Val.(int64)
			index.OrdinalPosition = int32(ordinalPosition)
		}
		if values[5] != nil {
			index.AscOrDesc = fmt.Sprintf("%s", values[5].Val)
		}
		if values[6] != nil {
			cardinality := values[6].Val.(int64)
			index.Cardinality = int32(cardinality)
		}
		if "primary" == strings.ToLower(index.IndexName) {
			index.IndexType = schema.IndexTypePrimary
		} else if !index.NonUnique {
			index.IndexType = schema.IndexTypeUnique
		} else {
			index.IndexType = schema.IndexTypeNormal
		}

		result = append(result, index)
	}
	return result, nil
}

func escape(tableName, cutset string) string {
	if strings.Contains(tableName, ".") {
		idx := strings.LastIndex(tableName, ".")
		tName := tableName[idx+1:]
		return strings.Trim(tName, cutset)
	} else {
		return strings.Trim(tableName, cutset)
	}
}

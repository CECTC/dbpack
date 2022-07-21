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

package crypto

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

const (
	cryptoFilter = "CryptoFilter"
	aesIV        = "awesome789dbpack"
)

type _factory struct{}

func (factory *_factory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	var (
		err     error
		content []byte
	)
	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal crypto filter config failed.")
	}
	v := &struct {
		ColumnCryptoList []*ColumnCrypto `yaml:"column_crypto_list" json:"column_crypto_list"`
	}{}
	if err = json.Unmarshal(content, &v); err != nil {
		log.Errorf("unmarshal crypto filter failed, %s", err)
		return nil, err
	}

	return &_filter{ColumnConfigs: v.ColumnCryptoList}, nil
}

type _filter struct {
	ColumnConfigs []*ColumnCrypto
}

type ColumnCrypto struct {
	Table   string
	Columns []string
	AesKey  string
}

type columnIndex struct {
	Column string
	Index  int
}

func (f *_filter) GetKind() string {
	return cryptoFilter
}

func (f *_filter) PreHandle(ctx context.Context) error {
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		stmt := proto.QueryStmt(ctx)
		switch stmtNode := stmt.(type) {
		case *ast.InsertStmt:
			config, err := f.checkInsertTable(stmtNode)
			if err != nil {
				return err
			}
			if config != nil {
				columns, err := retrieveNeedEncryptionInsertColumns(stmtNode, config)
				if err != nil {
					return err
				}
				if len(columns) != 0 {
					return encryptInsertValues(columns, config, stmtNode.Lists)
				}
			}
		case *ast.UpdateStmt:
			config, err := f.checkUpdateTable(stmtNode)
			if err != nil {
				return err
			}
			if config != nil {
				return encryptUpdateValues(stmtNode, config)
			}
		default:
			return nil
		}
	case constant.ComStmtExecute:
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmtNode := stmt.StmtNode.(type) {
		case *ast.InsertStmt:
			config, err := f.checkInsertTable(stmtNode)
			if err != nil {
				return err
			}
			if config != nil {
				columns, err := retrieveNeedEncryptionInsertColumns(stmtNode, config)
				if err != nil {
					return err
				}
				if len(columns) != 0 {
					return encryptBindVars(columns, config, &stmt.BindVars)
				}
			}
		case *ast.UpdateStmt:
			config, err := f.checkUpdateTable(stmtNode)
			if err != nil {
				return err
			}
			if config != nil {
				columns, err := retrieveNeedEncryptionUpdateColumns(stmtNode, config)
				if err != nil {
					return err
				}
				if len(columns) != 0 {
					return encryptBindVars(columns, config, &stmt.BindVars)
				}
			}
		default:
			return nil
		}
	}
	return nil
}

func (f *_filter) PostHandle(ctx context.Context, result proto.Result) error {
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		stmt := proto.QueryStmt(ctx)
		if stmtNode, ok := stmt.(*ast.SelectStmt); ok {
			if decodedResult, is := result.(*mysql.DecodedResult); is && len(decodedResult.Rows) > 0 {
				config, err := f.checkSelectTable(stmtNode)
				if err != nil {
					log.Error(err)
					return nil
				}
				if config != nil {
					columns, err := retrieveNeedDecryptionSelectColumns(decodedResult, config)
					if err != nil {
						log.Error(err)
						return nil
					}
					if len(columns) != 0 {
						decryptDecodedResult(decodedResult, config, columns)
					}
				}
			}
		}
	case constant.ComStmtExecute:
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		if stmtNode, ok := stmt.StmtNode.(*ast.SelectStmt); ok {
			if decodedResult, is := result.(*mysql.DecodedResult); is && len(decodedResult.Rows) > 0 {
				config, err := f.checkSelectTable(stmtNode)
				if err != nil {
					log.Error(err)
					return nil
				}
				if config != nil {
					columns, err := retrieveNeedDecryptionSelectColumns(decodedResult, config)
					if err != nil {
						log.Error(err)
						return nil
					}
					if len(columns) != 0 {
						decryptDecodedResult(decodedResult, config, columns)
					}
				}
			}
		}
	}
	return nil
}

func (f _filter) checkInsertTable(insertStmt *ast.InsertStmt) (*ColumnCrypto, error) {
	var sb strings.Builder
	if err := insertStmt.Table.TableRefs.Left.Restore(
		format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordUppercase, &sb)); err != nil {
		return nil, err
	}
	tableName := sb.String()
	for _, config := range f.ColumnConfigs {
		if strings.EqualFold(config.Table, tableName) {
			return config, nil
		}
	}
	return nil, nil
}

func (f _filter) checkUpdateTable(updateStmt *ast.UpdateStmt) (*ColumnCrypto, error) {
	var sb strings.Builder
	if err := updateStmt.TableRefs.TableRefs.Left.Restore(
		format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordUppercase, &sb)); err != nil {
		return nil, err
	}
	tableName := sb.String()
	for _, config := range f.ColumnConfigs {
		if strings.EqualFold(config.Table, tableName) {
			return config, nil
		}
	}
	return nil, nil
}

func (f _filter) checkSelectTable(selectStmt *ast.SelectStmt) (*ColumnCrypto, error) {
	var sb strings.Builder
	if err := selectStmt.From.TableRefs.Left.Restore(
		format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordUppercase, &sb)); err != nil {
		return nil, err
	}
	tableName := sb.String()
	for _, config := range f.ColumnConfigs {
		if strings.EqualFold(config.Table, tableName) {
			return config, nil
		}
	}
	return nil, nil
}

func retrieveNeedEncryptionInsertColumns(insertStmt *ast.InsertStmt, config *ColumnCrypto) ([]*columnIndex, error) {
	if insertStmt.Columns == nil {
		return nil, errors.New("The column to be inserted must be specified")
	}
	var result []*columnIndex
	for i, column := range insertStmt.Columns {
		if contains(config.Columns, column.Name.O) {
			result = append(result, &columnIndex{
				Column: column.Name.O,
				Index:  i,
			})
		}
	}
	return result, nil
}

func retrieveNeedEncryptionUpdateColumns(updateStmt *ast.UpdateStmt, config *ColumnCrypto) ([]*columnIndex, error) {
	var result []*columnIndex
	for i, column := range updateStmt.List {
		columnName := column.Column.Name.O
		if contains(config.Columns, columnName) {
			result = append(result, &columnIndex{
				Column: columnName,
				Index:  i,
			})
		}
	}
	return result, nil
}

func retrieveNeedDecryptionSelectColumns(decodedResult *mysql.DecodedResult, config *ColumnCrypto) ([]*columnIndex, error) {
	var result []*columnIndex
	for i, column := range decodedResult.Fields {
		if column.Name != "" && contains(config.Columns, column.Name) {
			result = append(result, &columnIndex{
				Column: column.Name,
				Index:  i,
			})
		}
	}
	return result, nil
}

// encryptInsertValues for com_query
func encryptInsertValues(columns []*columnIndex, config *ColumnCrypto, valueList [][]ast.ExprNode) error {
	for _, values := range valueList {
		for _, column := range columns {
			arg := values[column.Index]
			if param, ok := arg.(*driver.ValueExpr); ok {
				value := param.GetBytes()
				if len(value) != 0 {
					encoded, err := misc.AesEncryptCBC(value, []byte(config.AesKey), []byte(aesIV))
					if err != nil {
						return errors.Wrapf(err, "Encryption of %s failed", column.Column)
					}
					val := hex.EncodeToString(encoded)
					param.SetBytes([]byte(val))
				}
			}
		}
	}
	return nil
}

// encryptUpdateValues for com_query
func encryptUpdateValues(updateStmt *ast.UpdateStmt, config *ColumnCrypto) error {
	for _, column := range updateStmt.List {
		columnName := column.Column.Name.O
		if contains(config.Columns, columnName) {
			arg := column.Expr
			if param, ok := arg.(*driver.ValueExpr); ok {
				value := param.GetBytes()
				if len(value) != 0 {
					encoded, err := misc.AesEncryptCBC(value, []byte(config.AesKey), []byte(aesIV))
					if err != nil {
						return errors.Wrapf(err, "Encryption of %s failed", column.Column)
					}
					val := hex.EncodeToString(encoded)
					param.SetBytes([]byte(val))
				}
			}
		}
	}
	return nil
}

// encryptBindVars for com_stmt_execute
func encryptBindVars(columns []*columnIndex, config *ColumnCrypto, args *map[string]interface{}) error {
	for _, column := range columns {
		parameterID := fmt.Sprintf("v%d", column.Index+1)
		param := (*args)[parameterID]
		if arg, ok := param.(string); ok {
			encoded, err := misc.AesEncryptCBC([]byte(arg), []byte(config.AesKey), []byte(aesIV))
			if err != nil {
				return errors.Errorf("Encryption of %s failed: %v", column.Column, err)
			}
			val := hex.EncodeToString(encoded)
			(*args)[parameterID] = val
		} else if arg, ok := param.([]byte); ok {
			encoded, err := misc.AesEncryptCBC(arg, []byte(config.AesKey), []byte(aesIV))
			if err != nil {
				return errors.Errorf("Encryption of %s failed: %v", column.Column, err)
			}
			val := hex.EncodeToString(encoded)
			(*args)[parameterID] = []byte(val)
		}
	}
	return nil
}

func decryptDecodedResult(decodedResult *mysql.DecodedResult, config *ColumnCrypto, columns []*columnIndex) {
	for _, row := range decodedResult.Rows {
		switch r := row.(type) {
		case *mysql.TextRow:
			for _, column := range columns {
				protoValue := r.Values[column.Index]
				if protoValue != nil {
					if originalVal, ok := protoValue.Val.([]byte); ok {
						if n, err := hex.Decode(originalVal, originalVal); err == nil {
							if decodedVal, err := misc.AesDecryptCBC(originalVal[:n], []byte(config.AesKey), []byte(aesIV)); err == nil {
								r.Values[column.Index].Val = decodedVal
							}
						}
					}
				}
			}
		case *mysql.BinaryRow:
			for _, column := range columns {
				protoValue := r.Values[column.Index]
				if protoValue != nil {
					if originalVal, ok := protoValue.Val.([]byte); ok {
						if n, err := hex.Decode(originalVal, originalVal); err == nil {
							if decodedVal, err := misc.AesDecryptCBC(originalVal[:n], []byte(config.AesKey), []byte(aesIV)); err == nil {
								r.Values[column.Index].Val = decodedVal
							}
						}
					}
				}
			}
		}
	}
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if strings.EqualFold(v, str) {
			return true
		}
	}
	return false
}

func init() {
	filter.RegistryFilterFactory(cryptoFilter, &_factory{})
}

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

package audit_log

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/golang-module/carbon"
	"github.com/pkg/errors"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	auditLogFilter    = "AuditLogFilter"
	defaultMaxSize    = 500
	defaultMaxBackups = 1
	defaultMaxAge     = 30
)

type _factory struct {
}

func (factory *_factory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	var (
		err          error
		content      []byte
		filterConfig *AuditLogFilterConfig
	)

	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal audit log filter config failed.")
	}
	if err = json.Unmarshal(content, &filterConfig); err != nil {
		log.Errorf("unmarshal audit log filter failed, %s", err)
		return nil, err
	}
	if filterConfig.MaxSize == 0 {
		filterConfig.MaxSize = defaultMaxSize
	}
	if filterConfig.MaxBackups == 0 {
		filterConfig.MaxBackups = defaultMaxBackups
	}
	if filterConfig.MaxAge == 0 {
		filterConfig.MaxAge = defaultMaxAge
	}
	logger := &lumberjack.Logger{
		Filename:   auditLogFile(filterConfig.AuditLogDir),
		MaxSize:    filterConfig.MaxSize,
		MaxBackups: filterConfig.MaxBackups,
		MaxAge:     filterConfig.MaxAge,
		Compress:   filterConfig.Compress,
	}
	return &_filter{recordBefore: filterConfig.RecordBefore, log: logger}, nil
}

type AuditLogFilterConfig struct {
	AuditLogDir string `json:"audit_log_dir" yaml:"audit_log_dir"`
	// MaxSize is the maximum size in megabytes of the log file before it gets rotated
	MaxSize int `json:"max_size" yaml:"max_size"`
	// MaxAge is the maximum number of days to retain old log files
	MaxAge int `json:"max_age" yaml:"max_age"`
	// MaxBackups maximum number of old log files to retain
	MaxBackups int `json:"max_backups" yaml:"max_backups"`
	// Compress determines if the rotated log files should be compressed using gzip
	Compress bool `json:"compress" yaml:"compress"`
	// RecordBefore define whether to log before or after sql execution
	RecordBefore bool `json:"record_before" yaml:"record_before"`
}

type _filter struct {
	recordBefore bool
	log          *lumberjack.Logger
}

func (f *_filter) GetKind() string {
	return auditLogFilter
}

func (f *_filter) PreHandle(ctx context.Context, conn proto.Connection) error {
	if !f.recordBefore {
		return nil
	}
	userName := proto.UserName(ctx)
	remoteAddr := proto.RemoteAddr(ctx)
	connectionID := proto.ConnectionID(ctx)
	commandType := proto.CommandType(ctx)
	sqlText := proto.SqlText(ctx)

	var (
		commandTypeStr string
		args           strings.Builder
		stmtNode       ast.StmtNode
	)
	args.WriteByte('[')
	switch commandType {
	case constant.ComQuery:
		commandTypeStr = "COM_QUERY"
		stmtNode = proto.QueryStmt(ctx)
	case constant.ComStmtExecute:
		commandTypeStr = "COM_STMT_EXECUTE"
		statement := proto.PrepareStmt(ctx)
		stmtNode = statement.StmtNode
		for i := 0; i < len(statement.BindVars); i++ {
			parameterID := fmt.Sprintf("v%d", i+1)
			param := statement.BindVars[parameterID]
			switch arg := param.(type) {
			case []byte, string:
				args.WriteString(fmt.Sprintf("'%s'", arg))
			case nil:
				args.WriteString("NULL")
			default:
				args.WriteString(fmt.Sprintf("'%v'", arg))
			}
			if i < len(statement.BindVars)-1 {
				args.WriteByte(' ')
			}
		}
	default:
		return nil
	}
	args.WriteByte(']')

	command := misc.GetStmtLabel(stmtNode)
	command = strings.ToUpper(command)

	if _, err := f.log.Write([]byte(fmt.Sprintf("%s,%s,%s,%v,%s,%s,%s,%s,0\n", carbon.Now(), userName, remoteAddr, connectionID,
		commandTypeStr, command, sqlText, args.String()))); err != nil {
		return err
	}
	return nil
}

func (f *_filter) PostHandle(ctx context.Context, result proto.Result, conn proto.Connection) error {
	if f.recordBefore {
		return nil
	}
	userName := proto.UserName(ctx)
	remoteAddr := proto.RemoteAddr(ctx)
	connectionID := proto.ConnectionID(ctx)
	commandType := proto.CommandType(ctx)
	sqlText := proto.SqlText(ctx)

	var (
		commandTypeStr string
		args           strings.Builder
		stmtNode       ast.StmtNode
	)
	args.WriteByte('[')
	switch commandType {
	case constant.ComQuery:
		commandTypeStr = "COM_QUERY"
		stmtNode = proto.QueryStmt(ctx)
	case constant.ComStmtExecute:
		commandTypeStr = "COM_STMT_EXECUTE"
		statement := proto.PrepareStmt(ctx)
		stmtNode = statement.StmtNode
		for i := 0; i < len(statement.BindVars); i++ {
			parameterID := fmt.Sprintf("v%d", i+1)
			param := statement.BindVars[parameterID]
			switch arg := param.(type) {
			case []byte, string:
				args.WriteString(fmt.Sprintf("'%s'", arg))
			case nil:
				args.WriteString("NULL")
			default:
				args.WriteString(fmt.Sprintf("'%v'", arg))
			}
			if i < len(statement.BindVars)-1 {
				args.WriteByte(' ')
			}
		}
	default:
		return nil
	}
	args.WriteByte(']')

	command := misc.GetStmtLabel(stmtNode)
	command = strings.ToUpper(command)

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if _, err := f.log.Write([]byte(fmt.Sprintf("%s,%s,%s,%v,%s,%s,%s,%s,%v\n", carbon.Now(), userName, remoteAddr, connectionID,
		commandTypeStr, command, sqlText, args.String(), affected))); err != nil {
		return err
	}
	return nil
}

func auditLogFile(dir string) string {
	return filepath.Join(dir, "audit.log")
}

func init() {
	filter.RegistryFilterFactory(auditLogFilter, &_factory{})
}

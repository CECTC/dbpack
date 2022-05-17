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

package dt

import (
	"context"
	"fmt"
	"time"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/undolog"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
)

const (
	DeleteUndoLogByIDSql     = "DELETE FROM undo_log WHERE id = ?"
	DeleteUndoLogByXIDSql    = "DELETE FROM undo_log WHERE xid = ?"
	DeleteUndoLogByCreateSql = "DELETE FROM undo_log WHERE log_created <= ? LIMIT ?"
	InsertUndoLogSql         = `INSERT INTO undo_log (xid, branch_id, context, rollback_info, log_status, log_created,
		log_modified) VALUES (?, ?, ?, ?, ?, now(), now())`
	SelectUndoLogSql = `SELECT branch_id, context, rollback_info, log_status FROM undo_log
       WHERE xid = ? ORDER BY id DESC FOR UPDATE`
)

type State byte

const (
	Normal State = iota
	GlobalFinished
)

func (state State) String() string {
	switch state {
	case Normal:
		return "Normal"
	case GlobalFinished:
		return "GlobalFinished"
	default:
		return fmt.Sprintf("%d", state)
	}
}

type MysqlUndoLogManager struct {
}

func GetUndoLogManager() MysqlUndoLogManager {
	return MysqlUndoLogManager{}
}

func (manager MysqlUndoLogManager) Undo(db proto.DB, xid string) ([]string, error) {
	var (
		tx       proto.Tx
		lockKeys []string
		result   proto.Result
		err      error
	)

	if tx, _, err = db.Begin(context.Background()); err != nil {
		return lockKeys, err
	}

	if result, _, err = tx.ExecuteSql(context.Background(), SelectUndoLogSql, xid); err != nil {
		return lockKeys, err
	}

	exists := false
	undoLogs := make([]*undolog.SqlUndoLog, 0)
	rlt := result.(*mysql.Result)
	for {
		row, err := rlt.Rows.Next()
		if err != nil {
			break
		}

		binaryRow := mysql.BinaryRow{Row: row}
		values, err := binaryRow.Decode()
		if err != nil {
			break
		}

		branchID := values[0].Val.(int64)
		rollbackInfo := values[2].Val.([]byte)
		state := values[3].Val.(int64)
		exists = true

		if State(state) != Normal {
			log.Debugf("xid %s branch %d, ignore %s undo_log", xid, branchID, State(state).String())
			return lockKeys, nil
		}

		//serializer := getSerializer(context)
		parser := undolog.GetUndoLogParser()
		undoLog := parser.DecodeSqlUndoLog(rollbackInfo)
		undoLogs = append(undoLogs, undoLog)
	}

	for _, sqlUndoLog := range undoLogs {
		tableMeta, err := meta.GetTableMetaCache().GetTableMeta(
			proto.WithSchema(context.Background(), sqlUndoLog.SchemaName), db, sqlUndoLog.TableName)
		if err != nil {
			if _, err := tx.Rollback(context.Background()); err != nil {
				return lockKeys, err
			}
			return lockKeys, err
		}

		sqlUndoLog.SetTableMeta(tableMeta)
		err = NewMysqlUndoExecutor(sqlUndoLog).Execute(tx)
		if err != nil {
			if _, err := tx.Rollback(context.Background()); err != nil {
				return lockKeys, err
			}
			return lockKeys, err
		}
		lockKeys = append(lockKeys, sqlUndoLog.LockKey)
	}

	if exists {
		_, _, err := tx.ExecuteSql(context.Background(), DeleteUndoLogByXIDSql, xid)
		if err != nil {
			if _, err := tx.Rollback(context.Background()); err != nil {
				return lockKeys, err
			}
			return lockKeys, err
		}
		log.Infof("xid %s undo_log deleted with %s\n", xid,
			GlobalFinished.String())
		if _, err := tx.Commit(context.Background()); err != nil {
			return lockKeys, err
		}
	}
	return lockKeys, nil
}

func (manager MysqlUndoLogManager) DeleteUndoLogByID(db proto.DB, id int64) error {
	result, _, err := db.ExecuteSql(context.Background(), DeleteUndoLogByIDSql, id)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	log.Infof("%d undo log deleted by id:%d", affectCount, id)
	return nil
}

func (manager MysqlUndoLogManager) DeleteUndoLogByXID(db proto.DB, xid string) error {
	result, _, err := db.ExecuteSql(context.Background(), DeleteUndoLogByXIDSql, xid)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	log.Infof("%d undo log deleted by xid:%s", affectCount, xid)
	return nil
}

func (manager MysqlUndoLogManager) DeleteUndoLogByLogCreated(db proto.DB, logCreated time.Time, limitRows int) error {
	result, _, err := db.ExecuteSql(context.Background(), DeleteUndoLogByCreateSql, logCreated, limitRows)
	if err != nil {
		return err
	}
	affectCount, _ := result.RowsAffected()
	log.Infof("%d undo log deleted created before %v", affectCount, logCreated)
	return nil
}

func (manager MysqlUndoLogManager) InsertUndoLogWithNormal(conn proto.Connection, xid string, branchID int64, undoLog *undolog.SqlUndoLog) error {
	parser := undolog.GetUndoLogParser()
	undoLogContent := parser.EncodeSqlUndoLog(undoLog)
	return manager.insertUndoLog(conn, xid, branchID, buildContext(parser.GetName()), undoLogContent, Normal)
}

func (manager MysqlUndoLogManager) InsertUndoLogWithGlobalFinished(conn proto.Connection, xid string, branchID int64, undoLog *undolog.SqlUndoLog) error {
	parser := undolog.GetUndoLogParser()
	undoLogContent := parser.EncodeSqlUndoLog(undoLog)
	return manager.insertUndoLog(conn, xid, branchID, buildContext(parser.GetName()), undoLogContent, GlobalFinished)
}

func (manager MysqlUndoLogManager) insertUndoLog(conn proto.Connection, xid string, branchID int64, rollbackCtx string,
	undoLogContent []byte, state State) error {
	bc := conn.(*driver.BackendConnection)
	args := []interface{}{xid, branchID, rollbackCtx, undoLogContent, state}
	_, _, err := bc.PrepareExecuteArgs(InsertUndoLogSql, args)
	return err
}

func buildContext(serializer string) string {
	return fmt.Sprintf("serializer=%s", serializer)
}

func getSerializer(context string) string {
	return context[10:]
}

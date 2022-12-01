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

package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cectc/dbpack/pkg/log"
)

const (
	createSegment = "CREATE TABLE IF NOT EXISTS `segment` (" +
		"    `biz_id` VARCHAR ( 128 ) NOT NULL DEFAULT ''," +
		"    `step` INT NOT NULL DEFAULT '1000'," +
		"    `max_id` INT NOT NULL DEFAULT '0'," +
		"    PRIMARY KEY ( `business_id` )" +
		");"
	segmentExits  = "SELECT EXISTS (SELECT 1 FROM `segment` WHERE `biz_id` = ?)"
	insertSegment = "INSERT INTO `segment`(`biz_id`, `step`, `max_id`) VALUES (?, ?, 1)"
	selectSegment = "SELECT max_id FROM segment WHERE biz_id = ? FOR UPDATE"
	updateSegment = "UPDATE segment SET max_id = ? WHERE biz_id = ? AND max_id = ?"
)

type SegmentWorker struct {
	db     *sql.DB
	buffer chan int64
	min    int64
	max    int64
	bizID  string
	step   int64
}

func NewSegmentWorker(dsn string, len int64, biz string) (*SegmentWorker, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(createSegment); err != nil {
		log.Errorf("failed to create segment table: %w", err)
		return nil, err
	}

	var exists = false
	row := db.QueryRowContext(context.Background(), segmentExits, biz)
	if row.Err() == nil {
		if err := row.Scan(&exists); err != nil {
			log.Error(err)
			return nil, err
		}
	}
	if !exists {
		if _, err := db.ExecContext(context.Background(), insertSegment, biz, len); err != nil {
			log.Error(err)
			return nil, err
		}
	}
	worker := &SegmentWorker{
		db:     db,
		buffer: make(chan int64, len),
		bizID:  biz,
		step:   len,
	}
	go worker.ProduceID()

	return worker, nil
}

func (w *SegmentWorker) NextID() (int64, error) {
	select {
	case <-time.After(time.Second):
		return 0, fmt.Errorf("get id timeout")
	case id := <-w.buffer:
		return id, nil
	}
}

func (w *SegmentWorker) ProduceID() {
	if err := w.reload(); err != nil {
		log.Error(err)
	}
	for {
		if w.min >= w.max {
			if err := w.reload(); err != nil {
				log.Error(err)
				continue
			}
		}

		w.buffer <- w.min
		w.min++
	}
}

func (w *SegmentWorker) reload() error {
	var err error
	for {
		err = w.fetchSegmentFromDB()
		if err == nil {
			return nil
		}
		log.Errorf("failed to fetch id from db: %w", err)
		time.Sleep(time.Second)
	}
}

func (w *SegmentWorker) fetchSegmentFromDB() (err error) {
	var (
		maxID int64
		tx    *sql.Tx
	)

	tx, err = w.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Error(rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				log.Error(commitErr)
			}
		}
	}()

	row := tx.QueryRow(selectSegment, w.bizID)
	if err = row.Scan(&maxID); err != nil {
		return err
	}

	if _, err = tx.Exec(updateSegment, maxID+w.step, w.bizID, maxID); err != nil {
		return err
	}

	w.min = maxID
	w.max = maxID + w.step
	return nil
}

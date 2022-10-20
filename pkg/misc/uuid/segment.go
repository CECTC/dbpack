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

package uuid

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cectc/dbpack/pkg/log"
)

const createTable = "CREATE TABLE IF NOT EXISTS `segment` (`max_id` int NOT NULL DEFAULT '0',`step` int NOT NULL DEFAULT '1000',`business_id` varchar(50) NOT NULL DEFAULT '',PRIMARY KEY (`business_id`));"

type SegmentWorker struct {
	db     *sql.DB
	buffer chan int64
	min    int64
	max    int64
	bizID  string
	step   int64
}

// TODO: we should close the connection
func NewSegmentWorker(dsn string, len int64, biz string) (*SegmentWorker, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(createTable); err != nil {
		log.Errorf("failed to create segment table: %w", err)
		return nil, err
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
	w.reload()
	for {
		if w.min >= w.max {
			w.reload()
		}

		w.min++
		w.buffer <- w.min
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

func (w *SegmentWorker) fetchSegmentFromDB() error {
	var maxID int64

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row := tx.QueryRow("SELECT max_id FROM segment WHERE business_id = ? FOR UPDATE", w.bizID)
	err = row.Scan(&maxID)
	// it will be no rows when query first time
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	_, err = tx.Exec("UPDATE segment SET max_id = ? WHERE business_id = ?", maxID+w.step, w.bizID)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	w.min = maxID
	w.max = maxID + w.step
	return nil
}

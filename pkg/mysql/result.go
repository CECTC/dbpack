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

package mysql

import "github.com/cectc/dbpack/pkg/proto"

type Result struct {
	Fields       []*Field // Columns information
	AffectedRows uint64
	InsertId     uint64
	Rows         *Rows
}

func (res *Result) LastInsertId() (uint64, error) {
	return res.InsertId, nil
}

func (res *Result) RowsAffected() (uint64, error) {
	return res.AffectedRows, nil
}

type DecodedResult struct {
	Fields       []*Field
	AffectedRows uint64
	InsertId     uint64
	Rows         []proto.Row
}

func (res *DecodedResult) LastInsertId() (uint64, error) {
	return res.InsertId, nil
}

func (res *DecodedResult) RowsAffected() (uint64, error) {
	return res.AffectedRows, nil
}

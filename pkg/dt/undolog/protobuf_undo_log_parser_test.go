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

package undolog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
)

func getBranchUndoLog() *BranchUndoLog {
	var branchUndoLog = &BranchUndoLog{
		Xid:      ":0:2000042948",
		BranchID: 2000042936,
		SqlUndoLogs: []*SqlUndoLog{
			{
				SqlType:     constant.SQLType_INSERT,
				TableName:   "user",
				BeforeImage: nil,
				AfterImage: &schema.TableRecords{
					TableMeta: schema.TableMeta{},
					TableName: "user",
					Rows: []*schema.Row{
						{
							Fields: []*schema.Field{
								{
									Name:    "id",
									KeyType: schema.PrimaryKey,
									Type:    constant.BIGINT,
									Value:   int64(2000001),
								},
								{
									Name:    "name",
									KeyType: schema.Null,
									Type:    constant.VARCHAR,
									Value:   []byte("scott"),
								},
								{
									Name:    "age",
									KeyType: schema.Null,
									Type:    constant.INTEGER,
									Value:   int64(28),
								},
								{
									Name:    "avatar",
									KeyType: schema.Null,
									Type:    constant.BLOB,
									Value: []byte{1, 40, 1, 32, 0, 16, 74, 70, 73, 70, 0, 1, 1, 0, 0, 112, 0, 112,
										0, 0, 1, 31, 0, 116, 69, 120, 105, 102, 0, 0, 77, 77, 0, 42, 0, 0, 0, 8, 0, 4,
										1, 26, 0, 5, 0, 0, 0, 1, 0, 0, 0, 62, 1, 27, 0, 5, 0, 0, 0, 1, 0, 0, 0, 70, 1, 40,
										0, 3, 0, 0, 0, 1, 0, 2, 0, 0, 121, 105, 0, 4, 0, 0, 0, 1, 0, 0, 0, 78, 0, 0, 0,
										0, 0, 0, 0, 112, 0, 0, 0, 1, 0, 0, 81, 56, 0, 0, 1, 57, 0, 2, 96, 2, 0, 4, 0,
										0, 0, 1, 0, 0, 3, 24, 96, 3, 0, 4, 0, 0, 0, 1, 0, 0, 2, 113, 0, 0, 0, 0, 1, 19,
										0, 56, 80, 104, 111, 116, 111, 115, 104, 111, 112, 32, 51, 46, 48, 0, 56, 66, 73,
										77, 4, 4, 0, 0, 0, 0, 0, 0, 56, 66, 73, 77, 4, 37, 0, 0, 0, 0, 0, 16, 44, 29,
										116, 39, 113, 0, 78, 4, 23, 128, 9, 104, 20, 8, 66, 126, 1, 30, 15,
										84, 73, 67, 67, 95, 80, 82, 79, 70, 73, 76, 69, 0, 1, 1, 0, 0, 15, 100, 97,
										112, 112, 108, 2, 16, 0, 0, 109, 110, 116, 114, 82, 71, 66, 32, 88, 89, 90, 32,
										7, 28, 0, 2, 0, 20, 0, 23, 0, 5, 0, 48, 97, 99, 115, 112, 65, 80, 80, 76, 0, 0,
										0, 0, 65, 80, 80, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10,
										42, 0, 1, 0, 0, 0, 0, 45, 45, 97, 112, 112, 108, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
										0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
										0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 100, 101, 115, 99, 0, 0, 1, 80, 0, 0, 0, 98,
										100, 115, 99, 109, 0, 0, 1, 76, 0, 0, 4, 126, 99, 112, 114, 116, 0, 0, 6, 56,
										0, 0, 0, 35, 119, 116, 112, 116, 0, 0, 6, 92, 0, 0, 0, 20, 114, 88, 89, 90, 0,
										0, 6, 112, 0, 0, 0, 20, 103, 88, 89, 90, 0, 0, 6, 124, 0, 0, 0, 20, 98, 88, 89,
										90, 0, 0, 6, 104, 0, 0, 0, 20, 114, 84, 82, 67, 0, 0, 6, 84, 0, 0, 8, 12, 97,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return branchUndoLog
}

func TestJsonUndoLogParser_Encode(t *testing.T) {
	data := ProtoBufUndoLogParser{}.Encode(getBranchUndoLog())
	fmt.Printf("%s\n", data)
	assert.NotEqual(t, data, nil)
}

func TestJsonUndoLogParser_Decode(t *testing.T) {
	branchUndoLog := getBranchUndoLog()
	data := ProtoBufUndoLogParser{}.Encode(branchUndoLog)
	undoLog := ProtoBufUndoLogParser{}.Decode(data)
	assert.Equal(t, undoLog.BranchID, int64(2000042936))
}

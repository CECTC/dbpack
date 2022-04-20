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

type UndoLogParser interface {
	GetName() string

	// return the default content if undo log is empty
	GetDefaultContent() []byte

	Encode(branchUndoLog *BranchUndoLog) []byte

	Decode(data []byte) *BranchUndoLog

	EncodeSqlUndoLog(undoLog *SqlUndoLog) []byte

	DecodeSqlUndoLog(data []byte) *SqlUndoLog
}

func GetUndoLogParser() UndoLogParser {
	return ProtoBufUndoLogParser{}
}

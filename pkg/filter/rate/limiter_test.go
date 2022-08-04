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

package rate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
)

func TestRateLimiter(t *testing.T) {
	testCases := []string{
		"insert_limit",
		"update_limit",
		"delete_limit",
		"select_limit",
	}
	testSQLs := []string{
		"insert into student (id, name, age) values (1, 'scott', 28)",
		"update student set age = 30 where id = 1",
		"delete from student where id = 1",
		"select id, name, age from student where id = 1",
	}
	for _, tc := range testCases {
		for _, sql := range testSQLs {
			t.Run(strings.Join([]string{tc, sql}, "_"), func(t *testing.T) {
				p := parser.New()
				stmt, err := p.ParseOneStmt(sql, "", "")
				assert.Nil(t, err)
				stmt.Accept(&visitor.ParamVisitor{})

				ctx := proto.WithCommandType(context.Background(), constant.ComQuery)
				ctx = proto.WithQueryStmt(ctx, stmt)

				filter, err := (&_factory{}).NewFilter("test", map[string]interface{}{
					tc: 1,
				})
				assert.Nil(t, err)
				f := filter.(proto.DBPreFilter)

				for i := 0; i < 10; i++ {
					err := f.PreHandle(ctx)
					assert.Nil(t, err)
					t.Log(time.Now())
				}
			})
		}
	}

	testSQLs = []string{
		"insert into student (id, name, age) values (?, ?, ?)",
		"update student set age = ? where id = ?",
		"delete from student where id = ?",
		"select id, name, age from student where id = ?",
	}
	for _, tc := range testCases {
		for _, sql := range testSQLs {
			t.Run(strings.Join([]string{tc, sql}, "_"), func(t *testing.T) {
				p := parser.New()
				stmt, err := p.ParseOneStmt(sql, "", "")
				assert.Nil(t, err)
				stmt.Accept(&visitor.ParamVisitor{})

				protoStmt := &proto.Stmt{StmtNode: stmt}
				ctx := proto.WithCommandType(context.Background(), constant.ComStmtExecute)
				ctx = proto.WithPrepareStmt(ctx, protoStmt)

				filter, err := (&_factory{}).NewFilter("test", map[string]interface{}{
					tc: 1,
				})
				assert.Nil(t, err)
				f := filter.(proto.DBPreFilter)

				for i := 0; i < 10; i++ {
					err := f.PreHandle(ctx)
					assert.Nil(t, err)
					t.Log(time.Now())
				}
			})
		}
	}
}

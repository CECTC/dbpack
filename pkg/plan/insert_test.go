package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestInsertPlan(t *testing.T) {
	testCases := []struct {
		insertSql           string
		table               string
		expectedGenerateSql string
	}{
		{
			insertSql:           "insert into student(id, name, gender, age) values(?,?,?,?)",
			table:               "student_5",
			expectedGenerateSql: "INSERT INTO student_5(id,name,gender,age) VALUES (?,?,?,?)",
		},
	}

	for _, c := range testCases {
		t.Run(c.insertSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.insertSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			insertStmt := stmt.(*ast.InsertStmt)
			plan := &InsertPlan{
				Database: "school_0",
				Table:    c.table,
				Columns:  []string{"id", "name", "gender", "age"},
				Stmt:     insertStmt,
				Args:     nil,
				Executor: nil,
			}
			var sb strings.Builder
			err = plan.generate(&sb)
			assert.Nil(t, err)
			assert.Equal(t, c.expectedGenerateSql, sb.String())
		})
	}
}

package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestUpdateOnSingleDBPlan(t *testing.T) {
	testCases := []struct {
		deleteSql            string
		tables               []string
		expectedGenerateSqls []string
	}{
		{
			deleteSql: "update student set name = ?, age = ? where id in (?,?)",
			tables:    []string{"student_1", "student_5"},
			expectedGenerateSqls: []string{
				"UPDATE student_1 SET `name`=?, `age`=? WHERE `id` IN (?,?)",
				"UPDATE student_5 SET `name`=?, `age`=? WHERE `id` IN (?,?)",
			},
		},
		{
			deleteSql: "update student set name = ?, age = ? where id = 9",
			tables:    []string{"student_9"},
			expectedGenerateSqls: []string{
				"UPDATE student_9 SET `name`=?, `age`=? WHERE `id`=9",
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.deleteSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.deleteSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			updateStmt := stmt.(*ast.UpdateStmt)
			plan := &UpdateOnSingleDBPlan{
				Database: "school_0",
				Tables:   c.tables,
				Stmt:     updateStmt,
				Args:     nil,
				Executor: nil,
			}
			for i, table := range plan.Tables {
				var sb strings.Builder
				err := plan.generate(&sb, table)
				assert.Nil(t, err)
				assert.Equal(t, c.expectedGenerateSqls[i], sb.String())
			}
		})
	}
}

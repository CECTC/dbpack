package sql

import "fmt"

func addTraceSQLComment(sql string, traceID string) string {
	if sql[len(sql)-1] == ';' {
		return fmt.Sprintf("%s /* TraceID('%s') */", sql[0:len(sql)-1], traceID)
	}
	return fmt.Sprintf("%s /* TraceID('%s') */", sql, traceID)
}

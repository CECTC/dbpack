package misc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMysqlAppendInParam(t *testing.T) {
	cases := map[string]struct {
		in  int
		out string
	}{
		"1":  {1, "(?)"},
		"2":  {2, "(?,?)"},
		"3":  {3, "(?,?,?)"},
		"4":  {4, "(?,?,?,?)"},
		"5":  {5, "(?,?,?,?,?)"},
		"6":  {6, "(?,?,?,?,?,?)"},
		"7":  {7, "(?,?,?,?,?,?,?)"},
		"8":  {8, "(?,?,?,?,?,?,?,?)"},
		"9":  {9, "(?,?,?,?,?,?,?,?,?)"},
		"10": {10, "(?,?,?,?,?,?,?,?,?,?)"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := MysqlAppendInParam(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}

func TestPgsqlAppendInParam(t *testing.T) {
	cases := map[string]struct {
		in  int
		out string
	}{
		"1":  {1, "($1)"},
		"2":  {2, "($1,$2)"},
		"3":  {3, "($1,$2,$3)"},
		"4":  {4, "($1,$2,$3,$4)"},
		"5":  {5, "($1,$2,$3,$4,$5)"},
		"6":  {6, "($1,$2,$3,$4,$5,$6)"},
		"7":  {7, "($1,$2,$3,$4,$5,$6,$7)"},
		"8":  {8, "($1,$2,$3,$4,$5,$6,$7,$8)"},
		"9":  {9, "($1,$2,$3,$4,$5,$6,$7,$8,$9)"},
		"10": {10, "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := PgsqlAppendInParam(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}

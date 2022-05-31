package optimize

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

func (o Optimizer) optimizeInsert(ctx context.Context, stmt *ast.InsertStmt, args []interface{}) (proto.Plan, error) {
	var (
		alg       cond.ShardingAlgorithm
		topology  *topo.Topology
		tableMeta schema.TableMeta
		columns   []string
		exists    bool
		err       error
	)
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	for _, column := range stmt.Columns {
		columns = append(columns, column.Name.String())
	}

	if alg, exists = o.algorithms[tableName]; !exists {
		return nil, errors.New("sharding algorithm should not be nil")
	}
	if topology, exists = o.topologies[tableName]; !exists {
		return nil, errors.New("topology should not be nil")
	}

	for db, tables := range topology.DBs {
		sqlDB := resource.GetDBManager().GetDB(db)
		tableMeta, err = meta.GetTableMetaCache().GetTableMeta(ctx, sqlDB, tables[0])
		if err != nil {
			continue
		} else {
			break
		}
	}
	pk := tableMeta.GetPKName()
	index := findPkIndex(stmt, pk)
	// todo if index == -1, automatic generate a pk
	pkValue := getPkValue(ctx, stmt, index, args)

	cd := &cond.KeyCondition{
		Key:   pk,
		Op:    opcode.EQ,
		Value: pkValue,
	}
	shards, err := cd.Shard(alg)
	if err != nil {
		return nil, errors.Wrap(err, "compute shards failed")
	}
	fullScan, shardMap := shards.ParseTopology(topology)
	if fullScan && !alg.AllowFullScan() {
		return nil, errors.New("full scan not allowed")
	}

	if len(shardMap) == 1 {
		for k, v := range shardMap {
			executor, exists := o.dbGroupExecutors[k]
			if !exists {
				return nil, errors.Errorf("db group %s should not be nil", k)
			}

			return &plan.InsertPlan{
				Database: k,
				Table:    v[0],
				Columns:  columns,
				Stmt:     stmt,
				Args:     args,
				Executor: executor,
			}, nil
		}
	}
	return nil, errors.New("should never happen!")
}

func findPkIndex(stmt *ast.InsertStmt, pk string) int {
	if stmt.Columns != nil {
		for i, column := range stmt.Columns {
			if column.Name.String() == pk {
				return i
			}
		}
	}
	return -1
}

func getPkValue(ctx context.Context, stmt *ast.InsertStmt, pkIndex int, args []interface{}) interface{} {
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		var sb strings.Builder
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := stmt.Lists[0][pkIndex].Restore(ctx); err != nil {
			log.Panic(err)
		}
		return sb.String()
	case constant.ComStmtExecute:
		return args[pkIndex]
	default:
		log.Panicf("should never happen!")
	}
	return nil
}

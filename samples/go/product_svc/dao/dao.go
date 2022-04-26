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

package dao

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cectc/dbpack/pkg/log"
)

const (
	allocateInventorySql = `update /*+ XID('%s') */ product.inventory set available_qty = available_qty - ?, 
		allocated_qty = allocated_qty + ? where product_sysno = ? and available_qty >= ?`
)

type Dao struct {
	*sql.DB
}

type AllocateInventoryReq struct {
	ProductSysNo int64
	Qty          int32
}

func (dao *Dao) AllocateInventory(ctx context.Context, xid string, reqs []*AllocateInventoryReq) error {
	tx, err := dao.Begin()
	if err != nil {
		return err
	}
	updateInventory := fmt.Sprintf(allocateInventorySql, xid)
	for _, req := range reqs {
		_, err := tx.Exec(updateInventory, req.Qty, req.Qty, req.ProductSysNo, req.Qty)
		if err != nil {
			if err != tx.Rollback() {
				log.Error(err)
			}
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

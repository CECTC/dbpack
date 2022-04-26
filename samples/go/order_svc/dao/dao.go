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
	"time"

	"github.com/cectc/dbpack/pkg/log"
	"github.com/google/uuid"
)

const (
	insertSoMaster = `INSERT /*+ XID('%s') */ INTO order.so_master (sysno, so_id, buyer_user_sysno, seller_company_code, 
		receive_division_sysno, receive_address, receive_zip, receive_contact, receive_contact_phone, stock_sysno, 
        payment_type, so_amt, status, order_date, appid, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?)`
	insertSoItem = `INSERT /*+ XID('%s') */ INTO order.so_item(sysno, so_sysno, product_sysno, product_name, cost_price, 
		original_price, deal_price, quantity) VALUES (?,?,?,?,?,?,?,?)`
)

type Dao struct {
	*sql.DB
}

// SoMaster 现实中涉及金额可能使用长整形，这里使用 float64 仅作测试，不具有参考意义
type SoMaster struct {
	SysNo                int64   `json:"sysNo"`
	SoID                 string  `json:"soID"`
	BuyerUserSysNo       int64   `json:"buyerUserSysNo"`
	SellerCompanyCode    string  `json:"sellerCompanyCode"`
	ReceiveDivisionSysNo int64   `json:"receiveDivisionSysNo"`
	ReceiveAddress       string  `json:"receiveAddress"`
	ReceiveZip           string  `json:"receiveZip"`
	ReceiveContact       string  `json:"receiveContact"`
	ReceiveContactPhone  string  `json:"receiveContactPhone"`
	StockSysNo           int64   `json:"stockSysNo"`
	PaymentType          int32   `json:"paymentType"`
	SoAmt                float64 `json:"soAmt"`
	//10，创建成功，待支付；30；支付成功，待发货；50；发货成功，待收货；70，确认收货，已完成；90，下单失败；100已作废
	Status       int32     `json:"status"`
	OrderDate    time.Time `json:"orderDate"`
	PaymentDate  time.Time `json:"paymentDate"`
	DeliveryDate time.Time `json:"deliveryDate"`
	ReceiveDate  time.Time `json:"receiveDate"`
	AppID        string    `json:"appID"`
	Memo         string    `json:"memo"`
	CreateUser   string    `json:"createUser"`
	GmtCreate    time.Time `json:"gmtCreate"`
	ModifyUser   string    `json:"modifyUser"`
	GmtModified  time.Time `json:"gmtModified"`

	SoItems []*SoItem
}

type SoItem struct {
	SysNo         int64   `json:"sysNo"`
	SoSysNo       int64   `json:"soSysNo"`
	ProductSysNo  int64   `json:"productSysNo"`
	ProductName   string  `json:"productName"`
	CostPrice     float64 `json:"costPrice"`
	OriginalPrice float64 `json:"originalPrice"`
	DealPrice     float64 `json:"dealPrice"`
	Quantity      int32   `json:"quantity"`
}

func (dao *Dao) CreateSO(ctx context.Context, xid string, soMasters []*SoMaster) ([]uint64, error) {
	result := make([]uint64, 0, len(soMasters))
	tx, err := dao.Begin()
	if err != nil {
		return nil, err
	}
	createSoMaster := fmt.Sprintf(insertSoMaster, xid)
	createSoItem := fmt.Sprintf(insertSoItem, xid)
	for _, soMaster := range soMasters {
		soid := NextID()
		_, err = tx.Exec(createSoMaster, soid, soid, soMaster.BuyerUserSysNo, soMaster.SellerCompanyCode, soMaster.ReceiveDivisionSysNo,
			soMaster.ReceiveAddress, soMaster.ReceiveZip, soMaster.ReceiveContact, soMaster.ReceiveContactPhone, soMaster.StockSysNo,
			soMaster.PaymentType, soMaster.SoAmt, soMaster.Status, soMaster.AppID, soMaster.Memo)
		if err != nil {
			if err != tx.Rollback() {
				log.Error(err)
			}
			return nil, err
		}
		soItems := soMaster.SoItems
		for _, soItem := range soItems {
			soItemID := NextID()
			_, err = tx.Exec(createSoItem, soItemID, soid, soItem.ProductSysNo, soItem.ProductName, soItem.CostPrice, soItem.OriginalPrice,
				soItem.DealPrice, soItem.Quantity)
			if err != nil {
				if err != tx.Rollback() {
					log.Error(err)
				}
				return nil, err
			}
		}
		result = append(result, soid)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func NextID() uint64 {
	id, _ := uuid.NewUUID()
	return uint64(id.ID())
}

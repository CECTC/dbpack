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

package com.cectc.dbpack.order.mapper;

import com.cectc.dbpack.order.entity.SoItem;
import com.cectc.dbpack.order.entity.SoMaster;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SoMasterMapper {
    @Insert("INSERT /*+ XID('${xid}') */\n" +
            "INTO `seata_order`.`so_master` (\n" +
            "\t`sysno`,\n" +
            "\t`so_id`,\n" +
            "\t`buyer_user_sysno`,\n" +
            "\t`seller_company_code`,\n" +
            "\t`receive_division_sysno`,\n" +
            "\t`receive_address`,\n" +
            "\t`receive_zip`,\n" +
            "\t`receive_contact`,\n" +
            "\t`receive_contact_phone`,\n" +
            "\t`stock_sysno`,\n" +
            "\t`payment_type`,\n" +
            "\t`so_amt`,\n" +
            "\t`status`,\n" +
            "\t`order_date`,\n" +
            "\t`appid`\n" +
            ")\n" +
            "VALUES\n" +
            "\t(\n" +
            "\t\t#{sysNo},\n" +
            "\t\t#{soId},\n" +
            "\t\t#{buyerUserSysNo},\n" +
            "\t\t#{sellerCompanyCode},\n" +
            "\t\t#{receiveDivisionSysNo},\n" +
            "\t\t#{receiveAddress},\n" +
            "\t\t#{receiveZip},\n" +
            "\t\t#{receiveContact},\n" +
            "\t\t#{receiveContactPhone},\n" +
            "\t\t#{stockSysNo},\n" +
            "\t\t#{paymentType},\n" +
            "\t\t#{soAmt},\n" +
            "\t\t#{status},\n" +
            "\t\t#{orderDate},\n" +
            "\t\t#{appid}\n" +
            "\t);")
    void createSoMaster(SoMaster soMaster);
}

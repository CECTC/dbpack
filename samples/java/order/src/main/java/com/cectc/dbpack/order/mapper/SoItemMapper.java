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
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SoItemMapper {
    @Insert("INSERT /*+ XID('${xid}') */\n" +
            "INTO `seata_order`.`so_item` ( `so_sysno`, `product_sysno`, `product_name`, `cost_price`, `original_price`, `deal_price`, `quantity` )\n" +
            "VALUES\n" +
            "(#{soSysNo}, #{productSysNo}, #{productName}, #{costPrice}, #{originalPrice}, #{dealPrice}, #{quantity} );")
    void createSoItem(SoItem soItem);
}

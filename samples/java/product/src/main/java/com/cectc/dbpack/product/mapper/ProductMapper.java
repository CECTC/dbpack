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

package com.cectc.dbpack.product.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface ProductMapper {
    @Update("UPDATE /*+ XID('${xid}') */ `seata_product`.`inventory` SET `available_qty` = `available_qty` - #{qty}, allocated_qty = allocated_qty + #{qty} WHERE product_sysno = #{productSysNo} AND available_qty >= #{qty}")
    boolean allocateInventory(@Param("xid") String xid, @Param("productSysNo") long productSysNo, @Param("qty") int qty);
}

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

package com.cectc.dbpack.product.service;

import com.cectc.dbpack.product.mapper.ProductMapper;
import com.cectc.dbpack.product.req.AllocateInventoryReq;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

@Service
public class ProductService {
    @Resource
    ProductMapper productMapper;

    @Transactional
    public void allocateInventory(List<AllocateInventoryReq> reqs, String xid) {
        for (AllocateInventoryReq req : reqs) {
            productMapper.allocateInventory(xid, req.getProductSysNo(), req.getQty());
        }
    }
}
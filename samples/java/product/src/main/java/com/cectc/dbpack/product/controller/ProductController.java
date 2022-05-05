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

package com.cectc.dbpack.product.controller;

import com.cectc.dbpack.product.BaseController;
import com.cectc.dbpack.product.StandResponse;
import com.cectc.dbpack.product.req.AllocateInventoryReq;
import com.cectc.dbpack.product.service.ProductService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * 商品sku
 * @author scott lewis
 * @date 2019/05/27
 */
@RestController
@RequestMapping(value = "/v1/product")
public class ProductController extends BaseController {

    @Resource
    private ProductService productService;

    @RequestMapping(value = "/allocateInventory",method = RequestMethod.POST)
    public StandResponse<Object> allocateInventory(@RequestBody List<AllocateInventoryReq> reqs, @RequestHeader("xid") String xid) {
        productService.allocateInventory(reqs, xid);
        return success();
    }
}
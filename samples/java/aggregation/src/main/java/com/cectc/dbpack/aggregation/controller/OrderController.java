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

package com.cectc.dbpack.aggregation.controller;

import com.cectc.dbpack.aggregation.BaseController;
import com.cectc.dbpack.aggregation.StandResponse;
import com.cectc.dbpack.aggregation.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/order")
public class OrderController extends BaseController {
    @Autowired
    OrderService orderService;

    @RequestMapping(value = "create", method = RequestMethod.POST)
    public @ResponseBody
    StandResponse<List<Long>> createSo(@RequestHeader("x_dbpack_xid") String xid) {
        List<Long> soIds = orderService.CreateSo(xid);
        return success(soIds);
    }

    @RequestMapping(value = "create2", method = RequestMethod.POST)
    public @ResponseBody
    StandResponse<List<Long>> createSo2(@RequestHeader("x_dbpack_xid") String xid) {
        orderService.CreateSo(xid);
        throw new RuntimeException("exception");
    }
}

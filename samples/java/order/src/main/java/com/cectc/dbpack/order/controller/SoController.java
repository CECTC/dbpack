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

package com.cectc.dbpack.order.controller;

import com.cectc.dbpack.order.BaseController;
import com.cectc.dbpack.order.StandResponse;
import com.cectc.dbpack.order.entity.SoMaster;
import com.cectc.dbpack.order.service.SoService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * 订单表
 * @author scott lewis
 * @date 2019/05/27
 */
@RestController
@RequestMapping(value = "/v1/so")
public class SoController extends BaseController {

    @Resource
    private SoService soService;

    @RequestMapping(value = "/insert",method = RequestMethod.POST)
    public StandResponse<List<Long>> insert(@RequestBody List<SoMaster> soMasters, @RequestHeader("xid") String xid) {
        List<Long> result = soService.createSo(soMasters, xid);
        return success(result);
    }
}
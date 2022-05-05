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

package com.cectc.dbpack.aggregation.service;

import com.cectc.dbpack.aggregation.StandResponse;
import com.cectc.dbpack.aggregation.BaseController;
import com.cectc.dbpack.aggregation.SnowflakeIdGenerator;
import com.cectc.dbpack.aggregation.entity.SoItem;
import com.cectc.dbpack.aggregation.entity.SoMaster;
import com.cectc.dbpack.aggregation.req.AllocateInventoryReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderService {
    final static Logger logger = LoggerFactory.getLogger(BaseController.class);
    private static final String createSoUrl = "http://localhost:3001/order/v1/so/insert";
    private static final String allocateInventoryUrl = "http://localhost:3002/product/v1/product/allocateInventory";

    private final RestTemplate restTemplate = new RestTemplate();

    public List<Long> CreateSo(String xid) {
        return createSo(xid);
    }

    private List<Long> createSo(String xid) {
        List<Long> soSysNos = new ArrayList<>();
        List<SoMaster> soMasters = new ArrayList<>();
        SoMaster soMaster = new SoMaster();
        Long id = SnowflakeIdGenerator.getInstance().nextId();
        soMaster.setSysNo(id);
        soMaster.setAppid(id.toString());
        soMaster.setBuyerUserSysNo(1L);
        soMaster.setSellerCompanyCode("SC001");
        soMaster.setReceiveDivisionSysNo(110105L);
        soMaster.setReceiveAddress("beijing");
        soMaster.setReceiveZip("000001");
        soMaster.setReceiveContact("scott");
        soMaster.setReceiveContactPhone("18728828296");
        soMaster.setStockSysNo(1L);
        soMaster.setPaymentType(1);
        soMaster.setAppid("dk-order");

        SoItem soItem = new SoItem();
        soItem.setSoSysNo(id);
        soItem.setProductSysNo(1L);
        soItem.setProductName("apple iphone 13");
        soItem.setCostPrice(new BigDecimal(6799));
        soItem.setOriginalPrice(new BigDecimal(6799));
        soItem.setDealPrice(new BigDecimal(6999));
        soItem.setQuantity(2);

        List<SoItem> soItems = new ArrayList<>();
        soItems.add(soItem);

        soMaster.setSoAmt(soItem.getDealPrice().multiply(new BigDecimal(soItem.getQuantity())));
        soMaster.setSoItems(soItems);
        soMasters.add(soMaster);
        soSysNos.add(id);


        List<AllocateInventoryReq> reqs = new ArrayList<>();
        AllocateInventoryReq allocateInventoryReq = new AllocateInventoryReq();
        allocateInventoryReq.setProductSysNo(1L);
        allocateInventoryReq.setQty(soItem.getQuantity());
        reqs.add(allocateInventoryReq);

        HttpHeaders headers = new HttpHeaders();
        headers.add("xid", xid);
        HttpEntity<List<SoMaster>> req1 = new HttpEntity<>(soMasters, headers);
        HttpEntity<List<AllocateInventoryReq>> req2 = new HttpEntity<>(reqs, headers);
        ResponseEntity<StandResponse> response1 = restTemplate.postForEntity(createSoUrl, req1, StandResponse.class);
        ResponseEntity<StandResponse> response2 = restTemplate.postForEntity(allocateInventoryUrl, req2, StandResponse.class);
        logger.debug("create so response {}", response1);
        logger.debug("allocate inventory response {}", response2);

        return soSysNos;
    }
}

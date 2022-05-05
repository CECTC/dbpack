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

package com.cectc.dbpack.order.service;

import com.cectc.dbpack.order.SnowflakeIdGenerator;
import com.cectc.dbpack.order.entity.SoItem;
import com.cectc.dbpack.order.entity.SoMaster;
import com.cectc.dbpack.order.mapper.SoItemMapper;
import com.cectc.dbpack.order.mapper.SoMasterMapper;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
public class SoService {
    @Resource
    SoMasterMapper soMasterMapper;
    @Resource
    SoItemMapper soItemMapper;

    @Transactional(timeout = 10000)
    public List<Long> createSo(List<SoMaster> soMasters, String xid) {
        List<Long> results = new ArrayList<>();
        if (soMasters.size()>0) {
            for (SoMaster soMaster : soMasters) {
                if (soMaster.getSoItems()!=null&&soMaster.getSoItems().size()>0) {
                    if (soMaster.getSysNo()==null) {
                        Long id = SnowflakeIdGenerator.getInstance().nextId();
                        soMaster.setSysNo(id);
                    }
                    soMaster.setStatus(10);
                    soMaster.setXid(xid);
                    soMasterMapper.createSoMaster(soMaster);
                    for (SoItem soItem : soMaster.getSoItems()) {
                        soItem.setXid(xid);
                        soItemMapper.createSoItem(soItem);
                    }
                    results.add(soMaster.getSysNo());
                }
            }
        }
        return results;
    }
}

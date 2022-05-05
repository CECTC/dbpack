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

package com.cectc.dbpack.product.entity;

import java.io.Serializable;

/**
 *  商品库存
 * @author scott lewis 2019-05-27
 */
public class Inventory implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 主键
     */
    private Long sysno;

    /**
     * product_sysno
     */
    private Long productSysNo;

    /**
     * 财务库存
     */
    private Integer accountQty;

    /**
     * 可用库存
     */
    private Integer availableQty;

    /**
     * 分配库存
     */
    private Integer allocatedQty;

    /**
     * 调整锁定库存
     */
    private Integer adjustLockedQty;

    public Inventory() {
    }

    public Long getSysno() {
        return sysno;
    }

    public void setSysno(Long sysno) {
        this.sysno = sysno;
    }

    public Long getProductSysNo() {
        return productSysNo;
    }

    public void setProductSysNo(Long productSysNo) {
        this.productSysNo = productSysNo;
    }

    public Integer getAccountQty() {
        return accountQty;
    }

    public void setAccountQty(Integer accountQty) {
        this.accountQty = accountQty;
    }

    public Integer getAvailableQty() {
        return availableQty;
    }

    public void setAvailableQty(Integer availableQty) {
        this.availableQty = availableQty;
    }

    public Integer getAllocatedQty() {
        return allocatedQty;
    }

    public void setAllocatedQty(Integer allocatedQty) {
        this.allocatedQty = allocatedQty;
    }

    public Integer getAdjustLockedQty() {
        return adjustLockedQty;
    }

    public void setAdjustLockedQty(Integer adjustLockedQty) {
        this.adjustLockedQty = adjustLockedQty;
    }
}

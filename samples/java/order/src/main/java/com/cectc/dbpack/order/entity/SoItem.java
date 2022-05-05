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

package com.cectc.dbpack.order.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 *  订单明细表
 * @author scott lewis 2019-05-27
 */
public class SoItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private String xid;

    /**
     * sysno
     */
    private Long sysNo;

    /**
     * so_sysno
     */
    private Long soSysNo;

    /**
     * product_sysno
     */
    private Long productSysNo;

    /**
     * 商品名称
     */
    private String productName;

    /**
     * 成本价
     */
    private BigDecimal costPrice;

    /**
     * 原价
     */
    private BigDecimal originalPrice;

    /**
     * 成交价
     */
    private BigDecimal dealPrice;

    /**
     * 数量
     */
    private Integer quantity;


    public SoItem() {
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public Long getSysNo() {
        return sysNo;
    }

    public void setSysNo(Long sysNo) {
        this.sysNo = sysNo;
    }

    public Long getSoSysNo() {
        return soSysNo;
    }

    public void setSoSysNo(Long soSysNo) {
        this.soSysNo = soSysNo;
    }

    public Long getProductSysNo() {
        return productSysNo;
    }

    public void setProductSysNo(Long productSysNo) {
        this.productSysNo = productSysNo;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public BigDecimal getCostPrice() {
        return costPrice;
    }

    public void setCostPrice(BigDecimal costPrice) {
        this.costPrice = costPrice;
    }

    public BigDecimal getOriginalPrice() {
        return originalPrice;
    }

    public void setOriginalPrice(BigDecimal originalPrice) {
        this.originalPrice = originalPrice;
    }

    public BigDecimal getDealPrice() {
        return dealPrice;
    }

    public void setDealPrice(BigDecimal dealPrice) {
        this.dealPrice = dealPrice;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }
}
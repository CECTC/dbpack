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

package com.cectc.dbpack.aggregation.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 *  订单表
 * @author scott lewis 2019-05-27
 */
public class SoMaster implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * sysno
     */
    private Long sysNo;

    /**
     * so_id
     */
    private String soId;

    /**
     * 下单用户号
     */
    private Long buyerUserSysNo;

    /**
     * 卖家公司编号
     */
    private String sellerCompanyCode;

    /**
     * receive_division_sysno
     */
    private Long receiveDivisionSysNo;

    /**
     * receive_address
     */
    private String receiveAddress;

    /**
     * receive_zip
     */
    private String receiveZip;

    /**
     * receive_contact
     */
    private String receiveContact;

    /**
     * receive_contact_phone
     */
    private String receiveContactPhone;

    /**
     * stock_sysno
     */
    private Long stockSysNo;

    /**
     * 支付方式：1，支付宝，2，微信
     */
    private Integer paymentType;

    /**
     * 订单总额
     */
    private BigDecimal soAmt;

    /**
     * 10，创建成功，待支付；30；支付成功，待发货；50；发货成功，待收货；70，确认收货，已完成；90，下单失败；100已作废
     */
    private Integer status;

    /**
     * 下单时间
     */
    private Date orderDate;

    /**
     * 支付时间
     */
    private Date paymemtDate;

    /**
     * 发货时间
     */
    private Date deliveryDate;

    /**
     * 发货时间
     */
    private Date receiveDate;

    /**
     * 订单来源
     */
    private String appid;

    /**
     * 备注
     */
    private String memo;

    /**
     * create_user
     */
    private String createUser;

    /**
     * gmt_create
     */
    private Date gmtCreate;

    /**
     * modify_user
     */
    private String modifyUser;

    /**
     * gmt_modified
     */
    private Date gmtModified;

    private List<SoItem> soItems;

    public SoMaster() {
    }

    public Long getSysNo() {
        return sysNo;
    }

    public void setSysNo(Long sysNo) {
        this.sysNo = sysNo;
    }

    public String getSoId() {
        return soId;
    }

    public void setSoId(String soId) {
        this.soId = soId;
    }

    public Long getBuyerUserSysNo() {
        return buyerUserSysNo;
    }

    public void setBuyerUserSysNo(Long buyerUserSysNo) {
        this.buyerUserSysNo = buyerUserSysNo;
    }

    public String getSellerCompanyCode() {
        return sellerCompanyCode;
    }

    public void setSellerCompanyCode(String sellerCompanyCode) {
        this.sellerCompanyCode = sellerCompanyCode;
    }

    public Long getReceiveDivisionSysNo() {
        return receiveDivisionSysNo;
    }

    public void setReceiveDivisionSysNo(Long receiveDivisionSysNo) {
        this.receiveDivisionSysNo = receiveDivisionSysNo;
    }

    public String getReceiveAddress() {
        return receiveAddress;
    }

    public void setReceiveAddress(String receiveAddress) {
        this.receiveAddress = receiveAddress;
    }

    public String getReceiveZip() {
        return receiveZip;
    }

    public void setReceiveZip(String receiveZip) {
        this.receiveZip = receiveZip;
    }

    public String getReceiveContact() {
        return receiveContact;
    }

    public void setReceiveContact(String receiveContact) {
        this.receiveContact = receiveContact;
    }

    public String getReceiveContactPhone() {
        return receiveContactPhone;
    }

    public void setReceiveContactPhone(String receiveContactPhone) {
        this.receiveContactPhone = receiveContactPhone;
    }

    public Long getStockSysNo() {
        return stockSysNo;
    }

    public void setStockSysNo(Long stockSysNo) {
        this.stockSysNo = stockSysNo;
    }

    public Integer getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(Integer paymentType) {
        this.paymentType = paymentType;
    }

    public BigDecimal getSoAmt() {
        return soAmt;
    }

    public void setSoAmt(BigDecimal soAmt) {
        this.soAmt = soAmt;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public Date getPaymemtDate() {
        return paymemtDate;
    }

    public void setPaymemtDate(Date paymemtDate) {
        this.paymemtDate = paymemtDate;
    }

    public Date getDeliveryDate() {
        return deliveryDate;
    }

    public void setDeliveryDate(Date deliveryDate) {
        this.deliveryDate = deliveryDate;
    }

    public Date getReceiveDate() {
        return receiveDate;
    }

    public void setReceiveDate(Date receiveDate) {
        this.receiveDate = receiveDate;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getMemo() {
        return memo;
    }

    public void setMemo(String memo) {
        this.memo = memo;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public String getModifyUser() {
        return modifyUser;
    }

    public void setModifyUser(String modifyUser) {
        this.modifyUser = modifyUser;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public List<SoItem> getSoItems() {
        return soItems;
    }

    public void setSoItems(List<SoItem> soItems) {
        this.soItems = soItems;
    }
}
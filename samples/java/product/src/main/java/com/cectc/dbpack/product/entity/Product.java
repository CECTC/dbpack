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
import java.util.Date;

/**
 *  商品sku
 * @author scott lewis 2019-05-27
 */
public class Product implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 主键
     */
    private Long sysNo;

    /**
     * 品名
     */
    private String productName;

    /**
     * product_title
     */
    private String productTitle;

    /**
     * 描述
     */
    private String productDesc;

    /**
     * 描述
     */
    private String productDescLong;

    /**
     * default_image_src
     */
    private String defaultImageSrc;

    /**
     * c3_sysno
     */
    private Long c3SysNo;

    /**
     * barcode
     */
    private String barcode;

    /**
     * length
     */
    private Integer length;

    /**
     * width
     */
    private Integer width;

    /**
     * height
     */
    private Integer height;

    /**
     * weight
     */
    private Float weight;

    /**
     * merchant_sysno
     */
    private Long merchantSysNo;

    /**
     * merchant_productid
     */
    private String merchantProductId;

    /**
     * 1，待上架；2，上架；3，下架；4，售罄下架；5，违规下架
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Date gmtCreate;

    /**
     * 创建人
     */
    private String createUser;

    /**
     * 修改人
     */
    private String modifyUser;

    /**
     * 修改时间
     */
    private Date gmtModified;

    public Product() {
    }

    public Long getSysNo() {
        return sysNo;
    }

    public void setSysNo(Long sysNo) {
        this.sysNo = sysNo;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public String getProductDesc() {
        return productDesc;
    }

    public void setProductDesc(String productDesc) {
        this.productDesc = productDesc;
    }

    public String getProductDescLong() {
        return productDescLong;
    }

    public void setProductDescLong(String productDescLong) {
        this.productDescLong = productDescLong;
    }

    public String getDefaultImageSrc() {
        return defaultImageSrc;
    }

    public void setDefaultImageSrc(String defaultImageSrc) {
        this.defaultImageSrc = defaultImageSrc;
    }

    public Long getC3SysNo() {
        return c3SysNo;
    }

    public void setC3SysNo(Long c3SysNo) {
        this.c3SysNo = c3SysNo;
    }

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String barcode) {
        this.barcode = barcode;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public Float getWeight() {
        return weight;
    }

    public void setWeight(Float weight) {
        this.weight = weight;
    }

    public Long getMerchantSysNo() {
        return merchantSysNo;
    }

    public void setMerchantSysNo(Long merchantSysNo) {
        this.merchantSysNo = merchantSysNo;
    }

    public String getMerchantProductId() {
        return merchantProductId;
    }

    public void setMerchantProductId(String merchantProductId) {
        this.merchantProductId = merchantProductId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
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
}

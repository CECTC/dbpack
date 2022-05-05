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

package com.cectc.dbpack.aggregation.req;

import java.util.List;

public class CreateOrderReq {
    public List<ShoppingProduct> shoppingProducts;

    public List<ShoppingProduct> getShoppingProducts() {
        return shoppingProducts;
    }

    public void setShoppingProducts(List<ShoppingProduct> shoppingProducts) {
        this.shoppingProducts = shoppingProducts;
    }

    public class ShoppingProduct {
        private String productSysNo;

        private String quantity;

        public String getProductSysNo() {
            return productSysNo;
        }

        public void setProductSysNo(String productSysNo) {
            this.productSysNo = productSysNo;
        }

        public String getQuantity() {
            return quantity;
        }

        public void setQuantity(String quantity) {
            this.quantity = quantity;
        }
    }
}

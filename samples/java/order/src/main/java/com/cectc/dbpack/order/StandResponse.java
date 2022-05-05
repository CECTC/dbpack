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

package com.cectc.dbpack.order;

import java.io.Serializable;

public class StandResponse<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    public final static int SUCCESS=200;
    public final static int ACCESS_TOKEN_EXPIRED=401;
    public final static int INTERNAL_SERVER_ERROR=500;
    public final static int BUSINESS_EXCEPTION=600;
    public final static int ARGUMENT_EXCEPTION=700;
    public final static int ARGUMENT_MISSING=701;
    public final static int ARGUMENT_TYPE_MISS_MATCH=702;
    public final static int HEADER_MISSING=703;
    public final static int COOKIE_MISSING=702;

    private Boolean success;
    private Integer code;
    private String msg;
    private T data;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
        if(this.code==SUCCESS){
            this.success=true;
        }
        else{
            this.success=false;
        }
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}

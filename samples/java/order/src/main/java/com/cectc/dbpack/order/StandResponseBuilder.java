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

public class StandResponseBuilder {

    public static <E> StandResponse<E> ok() {
    return result(true,StandResponse.SUCCESS,"SUCCESS",null);
}

    public static <E> StandResponse<E> ok(E data) {
        return result(true,StandResponse.SUCCESS,"SUCCESS", data);
    }

    public static <E> StandResponse<E> result(int code, String msg) {
        return result(code,msg, null);
    }

    public static <E> StandResponse<E> result(Integer code, String msg, E data) {
        StandResponse<E> response = new StandResponse<>();
        response.setCode(code);
        response.setMsg(msg);
        response.setData(data);
        return response;
    }

    public static <E> StandResponse<E> result(Boolean success, Integer code, String msg, E data) {
        StandResponse<E> response = new StandResponse<>();
        response.setSuccess(success);
        response.setCode(code);
        response.setMsg(msg);
        response.setData(data);
        return response;
    }
}
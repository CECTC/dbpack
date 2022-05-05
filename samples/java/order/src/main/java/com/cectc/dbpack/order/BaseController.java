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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

public class BaseController implements InitializingBean {

    final static Logger logger = LoggerFactory.getLogger(BaseController.class);

    public BaseController() {
    }

    public <E> StandResponse<E> success() {
        return StandResponseBuilder.ok();
    }


    public <E> StandResponse<E> success(E data) {
        return StandResponseBuilder.ok(data);
    }


    public <E> StandResponse<E> fail() {
        return StandResponseBuilder.result(StandResponse.INTERNAL_SERVER_ERROR,"系统错误");
    }

    public <E> StandResponse<E> fail(Integer code, String message) {
        return StandResponseBuilder.result(code,message);
    }


    @Override
    public void afterPropertiesSet() throws Exception {

    }
}

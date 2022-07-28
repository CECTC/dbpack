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

package gin

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/log"
)

const XID = "xid"

func GlobalTransaction(timeout int32) gin.HandlerFunc {
	return func(context *gin.Context) {
		var err error
		transactionManager := dt.GetDistributedTransactionManager()
		xid, err := transactionManager.Begin(context, context.FullPath(), timeout)
		if err != nil {
			context.AbortWithError(http.StatusInternalServerError, err)
		}
		context.Set(XID, xid)
		context.Next()
		if context.Writer.Status() == http.StatusOK && len(context.Errors) == 0 {
			_, err = dt.GetDistributedTransactionManager().Commit(context, xid)
			if err != nil {
				log.Error(err)
				context.AbortWithError(http.StatusInternalServerError, err)
			}
		} else {
			_, err = dt.GetDistributedTransactionManager().Rollback(context, xid)
			if err != nil {
				log.Error(err)
			}
		}
	}
}

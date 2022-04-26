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

package main

import (
	"github.com/gin-gonic/gin"

	"github.com/dbpack/samples/aggregation_svc/svc"
)

func main() {
	r := gin.Default()

	r.POST("/v1/order/create", func(c *gin.Context) {
		xid := c.GetHeader("x_dbpack_xid")
		err := svc.GetSvc().CreateSo(c, xid, false)
		if err != nil {
			c.JSON(400, gin.H{
				"success": false,
				"message": "fail",
			})
		} else {
			c.JSON(200, gin.H{
				"success": true,
				"message": "success",
			})
		}
	})

	r.POST("/v1/order/create2", func(c *gin.Context) {
		xid := c.GetHeader("x_dbpack_xid")
		err := svc.GetSvc().CreateSo(c, xid, true)
		if err != nil {
			c.JSON(400, gin.H{
				"success": false,
				"message": "fail",
			})
		} else {
			c.JSON(200, gin.H{
				"success": true,
				"message": "success",
			})
		}
	})

	r.Run(":3000")
}

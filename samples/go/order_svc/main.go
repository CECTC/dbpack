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
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/dbpack/samples/order_svc/dao"
)

var dsn = `dksl:123456@tcp(127.0.0.1:13308)/order?timeout=60s&readTimeout=60s&writeTimeout=60s&parseTime=true&loc=Local&charset=utf8mb4,utf8`

func main() {
	r := gin.Default()

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(4 * time.Hour)

	d := &dao.Dao{
		DB: sqlDB,
	}

	r.POST("/createSo", func(c *gin.Context) {
		xid := c.GetHeader("xid")

		type req struct {
			Req []*dao.SoMaster
		}
		var q req
		if err := c.ShouldBindJSON(&q); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		_, err := d.CreateSO(c, xid, q.Req)

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
	r.Run(":3001")
}

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

package http

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/cectc/dbpack/pkg/resource"
)

const (
	healthCheckLivenessPath  = "/live"
	healthCheckReadinessPath = "/ready"
)

func registerHealthCheckRouter(router *mux.Router) {
	router.Methods(http.MethodGet).Path(healthCheckReadinessPath).HandlerFunc(readinessHandler)
	router.Methods(http.MethodGet).Path(healthCheckLivenessPath).HandlerFunc(livenessHandler)
}

func readinessHandler(w http.ResponseWriter, r *http.Request) {
	dbm := resource.GetDBManager()
	if dbm == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	err := dbm.(*resource.DBManager).GetResourcePoolStatus()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

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
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
)

const (
	deadBranchSessionsPath = "/deadBranchSessions"
)

func registerBranchSessionsRouter(router *mux.Router) {
	router.Methods(http.MethodGet).Path(deadBranchSessionsPath).HandlerFunc(deadBranchSessionHandler)
}

func deadBranchSessionHandler(w http.ResponseWriter, r *http.Request) {
	result := make(map[string][]*api.BranchSession)
	for _, applicationID := range applicationIDs {
		transactionManager := dt.GetTransactionManager(applicationID)
		if transactionManager != nil {
			deadBranchSessions, err := transactionManager.ListDeadBranchSessions(context.Background())
			if err != nil {
				log.Error(err)
			}
			if len(deadBranchSessions) != 0 {
				result[applicationID] = deadBranchSessions
			}
		}
	}
	b, err := json.Marshal(result)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
	w.WriteHeader(http.StatusOK)
}

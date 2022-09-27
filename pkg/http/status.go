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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt"
)

const (
	statusPath = "/status"
)

type ListenerStatus struct {
	ProtocolType  string               `json:"protocol_type"`
	SocketAddress config.SocketAddress `json:"socket_address"`
	Active        bool                 `json:"active"`
}

type ApplicationStatus struct {
	ListenersStatuses []ListenerStatus `json:"listeners"`
	DTEnabled         bool             `json:"distributed_transaction_enabled"`
	IsMaster          bool             `json:"is_master"`
}

func registerStatusRouter(router *mux.Router) {
	router.Methods(http.MethodGet).Path(statusPath).HandlerFunc(statusHandler)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	result := make(map[string]*ApplicationStatus)
	for _, applicationID := range applicationIDs {
		listenersStatuses := make([]ListenerStatus, 0)
		applicationConf := config.GetDBPackConfig(applicationID)
		for _, listener := range applicationConf.Listeners {
			active := false
			lisAddr := fmt.Sprintf("%s:%d", listener.SocketAddress.Address, listener.SocketAddress.Port)
			conn, err := net.DialTimeout("tcp", lisAddr, 5*time.Second)
			if err == nil && conn != nil {
				active = true
				conn.Close()
			}

			protocolType := "http"
			if listener.ProtocolType == config.Mysql {
				protocolType = "mysql"
			}

			status := ListenerStatus{
				ProtocolType:  protocolType,
				SocketAddress: listener.SocketAddress,
				Active:        active,
			}
			listenersStatuses = append(listenersStatuses, status)
		}
		applicationStatus := &ApplicationStatus{
			ListenersStatuses: listenersStatuses,
			DTEnabled:         false,
			IsMaster:          false,
		}
		if applicationConf.DistributedTransaction != nil {
			applicationStatus.DTEnabled = true
			distributedTransactionManager := dt.GetTransactionManager(applicationID)
			if distributedTransactionManager != nil {
				applicationStatus.IsMaster = distributedTransactionManager.IsMaster()
			}
		}
		result[applicationID] = applicationStatus
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

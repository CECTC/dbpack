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
)

const (
	statusPath = "/status"
)

var (
	Listeners                     []*config.Listener
	DistributedTransactionEnabled bool
	IsMaster                      bool
)

type ListenerStatus struct {
	ProtocolType  string               `json:"protocol_type"`
	SocketAddress config.SocketAddress `json:"socket_address"`
	Active        bool                 `json:"active"`
}

type Result struct {
	ListenersStatus []ListenerStatus `json:"listeners"`
	DTEnabled       bool             `json:"distributed_transaction_enabled"`
	IsMaster        bool             `json:"is_master"`
}

func registerStatusRouter(router *mux.Router) {
	router.Methods(http.MethodGet).Path(statusPath).HandlerFunc(statusHandler)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	listenersStatus := make([]ListenerStatus, len(Listeners))

	for i, listener := range Listeners {
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
		listenersStatus[i] = status
	}

	result := Result{ListenersStatus: listenersStatus}
	if DistributedTransactionEnabled {
		result.DTEnabled = true
		result.IsMaster = IsMaster
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

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

package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/log"
)

const XID = keyXID("XID")

type (
	keyXID string
)

type GlobalTransactionInfo struct {
	FullMethod string
	Timeout    int32
}

func GlobalTransactionInterceptor(globalTransactionInfos []*GlobalTransactionInfo) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		var xid string
		transactionManager := dt.GetDistributedTransactionManager()
		for _, gs := range globalTransactionInfos {
			if strings.EqualFold(gs.FullMethod, info.FullMethod) {
				xid, err = transactionManager.Begin(ctx, gs.FullMethod, gs.Timeout)
				if err != nil {
					return nil, err
				}
				ctx = context.WithValue(ctx, XID, xid)
				resp, err = handler(ctx, req)
				if err == nil {
					_, commitErr := dt.GetDistributedTransactionManager().Commit(ctx, xid)
					if err != nil {
						log.Error(err)
						return resp, commitErr
					}
				} else {
					_, rollbackErr := dt.GetDistributedTransactionManager().Rollback(ctx, xid)
					if rollbackErr != nil {
						log.Error(rollbackErr)
					}
				}
				return resp, err
			}
		}
		return handler(ctx, req)
	}
}

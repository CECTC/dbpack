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

package dt

import (
	"context"
	"fmt"
	"net/http"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
)

// handleHttp1GlobalBegin return bool, represent whether continue
func (f *_httpFilter) handleHttp1GlobalBegin(ctx *fasthttp.RequestCtx, transactionInfo *TransactionInfo) (bool, error) {
	// todo support transaction isolation level
	transactionManager := dt.GetDistributedTransactionManager()
	xid, err := transactionManager.Begin(ctx, string(ctx.Request.RequestURI()), transactionInfo.Timeout)
	if err != nil {
		ctx.Response.Reset()
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"failed to begin global transaction, %v"}`, err))

		return false, errors.Errorf("failed to begin global transaction, transaction info: %v, err: %v",
			transactionInfo, err)
	}
	ctx.SetUserValue(XID, xid)
	ctx.Request.Header.Add(XID, xid)
	return true, nil
}

func (f *_httpFilter) handleHttp1GlobalEnd(ctx *fasthttp.RequestCtx) error {
	xidParam := ctx.UserValue(XID)
	xid := xidParam.(string)

	if ctx.Response.StatusCode() == http.StatusOK {
		err := f.globalCommit(ctx, xid)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		err := f.globalRollback(ctx, xid)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// handleHttp1BranchRegister return bool, represent whether continue
func (f *_httpFilter) handleHttp1BranchRegister(ctx *fasthttp.RequestCtx, tccResource *TccResourceInfo) (bool, error) {
	xid := ctx.Request.Header.Peek(XID)
	if string(xid) == "" {
		ctx.Response.Reset()
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.SetBodyString(`{"success":false,"error":"failed to get XID from request header"}`)

		return false, errors.New("failed to get XID from request header")
	}

	bodyBytes := ctx.PostBody()

	requestContext := &dt.RequestContext{
		ActionContext: make(map[string]string),
		Headers:       ctx.Request.Header.Header(),
		Body:          bodyBytes,
	}

	requestContext.ActionContext[dt.VarHost] = f.conf.BackendHost
	requestContext.ActionContext[dt.CommitRequestPath] = tccResource.CommitRequestPath
	requestContext.ActionContext[dt.RollbackRequestPath] = tccResource.RollbackRequestPath
	queryString := ctx.Request.RequestURI()
	if string(queryString) != "" {
		requestContext.ActionContext[dt.VarQueryString] = string(queryString)
	}

	data, err := requestContext.Encode()
	if err != nil {
		ctx.Response.Reset()
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"encode request context failed, %v"}`, err))

		return false, errors.Errorf("encode request context failed, request context: %v, err: %v", requestContext, err)
	}

	transactionManager := dt.GetDistributedTransactionManager()
	branchID, _, err := transactionManager.BranchRegister(ctx, &api.BranchRegisterRequest{
		XID:             string(xid),
		ResourceID:      string(ctx.Request.RequestURI()),
		LockKey:         "",
		BranchType:      api.TCC,
		ApplicationData: data,
	})
	if err != nil {
		ctx.Response.Reset()
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"branch transaction register failed, %v"}`, err))

		return false, errors.Errorf("branch transaction register failed, XID: %s, err: %v", xid, err)
	}
	ctx.SetUserValue(XID, string(xid))
	ctx.SetUserValue(BranchID, branchID)
	return true, nil
}

func (f *_httpFilter) handleHttp1BranchEnd(ctx *fasthttp.RequestCtx) error {
	branchIDParam := ctx.UserValue(BranchID)
	branchID := branchIDParam.(string)

	if ctx.Response.StatusCode() != http.StatusOK {
		transactionManager := dt.GetDistributedTransactionManager()
		err := transactionManager.BranchReport(ctx, branchID, api.PhaseOneFailed)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *_httpFilter) globalCommit(ctx context.Context, xid string) error {
	var (
		err    error
		status api.GlobalSession_GlobalStatus
	)

	transactionManager := dt.GetDistributedTransactionManager()
	status, err = transactionManager.Commit(ctx, xid)
	log.Infof("[%s] commit status: %s", xid, status.String())
	return err
}

func (f *_httpFilter) globalRollback(ctx context.Context, xid string) error {
	var (
		err    error
		status api.GlobalSession_GlobalStatus
	)

	transactionManager := dt.GetDistributedTransactionManager()
	status, err = transactionManager.Rollback(ctx, xid)
	log.Infof("[%s] rollback status: %s", xid, status.String())
	return err
}

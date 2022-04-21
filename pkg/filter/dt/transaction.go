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
	"strconv"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
)

const (
	CommitRequestPath   = "tcc_commit_request_path"
	RollbackRequestPath = "tcc_rollback_request_path"
)

// handleHttp1GlobalBegin return bool, represent whether continue
func (f *_httpFilter) handleHttp1GlobalBegin(ctx *fasthttp.RequestCtx, transactionInfo *TransactionInfo) (bool, error) {
	// todo support transaction isolation level
	transactionManager := dt.GetDistributedTransactionManager()
	xid, err := transactionManager.BeginLocal(ctx, &api.GlobalBeginRequest{
		Addressing:      f.conf.Addressing,
		Timeout:         transactionInfo.Timeout,
		TransactionName: transactionInfo.RequestPath,
	})
	if err != nil {
		ctx.Error(fmt.Sprintf(`{"error":"failed to begin global transaction, %v"}`, err), http.StatusInternalServerError)
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
func (f *_httpFilter) handleHttp1BranchRegister(ctx *fasthttp.RequestCtx, tccResource *TCCResource) (bool, error) {
	xid := ctx.Request.Header.Peek(XID)
	if string(xid) == "" {
		ctx.Error(`{"error":"failed to get XID from request header"}`, http.StatusInternalServerError)
		return false, errors.New("failed to get XID from request header")
	}

	bodyBytes := ctx.PostBody()

	requestContext := &RequestContext{
		ActionContext: make(map[string]string),
		Headers:       ctx.Request.Header.Header(),
		Body:          bodyBytes,
	}

	requestContext.ActionContext[VarHost] = f.conf.BackendHost
	requestContext.ActionContext[CommitRequestPath] = tccResource.CommitRequestPath
	requestContext.ActionContext[RollbackRequestPath] = tccResource.RollbackRequestPath
	queryString := ctx.Request.RequestURI()
	if string(queryString) != "" {
		requestContext.ActionContext[VarQueryString] = string(queryString)
	}

	data, err := requestContext.Encode()
	if err != nil {
		ctx.Error(fmt.Sprintf(`{"error":"encode request context failed, %v"}`, err), http.StatusInternalServerError)
		return false, errors.Errorf("encode request context failed, request context: %v, err: %v", requestContext, err)
	}

	transactionManager := dt.GetDistributedTransactionManager()
	branchID, err := transactionManager.BranchRegisterLocal(ctx, &api.BranchRegisterRequest{
		Addressing:      f.conf.Addressing,
		XID:             string(xid),
		ResourceID:      tccResource.PrepareRequestPath,
		LockKey:         "",
		BranchType:      api.TCC,
		ApplicationData: data,
		Async:           false,
	})
	if err != nil {
		ctx.Error(fmt.Sprintf(`{"error":"branch transaction register failed, %v"}`, err), http.StatusInternalServerError)
		return false, errors.Errorf("branch transaction register failed, XID: %s, err: %v", xid, err)
	}
	ctx.SetUserValue(XID, string(xid))
	ctx.SetUserValue(BranchID, strconv.FormatInt(branchID, 10))
	return true, nil
}

func (f *_httpFilter) handleHttp1BranchEnd(ctx *fasthttp.RequestCtx) error {
	xidParam := ctx.UserValue(XID)
	xid := xidParam.(string)
	branchIDParam := ctx.UserValue(BranchID)
	branchID, err := strconv.ParseInt(branchIDParam.(string), 10, 64)
	if err != nil {
		return errors.WithStack(err)
	}
	if ctx.Response.StatusCode() != http.StatusOK {
		transactionManager := dt.GetDistributedTransactionManager()
		err := transactionManager.BranchReportLocal(ctx, &api.BranchReportRequest{
			XID:          xid,
			BranchID:     branchID,
			BranchType:   api.TCC,
			BranchStatus: api.PhaseOneFailed,
		})
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
	status, err = transactionManager.CommitLocal(ctx, &api.GlobalCommitRequest{
		XID: xid,
	})

	log.Infof("[%s] commit status: %s", xid, status.String())
	return err
}

func (f *_httpFilter) globalRollback(ctx context.Context, xid string) error {
	var (
		err    error
		status api.GlobalSession_GlobalStatus
	)

	transactionManager := dt.GetDistributedTransactionManager()
	status, err = transactionManager.RollbackLocal(ctx, &api.GlobalRollbackRequest{
		XID: xid,
	})

	log.Infof("[%s] rollback status: %s", xid, status.String())
	return err
}

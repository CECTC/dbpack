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
	"go.opentelemetry.io/otel/attribute"

	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/tracing"
)

// handleHttp1GlobalBegin return bool, represent whether continue
func (f *_httpFilter) handleHttp1GlobalBegin(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx, transactionInfo *TransactionInfo) (bool, error) {
	// todo support transaction isolation level
	globalTransactionName := string(fastHttpCtx.Request.RequestURI())
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GlobalTransactionBegin)
	span.SetAttributes(attribute.KeyValue{Key: "global-transaction-name", Value: attribute.StringValue(globalTransactionName)})
	defer span.End()
	transactionManager := dt.GetTransactionManager(f.conf.ApplicationID)
	xid, err := transactionManager.Begin(spanCtx, globalTransactionName, transactionInfo.Timeout)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		fastHttpCtx.Response.Reset()
		fastHttpCtx.SetStatusCode(http.StatusInternalServerError)
		fastHttpCtx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"failed to begin global transaction, %v"}`, err))

		return false, errors.Errorf("failed to begin global transaction, transaction info: %v, err: %v",
			transactionInfo, err)
	}
	fastHttpCtx.SetUserValue(XID, xid)
	fastHttpCtx.Request.Header.Add(XID, xid)
	return true, nil
}

func (f *_httpFilter) handleHttp1GlobalEnd(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GlobalTransactionEnd)
	defer span.End()
	xidParam := fastHttpCtx.UserValue(XID)
	xid := xidParam.(string)

	if fastHttpCtx.Response.StatusCode() == http.StatusOK {
		err := f.globalCommit(spanCtx, xid)
		if err != nil {
			tracing.RecordErrorSpan(span, err)
			return errors.WithStack(err)
		}
	} else {
		err := f.globalRollback(spanCtx, xid)
		if err != nil {
			tracing.RecordErrorSpan(span, err)
			return errors.WithStack(err)
		}
	}
	return nil
}

// handleHttp1BranchRegister return bool, represent whether continue
func (f *_httpFilter) handleHttp1BranchRegister(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx, tccResource *TccResourceInfo) (bool, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.BranchTransactionRegister)
	defer span.End()
	xid := fastHttpCtx.Request.Header.Peek(XID)
	if string(xid) == "" {
		fastHttpCtx.Response.Reset()
		fastHttpCtx.SetStatusCode(http.StatusInternalServerError)
		fastHttpCtx.SetBodyString(`{"success":false,"error":"failed to get XID from request header"}`)

		return false, errors.New("failed to get XID from request header")
	}

	bodyBytes := fastHttpCtx.PostBody()

	requestContext := &dt.RequestContext{
		ActionContext: make(map[string]string),
		Headers:       make(map[string]string),
		Body:          bodyBytes,
	}

	fastHttpCtx.Request.Header.VisitAll(func(key, value []byte) {
		requestContext.Headers[string(key)] = string(value)
	})

	requestContext.ActionContext[dt.VarHost] = fastHttpCtx.UserValue(dt.VarHost).(string)
	requestContext.ActionContext[dt.CommitRequestPath] = tccResource.CommitRequestPath
	requestContext.ActionContext[dt.RollbackRequestPath] = tccResource.RollbackRequestPath
	queryString := fastHttpCtx.QueryArgs().QueryString()

	if string(queryString) != "" {
		requestContext.ActionContext[dt.VarQueryString] = string(queryString)
	}

	data, err := requestContext.Encode()
	if err != nil {
		fastHttpCtx.Response.Reset()
		fastHttpCtx.SetStatusCode(http.StatusInternalServerError)
		fastHttpCtx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"encode request context failed, %v"}`, err))
		tracing.RecordErrorSpan(span, err)
		return false, errors.Errorf("encode request context failed, request context: %v, err: %v", requestContext, err)
	}

	transactionManager := dt.GetTransactionManager(f.conf.ApplicationID)
	branchID, _, err := transactionManager.BranchRegister(spanCtx, &api.BranchRegisterRequest{
		XID:             string(xid),
		ResourceID:      string(fastHttpCtx.Request.RequestURI()),
		LockKey:         "",
		BranchType:      api.TCC,
		ApplicationData: data,
	})
	if err != nil {
		fastHttpCtx.Response.Reset()
		fastHttpCtx.SetStatusCode(http.StatusInternalServerError)
		fastHttpCtx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"branch transaction register failed, %v"}`, err))
		tracing.RecordErrorSpan(span, err)
		return false, errors.Errorf("branch transaction register failed, XID: %s, err: %v", xid, err)
	}
	fastHttpCtx.SetUserValue(XID, string(xid))
	fastHttpCtx.SetUserValue(BranchID, branchID)
	return true, nil
}

func (f *_httpFilter) handleHttp1BranchEnd(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.BranchTransactionEnd)
	defer span.End()
	branchIDParam := fastHttpCtx.UserValue(BranchID)
	branchID := branchIDParam.(string)

	if fastHttpCtx.Response.StatusCode() != http.StatusOK {
		transactionManager := dt.GetTransactionManager(f.conf.ApplicationID)
		err := transactionManager.BranchReport(spanCtx, branchID, api.PhaseOneFailed)
		if err != nil {
			tracing.RecordErrorSpan(span, err)
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

	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GlobalTransactionCommit)
	defer span.End()

	transactionManager := dt.GetTransactionManager(f.conf.ApplicationID)
	status, err = transactionManager.Commit(spanCtx, xid)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
	}
	log.Infof("[%s] commit status: %s", xid, status.String())
	return err
}

func (f *_httpFilter) globalRollback(ctx context.Context, xid string) error {
	var (
		err    error
		status api.GlobalSession_GlobalStatus
	)
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GlobalTransactionRollback)
	defer span.End()

	transactionManager := dt.GetTransactionManager(f.conf.ApplicationID)
	status, err = transactionManager.Rollback(spanCtx, xid)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
	}
	log.Infof("[%s] rollback status: %s", xid, status.String())
	return err
}

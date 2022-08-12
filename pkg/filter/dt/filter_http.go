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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"

	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/tracing"
)

const httpFilter = "HttpDistributedTransaction"

type MatchType byte

const (
	Exact MatchType = iota
	Prefix
	Regex
)

func (t *MatchType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *MatchType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unsupported match type: %q", text)
	}
	return nil
}

func (t *MatchType) unmarshalText(text []byte) bool {
	switch string(text) {
	case "exact":
		*t = Exact
	case "prefix":
		*t = Prefix
	case "regex":
		*t = Regex
	default:
		return false
	}
	return true
}

type _httpFactory struct {
}

func (factory *_httpFactory) NewFilter(appid string, config map[string]interface{}) (proto.Filter, error) {
	var (
		err          error
		content      []byte
		filterConfig *HttpFilterConfig
	)

	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal http distributed transaction filter config failed.")
	}
	if err = json.Unmarshal(content, &filterConfig); err != nil {
		log.Errorf("unmarshal http distributed transaction filter failed, %v", err)
		return nil, err
	}

	filterConfig.ApplicationID = appid
	f := &_httpFilter{
		conf:               filterConfig,
		transactionInfoMap: make(map[string]*TransactionInfo),
		transactionInfos:   make([]*TransactionInfo, 0),
		tccResourceInfoMap: make(map[string]*TccResourceInfo),
	}

	for _, ti := range filterConfig.TransactionInfos {
		switch ti.MatchType {
		case Exact:
			f.transactionInfoMap[strings.ToLower(ti.RequestPath)] = ti
		case Prefix, Regex:
			f.transactionInfos = append(f.transactionInfos, ti)
		default:
			log.Warnf("unsupported match type, %s, request path: %s", ti.MatchType, ti.RequestPath)
		}
		log.Debugf("proxy %s, will create global transaction, put xid into request header", ti.RequestPath)
	}

	for _, r := range filterConfig.TCCResourceInfos {
		f.tccResourceInfoMap[strings.ToLower(r.PrepareRequestPath)] = r
		log.Debugf("proxy %s, will register branch transaction", r.PrepareRequestPath)
	}
	return f, nil
}

// TransactionInfo transaction info config
type TransactionInfo struct {
	RequestPath string    `yaml:"request_path" json:"request_path"`
	Timeout     int32     `yaml:"timeout" json:"timeout"`
	MatchType   MatchType `yaml:"match_type" json:"match_type"`
}

// TccResourceInfo tcc resource config
type TccResourceInfo struct {
	PrepareRequestPath  string `yaml:"prepare_request_path" json:"prepare_request_path"`
	CommitRequestPath   string `yaml:"commit_request_path" json:"commit_request_path"`
	RollbackRequestPath string `yaml:"rollback_request_path" json:"rollback_request_path"`
}

// HttpFilterConfig http filter config
type HttpFilterConfig struct {
	ApplicationID string `yaml:"-" json:"-"`

	TransactionInfos []*TransactionInfo `yaml:"transaction_infos" json:"transaction_infos"`
	TCCResourceInfos []*TccResourceInfo `yaml:"tcc_resource_infos" json:"tcc_resource_infos"`
}

type _httpFilter struct {
	conf *HttpFilterConfig

	transactionInfoMap map[string]*TransactionInfo
	transactionInfos   []*TransactionInfo
	tccResourceInfoMap map[string]*TccResourceInfo
}

var _ proto.HttpPostFilter = (*_httpFilter)(nil)
var _ proto.HttpPostFilter = (*_httpFilter)(nil)

func (f *_httpFilter) GetKind() string {
	return httpFilter
}

func (f _httpFilter) PreHandle(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.DTHttpFilterPreHandle)
	defer span.End()

	path := fastHttpCtx.Request.RequestURI()
	method := fastHttpCtx.Method()
	if !strings.EqualFold(string(method), fasthttp.MethodPost) {
		return nil
	}

	transactionInfo, found := f.matchTransactionInfo(string(path))
	if found {
		result, err := f.handleHttp1GlobalBegin(spanCtx, fastHttpCtx, transactionInfo)
		if !result {
			if err := f.handleHttp1GlobalEnd(spanCtx, fastHttpCtx); err != nil {
				log.Error(err)
			}
		}
		return err
	}

	tccResource, exists := f.tccResourceInfoMap[strings.ToLower(string(path))]
	if exists {
		result, err := f.handleHttp1BranchRegister(spanCtx, fastHttpCtx, tccResource)
		if !result {
			if err := f.handleHttp1BranchEnd(spanCtx, fastHttpCtx); err != nil {
				log.Error(err)
			}
		}
		return err
	}
	return nil
}

func (f _httpFilter) PostHandle(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.DTHttpFilterPostHandle)
	defer span.End()

	path := fastHttpCtx.Request.RequestURI()
	method := fastHttpCtx.Method()
	if !strings.EqualFold(string(method), fasthttp.MethodPost) {
		return nil
	}

	_, found := f.transactionInfoMap[strings.ToLower(string(path))]
	if found {
		if err := f.handleHttp1GlobalEnd(spanCtx, fastHttpCtx); err != nil {
			return err
		}
	}

	_, exists := f.tccResourceInfoMap[strings.ToLower(string(path))]
	if exists {
		if err := f.handleHttp1BranchEnd(spanCtx, fastHttpCtx); err != nil {
			return err
		}
	}
	return nil
}

func (f _httpFilter) matchTransactionInfo(requestPath string) (*TransactionInfo, bool) {
	path := strings.ToLower(requestPath)
	transactionInfo, found := f.transactionInfoMap[path]
	if found {
		return transactionInfo, found
	}
	for _, ti := range f.transactionInfos {
		switch ti.MatchType {
		case Prefix:
			if strings.HasPrefix(path, strings.ToLower(ti.RequestPath)) {
				return ti, true
			}
		case Regex:
			matched, err := regexp.Match(strings.ToLower(ti.RequestPath), []byte(path))
			if err != nil {
				log.Warnf("regular expression match error, regex string: %s, error: %s", ti.RequestPath, err)
				continue
			}
			if matched {
				return ti, true
			}
		default:
			continue
		}
	}
	return nil, false
}

func init() {
	filter.RegistryFilterFactory(httpFilter, &_httpFactory{})
}

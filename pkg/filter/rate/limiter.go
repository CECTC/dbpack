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

package rate

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"go.uber.org/ratelimit"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	rateLimiterFilter = "RateLimiterFilter"
)

type _factory struct{}

func (factory *_factory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	var (
		err           error
		content       []byte
		conf          *LimiterFilterConfig
		insertLimiter ratelimit.Limiter
		updateLimiter ratelimit.Limiter
		deleteLimiter ratelimit.Limiter
		selectLimiter ratelimit.Limiter
	)
	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal rate limit filter config failed.")
	}
	if err = json.Unmarshal(content, &conf); err != nil {
		log.Errorf("unmarshal rate limit filter failed, %v", err)
		return nil, err
	}
	if conf.InsertLimit != 0 {
		insertLimiter = ratelimit.New(conf.InsertLimit)
	}
	if conf.UpdateLimit != 0 {
		updateLimiter = ratelimit.New(conf.UpdateLimit)
	}
	if conf.DeleteLimit != 0 {
		deleteLimiter = ratelimit.New(conf.DeleteLimit)
	}
	if conf.SelectLimit != 0 {
		selectLimiter = ratelimit.New(conf.SelectLimit)
	}
	return &_filter{
		insertLimiter: insertLimiter,
		updateLimiter: updateLimiter,
		deleteLimiter: deleteLimiter,
		selectLimiter: selectLimiter,
	}, nil
}

type LimiterFilterConfig struct {
	InsertLimit int `yaml:"insert_limit" json:"insert_limit"`
	UpdateLimit int `yaml:"update_limit" json:"update_limit"`
	DeleteLimit int `yaml:"delete_limit" json:"delete_limit"`
	SelectLimit int `yaml:"select_limit" json:"select_limit"`
}

type _filter struct {
	insertLimiter ratelimit.Limiter
	updateLimiter ratelimit.Limiter
	deleteLimiter ratelimit.Limiter
	selectLimiter ratelimit.Limiter
}

func (f *_filter) GetKind() string {
	return rateLimiterFilter
}

func (f *_filter) PreHandle(ctx context.Context) error {
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		stmt := proto.QueryStmt(ctx)
		switch stmt.(type) {
		case *ast.InsertStmt:
			if f.insertLimiter != nil {
				f.insertLimiter.Take()
			}
		case *ast.UpdateStmt:
			if f.updateLimiter != nil {
				f.updateLimiter.Take()
			}
		case *ast.DeleteStmt:
			if f.deleteLimiter != nil {
				f.deleteLimiter.Take()
			}
		case *ast.SelectStmt:
			if f.selectLimiter != nil {
				f.selectLimiter.Take()
			}
		default:
			return nil
		}

	case constant.ComStmtExecute:
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmt.StmtNode.(type) {
		case *ast.InsertStmt:
			if f.insertLimiter != nil {
				f.insertLimiter.Take()
			}
		case *ast.UpdateStmt:
			if f.updateLimiter != nil {
				f.updateLimiter.Take()
			}
		case *ast.DeleteStmt:
			if f.deleteLimiter != nil {
				f.deleteLimiter.Take()
			}
		case *ast.SelectStmt:
			if f.selectLimiter != nil {
				f.selectLimiter.Take()
			}
		default:
			return nil
		}
	}
	return nil
}

func init() {
	filter.RegistryFilterFactory(rateLimiterFilter, &_factory{})
}

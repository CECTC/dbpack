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
	"encoding/json"

	"vimagination.zapto.org/byteio"

	"github.com/cectc/dbpack/pkg/log"
)

type RequestContext struct {
	ActionContext map[string]string
	Headers       []byte
	Body          []byte
}

func (ctx *RequestContext) Encode() ([]byte, error) {
	var (
		err               error
		actionContextData []byte
		b                 bytes.Buffer
	)

	w := byteio.BigEndianWriter{Writer: &b}

	if ctx.ActionContext == nil || len(ctx.ActionContext) == 0 {
		w.WriteUint32(0)
	} else {
		actionContextData, err = json.Marshal(ctx.ActionContext)
		if err != nil {
			return nil, err
		}

		w.WriteUint32(uint32(len(actionContextData)))
		w.Write(actionContextData)
	}

	if ctx.Headers == nil || len(ctx.Headers) == 0 {
		w.WriteUint32(0)
	} else {
		w.WriteUint32(uint32(len(ctx.Headers)))
		w.Write(ctx.Headers)
	}

	w.WriteUint32(uint32(len(ctx.Body)))
	b.Write(ctx.Body)

	return b.Bytes(), nil
}

func (ctx *RequestContext) Decode(b []byte) error {
	var (
		actionContextData []byte
		headersData       []byte
		bodyData          []byte
	)
	r := byteio.BigEndianReader{Reader: bytes.NewReader(b)}

	contextLength, _, err := r.ReadUint32()
	if err != nil {
		return err
	}
	if contextLength > 0 {
		actionContextData = make([]byte, contextLength, contextLength)
		r.Read(actionContextData)
	}

	headerLength, _, err := r.ReadUint32()
	if err != nil {
		return err
	}
	if headerLength > 0 {
		headersData = make([]byte, headerLength, headerLength)
		r.Read(headersData)
	}

	bodyLength, _, err := r.ReadUint32()
	if err != nil {
		return err
	}
	if bodyLength > 0 {
		bodyData = make([]byte, bodyLength, bodyLength)
		r.Read(bodyData)
	}

	if actionContextData != nil {
		err = json.Unmarshal(actionContextData, &(ctx.ActionContext))
		if err != nil {
			log.Errorf("unmarshal action context failed, %v", err)
		}
	}

	ctx.Headers = headersData
	ctx.Body = bodyData
	return nil
}

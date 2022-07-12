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
	Headers       map[string]string
	Body          []byte
}

func (ctx *RequestContext) Encode() ([]byte, error) {
	var (
		buffer            bytes.Buffer
		actionContextData []byte
		headersData       []byte
		err               error
	)

	w := byteio.BigEndianWriter{Writer: &buffer}

	if ctx.ActionContext == nil || len(ctx.ActionContext) == 0 {
		if _, err = w.WriteUint32(0); err != nil {
			return nil, err
		}
	} else {
		actionContextData, err = json.Marshal(ctx.ActionContext)
		if err != nil {
			return nil, err
		}

		if _, err = w.WriteUint32(uint32(len(actionContextData))); err != nil {
			return nil, err
		}
		if _, err = w.Write(actionContextData); err != nil {
			return nil, err
		}
	}

	if ctx.Headers == nil || len(ctx.Headers) == 0 {
		if _, err = w.WriteUint32(0); err != nil {
			return nil, err
		}
	} else {
		headersData, err = json.Marshal(ctx.Headers)
		if err != nil {
			return nil, err
		}
		if _, err = w.WriteUint32(uint32(len(headersData))); err != nil {
			return nil, err
		}
		if _, err = w.Write(headersData); err != nil {
			return nil, err
		}
	}

	if _, err = w.WriteUint32(uint32(len(ctx.Body))); err != nil {
		return nil, err
	}
	buffer.Write(ctx.Body)

	return buffer.Bytes(), nil
}

func (ctx *RequestContext) Decode(b []byte) error {
	var (
		actionContextData []byte
		headersData       []byte
		bodyData          []byte
		err               error
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
	if headersData != nil {
		err = json.Unmarshal(headersData, &(ctx.Headers))
		if err != nil {
			log.Errorf("unmarshal action context failed, %v", err)
		}
	}

	ctx.Body = bodyData
	return nil
}

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

package misc

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAesEncryptGCM(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373776f726420746f206120736563726574")
	plaintext := []byte("exampleplaintext")
	encrypted, err := AesEncryptGCM(plaintext, key, []byte("greatdbpack!"))
	assert.Nil(t, err)
	t.Logf("%x", encrypted)
}

func TestAesDecryptGCM(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373776f726420746f206120736563726574")
	encrypted, _ := hex.DecodeString("dbb2b731c2c7e9f637195ba70f85e6a26e5cbe3f536ad3457d72cf8cc4c66df1")
	decrypted, err := AesDecryptGCM(encrypted, key, []byte("greatdbpack!"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("exampleplaintext"), decrypted)
}

func TestAesEncryptCBC(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	plaintext := []byte("exampleplaintext")
	encrypted, err := AesEncryptCBC(plaintext, key, []byte("impressivedbpack"))
	assert.Nil(t, err)
	t.Logf("%x", encrypted)
}

func TestAesDecryptCBC(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	encrypted, _ := hex.DecodeString("25d5fc99f3bf7313d6f96ef83c744240d0adc7f5ad1712359ac4335b1da33a4a")
	decrypted, err := AesDecryptCBC(encrypted, key, []byte("impressivedbpack"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("exampleplaintext"), decrypted)
}

func TestAesEncryptECB(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	plaintext := []byte("exampleplaintext")
	encrypted, err := AesEncryptECB(plaintext, key)
	assert.Nil(t, err)
	t.Logf("%x", encrypted)
}

func TestAesDecryptECB(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	encrypted, _ := hex.DecodeString("f42512e1e4039213bd449ba47faa1b749c2f799fae8d6a326ffff2489e0a7e8a")
	decrypted, err := AesDecryptECB(encrypted, key)
	assert.Nil(t, err)
	assert.Equal(t, []byte("exampleplaintext"), decrypted)
}

func TestAesEncryptCFB(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	plaintext := []byte("exampleplaintext")
	encrypted, err := AesEncryptCFB(plaintext, key)
	assert.Nil(t, err)
	t.Logf("%x", encrypted)
}

func TestAesDecryptCFB(t *testing.T) {
	key, _ := hex.DecodeString("6368616e676520746869732070617373")
	encrypted, _ := hex.DecodeString("a5ee2fa16e5f7328fd2077a19ca0d7038bb239e498962b2b51aa40f11f9bc2d4")
	decrypted, err := AesDecryptCFB(encrypted, key)
	assert.Nil(t, err)
	assert.Equal(t, []byte("exampleplaintext"), decrypted)
}

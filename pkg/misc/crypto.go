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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/pkg/errors"
)

func AesEncryptCBC(origData []byte, key []byte, iv []byte) (encrypted []byte, err error) {
	var (
		block     cipher.Block
		blockMode cipher.BlockMode
	)
	block, err = aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	origData = pkcs7Padding(origData)
	blockMode = cipher.NewCBCEncrypter(block, iv)
	encrypted = make([]byte, len(origData))
	blockMode.CryptBlocks(encrypted, origData)
	return encrypted, err
}

func AesDecryptCBC(encrypted []byte, key []byte, iv []byte) (decrypted []byte, err error) {
	var (
		block     cipher.Block
		blockMode cipher.BlockMode
	)
	block, err = aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockMode = cipher.NewCBCDecrypter(block, iv)
	decrypted = make([]byte, len(encrypted))
	blockMode.CryptBlocks(decrypted, encrypted)
	decrypted = pkcs7UnPadding(decrypted)
	return decrypted, err
}

func pkcs7Padding(ciphertext []byte) []byte {
	padding := aes.BlockSize - len(ciphertext)%aes.BlockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padText...)
}

func pkcs7UnPadding(plantText []byte) []byte {
	length := len(plantText)
	unPadding := int(plantText[length-1])
	return plantText[:(length - unPadding)]
}

func AesEncryptECB(origData []byte, key []byte) (encrypted []byte, err error) {
	var block cipher.Block
	block, err = aes.NewCipher(generateKey(key))
	if err != nil {
		return nil, err
	}
	length := (len(origData) + aes.BlockSize) / aes.BlockSize
	plain := make([]byte, length*aes.BlockSize)
	copy(plain, origData)
	pad := byte(len(plain) - len(origData))
	for i := len(origData); i < len(plain); i++ {
		plain[i] = pad
	}
	encrypted = make([]byte, len(plain))
	for bs, be := 0, block.BlockSize(); bs <= len(origData); bs, be = bs+block.BlockSize(), be+block.BlockSize() {
		block.Encrypt(encrypted[bs:be], plain[bs:be])
	}
	return encrypted, err
}

func AesDecryptECB(encrypted []byte, key []byte) (decrypted []byte, err error) {
	var block cipher.Block
	block, err = aes.NewCipher(generateKey(key))
	if err != nil {
		return nil, err
	}
	decrypted = make([]byte, len(encrypted))
	for bs, be := 0, block.BlockSize(); bs < len(encrypted); bs, be = bs+block.BlockSize(), be+block.BlockSize() {
		block.Decrypt(decrypted[bs:be], encrypted[bs:be])
	}

	trim := 0
	if len(decrypted) > 0 {
		trim = len(decrypted) - int(decrypted[len(decrypted)-1])
	}
	return decrypted[:trim], err
}

func generateKey(key []byte) (genKey []byte) {
	genKey = make([]byte, 16)
	copy(genKey, key)
	for i := 16; i < len(key); {
		for j := 0; j < 16 && i < len(key); j, i = j+1, i+1 {
			genKey[j] ^= key[i]
		}
	}
	return genKey
}

func AesEncryptCFB(origData []byte, key []byte) (encrypted []byte, err error) {
	var block cipher.Block
	block, err = aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	encrypted = make([]byte, aes.BlockSize+len(origData))
	iv := encrypted[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(encrypted[aes.BlockSize:], origData)
	return encrypted, err
}

func AesDecryptCFB(encrypted []byte, key []byte) (decrypted []byte, err error) {
	var block cipher.Block
	block, err = aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(encrypted) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}
	iv := encrypted[:aes.BlockSize]
	encrypted = encrypted[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(encrypted, encrypted)
	return encrypted, err
}

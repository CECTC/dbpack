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

// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package driver

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"

	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/packet"
)

// server pub keys registry
var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

const (
	cachingSha2PasswordRequestPublicKey          = 2
	cachingSha2PasswordFastAuthSuccess           = 3
	cachingSha2PasswordPerformFullAuthentication = 4
)

// RegisterServerPubKey registers a server RSA public key which can be used to
// send Content in a secure manner to the server without receiving the public key
// in a potentially insecure way from the server first.
// Registered keys can afterwards be used adding serverPubKey=<name> to the DSN.
//
// Note: The provided rsa.PublicKey instance is exclusively owned by the driver
// after registering it and may not be modified.
//
//  Content, err := ioutil.ReadFile("mykey.pem")
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  block, _ := pem.Decode(Content)
//  if block == nil || block.Type != "PUBLIC KEY" {
//  	log.Fatal("failed to decode PEM block containing public key")
//  }
//
//  pub, err := x509.ParsePKIXPublicKey(block.Bytes)
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
//  	mysql.RegisterServerPubKey("mykey", rsaPubKey)
//  } else {
//  	log.Fatal("not a RSA public key")
//  }
//
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

// Hash password using pre 4.1 (old password) method
// https://github.com/atcurtis/mariadb/blob/master/mysys/my_rnd.c
type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

// Pseudo random number generator
func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

// Tested to be equivalent to MariaDB's floating point variant
// http://play.golang.org/p/QHvhd4qved
// http://play.golang.org/p/RG0q4ElWDx
func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

// Generate binary hash from byte string using insecure pre 4.1 method
func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		// skip spaces and tabs in password
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	// Remove sign bit (1<<31)-1)
	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

// Hash password using insecure pre 4.1 method
func scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

// Hash password using 4.1+ method (SHA1)
func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// Hash password using MySQL 8+ method (SHA256)
func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

func (conn *BackendConnection) sendEncryptedPassword(seed []byte, pub *rsa.PublicKey) error {
	enc, err := encryptPassword(conn.conf.Passwd, seed, pub)
	if err != nil {
		return err
	}
	return conn.writeAuthSwitchPacket(enc)
}

func (conn *BackendConnection) auth(authData []byte, plugin string) ([]byte, error) {
	switch plugin {
	case "caching_sha2_password":
		authResp := scrambleSHA256Password(authData, conn.conf.Passwd)
		return authResp, nil

	case "mysql_old_password":
		//if !mc.cfg.AllowOldPasswords {
		//	return nil, ErrOldPassword
		//}
		//if len(mc.cfg.Passwd) == 0 {
		//	return nil, nil
		//}
		// Note: there are edge cases where this should work but doesn't;
		// this is currently "wontfix":
		// https://github.com/go-sql-driver/mysql/issues/184
		authResp := append(scrambleOldPassword(authData[:8], conn.conf.Passwd), 0)
		return authResp, nil

	case "mysql_clear_password":
		//if !mc.cfg.AllowCleartextPasswords {
		//	return nil, ErrCleartextPassword
		//}
		// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
		// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
		return append([]byte(conn.conf.Passwd), 0), nil

	case "mysql_native_password":
		//if !mc.cfg.AllowNativePasswords {
		//	return nil, ErrNativePassword
		//}
		// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
		// Native password authentication only need and will need 20-byte challenge.
		authResp := scramblePassword(authData[:20], conn.conf.Passwd)
		return authResp, nil

	case "sha256_password":
		//if len(mc.cfg.Passwd) == 0 {
		//	return []byte{0}, nil
		//}
		//if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
		//	// write cleartext auth packet
		//	return append([]byte(mc.cfg.Passwd), 0), nil
		//}
		//
		//pubKey := mc.cfg.pubKey
		//if pubKey == nil {
		//	// request public key from server
		return []byte{1}, nil
		//}
		//
		//// encrypted password
		//enc, err := encryptPassword(passwd, authData, pubKey)
		//return enc, err

	default:
		log.Error("unknown auth plugin:", plugin)
		return nil, err2.ErrUnknownPlugin
	}
}

func (conn *BackendConnection) handleAuthResult(oldAuthData []byte, plugin string) error {
	// Read Result Packet
	authData, newPlugin, err := conn.readAuthResult()
	if err != nil {
		return err
	}

	// handle auth plugin switch, if requested
	if newPlugin != "" {
		// If CLIENT_PLUGIN_AUTH capability is not supported, no new cipher is
		// sent and we have to keep using the cipher sent in the init packet.
		if authData == nil {
			authData = oldAuthData
		} else {
			// copy Content from read buffer to owned slice
			copy(oldAuthData, authData)
		}

		plugin = newPlugin

		authResp, err := conn.auth(authData, plugin)
		if err != nil {
			return err
		}
		if err = conn.writeAuthSwitchPacket(authResp); err != nil {
			return err
		}

		// Read Result Packet
		authData, newPlugin, err = conn.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if newPlugin != "" {
			return err2.ErrMalformedPkt
		}
	}

	switch plugin {

	// https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	case "caching_sha2_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		case 1:
			switch authData[0] {
			case cachingSha2PasswordFastAuthSuccess:
				if err = conn.readResultOK(); err == nil {
					return nil // auth successful
				}

			case cachingSha2PasswordPerformFullAuthentication:
				if conn.conf.tls != nil || conn.conf.Net == "unix" {
					// write cleartext auth packet
					err = conn.writeAuthSwitchPacket(append([]byte(conn.conf.Passwd), 0))
					if err != nil {
						return err
					}
				} else {
					pubKey := conn.conf.pubKey
					if pubKey == nil {
						//request public key from server
						var data []byte
						if err = conn.WritePacket([]byte{cachingSha2PasswordRequestPublicKey}); err != nil {
							return err
						}

						// parse public key
						if data, err = conn.ReadPacket(); err != nil {
							return err
						}

						block, rest := pem.Decode(data[1:])
						if block == nil {
							return fmt.Errorf("No Pem Content found, Content: %s", rest)
						}
						pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
						if err != nil {
							return err
						}
						pubKey = pkix.(*rsa.PublicKey)
					}

					// send encrypted password
					err = conn.sendEncryptedPassword(oldAuthData, pubKey)
					if err != nil {
						return err
					}
				}
				return conn.readResultOK()

			default:
				return err2.ErrMalformedPkt
			}
		default:
			return err2.ErrMalformedPkt
		}

	case "sha256_password":
		switch len(authData) {
		case 0:
			return nil // auth successful
		default:
			block, _ := pem.Decode(authData)
			pub, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return err
			}

			// send encrypted password
			err = conn.sendEncryptedPassword(oldAuthData, pub.(*rsa.PublicKey))
			if err != nil {
				return err
			}
			return conn.readResultOK()
		}

	default:
		return nil // auth successful
	}

	return err
}

func (conn *BackendConnection) readAuthResult() ([]byte, string, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, "", err
	}

	// packet indicator
	switch data[0] {

	case constant.OKPacket:
		_, _, _, _, err := packet.ParseOKPacket(data)
		return nil, "", err

	case constant.ComQuit:
		return data[1:], "", err

	case constant.EOFPacket:
		if len(data) == 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, "mysql_old_password", nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", err2.ErrMalformedPkt
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", packet.ParseErrorPacket(data)
	}
}

// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (conn *BackendConnection) writeAuthSwitchPacket(authData []byte) error {
	return conn.WritePacket(authData)
}

// Returns error if Packet is not an 'Result OK'-Packet
func (conn *BackendConnection) readResultOK() error {
	data, err := conn.ReadPacket()
	if err != nil {
		return err
	}

	if data[0] == constant.OKPacket {
		_, _, _, _, err := packet.ParseOKPacket(data)
		return err
	}
	return packet.ParseErrorPacket(data)
}

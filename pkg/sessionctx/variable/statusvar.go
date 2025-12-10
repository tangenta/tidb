// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"bytes"
	"crypto/tls"
	"sync"

	"gitee.com/Trisia/gotlcp/tlcp"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	tlsutil "github.com/pingcap/tidb/pkg/util/tls"
)

var statisticsList []Statistics
var statisticsListLock sync.RWMutex

// DefaultStatusVarScopeFlag is the default scope of status variables.
var DefaultStatusVarScopeFlag = vardef.ScopeGlobal | vardef.ScopeSession

// StatusVal is the value of the corresponding status variable.
type StatusVal struct {
	Scope vardef.ScopeFlag
	Value any
}

// Statistics is the interface of statistics.
type Statistics interface {
	// GetScope gets the status variables scope.
	GetScope(status string) vardef.ScopeFlag
	// Stats returns the statistics status variables.
	Stats(*SessionVars) (map[string]any, error)
}

// RegisterStatistics registers statistics.
func RegisterStatistics(s Statistics) {
	statisticsListLock.Lock()
	statisticsList = append(statisticsList, s)
	statisticsListLock.Unlock()
}

// UnregisterStatistics unregisters statistics.
func UnregisterStatistics(s Statistics) {
	statisticsListLock.Lock()
	defer statisticsListLock.Unlock()
	idx := -1
	for i := range statisticsList {
		if statisticsList[i] == s {
			idx = i
		}
	}
	if idx < 0 {
		return
	}
	last := len(statisticsList) - 1
	statisticsList[idx] = statisticsList[last]
	statisticsList[last] = nil
	statisticsList = statisticsList[:last]
}

// GetStatusVars gets registered statistics status variables.
// TODO: Refactor this function to avoid repeated memory allocation / dealloc
func GetStatusVars(vars *SessionVars) (map[string]*StatusVal, error) {
	statusVars := make(map[string]*StatusVal)
	statisticsListLock.RLock()
	defer statisticsListLock.RUnlock()

	for _, statistics := range statisticsList {
		vals, err := statistics.Stats(vars)
		if err != nil {
			return nil, err
		}

		for name, val := range vals {
			scope := statistics.GetScope(name)
			statusVars[name] = &StatusVal{Value: val, Scope: scope}
		}
	}

	return statusVars, nil
}

// Taken from https://golang.org/pkg/crypto/tls/#pkg-constants .
var tlsCiphers = []uint16{
	tls.TLS_RSA_WITH_RC4_128_SHA,
	tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
	tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_AES_128_GCM_SHA256,
	tls.TLS_AES_256_GCM_SHA384,
	tls.TLS_CHACHA20_POLY1305_SHA256,
}

// TLCP ciphers from https://github.com/Trisia/gotlcp/blob/c9f7412bc2f041e4169553a0dca0fd415d758027/tlcp/cipher_suites.go#L41
var tlcpCiphers = []uint16{
	tlcp.TLCP_ECC_SM4_GCM_SM3,
	tlcp.TLCP_ECC_SM4_CBC_SM3,
	tlcp.TLCP_ECDHE_SM4_GCM_SM3,
	tlcp.TLCP_ECDHE_SM4_CBC_SM3,
}

var tlsSupportedCiphers, tlcpSupportedCiphers string

// tlcp version string from https://github.com/Trisia/gotlcp/blob/c9f7412bc2f041e4169553a0dca0fd415d758027/tlcp/common.go#L29
var tlcpVersionString = map[uint16]string{
	tlcp.VersionTLCP: "TLCPv1.1",
}

var defaultStatus = map[string]*StatusVal{
	"Ssl_cipher":      {vardef.ScopeGlobal | vardef.ScopeSession, ""},
	"Ssl_cipher_list": {vardef.ScopeGlobal | vardef.ScopeSession, ""},
	"Ssl_verify_mode": {vardef.ScopeGlobal | vardef.ScopeSession, 0},
	"Ssl_version":     {vardef.ScopeGlobal | vardef.ScopeSession, ""},
	// tlcp status variables
	"tlcp_cipher":      {vardef.ScopeGlobal | vardef.ScopeSession, ""},
	"tlcp_cipher_list": {vardef.ScopeGlobal | vardef.ScopeSession, ""},
	"tlcp_version":     {vardef.ScopeGlobal | vardef.ScopeSession, ""},
}

type defaultStatusStat struct {
}

func (s defaultStatusStat) GetScope(status string) vardef.ScopeFlag {
	return defaultStatus[status].Scope
}

func (s defaultStatusStat) Stats(vars *SessionVars) (map[string]any, error) {
	statusVars := make(map[string]any, len(defaultStatus))

	for name, v := range defaultStatus {
		statusVars[name] = v.Value
	}

	// `vars` may be nil in unit tests.
	if vars != nil && vars.TLSConnectionState != nil {
		statusVars["Ssl_cipher"] = tlsutil.CipherSuiteName(vars.TLSConnectionState.CipherSuite)
		statusVars["Ssl_cipher_list"] = tlsSupportedCiphers
		// tls.VerifyClientCertIfGiven == SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE
		statusVars["Ssl_verify_mode"] = 0x01 | 0x04
		statusVars["Ssl_version"] = tlsutil.VersionName(vars.TLSConnectionState.Version)
	}

	if vars != nil && vars.TLCPConnectionState != nil {
		statusVars["tlcp_cipher"] = util.TLCPCipher2String(vars.TLCPConnectionState.CipherSuite)
		statusVars["tlcp_cipher_list"] = tlcpSupportedCiphers
		//statusVars["tlcp_verify_mode"] = 0x01 | 0x04
		if tlcpVersion, tlcpVersionKnown := tlcpVersionString[vars.TLCPConnectionState.Version]; tlcpVersionKnown {
			statusVars["tlcp_version"] = tlcpVersion
		} else {
			statusVars["tlcp_version"] = "unknown_tlcp_version"
		}
	}

	return statusVars, nil
}

func init() {
	var ciphersBuffer bytes.Buffer
	for _, v := range tlsCiphers {
		ciphersBuffer.WriteString(tlsutil.CipherSuiteName(v))
		ciphersBuffer.WriteString(":")
	}
	tlsSupportedCiphers = ciphersBuffer.String()

	// set supported tlcp ciphers
	ciphersBuffer.Reset()
	for _, v := range tlcpCiphers {
		ciphersBuffer.WriteString(util.TLCPCipher2String(v))
		ciphersBuffer.WriteString(":")
	}
	tlcpSupportedCiphers = ciphersBuffer.String()
	var stat defaultStatusStat
	RegisterStatistics(stat)
}

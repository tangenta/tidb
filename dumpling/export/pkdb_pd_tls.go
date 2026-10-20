// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"github.com/spf13/pflag"
	pd "github.com/tikv/pd/client"
)

const (
	flagClusterSSLCA   = "cluster-ssl-ca"
	flagClusterSSLCert = "cluster-ssl-cert"
	flagClusterSSLKey  = "cluster-ssl-key"
)

func defineClusterSSLFlags(flags *pflag.FlagSet) {
	flags.String(flagClusterSSLCA, "", "CA certificate path for TLS connections to PD endpoints used by GC control; if empty, reuse --ca")
	flags.String(flagClusterSSLCert, "", "Client certificate path for TLS connections to PD endpoints used by GC control; if empty, reuse --cert")
	flags.String(flagClusterSSLKey, "", "Client private key path for TLS connections to PD endpoints used by GC control; if empty, reuse --key")
}

func (conf *Config) parseClusterSSLFlags(flags *pflag.FlagSet) error {
	var err error
	conf.ClusterSSLCA, err = flags.GetString(flagClusterSSLCA)
	if err != nil {
		return err
	}
	conf.ClusterSSLCert, err = flags.GetString(flagClusterSSLCert)
	if err != nil {
		return err
	}
	conf.ClusterSSLKey, err = flags.GetString(flagClusterSSLKey)
	return err
}

// pdSecurityOptionForGC separates PD trust from SQL TLS. Fall back per field
// when an override is empty, preserving existing --ca/--cert/--key behavior.
func pdSecurityOptionForGC(conf *Config) pd.SecurityOption {
	option := pd.SecurityOption{
		CAPath:   conf.Security.CAPath,
		CertPath: conf.Security.CertPath,
		KeyPath:  conf.Security.KeyPath,
	}
	if conf.ClusterSSLCA != "" {
		option.CAPath = conf.ClusterSSLCA
	}
	if conf.ClusterSSLCert != "" {
		option.CertPath = conf.ClusterSSLCert
	}
	if conf.ClusterSSLKey != "" {
		option.KeyPath = conf.ClusterSSLKey
	}
	return option
}

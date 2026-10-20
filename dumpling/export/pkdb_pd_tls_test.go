// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestClusterSSLFlagsDoNotConfigureSQLTLS(t *testing.T) {
	// Parse PD-only paths through the public CLI configuration flow, then build
	// SQL TLS. PD certificates must not be read or installed as SQL credentials.
	conf := DefaultConfig()
	flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	conf.DefineFlags(flags)
	originalFlags := pflag.CommandLine
	pflag.CommandLine = flags
	t.Cleanup(func() { pflag.CommandLine = originalFlags })
	require.NoError(t, flags.Parse([]string{
		"--cluster-ssl-ca=pd-ca.pem",
		"--cluster-ssl-cert=pd-client.pem",
		"--cluster-ssl-key=pd-client-key.pem",
	}))
	require.NoError(t, conf.ParseFromFlags(flags))
	require.NoError(t, buildTLSConfig(conf))
	require.Nil(t, conf.Security.TLS)
	require.Empty(t, conf.Security.CAPath)
	require.Empty(t, conf.Security.CertPath)
	require.Empty(t, conf.Security.KeyPath)
	require.Equal(t, pd.SecurityOption{
		CAPath: "pd-ca.pem", CertPath: "pd-client.pem", KeyPath: "pd-client-key.pem",
	}, pdSecurityOptionForGC(conf))
}

func TestPDSecurityOverridesDoNotMutateSQLTLS(t *testing.T) {
	// Select PD credentials from independent overrides and legacy SQL paths.
	// Each case checks the effective PD option and that SQL paths stay intact.
	for _, tc := range []struct {
		name      string
		clusterCA string
		cert      string
		key       string
		expected  pd.SecurityOption
	}{
		{
			// Empty overrides retain the existing shared certificate behavior.
			name: "legacy SQL credentials",
			expected: pd.SecurityOption{
				CAPath: "sql-ca.pem", CertPath: "sql-client.pem", KeyPath: "sql-client-key.pem",
			},
		},
		{
			// Different SQL and PD trust domains use independent credentials.
			name: "separate PD credentials", clusterCA: "pd-ca.pem", cert: "pd-client.pem", key: "pd-client-key.pem",
			expected: pd.SecurityOption{
				CAPath: "pd-ca.pem", CertPath: "pd-client.pem", KeyPath: "pd-client-key.pem",
			},
		},
		{
			// A CA-only override keeps the legacy client certificate and key.
			name: "PD CA override", clusterCA: "pd-ca.pem",
			expected: pd.SecurityOption{
				CAPath: "pd-ca.pem", CertPath: "sql-client.pem", KeyPath: "sql-client-key.pem",
			},
		},
		{
			// A client-only override keeps the legacy CA trust file.
			name: "PD client override", cert: "pd-client.pem", key: "pd-client-key.pem",
			expected: pd.SecurityOption{
				CAPath: "sql-ca.pem", CertPath: "pd-client.pem", KeyPath: "pd-client-key.pem",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := DefaultConfig()
			conf.Security.CAPath = "sql-ca.pem"
			conf.Security.CertPath = "sql-client.pem"
			conf.Security.KeyPath = "sql-client-key.pem"
			conf.ClusterSSLCA, conf.ClusterSSLCert, conf.ClusterSSLKey = tc.clusterCA, tc.cert, tc.key
			require.Equal(t, tc.expected, pdSecurityOptionForGC(conf))
			require.Equal(t, "sql-ca.pem", conf.Security.CAPath)
			require.Equal(t, "sql-client.pem", conf.Security.CertPath)
			require.Equal(t, "sql-client-key.pem", conf.Security.KeyPath)
		})
	}
}

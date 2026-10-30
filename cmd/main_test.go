package main

import (
	"net/url"
	"strings"
	"testing"
)

func TestParsePort(t *testing.T) {
	const wantDefault = 3500
	if defaultGRPCPort != wantDefault {
		t.Fatalf("defaultGRPCPort = %d, want %d", defaultGRPCPort, wantDefault)
	}

	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "empty means default", raw: "", want: wantDefault},
		{name: "valid port", raw: "9090", want: 9090},
		{name: "whitespace trimmed", raw: "  8080  ", want: 8080},
		{name: "lower bound", raw: "1", want: 1},
		{name: "upper bound", raw: "65535", want: 65535},
		{name: "leading plus", raw: "+3500", want: 3500},
		{name: "trailing space", raw: "3500 ", want: 3500},
		{name: "surrounding tab and newline", raw: "\t3500\n", want: 3500},
		{name: "whitespace only means default", raw: "   ", want: wantDefault},
		{name: "non-numeric", raw: "abc", wantErr: true},
		{name: "zero", raw: "0", wantErr: true},
		{name: "above upper bound", raw: "65536", wantErr: true},
		{name: "negative", raw: "-1", wantErr: true},
		{name: "not an integer", raw: "3.5", wantErr: true},
		{name: "hexadecimal", raw: "0x10", wantErr: true},
		{name: "integer overflow", raw: "99999999999999999999", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePort(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePort(%q) = %d, want error", tt.raw, got)
				}
				if !strings.Contains(err.Error(), tt.raw) {
					t.Errorf("parsePort(%q) error = %q, want it to mention the raw value", tt.raw, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePort(%q) unexpected error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("parsePort(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestDatabaseDSN(t *testing.T) {
	const fullDSN = "postgres://user:pass@host:5432/db?sslmode=disable"

	tests := []struct {
		name     string
		raw      string
		username string
		password string
		want     string
	}{
		{name: "empty raw", raw: "", want: ""},
		{name: "full DSN returned verbatim", raw: fullDSN, want: fullDSN},
		{
			name:     "full DSN verbatim even with creds set",
			raw:      fullDSN,
			username: "other",
			password: "otherpass",
			want:     fullDSN,
		},
		{
			name: "full postgresql scheme verbatim",
			raw:  "postgresql://user:pass@host:5432/db",
			want: "postgresql://user:pass@host:5432/db",
		},
		{
			name:     "legacy form with credentials",
			raw:      "host:5432/db?sslmode=disable",
			username: "user",
			password: "pass",
			want:     "postgres://user:pass@host:5432/db?sslmode=disable",
		},
		{
			name: "legacy form without credentials",
			raw:  "host:5432/db?sslmode=disable",
			want: "postgres://host:5432/db?sslmode=disable",
		},
		{
			name:     "legacy form with username only",
			raw:      "host:5432/db",
			username: "user",
			want:     "postgres://host:5432/db",
		},
		{
			name:     "legacy form with password only",
			raw:      "host:5432/db",
			password: "pass",
			want:     "postgres://host:5432/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := databaseDSN(tt.raw, tt.username, tt.password)
			if got != tt.want {
				t.Errorf("databaseDSN(%q, %q, %q) = %q, want %q", tt.raw, tt.username, tt.password, got, tt.want)
			}
		})
	}

	// A password containing URL metacharacters must be escaped in the built
	// DSN (and round-trip through url.Parse), never embedded raw.
	t.Run("special characters in password are escaped", func(t *testing.T) {
		const password = "p@ss:w:rd"
		got := databaseDSN("host:5432/db", "user", password)
		if strings.Contains(got, password) {
			t.Fatalf("databaseDSN() = %q, password appears unescaped", got)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Fatalf("url.Parse(%q) unexpected error: %v", got, err)
		}
		pw, ok := u.User.Password()
		if !ok || pw != password {
			t.Errorf("round-tripped password = %q (ok=%v), want %q", pw, ok, password)
		}
	})
}

func TestSanitizedDSNForLog(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    string
		secrets []string
	}{
		{
			name:    "full DSN drops password and query values",
			dsn:     "postgres://alice:s3cret@pg:5432/stocker?sslmode=disable&password=oops",
			want:    "postgres://alice@pg:5432/stocker",
			secrets: []string{"s3cret", "oops", "sslmode"},
		},
		{
			name:    "DSN built from credentials",
			dsn:     databaseDSN("pg:5432/stocker", "bob", "hunter2"),
			want:    "postgres://bob@pg:5432/stocker",
			secrets: []string{"hunter2"},
		},
		{
			name: "no userinfo",
			dsn:  "postgres://pg:5432/stocker",
			want: "postgres://pg:5432/stocker",
		},
		{
			name: "unparseable DSN yields generic placeholder without the raw value",
			dsn:  "postgres://%zz:secret@host/db",
			want: "<unparseable DSN>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizedDSN(tt.dsn)
			if got != tt.want {
				t.Errorf("sanitizedDSN(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
			for _, secret := range tt.secrets {
				if strings.Contains(got, secret) {
					t.Errorf("sanitizedDSN(%q) = %q, must not contain %q", tt.dsn, got, secret)
				}
			}
			if strings.Contains(got, "secret") || strings.Contains(got, "%zz") {
				t.Errorf("sanitizedDSN(%q) = %q, must not leak the raw value", tt.dsn, got)
			}
		})
	}
}

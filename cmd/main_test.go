package main

import (
	"strings"
	"testing"
)

func TestParsePort(t *testing.T) {
	// defaultGRPCPort is introduced with parsePort; until then the documented
	// default is asserted as a literal.
	const wantDefault = 3500

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
		{name: "trailing space", raw: "3500 ", want: 3500},
		{name: "surrounding tab and newline", raw: "\t3500\n", want: 3500},
		{name: "non-numeric", raw: "abc", wantErr: true},
		{name: "zero", raw: "0", wantErr: true},
		{name: "above upper bound", raw: "65536", wantErr: true},
		{name: "negative", raw: "-1", wantErr: true},
		{name: "not an integer", raw: "3.5", wantErr: true},
		{name: "hexadecimal", raw: "0x10", wantErr: true},
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

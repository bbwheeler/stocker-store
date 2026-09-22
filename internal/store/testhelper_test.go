package store

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

// testDSN returns the DSN for an external test database, or "" if none is
// configured. Tests using this skip when unset. The DSN may come from the
// DATABASE_URL env var or, for CI/local convenience, from a file at the path
// in the DATABASE_URL_FILE env var.
func testDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	if f := os.Getenv("DATABASE_URL_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read DATABASE_URL_FILE %s: %v", f, err)
		}
		return string(bytes.TrimSpace(b))
	}
	return ""
}

// testStore connects to the external test database, skipping the test when no
// DSN is configured.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := testDSN(t)
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping database-backed test")
	}
	s, err := NewStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s
}

// seedStock inserts (or updates) a stock with an explicit timestamp.
func seedStock(t *testing.T, s *Store, symbol, exchange string, ts time.Time) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO stocks (symbol, exchange, timestamp) VALUES ($1,$2,$3)
		 ON CONFLICT (symbol, exchange) DO UPDATE SET timestamp = $3`,
		symbol, exchange, ts); err != nil {
		t.Fatalf("seed stock: %v", err)
	}
}

func seedScore(t *testing.T, s *Store, symbol, exchange, category string, val float64, ts time.Time) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO scores (symbol, exchange, category, value, timestamp)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (symbol, exchange, category) DO UPDATE SET value=$4, timestamp=$5`,
		symbol, exchange, category, val, ts); err != nil {
		t.Fatalf("seed score: %v", err)
	}
}

// readScore reads a single (symbol, exchange, category) score row back,
// including its timestamp (which surfaces as ScoreEntry.UpdatedAt in the
// domain model).
func readScore(t *testing.T, s *Store, symbol, exchange, category string) ScoreEntry {
	t.Helper()
	var e ScoreEntry
	if err := s.pool.QueryRow(context.Background(),
		`SELECT category, value, timestamp FROM scores WHERE symbol=$1 AND exchange=$2 AND category=$3`,
		symbol, exchange, category).Scan(&e.Category, &e.Value, &e.UpdatedAt); err != nil {
		t.Fatalf("read score %s/%s/%s: %v", symbol, exchange, category, err)
	}
	return e
}

func stockExists(t *testing.T, s *Store, symbol, exchange string) bool {
	t.Helper()
	var n int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM stocks WHERE symbol=$1 AND exchange=$2`, symbol, exchange).Scan(&n); err != nil {
		t.Fatalf("count stocks: %v", err)
	}
	return n > 0
}

func scoreCount(t *testing.T, s *Store, symbol, exchange string) int64 {
	t.Helper()
	var n int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM scores WHERE symbol=$1 AND exchange=$2`, symbol, exchange).Scan(&n); err != nil {
		t.Fatalf("count scores: %v", err)
	}
	return n
}

// clearStock removes a test stock and its scores.
func clearStock(t *testing.T, s *Store, symbol, exchange string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`DELETE FROM scores WHERE symbol=$1 AND exchange=$2`, symbol, exchange); err != nil {
		t.Fatalf("cleanup scores: %v", err)
	}
	if _, err := s.pool.Exec(context.Background(),
		`DELETE FROM stocks WHERE symbol=$1 AND exchange=$2`, symbol, exchange); err != nil {
		t.Fatalf("cleanup stock: %v", err)
	}
}

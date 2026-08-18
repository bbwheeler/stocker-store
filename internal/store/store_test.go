package store

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// uniqueTag generates a unique symbol/exchange pair for the test that will be
// cleaned up via the deferred clearStock helper.
func uniqueTag() (symbol, exchange string) {
	nanos := time.Now().UnixNano()
	return "TST" + strconv.FormatInt(nanos, 10), "X" + strconv.FormatInt(nanos, 10)
}

// TestUpdateStockRefreshesTimestamp verifies that adding/updating a stock
// (even with identical, or no, scores) advances its timestamp to roughly
// "now," i.e. any add/update refreshes the timestamp even if everything else
// is the same.
func TestUpdateStockRefreshesTimestamp(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	sym, exh := uniqueTag()
	defer clearStock(t, s, sym, exh)

	ctx := context.Background()
	before := time.Now().Add(-2 * time.Hour)

	// Seed a stock with an explicitly old timestamp.
	seedStock(t, s, sym, exh, before)

	// 1) UpdateStock with the same scores should refresh the stock timestamp.
	// Use identical scores every time to prove that even when nothing else
	// changes the timestamp advances.
	scores := map[string]float64{"momentum": 0.25}

	// First update: timestamp should jump past the seeded (old) value.
	got, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("first UpdateStock: %v", err)
	}
	if got.Created.IsZero() || got.Created.Before(before) {
		t.Fatalf("first UpdateStock should refresh timestamp: got %v, seeded %v", got.Created, before)
	}
	after1 := got.Created

	// Small sleep so the second update is observable.
	time.Sleep(50 * time.Millisecond)

	// 2) Update with the exact same scores (no payload change) must still
	// refresh the timestamp.
	got2, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("second UpdateStock: %v", err)
	}
	if !got2.Created.After(after1) {
		t.Fatalf("second UpdateStock should refresh timestamp even when payload is identical: got %v (previously %v)", got2.Created, after1)
	}

	// 3) Update with no scores at all must still refresh the timestamp.
	time.Sleep(50 * time.Millisecond)
	got3, err := s.UpdateStock(ctx, sym, exh, nil)
	if err != nil {
		t.Fatalf("third UpdateStock (no scores): %v", err)
	}
	if !got3.Created.After(got2.Created) {
		t.Fatalf("UpdateStock with no scores should still refresh timestamp: got %v (previously %v)", got3.Created, got2.Created)
	}
}

// TestRemoveOldStocksRemovesOldStocks verifies the retention behavior:
// stocks older than the retention window are removed, together with their
// dependent scores; stocks inside the window are kept.
func TestRemoveOldStocksRemovesOldStocks(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	olderSym, olderExh := uniqueTag()
	freshSym, freshExh := uniqueTag()
	defer clearStock(t, s, olderSym, olderExh)
	defer clearStock(t, s, freshSym, freshExh)

	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Add(-31 * 24 * time.Hour)
	fresh := now.Add(-1 * time.Hour)

	seedStock(t, s, olderSym, olderExh, old)
	seedScore(t, s, olderSym, olderExh, "momentum", 0.3)
	if got := scoreCount(t, s, olderSym, olderExh); got != 1 {
		t.Fatalf("expected 1 score seeded, got %d", got)
	}

	seedStock(t, s, freshSym, freshExh, fresh)

	retention := 30 * 24 * time.Hour // default
	removed, err := s.RemoveOldStocks(ctx, retention)
	if err != nil {
		t.Fatalf("RemoveOldStocks: %v", err)
	}
	if removed == 0 {
		t.Fatalf("expected at least one old stock to be removed, got 0")
	}

	if stockExists(t, s, olderSym, olderExh) {
		t.Fatalf("stock older than retention window should be removed")
	}
	if got := scoreCount(t, s, olderSym, olderExh); got != 0 {
		t.Fatalf("scores of a removed stock should also be removed; got %d", got)
	}
	if !stockExists(t, s, freshSym, freshExh) {
		t.Fatalf("stock inside the retention window should not be removed")
	}
}

// TestRemoveOldStocksDisabledWhenZero verifies that a zero/negative retention
// window is a no-op (nothing removed).
func TestRemoveOldStocksDisabledWhenZero(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	sym, exh := uniqueTag()
	defer clearStock(t, s, sym, exh)

	ctx := context.Background()
	old := time.Now().Add(-40 * 24 * time.Hour)
	seedStock(t, s, sym, exh, old)

	removed, err := s.RemoveOldStocks(ctx, 0)
	if err != nil {
		t.Fatalf("RemoveOldStocks(0): %v", err)
	}
	if removed != 0 {
		t.Fatalf("zero retention should be a no-op, removed=%d", removed)
	}
	if !stockExists(t, s, sym, exh) {
		t.Fatalf("zero retention must not remove stocks")
	}
}

// TestRemoveOldStocksConfigurableRetention verifies that the retention window
// is configurable: a stock older than a small custom window is removed, while
// one older than the default 30-day window but younger than "1 hour" would be
// kept — i.e. the caller's window, not the default, is applied.
func TestRemoveOldStocksConfigurableRetention(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	sym, exh := uniqueTag()
	defer clearStock(t, s, sym, exh)

	ctx := context.Background()
	// 1 hour old: outside a 30-minute retention window, inside a 30-day one.
	ts := time.Now().Add(-1 * time.Hour)
	seedStock(t, s, sym, exh, ts)

	removed, err := s.RemoveOldStocks(ctx, 30*time.Minute)
	if err != nil {
		t.Fatalf("RemoveOldStocks(30m): %v", err)
	}
	if removed == 0 {
		t.Fatalf("stock 1h old should be removed with a 30m retention window")
	}

	// Re-seed for the default-window sanity check.
	seedStock(t, s, sym, exh, ts)
	removedDef, err := s.RemoveOldStocks(ctx, DefaultRetention)
	if err != nil {
		t.Fatalf("RemoveOldStocks(default): %v", err)
	}
	if removedDef != 0 {
		t.Fatalf("stock 1h old should NOT be removed with default 30-day retention")
	}
	if !stockExists(t, s, sym, exh) {
		t.Fatalf("stock should still exist after applying the default window")
	}
}

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
	got, _, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("first UpdateStock: %v", err)
	}
	if got.Updated.IsZero() || got.Updated.Before(before) {
		t.Fatalf("first UpdateStock should refresh timestamp: got %v, seeded %v", got.Updated, before)
	}
	after1 := got.Updated

	// The written score must also carry a non-zero last-updated timestamp:
	// read the score back from the DB and check ScoreEntry.UpdatedAt.
	gotEntry, err := s.getStockScores(ctx, sym, exh)
	if err != nil {
		t.Fatalf("getStockScores: %v", err)
	}
	if len(gotEntry) != 1 {
		t.Fatalf("expected 1 score after update, got %d", len(gotEntry))
	}
	if gotEntry[0].UpdatedAt.IsZero() {
		t.Fatalf("score UpdatedAt = zero, want non-zero")
	}
	if gotEntry[0].UpdatedAt.Before(before) {
		t.Fatalf("score UpdatedAt %v is before the seeded 'before' time %v", gotEntry[0].UpdatedAt, before)
	}

	// Small sleep so the second update is observable.
	time.Sleep(50 * time.Millisecond)

	// 2) Update with the exact same scores (no payload change) must still
	// refresh the timestamp.
	got2, _, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("second UpdateStock: %v", err)
	}
	if !got2.Updated.After(after1) {
		t.Fatalf("second UpdateStock should refresh timestamp even when payload is identical: got %v (previously %v)", got2.Updated, after1)
	}

	// 3) Update with no scores at all must still refresh the timestamp.
	time.Sleep(50 * time.Millisecond)
	got3, _, err := s.UpdateStock(ctx, sym, exh, nil)
	if err != nil {
		t.Fatalf("third UpdateStock (no scores): %v", err)
	}
	if !got3.Updated.After(got2.Updated) {
		t.Fatalf("UpdateStock with no scores should still refresh timestamp: got %v (previously %v)", got3.Updated, got2.Updated)
	}
}

// TestUpdateStockInsertedFlag verifies that the second return value of
// UpdateStock is true on the first write to a (symbol, exchange) pair
// (INSERT) and false on subsequent writes to the same pair (UPDATE).
func TestUpdateStockInsertedFlag(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	sym, exh := uniqueTag()
	defer clearStock(t, s, sym, exh)

	ctx := context.Background()
	scores := map[string]float64{"momentum": 0.25}

	first, inserted, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("first UpdateStock: %v", err)
	}
	if inserted != true {
		t.Fatalf("inserted = %v on first write, want true", inserted)
	}
	if first == nil {
		t.Fatalf("first stock is nil")
	}

	second, inserted2, err := s.UpdateStock(ctx, sym, exh, scores)
	if err != nil {
		t.Fatalf("second UpdateStock: %v", err)
	}
	if inserted2 != false {
		t.Fatalf("inserted = %v on second write, want false", inserted2)
	}
	if second == nil {
		t.Fatalf("second stock is nil")
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
	seedScore(t, s, olderSym, olderExh, "momentum", 0.3, time.Now())
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

// TestTopStocks verifies deterministic top-N retrieval by score value in a
// category, optional exchange scoping, deterministic tie-breaking (value DESC,
// then symbol ASC), score hydration, and the empty-input/empty-result cases.
func TestTopStocks(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	baseSym, baseExh := uniqueTag()
	ex1 := "EX1_" + baseExh
	ex2 := "EX2_" + baseExh
	a, b, c, d := baseSym+"A", baseSym+"B", baseSym+"C", baseSym+"D"

	defer clearStock(t, s, a, ex1)
	defer clearStock(t, s, b, ex1)
	defer clearStock(t, s, c, ex2)
	defer clearStock(t, s, d, ex2)

	ctx := context.Background()
	now := time.Now()

	seedStock(t, s, a, ex1, now)
	seedStock(t, s, b, ex1, now)
	seedStock(t, s, c, ex2, now)
	seedStock(t, s, d, ex2, now)
	seedScore(t, s, a, ex1, "sentiment", 0.9, now)
	seedScore(t, s, b, ex1, "sentiment", 0.5, now)
	seedScore(t, s, c, ex2, "sentiment", 0.95, now)
	seedScore(t, s, d, ex2, "sentiment", 0.5, now)
	// Unrelated category on A must not influence the "sentiment" ranking.
	seedScore(t, s, a, ex1, "momentum", 0.3, now)

	pairs := func(stocks []Stock) []string {
		got := make([]string, len(stocks))
		for i, st := range stocks {
			got[i] = st.Symbol + "/" + st.Exchange
		}
		return got
	}
	assertOrder := func(name string, want []string, limit int32, category string, exchange *string) []Stock {
		t.Helper()
		got, err := s.TopStocks(ctx, limit, category, exchange)
		if err != nil {
			t.Fatalf("%s: TopStocks: %v", name, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: got %d stocks %v, want %d %v", name, len(got), pairs(got), len(want), want)
		}
		if diff := pairs(got); !equalStrings(diff, want) {
			t.Fatalf("%s: order = %v, want %v", name, diff, want)
		}
		return got
	}

	t.Run("limit returns top ordered", func(t *testing.T) {
		assertOrder("limit 2", []string{c + "/" + ex2, a + "/" + ex1}, 2, "sentiment", nil)
	})

	t.Run("exchange scoped", func(t *testing.T) {
		assertOrder("ex1", []string{a + "/" + ex1, b + "/" + ex1}, 10, "sentiment", &ex1)
	})

	t.Run("all with deterministic tie break", func(t *testing.T) {
		got := assertOrder("all", []string{c + "/" + ex2, a + "/" + ex1, b + "/" + ex1, d + "/" + ex2}, 10, "sentiment", nil)

		// Scores must be hydrated; A carries both its sentiment and momentum scores.
		var stockA *Stock
		for i := range got {
			if got[i].Symbol == a && got[i].Exchange == ex1 {
				stockA = &got[i]
			}
		}
		if stockA == nil {
			t.Fatalf("stock A not present in result")
		}
		if len(stockA.Scores) != 2 {
			t.Fatalf("stock A scores = %v, want 2 entries", stockA.Scores)
		}
		sentiment := scoreByCategory(stockA.Scores, "sentiment")
		if sentiment == nil {
			t.Fatalf("stock A missing sentiment score: %v", stockA.Scores)
		}
		if sentiment.Value != 0.9 {
			t.Fatalf("stock A sentiment = %v, want 0.9", sentiment.Value)
		}
	})

	t.Run("non-positive limit", func(t *testing.T) {
		got, err := s.TopStocks(ctx, 0, "sentiment", nil)
		if err != nil {
			t.Fatalf("limit 0: %v", err)
		}
		if got == nil {
			t.Fatalf("limit 0: want non-nil empty slice")
		}
		if len(got) != 0 {
			t.Fatalf("limit 0: got %d stocks, want 0", len(got))
		}
	})

	t.Run("empty category", func(t *testing.T) {
		got, err := s.TopStocks(ctx, 10, "", nil)
		if err != nil {
			t.Fatalf("empty category: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("empty category: got %d stocks, want 0", len(got))
		}
	})

	t.Run("unknown category", func(t *testing.T) {
		got, err := s.TopStocks(ctx, 10, "no_such_category_"+baseSym, nil)
		if err != nil {
			t.Fatalf("unknown category: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("unknown category: got %d stocks, want 0", len(got))
		}
	})
}

// TestUpdateStockPreservesUnmentionedScores proves that an upsert merges the
// incoming score categories into the stored set: categories present in the
// request are inserted or refreshed, while categories absent from the request
// keep both their value and their last-updated timestamp. nil and empty maps
// refresh only the stock-level timestamp.
func TestUpdateStockPreservesUnmentionedScores(t *testing.T) {
	s := testStore(t)
	defer s.Close()

	ctx := context.Background()

	// assertScorelessUpsert keeps the nil and empty-map cases identical: both
	// must leave every score untouched while still touching the stock row.
	assertScorelessUpsert := func(t *testing.T, scores map[string]float64) {
		t.Helper()

		sym, exh := uniqueTag()
		defer clearStock(t, s, sym, exh)

		base := time.Now().UTC().Add(-3 * time.Hour)
		seedStock(t, s, sym, exh, base)
		seedScore(t, s, sym, exh, "momentum", 0.25, base)
		seedScore(t, s, sym, exh, "sentiment", 0.9, base.Add(time.Hour))

		baselineMomentum := readScore(t, s, sym, exh, "momentum")
		baselineSentiment := readScore(t, s, sym, exh, "sentiment")

		got, _, err := s.UpdateStock(ctx, sym, exh, scores)
		if err != nil {
			t.Fatalf("UpdateStock: %v", err)
		}
		if got == nil {
			t.Fatalf("UpdateStock returned nil stock")
		}

		momentum := readScore(t, s, sym, exh, "momentum")
		if momentum.Value != baselineMomentum.Value {
			t.Fatalf("momentum value = %v, want unchanged %v", momentum.Value, baselineMomentum.Value)
		}
		if !momentum.UpdatedAt.Equal(baselineMomentum.UpdatedAt) {
			t.Fatalf("momentum UpdatedAt = %v, want unchanged %v", momentum.UpdatedAt, baselineMomentum.UpdatedAt)
		}

		sentiment := readScore(t, s, sym, exh, "sentiment")
		if sentiment.Value != baselineSentiment.Value {
			t.Fatalf("sentiment value = %v, want unchanged %v", sentiment.Value, baselineSentiment.Value)
		}
		if !sentiment.UpdatedAt.Equal(baselineSentiment.UpdatedAt) {
			t.Fatalf("sentiment UpdatedAt = %v, want unchanged %v", sentiment.UpdatedAt, baselineSentiment.UpdatedAt)
		}

		if n := scoreCount(t, s, sym, exh); n != 2 {
			t.Fatalf("score count = %d, want 2", n)
		}

		requireScoreValue(t, got.Scores, "momentum", 0.25)
		requireScoreValue(t, got.Scores, "sentiment", 0.9)

		// The stock-level timestamp is always refreshed by an upsert, even when
		// no score rows are written.
		if !got.Updated.After(base) {
			t.Fatalf("stock Updated = %v, want after seeded %v", got.Updated, base)
		}
	}

	t.Run("subset update keeps unmentioned value and timestamp", func(t *testing.T) {
		sym, exh := uniqueTag()
		defer clearStock(t, s, sym, exh)

		base := time.Now().UTC().Add(-3 * time.Hour)
		seedStock(t, s, sym, exh, base)
		seedScore(t, s, sym, exh, "momentum", 0.25, base)
		seedScore(t, s, sym, exh, "sentiment", 0.9, base.Add(time.Hour))

		baselineMomentum := readScore(t, s, sym, exh, "momentum")
		baselineSentiment := readScore(t, s, sym, exh, "sentiment")

		got, _, err := s.UpdateStock(ctx, sym, exh, map[string]float64{"momentum": -0.4})
		if err != nil {
			t.Fatalf("UpdateStock: %v", err)
		}
		if got == nil {
			t.Fatalf("UpdateStock returned nil stock")
		}

		momentum := readScore(t, s, sym, exh, "momentum")
		if momentum.Value != -0.4 {
			t.Fatalf("momentum value = %v, want -0.4", momentum.Value)
		}
		if !momentum.UpdatedAt.After(baselineMomentum.UpdatedAt) {
			t.Fatalf("momentum UpdatedAt = %v, want after baseline %v", momentum.UpdatedAt, baselineMomentum.UpdatedAt)
		}

		sentiment := readScore(t, s, sym, exh, "sentiment")
		if sentiment.Value != 0.9 {
			t.Fatalf("sentiment value = %v, want 0.9", sentiment.Value)
		}
		if !sentiment.UpdatedAt.Equal(baselineSentiment.UpdatedAt) {
			t.Fatalf("sentiment UpdatedAt = %v, want unchanged %v", sentiment.UpdatedAt, baselineSentiment.UpdatedAt)
		}

		if n := scoreCount(t, s, sym, exh); n != 2 {
			t.Fatalf("score count = %d, want 2", n)
		}

		requireScoreValue(t, got.Scores, "momentum", -0.4)
		requireScoreValue(t, got.Scores, "sentiment", 0.9)
	})

	t.Run("new category keeps existing scores", func(t *testing.T) {
		sym, exh := uniqueTag()
		defer clearStock(t, s, sym, exh)

		base := time.Now().UTC().Add(-3 * time.Hour)
		seedStock(t, s, sym, exh, base)
		seedScore(t, s, sym, exh, "momentum", 0.25, base)

		baselineMomentum := readScore(t, s, sym, exh, "momentum")

		got, _, err := s.UpdateStock(ctx, sym, exh, map[string]float64{"quality": 0.5})
		if err != nil {
			t.Fatalf("UpdateStock: %v", err)
		}
		if got == nil {
			t.Fatalf("UpdateStock returned nil stock")
		}

		momentum := readScore(t, s, sym, exh, "momentum")
		if momentum.Value != 0.25 {
			t.Fatalf("momentum value = %v, want unchanged 0.25", momentum.Value)
		}
		if !momentum.UpdatedAt.Equal(baselineMomentum.UpdatedAt) {
			t.Fatalf("momentum UpdatedAt = %v, want unchanged %v", momentum.UpdatedAt, baselineMomentum.UpdatedAt)
		}

		quality := readScore(t, s, sym, exh, "quality")
		if quality.Value != 0.5 {
			t.Fatalf("quality value = %v, want 0.5", quality.Value)
		}
		if quality.UpdatedAt.IsZero() {
			t.Fatalf("quality UpdatedAt = zero, want non-zero")
		}

		if n := scoreCount(t, s, sym, exh); n != 2 {
			t.Fatalf("score count = %d, want 2", n)
		}

		requireScoreValue(t, got.Scores, "momentum", 0.25)
		requireScoreValue(t, got.Scores, "quality", 0.5)
	})

	t.Run("nil scores keeps all scores", func(t *testing.T) {
		assertScorelessUpsert(t, nil)
	})

	t.Run("empty map keeps all scores", func(t *testing.T) {
		assertScorelessUpsert(t, map[string]float64{})
	})
}

// requireScoreValue asserts that a hydrated score set contains category with
// exactly want.
func requireScoreValue(t *testing.T, scores []ScoreEntry, category string, want float64) {
	t.Helper()
	entry := scoreByCategory(scores, category)
	if entry == nil {
		t.Fatalf("response missing %q score: %v", category, scores)
	}
	if entry.Value != want {
		t.Fatalf("response %q value = %v, want %v", category, entry.Value, want)
	}
}

// scoreByCategory returns the ScoreEntry for category, or nil if absent.
func scoreByCategory(scores []ScoreEntry, category string) *ScoreEntry {
	for i := range scores {
		if scores[i].Category == category {
			return &scores[i]
		}
	}
	return nil
}

// equalStrings reports whether two string slices are equal in order and length.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

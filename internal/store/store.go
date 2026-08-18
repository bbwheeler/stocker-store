package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultRetention is how long a stock may live without being added or updated
// before it is considered stale and removed.
const DefaultRetention = 30 * 24 * time.Hour

// Store handles all PostgreSQL interactions for the stock store service.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store connected to the database at dsn.
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	store := &Store{pool: pool}

	if err := store.initializeTables(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return store, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() { s.pool.Close() }

// initializeTables creates the stocks and scores tables if they don't exist.
func (s *Store) initializeTables(ctx context.Context) error {
	stmt := `
	CREATE TABLE IF NOT EXISTS stocks (
		symbol    TEXT        NOT NULL,
		exchange  TEXT        NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (symbol, exchange)
	);

	CREATE TABLE IF NOT EXISTS scores (
		symbol   TEXT             NOT NULL,
		exchange TEXT             NOT NULL,
		category TEXT             NOT NULL,
		value    DOUBLE PRECISION CHECK(value BETWEEN -1.0 AND 1.0) DEFAULT 0.0,
		timestamp TIMESTAMPTZ      NOT NULL DEFAULT now(),
		UNIQUE (symbol, exchange, category),
		FOREIGN KEY (symbol, exchange) REFERENCES stocks(symbol, exchange)
	);

	CREATE INDEX IF NOT EXISTS idx_scores_category_value ON scores(category, value DESC);
	`

	if _, err := s.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("initialize tables: %w", err)
	}

	return nil
}

// UpdateStock updates a stock and optionally its scores. Returns the updated stock record.
func (s *Store) UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*Stock, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO stocks (symbol, exchange, timestamp)
		VALUES ($1, $2, now())
		ON CONFLICT (symbol, exchange) DO UPDATE SET timestamp = now()
	`, symbol, exchange)
	if err != nil {
		return nil, fmt.Errorf("update stock: %w", err)
	}

	if len(scores) > 0 {
		upsertScore := `
			INSERT INTO scores (symbol, exchange, category, value, timestamp)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (symbol, exchange, category)
			DO UPDATE SET value = EXCLUDED.value, timestamp = now()
		`

		for cat, val := range scores {
			if _, err := tx.Exec(ctx, upsertScore, symbol, exchange, cat, val); err != nil {
				return nil, fmt.Errorf("upsert score %s: %w", cat, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	var ts time.Time
	if err := s.pool.QueryRow(ctx,
		`SELECT timestamp FROM stocks WHERE symbol = $1 AND exchange = $2`,
		symbol, exchange).Scan(&ts); err != nil {
		return nil, fmt.Errorf("get stock timestamp after update: %w", err)
	}

	stock := &Stock{Symbol: symbol, Exchange: exchange, Created: ts}
	stock.Scores, err = s.getStockScores(ctx, symbol, exchange)
	if err != nil {
		return nil, fmt.Errorf("get stock scores after update: %w", err)
	}

	return stock, nil
}

// RemoveOldStocks deletes stocks (and their dependent scores) whose timestamp is
// older than the given retention window. A zero or negative retention window
// disables the check. Returns the number of stock rows removed.
func (s *Store) RemoveOldStocks(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		WITH old AS (
			SELECT symbol, exchange FROM stocks
			WHERE timestamp < now() - $1::interval
		)
		DELETE FROM scores
		USING old
		WHERE scores.symbol = old.symbol AND scores.exchange = old.exchange
	`, retention.String()); err != nil {
		return 0, fmt.Errorf("delete old scores: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		DELETE FROM stocks
		WHERE timestamp < now() - $1::interval
	`, retention.String())
	if err != nil {
		return 0, fmt.Errorf("delete old stocks: %w", err)
	}
	removed := tag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	return removed, nil
}

// RemoveStock removes a stock and its scores by symbol and exchange. Returns true if anything was removed.
func (s *Store) RemoveStock(ctx context.Context, symbol, exchange string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM scores WHERE symbol = $1 AND exchange = $2`, symbol, exchange); err != nil {
		return false, fmt.Errorf("delete scores: %w", err)
	}

	result, err := tx.Exec(ctx, `DELETE FROM stocks WHERE symbol = $1 AND exchange = $2`, symbol, exchange)
	if err != nil {
		return false, fmt.Errorf("delete stock: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}

	return result.RowsAffected() > 0, nil
}

// GetStock retrieves a single stock by symbol with optional exchange filter.
func (s *Store) GetStock(ctx context.Context, symbol string, exchange *string) (*Stock, error) {
	query := "SELECT symbol, exchange, timestamp FROM stocks WHERE symbol = $1"
	args := []interface{}{symbol}
	argIdx := 2

	if exchange != nil {
		query += fmt.Sprintf(" AND exchange = $%d", argIdx)
		args = append(args, *exchange)
		argIdx++
	}

	var stock Stock
	err := s.pool.QueryRow(ctx, query, args...).Scan(&stock.Symbol, &stock.Exchange, &stock.Created)
	if err != nil {
		return nil, fmt.Errorf("get stock by symbol: %w", err)
	}

	stock.Scores, err = s.getStockScores(ctx, stock.Symbol, stock.Exchange)
	if err != nil {
		return nil, fmt.Errorf("get stock scores: %w", err)
	}

	return &stock, nil
}

// GetStocks retrieves a list of stocks with optional filters.
func (s *Store) GetStocks(ctx context.Context, limit int32, exchange *string, minScores, maxScores map[string]float64) ([]Stock, error) {
	query := "SELECT symbol, exchange, timestamp FROM stocks WHERE true"
	args := []interface{}{}
	argIdx := 1

	if exchange != nil {
		query += fmt.Sprintf(" AND exchange = $%d", argIdx)
		args = append(args, *exchange)
		argIdx++
	}

	for cat, minVal := range minScores {
		query += fmt.Sprintf(` AND symbol IN (SELECT symbol FROM scores WHERE category = $%d AND value >= $%d)`, argIdx, argIdx+1)
		args = append(args, cat, minVal)
		argIdx += 2
	}

	for cat, maxVal := range maxScores {
		query += fmt.Sprintf(` AND symbol IN (SELECT symbol FROM scores WHERE category = $%d AND value <= $%d)`, argIdx, argIdx+1)
		args = append(args, cat, maxVal)
		argIdx += 2
	}

	query += " ORDER BY RANDOM() LIMIT $" + fmt.Sprintf("%d", argIdx)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get stocks: %w", err)
	}
	defer rows.Close()

	var all []Stock
	for rows.Next() {
		var stock Stock
		if err := rows.Scan(&stock.Symbol, &stock.Exchange, &stock.Created); err != nil {
			return nil, fmt.Errorf("scan stock row: %w", err)
		}
		all = append(all, stock)
	}

	for i := range all {
		scores, err := s.getStockScores(ctx, all[i].Symbol, all[i].Exchange)
		if err != nil {
			return nil, fmt.Errorf("get scores for %s: %w", all[i].Symbol, err)
		}
		all[i].Scores = scores
	}

	return all, nil
}

func (s *Store) getStockScores(ctx context.Context, symbol, exchange string) ([]ScoreEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT category, value FROM scores WHERE symbol = $1 AND exchange = $2 ORDER BY category
	`, symbol, exchange)
	if err != nil {
		return nil, fmt.Errorf("query scores: %w", err)
	}
	defer rows.Close()

	var scores []ScoreEntry
	for rows.Next() {
		var score ScoreEntry
		if err := rows.Scan(&score.Category, &score.Value); err != nil {
			return nil, fmt.Errorf("scan score row: %w", err)
		}
		scores = append(scores, score)
	}

	return scores, nil
}

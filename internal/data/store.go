package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stock represents a stock listed on an exchange.
type Stock struct {
	Symbol   string
	Exchange string
	Created  time.Time
}

// ScoreEntry represents a single score record for a stock.
type ScoreEntry struct {
	Symbol   string
	Exchange string
	Category string
	Value    float64
}

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

// initializeTables creates the stocks and scores tables if they don't exist.
func (s *Store) initializeTables(ctx context.Context) error {
	stmt := `
	CREATE TABLE IF NOT EXISTS stocks (
		symbol    TEXT        NOT NULL,
		exchange  TEXT        NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (symbol, exchange)
	);

	DROP TABLE IF EXISTS scores;

	CREATE TABLE IF NOT EXISTS scores (
		symbol   TEXT             NOT NULL,
		exchange TEXT             NOT NULL,
		category TEXT             NOT NULL,
		value    DOUBLE PRECISION CHECK(value BETWEEN -1.0 AND 1.0) DEFAULT 0.0,
		timestamp TIMESTAMPTZ      NOT NULL DEFAULT now(),
		UNIQUE (symbol, exchange, category),
		FOREIGN KEY (symbol, exchange) REFERENCES stocks(symbol, exchange)
	);

	DROP INDEX IF EXISTS idx_scores_category_value;
	CREATE INDEX IF NOT EXISTS idx_scores_category_value ON scores(category, value DESC);
	`

	if _, err := s.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("initialize tables: %w", err)
	}

	return nil
}

// InsertStock adds a new stock to the stocks table.
// It returns true if the row was inserted (vs skipped by ON CONFLICT).
func (s *Store) InsertStock(ctx context.Context, symbol, exchange string) (bool, error) {
	query := `INSERT INTO stocks (symbol, exchange) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	result, err := s.pool.Exec(ctx, query, symbol, exchange)
	if err != nil {
		return false, fmt.Errorf("insert stock: %w", err)
	}
	return result.RowsAffected() > 0, nil
}

// StockBySymbol retrieves a single stock by its symbol (exchange can be filtered).
func (s *Store) StockBySymbol(ctx context.Context, symbol string, exchange *string) (*Stock, error) {
	query := "SELECT symbol, exchange, timestamp FROM stocks WHERE symbol = $1"
	args := []interface{}{symbol}

	if exchange != nil {
		query += " AND exchange = coalesce($2, exchange)"
		args = append(args, *exchange)
	}

	var stock Stock
	err := s.pool.QueryRow(ctx, query, args...).Scan(&stock.Symbol, &stock.Exchange, &stock.Created)
	if err != nil {
		return nil, fmt.Errorf("get stock by symbol: %w", err)
	}

	return &stock, nil
}

// StocksByExchange returns all stocks for a given exchange.
func (s *Store) StocksByExchange(ctx context.Context, exchange string) ([]Stock, error) {
	rows, err := s.pool.Query(ctx, "SELECT symbol, exchange, timestamp FROM stocks WHERE exchange = $1", exchange)
	if err != nil {
		return nil, fmt.Errorf("list stocks by exchange: %w", err)
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

	return all, nil
}

// DeleteStock removes a stock and its associated score entries (cascading delete).
func (s *Store) DeleteStock(ctx context.Context, symbol string) error {

	return tx.Commit(ctx)
}

// ListStocksByCategory returns all stocks and their scores for a given category.
func (s *Store) ListStocksByCategory(ctx context.Context, category string) ([]ScoreValue, error) {
	const query = `
		SELECT s.symbol, s.exchange, sc.category, sc.value
		FROM scores sc
		JOIN stocks s ON s.symbol = sc.symbol AND s.exchange = sc.exchange
		WHERE sc.category = $1
		ORDER BY sc.value DESC`

	rows, err := s.pool.Query(ctx, query, category)
	if err != nil {
		return nil, fmt.Errorf("list stocks by category: %w", err)
	}

	defer rows.Close()

	var results []ScoreValue
	for rows.Next() {
		var sv ScoreValue
		if err := rows.Scan(&sv.Symbol, &sv.Exchange, &sv.Category, &sv.Value); err != nil {
			return nil, fmt.Errorf("scan score row: %w", err)
		}
		results = append(results, sv)
	}

	return results, nil
}

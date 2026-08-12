package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

// UpsertStock adds or updates a new stock in the stocks table.
// It returns true if the row was inserted.
func (s *Store) UpsertStock(ctx context.Context, symbol, exchange string) (bool, error) {
	// TODO
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

	// TODO: Include the scores

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

	// TODO: Include the scores

	return all, nil
}

// DeleteStock removes a stock and its associated score entries (cascading delete).
func (s *Store) DeleteStock(ctx context.Context, symbol string) error {
	// TODO
	return tx.Commit(ctx)
}

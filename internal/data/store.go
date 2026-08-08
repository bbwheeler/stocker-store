package data

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
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
// Per the design doc, schema migrations are "not necessary" — we simply DROP and recreate.
func (s *Store) initializeTables(ctx context.Context) error {
	stmt := `
	CREATE TABLE IF NOT EXISTS stocks (
		symbol    TEXT        NOT NULL,
		exchange  TEXT        NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (symbol, exchange)
	);

	CREATE TABLE IF NOT EXISTS scores (
		stock_id  TEXT             NOT NULL REFERENCES stocks(symbol, exchange),
		category  TEXT             NOT NULL,
		value     DOUBLE PRECISION CHECK(value BETWEEN -1.0 AND 1.0) DEFAULT 0.0,
		timestamp TIMESTAMPTZ      NOT NULL DEFAULT now(),
		UNIQUE (stock_id, category)
	);

	CREATE INDEX IF NOT EXISTS idx_scores_category_value ON scores(category, value DESC);
	`

	if _, err := s.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("initialize tables: %w", err)
	}

	return nil
}

// InsertStock adds a new stock to the stocks table.
func (s *Store) InsertStock(ctx context.Context, symbol, exchange string) error {
	query := `INSERT INTO stocks (symbol, exchange) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	if _, err := s.pool.Exec(ctx, query, symbol, exchange); err != nil {
		return fmt.Errorf("insert stock: %w", err)
	}
	return nil
}

// StockByID retrieves a single stock by its symbol (exchange can be filtered).
func (s *Store) StockByID(ctx context.Context, symbol string, exchange *string) (*Stock, error) {
	query := "SELECT symbol, exchange, timestamp FROM stocks WHERE symbol = $1"
	args := []interface{}{symbol}

	if exchange != nil {
		query += " AND exchange = $" + fmt.Sprintf("%d", len(args)+1)
		args = append(args, *exchange)
	}

	var stock Stock
	err := s.pool.QueryRow(ctx, query, args...).Scan(&stock.Symbol, &stock.Exchange, &stock.Created)
	if err != nil {
		return nil, fmt.Errorf("get stock by id: %w", err)
	}

	return &stock, nil
}

// StocksByExchange returns all stocks for a given exchange.
func (s *Store) StocksByExchange(ctx context.Context, exchange string) ([]Stock, error) {
	rows, err := s.pool.Query(ctx, "SELECT symbol, exchange, timestamp FROM stocks WHERE exchange = $1", exchange)
	if err != nil {
		return nil, fmt.Errorf("list stocks by exchange: %w", err)
	}

	all, err := pgx.CollectRows(rows, pgx.RowToStructedByName[Stock])
	return all, err
}

// DeleteStock removes a stock from the database.
func (s *Store) DeleteStock(ctx context.Context, symbol string) error {
	var sb strings.Builder
	sb.WriteString("DELETE FROM stocks WHERE symbol = $1")

	rows, err := pgx.CollectRows(rows, pgx.RowToStructedByName[ScoreEntry]) // score entries for this stock
	return rows, err // return scores

	
Query  return StockList

}

// ListStocksByCategory returns all stocks and their scores.
func (s *Store) ListStocksByCategory(ctx context.Context, category string) ([]ScoreValue, error) {
	return []ScoreValue{}, nil // TODO - implement based on design doc
	
}

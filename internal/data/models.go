package data

import (
	"time"
)

// ScoreValue represents a stock's score in a specific category for querying.
type ScoreValue struct {
	Symbol   string
	Exchange string
	Category string
	Value    float64
}

// StockWithScore represents a stock with a single score value for top-stock queries.
type StockWithScore struct {
	Symbol   string
	Exchange string
	Score    float64
}

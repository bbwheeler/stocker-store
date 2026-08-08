package data

import (
	"context"
	"fmt"
	"time"
)

// Stock represents a stock from the `stocks` table.
type Stock struct {
	Symbol   string
	Exchange string
	Created  time.Time
}

// ScoreEntry holds a single score category and value for a stock.
type ScoreEntry struct {
	Category string
	Value    float64
}

// ScoreSnapshot captures the current score + history of past values.
type ScoreHistoryEntry struct {
	Value     float64
	Timestamp time.Time
}

// UpdateScoreInput is used to submit or update a score for a given stock/category.

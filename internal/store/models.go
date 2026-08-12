package store

import "time"

// Stock represents a stock listed on an exchange.
type Stock struct {
	Symbol   string
	Exchange string
	Scores   []ScoreEntry
	Created  time.Time
}

// ScoreEntry represents a single score record for a stock.
type ScoreEntry struct {
	Category string
	Value    float64
}

// Package events defines the neutral publisher contract shared by the gRPC
// server and the broker implementations. It lives in neither the gRPC server
// nor any single broker package so ingestion can swap implementations without
// changing callers.
package events

import (
	"context"

	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"
)

// Publisher emits new-stock events whenever a new stock (an INSERT, not an
// UPDATE) is written to the store.
type Publisher interface {
	// Publish emits the stock event. It is best-effort: callers are expected
	// to log (not return) an error so a publish failure does not abort the
	// primary DB write or the handler response.
	Publish(ctx context.Context, stock *stockv1.Stock) error
	// Close releases any resources held by the publisher.
	Close() error
}

// Noop is a Publisher that does nothing: Publish and Close return nil without
// side effects. It is the fallback when no broker is configured.
type Noop struct{}

// Publish implements Publisher.
func (Noop) Publish(context.Context, *stockv1.Stock) error { return nil }

// Close implements Publisher.
func (Noop) Close() error { return nil }

// Package grpc provides the gRPC server implementation for the stocker-store service.
package grpc

import (
	"context"

	"stocker-store/internal/store"
)

// Store exposes the data store methods needed by the gRPC handlers.
type Store interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, error)
	RemoveStock(ctx context.Context, symbol, exchange string) (bool, error)
	GetStock(ctx context.Context, symbol string, exchange *string) (*store.Stock, error)
	GetStocks(ctx context.Context, limit int32, exchange *string, minScores, maxScores map[string]float64) ([]store.Stock, error)
}

// Server holds the dependencies for the gRPC service.
type Server struct {
	store Store
}

// NewServer creates a new Server with the given store.
func NewServer(store Store) *Server {
	return &Server{store: store}
}

// GRPCServer returns a new gRPC server instance (stub until proto code is generated).
func (s *Server) GRPCServer() interface {
	Serve(lis interface{}) error
	GracefulStop()
} {
	return noopGRPCServer{}
}

type noopGRPCServer struct{}

func (noopGRPCServer) Serve(lis interface{}) error { return nil }
func (noopGRPCServer) GracefulStop()               {}

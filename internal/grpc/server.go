// Package grpc provides the gRPC server implementation for the stocker-store service.
package grpc

import (
	"net/http"

	"stocker-store/internal/data"
)

// Server holds the dependencies for the gRPC/HTTP service.
type Server struct {
	store *data.Store
}

// NewServer creates a new Server with the given store.
func NewServer(store *data.Store) *Server {
	return &Server{store: store}
}

// GRPCServer returns a new gRPC server instance (stub until proto code is generated).
func (s *Server) GRPCServer() interface {
	Serve(lis interface{}) error
	GracefulStop()
} {
	return noopGRPCServer{}
}

// HTTPServer creates and starts a health check HTTP server on the given address.
// It returns the http.Server so callers can track it for graceful shutdown.
func (s *Server) HTTPServer(addr string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return &http.Server{Addr: addr, Handler: mux}, nil
}

type noopGRPCServer struct{}

func (noopGRPCServer) Serve(lis interface{}) error   { return nil }
func (noopGRPCServer) GracefulStop()                {}

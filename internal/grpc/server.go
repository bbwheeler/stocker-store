// Package grpc provides the gRPC server implementation for the stocker-store service.
package grpc

// Abstraction for the data store ()
type Store interface {
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

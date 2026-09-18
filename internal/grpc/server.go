// Package grpc provides the gRPC server implementation for the stocker-store service.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"stocker-store/internal/store"
	st "stocker-store/proto/v1"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	st.UnimplementedStockStoreServer
	store Store
}

// NewServer creates a new Server with the given store.
func NewServer(store Store) *Server {
	return &Server{store: store}
}

// GRPCServer returns a new gRPC server with the StockStore service registered.
func (s *Server) GRPCServer() *grpc.Server {
	srv := grpc.NewServer()
	st.RegisterStockStoreServer(srv, s)
	return srv
}

// AddStocks receives a stream of stock updates, upserting each one, and
// responds with the last updated stock record.
func (s *Server) AddStocks(stream grpc.ClientStreamingServer[st.UpdateStockRequest, st.Stock]) error {
	ctx := stream.Context()
	var last *store.Stock

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "add stocks: %v", err)
		}

		stock, err := s.store.UpdateStock(ctx, req.GetSymbol(), req.GetExchange(), req.GetScores())
		if err != nil {
			log.Printf("update stock %s/%s: %v", req.GetSymbol(), req.GetExchange(), err)
			return status.Errorf(codes.Internal, "update stock: %v", err)
		}
		last = stock
	}

	if last == nil {
		log.Printf("add stocks: empty stream")
		return status.Error(codes.InvalidArgument, "empty stream")
	}

	if err := stream.SendAndClose(toProtoStock(last)); err != nil {
		return err
	}
	return nil
}

// UpdateStock upserts a stock and returns the updated record.
func (s *Server) UpdateStock(ctx context.Context, req *st.UpdateStockRequest) (*st.Stock, error) {
	stock, err := s.store.UpdateStock(ctx, req.GetSymbol(), req.GetExchange(), req.GetScores())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update stock: %v", err)
	}
	return toProtoStock(stock), nil
}

// RemoveStock deletes a stock and its scores.
func (s *Server) RemoveStock(ctx context.Context, req *st.RemoveStockRequest) (*st.RemoveStockResponse, error) {
	removed, err := s.store.RemoveStock(ctx, req.GetSymbol(), req.GetExchange())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "remove stock: %v", err)
	}
	return &st.RemoveStockResponse{Removed: removed}, nil
}

// GetStock retrieves a single stock by symbol, optionally filtered by exchange.
func (s *Server) GetStock(ctx context.Context, req *st.GetStockRequest) (*st.Stock, error) {
	stock, err := s.store.GetStock(ctx, req.GetSymbol(), req.Exchange)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("stock %s not found", req.GetSymbol()))
		}
		return nil, status.Errorf(codes.Internal, "get stock: %v", err)
	}
	return toProtoStock(stock), nil
}

// GetStocks retrieves stocks matching the given filters, up to the requested limit.
func (s *Server) GetStocks(ctx context.Context, req *st.GetStocksRequest) (*st.StockList, error) {
	stocks, err := s.store.GetStocks(ctx, req.GetLimit(), req.Exchange, req.GetMinScores(), req.GetMaxScores())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get stocks: %v", err)
	}

	list := &st.StockList{Stocks: make([]*st.Stock, 0, len(stocks))}
	for i := range stocks {
		list.Stocks = append(list.Stocks, toProtoStock(&stocks[i]))
	}
	return list, nil
}

func toProtoScores(entries []store.ScoreEntry) []*st.ScoreEntry {
	out := make([]*st.ScoreEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &st.ScoreEntry{Category: e.Category, Value: e.Value, UpdatedAt: timestamppb.New(e.UpdatedAt)})
	}
	return out
}

func toProtoStock(s *store.Stock) *st.Stock {
	return &st.Stock{
		Symbol:   s.Symbol,
		Exchange: s.Exchange,
		Scores:   toProtoScores(s.Scores),
	}
}

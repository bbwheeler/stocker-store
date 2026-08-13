// Package main is the entry point for the stocker-store gRPC service.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"

	"stocker-store/internal/grpc"
	"stocker-store/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost:5432/stocker?sslmode=disable"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	st, err := store.NewStore(ctx, dsn)
	if err != nil {
		log.Fatalf("initialize store: %v", err)
	}
	defer st.Close()

	server := grpc.NewServer(st)

	lis, err := net.Listen("tcp", ":3500")
	if err != nil {
		log.Fatalf("listen on port 3500: %v", err)
	}

	gRPCServer := server.GRPCServer()

	go func() {
		if err := gRPCServer.Serve(lis); err != nil {
			log.Printf("gRPC serve error: %v", err)
		}
	}()

	log.Println("stocker-store listening on :3500")

	<-ctx.Done()
	log.Println("shutting down...")

	gRPCServer.GracefulStop()
	cancel()
}

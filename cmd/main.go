// Package main is the entry point for the stocker-store gRPC service.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"

	"stocker-store/internal/data"
	"stocker-store/internal/grpc"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost:5432/stocker?sslmode=disable"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	store, err := data.NewStore(ctx, dsn)
	if err != nil {
		log.Fatalf("initialize store: %v", err)
	}
	defer store.Close()

	server := grpc.NewServer(store)

	lis, err := net.Listen("tcp", ":3500")
	if err != nil {
		log.Fatalf("listen on port 3500: %v", err)
	}

	gRPCServer := server.GRPCServer()
	errServer := server.HTTPServer(":3501")

	go func() {
		if err := gRPCServer.Serve(lis); err != nil {
			log.Printf("gRPC serve error: %v", err)
		}
	}()

	go func() {
		if err := errServer; err != nil {
			log.Printf("health server error: %v", err)
		}
	}()

	log.Println("stocker-store listening on :3500")

	<-ctx.Done()
	log.Println("shutting down...")

	gRPCServer.GracefulStop()
	cancel()
}

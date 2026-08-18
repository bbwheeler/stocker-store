// Package main is the entry point for the stocker-store gRPC service.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

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

	go runRetention(ctx, st)

	log.Println("stocker-store listening on :3500")

	<-ctx.Done()
	log.Println("shutting down...")

	gRPCServer.GracefulStop()
	cancel()
}

// runRetention removes stocks older than the configured retention window. The
// window default is 30 days (720h) and is overridden by STOCK_TTL, a Go
// duration string such as "48h" or "720h"; setting it to "0" disables expiry.
func runRetention(ctx context.Context, st *store.Store) {
	retention := store.DefaultRetention
	if ttl := os.Getenv("STOCK_TTL"); ttl != "" {
		d, err := time.ParseDuration(ttl)
		if err != nil {
			log.Printf("invalid STOCK_TTL %q, using default %s: %v", ttl, store.DefaultRetention, err)
		} else {
			retention = d
		}
	}

	if retention <= 0 {
		log.Println("stock retention disabled (STOCK_TTL=0)")
		return
	}

	log.Printf("stock retention enabled: removing stocks older than %s", retention)

	n, err := st.RemoveOldStocks(ctx, retention)
	if err != nil {
		log.Printf("retention cleanup: %v", err)
	} else if n > 0 {
		log.Printf("removed %d stock(s) older than %s", n, retention)
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.RemoveOldStocks(ctx, retention)
			if err != nil {
				log.Printf("retention cleanup: %v", err)
			} else if n > 0 {
				log.Printf("removed %d stock(s) older than %s", n, retention)
			}
		}
	}
}

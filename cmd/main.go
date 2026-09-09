// Package main is the entry point for the stocker-store gRPC service.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"stocker-store/internal/grpc"
	"stocker-store/internal/kafka"
	"stocker-store/internal/store"
)

// Store is the subset of the stock store needed to ingest kafka messages.
type stockStore interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, error)
}

func main() {
	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		log.Fatal("DATABASE_URL required")
		dbUrl = "localhost:5432/stocker?sslmode=disable"
	}
	password := os.Getenv("DATABASE_PASSWORD")
	username := os.Getenv("DATABASE_USERNAME")
	if password != "" && username != "" {
		dbUrl = fmt.Sprintf("postgres://%s:%s@%s", username, url.QueryEscape(password), dbUrl)
	} else {
		dbUrl = fmt.Sprintf("postgres://%s", dbUrl)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	st, err := store.NewStore(ctx, dbUrl)
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

	if err := runKafkaSubscriber(ctx, st); err != nil {
		log.Printf("kafka subscriber: %v", err)
	}

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

func runKafkaSubscriber(ctx context.Context, st stockStore) error {
	brokers := envList("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	if len(brokers) == 0 || topic == "" {
		log.Fatal("KAFKA_BROKERS and KAFKA_TOPIC required")
		return nil
	}

	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "stocker-store"
	}

	client := kafka.New(kafka.Config{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}, st)

	log.Printf("kafka subscriber: consuming %q via %s", topic, strings.Join(brokers, ","))
	return client.Run(ctx)
}

func envList(name string) []string {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

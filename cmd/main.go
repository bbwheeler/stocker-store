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
	"strconv"
	"strings"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/events"
	"git.wheeli.ca/brian/stocker-store/internal/grpc"
	"git.wheeli.ca/brian/stocker-store/internal/rabbitmq"
	"git.wheeli.ca/brian/stocker-store/internal/store"
)

// stockStore is the subset of the stock store needed to ingest broker messages.
type stockStore interface {
	UpdateStock(ctx context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error)
}

func main() {
	rawPort := os.Getenv("GRPC_PORT")
	port, err := parsePort(rawPort)
	if err != nil {
		log.Fatalf("invalid GRPC_PORT %q: %v", rawPort, err)
	}
	addr := fmt.Sprintf(":%d", port)

	dsn := databaseDSN(os.Getenv("DATABASE_URL"), os.Getenv("DATABASE_USERNAME"), os.Getenv("DATABASE_PASSWORD"))
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	// Log the sanitized target once (scheme, username, host and path only —
	// never the password or query values) so the operator can confirm which
	// database is being written.
	log.Printf("database: %s", sanitizedDSN(dsn))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	st, err := store.NewStore(ctx, dsn)
	if err != nil {
		log.Fatalf("initialize store: %v", err)
	}
	defer st.Close()

	cfg, err := rabbitmq.LoadConfig()
	if err != nil {
		log.Fatalf("rabbitmq config: %v", err)
	}

	// Construct the publisher once; it is a no-op unless
	// RABBITMQ_OUTPUT_EXCHANGE is set. It is shared by the gRPC server and the
	// rabbitmq consumer so that *any* path that creates a new stock emits the
	// event.
	publisher, err := rabbitmq.NewPublisher(cfg)
	if err != nil {
		log.Fatalf("rabbitmq publisher: %v", err)
	}

	server := grpc.NewServer(st, publisher)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", addr, err)
	}

	gRPCServer := server.GRPCServer()

	go func() {
		if err := gRPCServer.Serve(lis); err != nil {
			log.Printf("gRPC serve error: %v", err)
		}
	}()

	go runRetention(ctx, st)

	log.Printf("stocker-store listening on %s", addr)

	// Run reconnects forever and returns only on context cancellation (nil)
	// or a configuration error; a non-nil return is misconfiguration and
	// must be fatal so systemd's Restart=always surfaces it instead of
	// leaving a healthy-looking process with dead ingestion.
	if err := runRabbitMQConsumer(ctx, st, publisher, cfg); err != nil {
		log.Fatalf("rabbitmq consumer: %v", err)
	}

	<-ctx.Done()
	log.Println("shutting down...")

	gRPCServer.GracefulStop()
	// Flush in-flight publisher writes before exiting.
	if err := publisher.Close(); err != nil {
		log.Printf("close publisher: %v", err)
	}
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

func runRabbitMQConsumer(ctx context.Context, st stockStore, publisher events.Publisher, cfg rabbitmq.Config) error {
	if !cfg.IngestionEnabled() {
		log.Printf("rabbitmq: ingestion disabled (RABBITMQ_URL explicitly empty)")
		return nil
	}
	log.Printf("rabbitmq: consuming queue=%q exchange=%q routing_key=%q prefetch=%d",
		cfg.Queue, cfg.Exchange, cfg.RoutingKey, cfg.Prefetch)
	return rabbitmq.New(cfg, st, publisher).Run(ctx)
}

// databaseDSN builds the Postgres DSN handed to the store. A full DSN
// (postgres://… or postgresql://…) is authoritative and returned verbatim;
// otherwise the legacy host:port/db form is prefixed with postgres:// and,
// when both username and password are non-empty, credentials are embedded.
// An empty raw value yields "" (the caller treats it as missing config).
func databaseDSN(raw, username, password string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "postgres://") || strings.HasPrefix(raw, "postgresql://") {
		return raw
	}
	if username != "" && password != "" {
		return "postgres://" + url.UserPassword(username, password).String() + "@" + raw
	}
	return "postgres://" + raw
}

// sanitizedDSN renders a DSN for logs: scheme, username, host and path only.
// The password and all query values are dropped. If the DSN cannot be
// parsed, a generic placeholder is returned so the raw value (which may
// contain secrets) never reaches the log.
func sanitizedDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<unparseable DSN>"
	}
	var user string
	if u.User != nil && u.User.Username() != "" {
		user = u.User.Username() + "@"
	}
	return fmt.Sprintf("%s://%s%s%s", u.Scheme, user, u.Host, u.Path)
}

// defaultGRPCPort is the listen port used when GRPC_PORT is unset or empty.
const defaultGRPCPort = 3500

// parsePort validates a decimal TCP port. An empty or whitespace-only value
// yields defaultGRPCPort; anything outside 1..65535 is an error.
func parsePort(raw string) (int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultGRPCPort, nil
	}

	port, err := strconv.Atoi(trimmed)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port %q: must be an integer between 1 and 65535", raw)
	}
	return port, nil
}

package rabbitmq

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultURL                = "amqp://guest:guest@192.168.1.48:5672/"
	defaultExchange           = "stock.update"
	defaultExchangeType       = "topic"
	defaultRoutingKey         = "stock.update.v1"
	defaultQueue              = "stocker-store.stock.update.v1"
	defaultPrefetch           = 1
	defaultOutputExchangeType = "topic"
	defaultOutputRoutingKey   = "stock.created.v1"
)

// Config holds the RabbitMQ settings for the consumer and the optional
// new-stock publisher.
type Config struct {
	URL                string
	Exchange           string
	ExchangeType       string
	RoutingKey         string
	Queue              string
	Prefetch           int
	OutputExchange     string
	OutputExchangeType string
	OutputRoutingKey   string
}

// IngestionEnabled reports whether the consumer should run. Ingestion is
// disabled only when RABBITMQ_URL is explicitly empty.
func (c Config) IngestionEnabled() bool { return c.URL != "" }

// OutputEnabled reports whether the new-stock publisher should run.
func (c Config) OutputEnabled() bool { return c.OutputExchange != "" }

// LoadConfig reads the RabbitMQ environment configuration. An absent
// RABBITMQ_URL selects the default broker; an explicitly empty value disables
// ingestion. Every other variable treats a blank value as absent and falls
// back to its default.
func LoadConfig() (Config, error) {
	cfg := Config{
		URL:                envURL(),
		Exchange:           getEnv("RABBITMQ_EXCHANGE", defaultExchange),
		ExchangeType:       strings.ToLower(getEnv("RABBITMQ_EXCHANGE_TYPE", defaultExchangeType)),
		RoutingKey:         getEnv("RABBITMQ_ROUTING_KEY", defaultRoutingKey),
		Queue:              getEnv("RABBITMQ_QUEUE", defaultQueue),
		Prefetch:           defaultPrefetch,
		OutputExchange:     getEnv("RABBITMQ_OUTPUT_EXCHANGE", ""),
		OutputExchangeType: strings.ToLower(getEnv("RABBITMQ_OUTPUT_EXCHANGE_TYPE", defaultOutputExchangeType)),
		OutputRoutingKey:   getEnv("RABBITMQ_OUTPUT_ROUTING_KEY", defaultOutputRoutingKey),
	}

	if raw, ok := os.LookupEnv("RABBITMQ_PREFETCH"); ok {
		if raw = strings.TrimSpace(raw); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				return Config{}, fmt.Errorf("RABBITMQ_PREFETCH %q: must be a base-10 integer", raw)
			}
			if n < 1 {
				return Config{}, fmt.Errorf("RABBITMQ_PREFETCH %q: must be at least 1", raw)
			}
			cfg.Prefetch = n
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// envURL reads RABBITMQ_URL, preserving the absent-versus-empty distinction:
// absent means the default broker, explicitly empty disables ingestion.
func envURL() string {
	raw, ok := os.LookupEnv("RABBITMQ_URL")
	if !ok {
		return defaultURL
	}
	return strings.TrimSpace(raw)
}

// getEnv returns the trimmed value of key, or fallback when the variable is
// absent or blank.
func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// validate reports the first misconfiguration. It is called by LoadConfig and
// by the consumer for programmatically constructed configs.
func (c Config) validate() error {
	if c.URL != "" {
		if _, err := amqp.ParseURI(c.URL); err != nil {
			return fmt.Errorf("RABBITMQ_URL: %w", redactURLError(err))
		}
	}

	switch {
	case c.Exchange == "":
		return errors.New("rabbitmq: exchange must not be empty")
	case c.RoutingKey == "":
		return errors.New("rabbitmq: routing key must not be empty")
	case c.Queue == "":
		return errors.New("rabbitmq: queue must not be empty")
	case c.Prefetch < 1:
		return fmt.Errorf("rabbitmq: prefetch must be at least 1, got %d", c.Prefetch)
	}

	if err := validateExchangeType("RABBITMQ_EXCHANGE_TYPE", c.ExchangeType); err != nil {
		return err
	}

	if c.OutputExchange == "" {
		if c.OutputExchangeType != "" {
			return validateExchangeType("RABBITMQ_OUTPUT_EXCHANGE_TYPE", c.OutputExchangeType)
		}
		return nil
	}

	if c.URL == "" {
		return errors.New("rabbitmq: RABBITMQ_OUTPUT_EXCHANGE requires RABBITMQ_URL")
	}
	if c.OutputRoutingKey == "" {
		return errors.New("rabbitmq: output routing key must not be empty")
	}
	if err := validateExchangeType("RABBITMQ_OUTPUT_EXCHANGE_TYPE", c.OutputExchangeType); err != nil {
		return err
	}
	if c.OutputExchange == c.Exchange && c.OutputRoutingKey == c.RoutingKey {
		return errors.New("rabbitmq: output exchange and routing key must not both match the input (self-feeding loop)")
	}
	return nil
}

// validateExchangeType accepts only the four AMQP exchange kinds.
func validateExchangeType(name, kind string) error {
	switch kind {
	case "direct", "topic", "fanout", "headers":
		return nil
	default:
		return fmt.Errorf("rabbitmq: %s must be one of direct, topic, fanout, headers, got %q", name, kind)
	}
}

// redactURLError drops the *url.Error wrapper from an AMQP URI parse error so
// the raw URI (which may embed a password) never reaches the returned error.
func redactURLError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

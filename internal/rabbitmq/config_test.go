package rabbitmq

import (
	"os"
	"strings"
	"testing"
)

// rabbitEnvKeys lists every environment variable LoadConfig reads. Tests clear
// all of them so a developer's shell cannot affect the defaults.
var rabbitEnvKeys = []string{
	"RABBITMQ_URL",
	"RABBITMQ_EXCHANGE",
	"RABBITMQ_EXCHANGE_TYPE",
	"RABBITMQ_ROUTING_KEY",
	"RABBITMQ_QUEUE",
	"RABBITMQ_PREFETCH",
	"RABBITMQ_OUTPUT_EXCHANGE",
	"RABBITMQ_OUTPUT_EXCHANGE_TYPE",
	"RABBITMQ_OUTPUT_ROUTING_KEY",
}

// clearRabbitEnv unsets every RabbitMQ variable and restores the original
// values when the test ends.
func clearRabbitEnv(t *testing.T) {
	t.Helper()
	for _, key := range rabbitEnvKeys {
		key := key
		old, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unsetenv(%s): %v", key, err)
		}
		t.Cleanup(func() {
			if ok {
				_ = os.Setenv(key, old)
				return
			}
			_ = os.Unsetenv(key)
		})
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	clearRabbitEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	want := Config{
		URL:                defaultURL,
		Exchange:           defaultExchange,
		ExchangeType:       defaultExchangeType,
		RoutingKey:         defaultRoutingKey,
		Queue:              defaultQueue,
		Prefetch:           defaultPrefetch,
		OutputExchange:     "",
		OutputExchangeType: defaultOutputExchangeType,
		OutputRoutingKey:   defaultOutputRoutingKey,
	}
	if cfg != want {
		t.Errorf("LoadConfig() = %+v, want %+v", cfg, want)
	}
	if !cfg.IngestionEnabled() {
		t.Error("IngestionEnabled() = false, want true with the default URL")
	}
	if cfg.OutputEnabled() {
		t.Error("OutputEnabled() = true, want false by default")
	}
}

func TestLoadConfig_URLAbsentVsExplicitlyEmpty(t *testing.T) {
	t.Run("absent uses the default", func(t *testing.T) {
		clearRabbitEnv(t)
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("LoadConfig() error = %v", err)
		}
		if cfg.URL != defaultURL {
			t.Errorf("URL = %q, want %q", cfg.URL, defaultURL)
		}
		if !cfg.IngestionEnabled() {
			t.Error("IngestionEnabled() = false, want true")
		}
	})

	t.Run("explicitly empty disables ingestion", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_URL", "")
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("LoadConfig() error = %v, want nil", err)
		}
		if cfg.URL != "" {
			t.Errorf("URL = %q, want empty", cfg.URL)
		}
		if cfg.IngestionEnabled() {
			t.Error("IngestionEnabled() = true, want false")
		}
	})

	t.Run("whitespace only disables ingestion", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_URL", "   ")
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("LoadConfig() error = %v, want nil", err)
		}
		if cfg.IngestionEnabled() {
			t.Error("IngestionEnabled() = true, want false")
		}
	})
}

func TestLoadConfig_TrimsValuesAndLowercasesExchangeTypes(t *testing.T) {
	clearRabbitEnv(t)
	t.Setenv("RABBITMQ_URL", "  amqp://guest:guest@localhost:5672/  ")
	t.Setenv("RABBITMQ_EXCHANGE", "  custom.exchange  ")
	t.Setenv("RABBITMQ_EXCHANGE_TYPE", "  Topic ")
	t.Setenv("RABBITMQ_ROUTING_KEY", "  custom.key  ")
	t.Setenv("RABBITMQ_QUEUE", "  custom.queue  ")
	t.Setenv("RABBITMQ_PREFETCH", " 7 ")
	t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", "  custom.out  ")
	t.Setenv("RABBITMQ_OUTPUT_EXCHANGE_TYPE", "  DIRECT ")
	t.Setenv("RABBITMQ_OUTPUT_ROUTING_KEY", "  custom.out.key  ")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.URL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("URL = %q", cfg.URL)
	}
	if cfg.Exchange != "custom.exchange" {
		t.Errorf("Exchange = %q", cfg.Exchange)
	}
	if cfg.ExchangeType != "topic" {
		t.Errorf("ExchangeType = %q, want topic", cfg.ExchangeType)
	}
	if cfg.RoutingKey != "custom.key" {
		t.Errorf("RoutingKey = %q", cfg.RoutingKey)
	}
	if cfg.Queue != "custom.queue" {
		t.Errorf("Queue = %q", cfg.Queue)
	}
	if cfg.Prefetch != 7 {
		t.Errorf("Prefetch = %d, want 7", cfg.Prefetch)
	}
	if cfg.OutputExchange != "custom.out" {
		t.Errorf("OutputExchange = %q", cfg.OutputExchange)
	}
	if cfg.OutputExchangeType != "direct" {
		t.Errorf("OutputExchangeType = %q, want direct", cfg.OutputExchangeType)
	}
	if cfg.OutputRoutingKey != "custom.out.key" {
		t.Errorf("OutputRoutingKey = %q", cfg.OutputRoutingKey)
	}
}

func TestLoadConfig_BlankValuesFallBackToDefaults(t *testing.T) {
	clearRabbitEnv(t)
	for _, key := range []string{
		"RABBITMQ_EXCHANGE",
		"RABBITMQ_EXCHANGE_TYPE",
		"RABBITMQ_ROUTING_KEY",
		"RABBITMQ_QUEUE",
		"RABBITMQ_PREFETCH",
		"RABBITMQ_OUTPUT_EXCHANGE_TYPE",
		"RABBITMQ_OUTPUT_ROUTING_KEY",
	} {
		t.Setenv(key, "   ")
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Exchange != defaultExchange {
		t.Errorf("Exchange = %q, want %q", cfg.Exchange, defaultExchange)
	}
	if cfg.ExchangeType != defaultExchangeType {
		t.Errorf("ExchangeType = %q, want %q", cfg.ExchangeType, defaultExchangeType)
	}
	if cfg.RoutingKey != defaultRoutingKey {
		t.Errorf("RoutingKey = %q, want %q", cfg.RoutingKey, defaultRoutingKey)
	}
	if cfg.Queue != defaultQueue {
		t.Errorf("Queue = %q, want %q", cfg.Queue, defaultQueue)
	}
	if cfg.Prefetch != defaultPrefetch {
		t.Errorf("Prefetch = %d, want %d", cfg.Prefetch, defaultPrefetch)
	}
	if cfg.OutputExchangeType != defaultOutputExchangeType {
		t.Errorf("OutputExchangeType = %q, want %q", cfg.OutputExchangeType, defaultOutputExchangeType)
	}
	if cfg.OutputRoutingKey != defaultOutputRoutingKey {
		t.Errorf("OutputRoutingKey = %q, want %q", cfg.OutputRoutingKey, defaultOutputRoutingKey)
	}
	if cfg.OutputEnabled() {
		t.Error("OutputEnabled() = true, want false for a blank output exchange")
	}
}

func TestLoadConfig_InvalidExchangeType(t *testing.T) {
	testCases := []struct {
		name string
		key  string
		val  string
		want string
	}{
		{"input type", "RABBITMQ_EXCHANGE_TYPE", "queue", "RABBITMQ_EXCHANGE_TYPE"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clearRabbitEnv(t)
			t.Setenv(tc.key, tc.val)
			_, err := LoadConfig()
			if err == nil {
				t.Fatal("LoadConfig() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("LoadConfig() error = %q, want it to mention %q", err.Error(), tc.want)
			}
		})
	}

	t.Run("output type", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", "out")
		t.Setenv("RABBITMQ_OUTPUT_EXCHANGE_TYPE", "exchange")
		_, err := LoadConfig()
		if err == nil {
			t.Fatal("LoadConfig() error = nil, want error")
		}
		if !strings.Contains(err.Error(), "RABBITMQ_OUTPUT_EXCHANGE_TYPE") {
			t.Errorf("LoadConfig() error = %q, want it to mention RABBITMQ_OUTPUT_EXCHANGE_TYPE", err.Error())
		}
	})
}

func TestLoadConfig_InvalidPrefetch(t *testing.T) {
	for _, raw := range []string{"abc", "0", "-1", "1.5", "999999999999999999999999"} {
		t.Run(raw, func(t *testing.T) {
			clearRabbitEnv(t)
			t.Setenv("RABBITMQ_PREFETCH", raw)
			_, err := LoadConfig()
			if err == nil {
				t.Fatalf("LoadConfig() error = nil for RABBITMQ_PREFETCH=%q, want error", raw)
			}
			if !strings.Contains(err.Error(), "RABBITMQ_PREFETCH") {
				t.Errorf("LoadConfig() error = %q, want it to mention RABBITMQ_PREFETCH", err.Error())
			}
		})
	}
}

func TestLoadConfig_OutputDisabledUnlessExplicitlySet(t *testing.T) {
	clearRabbitEnv(t)
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.OutputEnabled() {
		t.Error("OutputEnabled() = true, want false by default")
	}

	t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", "stock.created")
	cfg, err = LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !cfg.OutputEnabled() {
		t.Error("OutputEnabled() = false, want true with a non-empty RABBITMQ_OUTPUT_EXCHANGE")
	}
}

func TestLoadConfig_OutputRequiresURL(t *testing.T) {
	clearRabbitEnv(t)
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", "stock.created")
	_, err := LoadConfig()
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "RABBITMQ_URL") {
		t.Errorf("LoadConfig() error = %q, want it to mention RABBITMQ_URL", err.Error())
	}
}

func TestLoadConfig_RejectsOutputSelfLoop(t *testing.T) {
	t.Run("same exchange and routing key", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", defaultExchange)
		t.Setenv("RABBITMQ_OUTPUT_ROUTING_KEY", defaultRoutingKey)
		_, err := LoadConfig()
		if err == nil {
			t.Fatal("LoadConfig() error = nil, want self-loop error")
		}
		if !strings.Contains(err.Error(), "loop") {
			t.Errorf("LoadConfig() error = %q, want it to mention the loop", err.Error())
		}
	})

	t.Run("same exchange different routing key", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", defaultExchange)
		t.Setenv("RABBITMQ_OUTPUT_ROUTING_KEY", "stock.created.v1")
		if _, err := LoadConfig(); err != nil {
			t.Errorf("LoadConfig() error = %v, want nil for a distinct routing key", err)
		}
	})

	t.Run("different exchange same routing key", func(t *testing.T) {
		clearRabbitEnv(t)
		t.Setenv("RABBITMQ_OUTPUT_EXCHANGE", "stock.created")
		t.Setenv("RABBITMQ_OUTPUT_ROUTING_KEY", defaultRoutingKey)
		if _, err := LoadConfig(); err != nil {
			t.Errorf("LoadConfig() error = %v, want nil for a distinct exchange", err)
		}
	})
}

func TestLoadConfig_URLParseErrorRedactsPassword(t *testing.T) {
	clearRabbitEnv(t)
	t.Setenv("RABBITMQ_URL", "amqp://user:s3cret-password@host:5672/%zz")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("LoadConfig() error = nil, want a URL parse error")
	}
	msg := err.Error()
	if strings.Contains(msg, "s3cret-password") {
		t.Errorf("LoadConfig() error leaked the password: %q", msg)
	}
	if !strings.Contains(msg, "RABBITMQ_URL") {
		t.Errorf("LoadConfig() error = %q, want it to mention RABBITMQ_URL", msg)
	}
}

func TestConfig_Validate(t *testing.T) {
	valid := Config{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     defaultExchange,
		ExchangeType: defaultExchangeType,
		RoutingKey:   defaultRoutingKey,
		Queue:        defaultQueue,
		Prefetch:     1,
	}

	testCases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid ingestion only", func(*Config) {}, ""},
		{"empty exchange", func(c *Config) { c.Exchange = "" }, "exchange"},
		{"empty routing key", func(c *Config) { c.RoutingKey = "" }, "routing key"},
		{"empty queue", func(c *Config) { c.Queue = "" }, "queue"},
		{"zero prefetch", func(c *Config) { c.Prefetch = 0 }, "prefetch"},
		{"negative prefetch", func(c *Config) { c.Prefetch = -1 }, "prefetch"},
		{"bad exchange type", func(c *Config) { c.ExchangeType = "queue" }, "RABBITMQ_EXCHANGE_TYPE"},
		{"bad url", func(c *Config) { c.URL = "http://guest:guest@localhost/" }, "RABBITMQ_URL"},
		{
			"output without url",
			func(c *Config) { c.URL = ""; c.OutputExchange = "out" },
			"RABBITMQ_URL",
		},
		{
			"output without routing key",
			func(c *Config) { c.OutputExchange = "out"; c.OutputRoutingKey = "" },
			"output routing key",
		},
		{
			"output with bad type",
			func(c *Config) {
				c.OutputExchange = "out"
				c.OutputExchangeType = "queue"
				c.OutputRoutingKey = "out.key"
			},
			"RABBITMQ_OUTPUT_EXCHANGE_TYPE",
		},
		{
			"output self loop",
			func(c *Config) {
				c.OutputExchange = c.Exchange
				c.OutputExchangeType = c.ExchangeType
				c.OutputRoutingKey = c.RoutingKey
			},
			"loop",
		},
		{
			"valid output",
			func(c *Config) {
				c.OutputExchange = "stock.created"
				c.OutputExchangeType = "topic"
				c.OutputRoutingKey = "stock.created.v1"
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			err := cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate() error = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("validate() error = %q, want it to mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

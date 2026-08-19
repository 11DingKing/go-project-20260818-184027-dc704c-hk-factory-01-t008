package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all runtime configuration for the registration dispatch service.
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Storage    StorageConfig    `yaml:"storage"`
	Dispatch   DispatchConfig   `yaml:"dispatch"`
	Compaction CompactionConfig `yaml:"compaction"`
	Upstream   UpstreamConfig   `yaml:"upstream"`
	Logging    LoggingConfig    `yaml:"logging"`
}

type ServerConfig struct {
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type StorageConfig struct {
	DataDir string `yaml:"data_dir"`
	DBPath  string `yaml:"db_path"`
}

type DispatchConfig struct {
	AckTimeout     time.Duration `yaml:"ack_timeout"`
	MaxRetries     int           `yaml:"max_retries"`
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	RetryMaxDelay  time.Duration `yaml:"retry_max_delay"`
}

type CompactionConfig struct {
	Interval     time.Duration `yaml:"interval"`
	RetainEvents int           `yaml:"retain_events"`
}

type UpstreamConfig struct {
	MockURL            string        `yaml:"mock_url"`
	Timeout            time.Duration `yaml:"timeout"`
	BreakerThreshold   uint32        `yaml:"breaker_threshold"`
	BreakerTimeout     time.Duration `yaml:"breaker_timeout"`
	BreakerHalfOpenMax uint32        `yaml:"breaker_half_open_max"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns a config with sensible production defaults.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Port:            48443,
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    60 * time.Second,
			ShutdownTimeout: 15 * time.Second,
		},
		Storage: StorageConfig{
			DataDir: "./data",
			DBPath:  "./data/regdispatch.db",
		},
		Dispatch: DispatchConfig{
			AckTimeout:     60 * time.Second,
			MaxRetries:     5,
			RetryBaseDelay: 2 * time.Second,
			RetryMaxDelay:  30 * time.Second,
		},
		Compaction: CompactionConfig{
			Interval:     5 * time.Minute,
			RetainEvents: 10000,
		},
		Upstream: UpstreamConfig{
			MockURL:            "http://127.0.0.1:48444",
			Timeout:            10 * time.Second,
			BreakerThreshold:   5,
			BreakerTimeout:     30 * time.Second,
			BreakerHalfOpenMax: 3,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "console",
		},
	}
}

// Load reads a YAML config file and then applies environment variable overrides.
// Environment variables use the REGDISPATCH_ prefix and uppercase field names
// with underscores, e.g. REGDISPATCH_PORT overrides server.port.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("read config file %s: %w", path, err)
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err)
			}
		}
	}
	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyEnv(cfg *Config) error {
	if v := os.Getenv("REGDISPATCH_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid REGDISPATCH_PORT: %w", err)
		}
		cfg.Server.Port = port
	}
	if v := os.Getenv("REGDISPATCH_DATA_DIR"); v != "" {
		cfg.Storage.DataDir = v
	}
	if v := os.Getenv("REGDISPATCH_DB_PATH"); v != "" {
		cfg.Storage.DBPath = v
	}
	if v := os.Getenv("REGDISPATCH_LOG_LEVEL"); v != "" {
		cfg.Logging.Level = strings.ToLower(v)
	}
	if v := os.Getenv("REGDISPATCH_DISPATCH_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid REGDISPATCH_DISPATCH_TIMEOUT: %w", err)
		}
		cfg.Dispatch.AckTimeout = d
	}
	if v := os.Getenv("REGDISPATCH_MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid REGDISPATCH_MAX_RETRIES: %w", err)
		}
		cfg.Dispatch.MaxRetries = n
	}
	if v := os.Getenv("REGDISPATCH_RETRY_BASE_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid REGDISPATCH_RETRY_BASE_DELAY: %w", err)
		}
		cfg.Dispatch.RetryBaseDelay = d
	}
	if v := os.Getenv("REGDISPATCH_MOCK_UPSTREAM_PORT"); v != "" {
		// Used by the mock upstream binary; no effect on main config.
	}
	return nil
}

// Validate checks that the configuration is internally consistent.
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}
	if c.Storage.DBPath == "" {
		return fmt.Errorf("storage db_path is required")
	}
	if c.Dispatch.MaxRetries < 1 {
		return fmt.Errorf("max_retries must be at least 1")
	}
	if c.Dispatch.RetryBaseDelay <= 0 {
		return fmt.Errorf("retry_base_delay must be positive")
	}
	if c.Dispatch.RetryMaxDelay < c.Dispatch.RetryBaseDelay {
		return fmt.Errorf("retry_max_delay must be >= retry_base_delay")
	}
	if c.Upstream.BreakerThreshold == 0 {
		return fmt.Errorf("breaker_threshold must be positive")
	}
	return nil
}

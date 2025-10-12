package config

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for the sensorium application
type Config struct {
	// Global settings
	Roles     []string `mapstructure:"roles"`
	PulsarURL string   `mapstructure:"pulsar_url"`
	SubPrefix string   `mapstructure:"sub_prefix"`
	LogLevel  string   `mapstructure:"log_level"`

	// Role-specific configurations
	Agent    AgentConfig    `mapstructure:"agent"`
	Detector DetectorConfig `mapstructure:"detector"`
	Alerter  AlerterConfig  `mapstructure:"alerter"`
	UI       UIConfig       `mapstructure:"ui"`
}

type AgentConfig struct {
	NodeID       string        `mapstructure:"node_id"`
	Topic        string        `mapstructure:"topic"`
	SamplePeriod time.Duration `mapstructure:"sample_period"`
}

type DetectorConfig struct {
	SubName  string  `mapstructure:"sub_name"`
	InTopic  string  `mapstructure:"in_topic"`
	OutTopic string  `mapstructure:"out_topic"`
	CPUWarn  float32 `mapstructure:"cpu_warn"`
	CPUCrit  float32 `mapstructure:"cpu_crit"`
	MemWarn  float32 `mapstructure:"mem_warn"`
	MemCrit  float32 `mapstructure:"mem_crit"`
}

type AlerterConfig struct {
	SubName  string        `mapstructure:"sub_name"`
	InTopic  string        `mapstructure:"in_topic"`
	OutTopic string        `mapstructure:"out_topic"`
	Cooldown time.Duration `mapstructure:"cooldown"`
}

type UIConfig struct {
	HTTPAddr     string `mapstructure:"http_addr"`
	AlertsTopic  string `mapstructure:"alerts_topic"`
	MetricsTopic string `mapstructure:"metrics_topic"`
	SubPrefix    string `mapstructure:"sub_prefix"`
}

// setupFlags defines command-line flags using pflag
func setupFlags() {
	pflag.StringSlice("roles", []string{"ui"}, "Comma-separated list of roles to run")
	pflag.String("pulsar-url", "pulsar://localhost:6650", "Pulsar broker URL")
	pflag.String("sub-prefix", "sensorium", "Subscription prefix")
	pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	pflag.String("config", "", "Config file path")

	// Agent flags
	pflag.String("agent.node-id", "", "Node ID for agent role")
	pflag.Duration("agent.sample-period", 2*time.Second, "Sample period for metrics collection")

	// UI flags
	pflag.String("ui.http-addr", ":8088", "HTTP address for UI server")

	// Alerter flags
	pflag.Duration("alerter.cooldown", 10*time.Minute, "Cooldown period between alerts")

	// Detector flags
	pflag.Float32("detector.cpu-warn", 85.0, "CPU warning threshold")
	pflag.Float32("detector.cpu-crit", 92.0, "CPU critical threshold")
	pflag.Float32("detector.mem-warn", 0.90, "Memory warning threshold")
	pflag.Float32("detector.mem-crit", 0.95, "Memory critical threshold")
}

// initViper sets up viper with defaults, env bindings, and config file support
func initViper() *viper.Viper {
	v := viper.New()

	// Set config name and paths (for auto-discovery)
	v.SetConfigName("sensorium")
	v.SetConfigType("yaml")
	v.AddConfigPath("/etc/sensorium/")
	v.AddConfigPath("$HOME/.sensorium")
	v.AddConfigPath(".")

	// Set defaults
	setDefaults(v)

	// Enable environment variables with automatic binding
	v.SetEnvPrefix("SENSORIUM")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	// Bind specific environment variables with custom names for backwards compatibility
	v.BindEnv("pulsar_url", "PULSAR_URL")
	v.BindEnv("sub_prefix", "SUB_PREFIX")
	v.BindEnv("log_level", "LOG_LEVEL")
	v.BindEnv("agent.node_id", "NODE_ID")
	v.BindEnv("agent.sample_period", "SAMPLE_MS")
	v.BindEnv("alerter.cooldown", "ALERT_COOLDOWN")
	v.BindEnv("ui.http_addr", "HTTP_ADDR")

	return v
}

func setDefaults(v *viper.Viper) {
	// Global defaults
	v.SetDefault("roles", []string{"ui"})
	v.SetDefault("pulsar_url", "pulsar://localhost:6650")
	v.SetDefault("sub_prefix", "sensorium")
	v.SetDefault("log_level", "info")

	// Agent defaults
	v.SetDefault("agent.node_id", getHostname())
	v.SetDefault("agent.topic", "sensor.node.metrics")
	v.SetDefault("agent.sample_period", "2s")

	// Detector defaults
	v.SetDefault("detector.sub_name", "sensorium-detector")
	v.SetDefault("detector.in_topic", "sensor.node.metrics")
	v.SetDefault("detector.out_topic", "sensor.node.health")
	v.SetDefault("detector.cpu_warn", 85.0)
	v.SetDefault("detector.cpu_crit", 92.0)
	v.SetDefault("detector.mem_warn", 0.90)
	v.SetDefault("detector.mem_crit", 0.95)

	// Alerter defaults
	v.SetDefault("alerter.sub_name", "sensorium-alerter")
	v.SetDefault("alerter.in_topic", "sensor.node.health")
	v.SetDefault("alerter.out_topic", "sensor.alerts")
	v.SetDefault("alerter.cooldown", "10m")

	// UI defaults
	v.SetDefault("ui.http_addr", ":8088")
	v.SetDefault("ui.alerts_topic", "sensor.alerts")
	v.SetDefault("ui.metrics_topic", "sensor.node.metrics")
	v.SetDefault("ui.sub_prefix", "sensorium")
}

// Load reads configuration from file, env, and flags
func Load() (*Config, error) {
	// Setup command-line flags
	setupFlags()
	pflag.Parse()

	// Initialize viper
	v := initViper()

	// Bind command-line flags to viper
	if err := v.BindPFlags(pflag.CommandLine); err != nil {
		return nil, fmt.Errorf("failed to bind flags: %w", err)
	}

	// Check if a specific config file was provided
	if configFile := v.GetString("config"); configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
		}
	} else {
		// Try to read config file from standard locations (optional)
		if err := v.ReadInConfig(); err != nil {
			// It's ok if config file doesn't exist
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("error reading config file: %w", err)
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Handle SAMPLE_MS environment variable for backwards compatibility
	if sampleMs := os.Getenv("SAMPLE_MS"); sampleMs != "" {
		if ms, err := time.ParseDuration(sampleMs + "ms"); err == nil {
			cfg.Agent.SamplePeriod = ms
		}
	}

	// Update subscription names based on sub_prefix
	if cfg.SubPrefix != "" {
		cfg.Detector.SubName = cfg.SubPrefix + "-detector"
		cfg.Alerter.SubName = cfg.SubPrefix + "-alerter"
		cfg.UI.SubPrefix = cfg.SubPrefix
	}

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if len(c.Roles) == 0 {
		return fmt.Errorf("no roles specified")
	}

	for _, role := range c.Roles {
		switch role {
		case "agent", "detector", "alerter", "ui":
			// valid roles
		default:
			return fmt.Errorf("unknown role: %q", role)
		}
	}

	if c.PulsarURL == "" {
		return fmt.Errorf("pulsar URL is required")
	}

	return nil
}

// HasRole checks if the config includes a specific role
func (c *Config) HasRole(role string) bool {
	return slices.Contains(c.Roles, role)
}

func getHostname() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node-1"
	}
	return hostname
}
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

type Config struct {
	// Global Configs
	Roles     []string  `mapstructure:"roles"`
	PulsarURL string    `mapstructure:"pulsar_url"`
	SubPrefix string    `mapstructure:"sub_prefix"`
	LogLevel  string    `mapstructure:"log_level"`
	TLS       TLSConfig `mapstructure:"tls"`

	// Role Configs - these are getting passed to each role
	Agent    AgentConfig    `mapstructure:"agent"`
	Detector DetectorConfig `mapstructure:"detector"`
	Alerter  AlerterConfig  `mapstructure:"alerter"`
	UI       UIConfig       `mapstructure:"ui"`
}

type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	TrustCertsFilePath string `mapstructure:"trust_certs_file"`  // Broker CA ~ tls
	CertFilePath       string `mapstructure:"cert_file"`         // Client cert ~ mtls
	KeyFilePath        string `mapstructure:"key_file"`          // Client private key ~ mtls
	AllowInsecureConn  bool   `mapstructure:"allow_insecure"`    // Skip verify
	ServerName         string `mapstructure:"server_name"`       // Expected CN/SAN
	ValidateHostname   bool   `mapstructure:"validate_hostname"` // SNI verify
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

// For CLI args
func setupFlags() {
	pflag.StringSlice("roles", []string{"ui"}, "Comma-separated list of roles to run")
	pflag.String("pulsar-url", "pulsar://localhost:6650", "Pulsar broker URL")
	pflag.String("sub-prefix", "sensorium", "Subscription prefix")
	pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	pflag.String("config", "", "Config file path")

	// Agent
	pflag.String("agent.node-id", "", "Node ID for agent role")
	pflag.Duration("agent.sample-period", 2*time.Second, "Sample period for metrics collection")

	// UI
	pflag.String("ui.http-addr", ":8088", "HTTP address for UI server")

	// Alerter
	pflag.Duration("alerter.cooldown", 10*time.Minute, "Cooldown period between alerts")

	// Detector
	pflag.Float32("detector.cpu-warn", 85.0, "CPU warning threshold")
	pflag.Float32("detector.cpu-crit", 92.0, "CPU critical threshold")
	pflag.Float32("detector.mem-warn", 0.90, "Memory warning threshold")
	pflag.Float32("detector.mem-crit", 0.95, "Memory critical threshold")

	// TLS
	pflag.Bool("tls.enabled", false, "Enable TLS for Pulsar connections")
	pflag.String("tls.trust-certs-file", "", "Path to CA certificate file")
	pflag.String("tls.cert-file", "", "Path to client certificate file (for mTLS)")
	pflag.String("tls.key-file", "", "Path to client private key file (for mTLS)")
	pflag.Bool("tls.allow-insecure", false, "Allow insecure TLS connections (dev only)")
	pflag.String("tls.server-name", "", "Expected server name in certificate")
	pflag.Bool("tls.validate-hostname", false, "Validate server hostname")
}

func initViper() *viper.Viper {
	v := viper.New()

	// Set config file
	v.SetConfigName("sensorium")
	v.SetConfigType("yaml")
	v.AddConfigPath("/etc/sensorium/")
	v.AddConfigPath("$HOME/.sensorium")
	v.AddConfigPath(".")

	// Set defaults
	setDefaults(v)

	// Auto bind env
	v.SetEnvPrefix("SENSORIUM")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

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
	v.SetDefault("agent.sample_period", "10s")

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

	// TLS defaults
	v.SetDefault("tls.enabled", false)
	v.SetDefault("tls.trust_certs_file", "")
	v.SetDefault("tls.cert_file", "")
	v.SetDefault("tls.key_file", "")
	v.SetDefault("tls.allow_insecure", false)
	v.SetDefault("tls.server_name", "")
	v.SetDefault("tls.validate_hostname", false)
}

func Load() (*Config, error) {
	setupFlags()
	pflag.Parse()
	v := initViper()

	// Bind CLI
	if err := v.BindPFlags(pflag.CommandLine); err != nil {
		return nil, fmt.Errorf("failed to bind flags: %w", err)
	}

	if err := v.ReadInConfig(); err != nil {
		if v.GetString("config") == "" { // It's ok if config file doesn't exist
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("error reading config file: %w", err)
			}
		} else { // It's NOT ok if config file doesn't exist
			return nil, fmt.Errorf("failed to read config file %s: %w", v.GetString("config"), err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Update sub names
	if cfg.SubPrefix != "" {
		cfg.Detector.SubName = cfg.SubPrefix + "-detector"
		cfg.Alerter.SubName = cfg.SubPrefix + "-alerter"
		cfg.UI.SubPrefix = cfg.SubPrefix
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

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

	if c.Agent.SamplePeriod < (2 * time.Second) {
		return fmt.Errorf("agent sampling interval min is 2 seconds")
	}

	return nil
}

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

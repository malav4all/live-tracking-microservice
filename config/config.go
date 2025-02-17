package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// Config structure for holding configuration values
type Config struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Group   string   `yaml:"group"`
	} `yaml:"kafka"`
}

// LoadConfig loads the configuration from a YAML file
func LoadConfig(configFile string) (*Config, error) {
	config := &Config{}
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config data: %w", err)
	}
	return config, nil
}

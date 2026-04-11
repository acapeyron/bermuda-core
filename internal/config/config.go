// internal/config/config.go
package config

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Exchanges []ExchangeConfig `yaml:"exchanges"`
}

type ExchangeConfig struct {
	Name            string       `yaml:"name"`
	BaseSnapshotURL string       `yaml:"base_snapshot_url"`
	BaseWSURL       string       `yaml:"base_ws_url"`
	Pairs           []PairConfig `yaml:"pairs"`
}

type PairConfig struct {
	Symbol      string `yaml:"symbol"`
	SnapshotURL string // computed
	WSURL       string // computed
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	// Build URLs from base_url + symbol
	for i, exchange := range cfg.Exchanges {
		for j, pair := range exchange.Pairs {
			symbol := strings.ToLower(pair.Symbol)
			cfg.Exchanges[i].Pairs[j].SnapshotURL = strings.ReplaceAll(exchange.BaseSnapshotURL, "{symbol}", symbol)
			cfg.Exchanges[i].Pairs[j].WSURL = strings.ReplaceAll(exchange.BaseWSURL, "{symbol}", symbol)
		}
	}

	return &cfg, nil
}

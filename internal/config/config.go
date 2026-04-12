package config

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Exchange ExchangeConfig `yaml:"exchange"`
}

type ExchangeConfig struct {
	Name            string       `yaml:"name"`
	BaseSnapshotURL string       `yaml:"base_snapshot_url"`
	BaseWSURL       string       `yaml:"base_ws_url"`
	Pairs           []PairConfig `yaml:"pairs"`
}

type PairConfig struct {
	Symbol      string `yaml:"symbol"`
	SnapshotURL string // computed: REST endpoint for initial snapshot
	WSStream    string // computed: e.g. "btcusdt@depth"
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
	for j, pair := range cfg.Exchange.Pairs {
		symbol := strings.ToLower(pair.Symbol)
		cfg.Exchange.Pairs[j].SnapshotURL = strings.ReplaceAll(cfg.Exchange.BaseSnapshotURL, "{symbol}", symbol)
		cfg.Exchange.Pairs[j].WSStream = symbol + "@depth"
	}

	return &cfg, nil
}

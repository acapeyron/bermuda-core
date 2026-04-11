package registry

import (
	"fmt"

	"github.com/acapeyron/bermuda-core/adapters/binance"
	"github.com/acapeyron/bermuda-core/internal/market"
)

var registry = map[string]func() market.Parser{
	"binance": func() market.Parser { return &binance.BinanceParser{} },
}

func NewParser(name string) (market.Parser, error) {
	fn, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown parser: %s", name)
	}
	return fn(), nil
}

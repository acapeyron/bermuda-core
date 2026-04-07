package storage

import (
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

// Storage interface
type Storage interface {
	Run(trades <-chan market.Trade, obs <-chan market.OrderBookUpdate)
}

// Implémentation minimaliste (in-memory)
type InMemoryStorage struct {
	Orders []market.OrderBookUpdate
	Trades []market.Trade
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		Orders: []market.OrderBookUpdate{},
		Trades: []market.Trade{},
	}
}

// Run consumes channels
func (s *InMemoryStorage) Run(trades <-chan market.Trade, obs <-chan market.OrderBookUpdate) {
	for {
		select {
		case t := <-trades:
			s.Trades = append(s.Trades, t)
			logger.Info("Trade saved: %s %f @ %f", t.Pair, t.Size, t.Price)
		case ob := <-obs:
			s.Orders = append(s.Orders, ob)
			logger.Info("OrderBook saved: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		}
	}
}

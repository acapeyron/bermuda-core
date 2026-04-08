package storage

import (
	"context"
	"sync"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

// Storage interface
type Storage interface {
	Run(ctx context.Context, trades <-chan market.Trade, obs <-chan market.OrderBookUpdate)
}

// Implémentation minimaliste (in-memory)
type InMemoryStorage struct {
	Orders []market.OrderBookUpdate
	Trades []market.Trade
	mu     sync.RWMutex
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		Orders: []market.OrderBookUpdate{},
		Trades: []market.Trade{},
	}
}

// Run consumes channels
func (s *InMemoryStorage) Run(ctx context.Context, trades <-chan market.Trade, obs <-chan market.OrderBookUpdate) {
	for trades != nil || obs != nil {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-trades:
			if !ok {
				trades = nil
				continue
			}
			s.mu.Lock()
			s.Trades = append(s.Trades, t)
			s.mu.Unlock()
			logger.Info("Trade saved: %s %f @ %f", t.Pair, t.Size, t.Price)
		case ob, ok := <-obs:
			if !ok {
				trades = nil
				continue
			}
			s.mu.Lock()
			s.Orders = append(s.Orders, ob)
			s.mu.Unlock()
			logger.Info("OrderBook saved: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		}
	}
}

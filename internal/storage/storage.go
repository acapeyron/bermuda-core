package storage

import (
	"context"
	"sync"

	"github.com/acapeyron/bermuda-core/internal/market"
)

// Storage interface
type Storage interface {
	Run(ctx context.Context, obs <-chan market.OrderBookUpdate)
}

// Implémentation minimaliste (in-memory)
type InMemoryStorage struct {
	Orders []market.OrderBookUpdate
	mu     sync.RWMutex
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		Orders: []market.OrderBookUpdate{},
	}
}

// Run consumes channels
func (s *InMemoryStorage) Run(ctx context.Context, obs <-chan market.OrderBookUpdate) {
	for obs != nil {
		select {
		case <-ctx.Done():
			return
		case ob, ok := <-obs:
			if !ok {
				obs = nil
				continue
			}
			s.mu.Lock()
			s.Orders = append(s.Orders, ob)
			s.mu.Unlock()
			// logger.Info("OrderBook saved: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		}
	}
}

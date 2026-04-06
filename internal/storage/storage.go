package storage

import (
	"sync"

	"github.com/acapeyron/bermuda-core/internal/market"
)

// Interface abstraite
type Storage interface {
	SaveOrderBook(ob market.OrderBookUpdate)
	SaveTrade(trade market.Trade)
}

// Implémentation minimaliste (in-memory)
type InMemoryStorage struct {
	mu     sync.Mutex
	Orders []market.OrderBookUpdate
	Trades []market.Trade
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		Orders: []market.OrderBookUpdate{},
		Trades: []market.Trade{},
	}
}

func (s *InMemoryStorage) SaveOrderBook(ob market.OrderBookUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Orders = append(s.Orders, ob)
}

func (s *InMemoryStorage) SaveTrade(trade market.Trade) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Trades = append(s.Trades, trade)
}

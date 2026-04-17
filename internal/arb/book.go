package arb

import "github.com/acapeyron/bermuda-core/internal/market"

type orderBook struct {
	bids map[float64]float64 // price → size
	asks map[float64]float64
}

func newOrderBook() *orderBook {
	return &orderBook{
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
}

func (b *orderBook) applyUpdate(ob *market.OrderBookUpdate) {
	for _, lvl := range ob.Bids {
		if lvl.Size == 0 {
			delete(b.bids, lvl.Price)
		} else {
			b.bids[lvl.Price] = lvl.Size
		}
	}
	for _, lvl := range ob.Asks {
		if lvl.Size == 0 {
			delete(b.asks, lvl.Price)
		} else {
			b.asks[lvl.Price] = lvl.Size
		}
	}
}

func (b *orderBook) bestBid() float64 {
	best := 0.0
	for p := range b.bids {
		if p > best {
			best = p
		}
	}
	return best
}

func (b *orderBook) bestAsk() float64 {
	best := 0.0
	for p := range b.asks {
		if best == 0 || p < best {
			best = p
		}
	}
	return best
}

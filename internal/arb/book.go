package arb

import (
	"sort"
	"sync"

	"github.com/acapeyron/bermuda-core/internal/market"
)

// LiquidityResult is returned by bestAskForSize / bestBidForSize.
type LiquidityResult struct {
	AvgPrice         float64 // volume-weighted average fill price
	HasFullLiquidity bool    // false if the book didn't have enough size
}

type orderBook struct {
	bids       map[float64]float64 // price → size
	asks       map[float64]float64
	sortedBids []float64 // desc: highest first
	sortedAsks []float64 // asc:  lowest first
	dirty      bool
	mu         sync.RWMutex
}

func newOrderBook() *orderBook {
	return &orderBook{
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
}

func (b *orderBook) applyUpdate(ob *market.OrderBookUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()

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
	b.dirty = true
}

// rebuild sorts bid/ask price levels when the book has changed.
func (b *orderBook) rebuild() {
	if !b.dirty {
		return
	}

	b.sortedBids = b.sortedBids[:0]
	for p := range b.bids {
		b.sortedBids = append(b.sortedBids, p)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(b.sortedBids))) // desc

	b.sortedAsks = b.sortedAsks[:0]
	for p := range b.asks {
		b.sortedAsks = append(b.sortedAsks, p)
	}
	sort.Float64s(b.sortedAsks) // asc

	b.dirty = false
}

// bestAskForSize walks the ask side and returns the VWAP fill price for
// the given notional in quote currency (e.g. 100 USDT).
// HasFullLiquidity is false if the book ran out of size before filling.
func (b *orderBook) bestAskForSize(quoteSize float64) LiquidityResult {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.rebuild()

	remaining := quoteSize
	totalBase := 0.0

	for _, price := range b.sortedAsks {
		size := b.asks[price]     // size in base currency
		available := size * price // convert to quote currency
		if available >= remaining {
			totalBase += remaining / price
			remaining = 0
			break
		}
		totalBase += size
		remaining -= available
	}

	if totalBase == 0 {
		return LiquidityResult{}
	}

	return LiquidityResult{
		AvgPrice:         quoteSize / totalBase,
		HasFullLiquidity: remaining == 0,
	}
}

// bestBidForSize walks the bid side and returns the VWAP fill price for
// the given notional in quote currency (e.g. 100 USDT).
// HasFullLiquidity is false if the book ran out of size before filling.
func (b *orderBook) bestBidForSize(quoteSize float64) LiquidityResult {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.rebuild()

	remaining := quoteSize
	totalBase := 0.0

	for _, price := range b.sortedBids {
		size := b.bids[price]     // size in base currency
		available := size * price // convert to quote currency
		if available >= remaining {
			totalBase += remaining / price
			remaining = 0
			break
		}
		totalBase += size
		remaining -= available
	}

	if totalBase == 0 {
		return LiquidityResult{}
	}

	return LiquidityResult{
		AvgPrice:         quoteSize / totalBase,
		HasFullLiquidity: remaining == 0,
	}
}

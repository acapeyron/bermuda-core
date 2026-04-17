package arb

import (
	"sync"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

type Leg struct {
	Pair string
	Side string // "buy" or "sell"
}

type Opportunity struct {
	Legs      [3]Leg
	EntryRate float64 // effective multiplier after fees (>1 = profitable)
	ProfitPct float64 // e.g. 0.12 means +0.12%
}

type bestQuote struct {
	BestBid float64
	BestAsk float64
}

// TriangleDetector watches BTCUSDT / ETHUSDT / ETHBTC.
//
// Cycle A (forward):  USDT → ETH → BTC → USDT
//
//	buy  ETHUSDT (pay ask)
//	sell ETHBTC  (receive bid)
//	sell BTCUSDT (receive bid)
//
// Cycle B (reverse):  USDT → BTC → ETH → USDT
//
//	buy  BTCUSDT (pay ask)
//	buy  ETHBTC  (pay ask)
//	sell ETHUSDT (receive bid)
type TriangleDetector struct {
	fee    float64
	mu     sync.RWMutex
	quotes map[string]*bestQuote
	OpChan chan Opportunity
}

func NewTriangleDetector(fee float64) *TriangleDetector {
	return &TriangleDetector{
		fee: fee,
		quotes: map[string]*bestQuote{
			"BTCUSDT": {},
			"ETHUSDT": {},
			"ETHBTC":  {},
		},
		OpChan: make(chan Opportunity, 1024),
	}
}

func (d *TriangleDetector) UpdateOrderBook(ob *market.OrderBookUpdate) {
	d.mu.Lock()
	q, ok := d.quotes[ob.Pair]
	if !ok {
		d.mu.Unlock()
		return
	}
	if len(ob.Bids) > 0 {
		q.BestBid = ob.Bids[0].Price
	}
	if len(ob.Asks) > 0 {
		q.BestAsk = ob.Asks[0].Price
	}
	d.mu.Unlock()

	d.evaluate()
}

func (d *TriangleDetector) evaluate() {
	d.mu.RLock()
	btc := *d.quotes["BTCUSDT"]
	eth := *d.quotes["ETHUSDT"]
	ebt := *d.quotes["ETHBTC"]
	d.mu.RUnlock()

	if btc.BestBid == 0 || btc.BestAsk == 0 ||
		eth.BestBid == 0 || eth.BestAsk == 0 ||
		ebt.BestBid == 0 || ebt.BestAsk == 0 {
		return
	}

	keep := 1.0 - d.fee

	// Cycle A: USDT → ETH → BTC → USDT
	rateA := (1.0 / eth.BestAsk) * ebt.BestBid * btc.BestBid * keep * keep * keep
	if rateA > 1.0 {
		pct := (rateA - 1.0) * 100
		logger.Info("[DETECTOR] Cycle A  USDT→ETH→BTC→USDT  profit=+%.4f%%  rate=%.8f", pct, rateA)
		d.emit(Opportunity{
			Legs:      [3]Leg{{"ETHUSDT", "buy"}, {"ETHBTC", "sell"}, {"BTCUSDT", "sell"}},
			EntryRate: rateA,
			ProfitPct: pct,
		})
	}

	// Cycle B: USDT → BTC → ETH → USDT
	rateB := (1.0 / btc.BestAsk) * (1.0 / ebt.BestAsk) * eth.BestBid * keep * keep * keep
	if rateB > 1.0 {
		pct := (rateB - 1.0) * 100
		logger.Info("[DETECTOR] Cycle B  USDT→BTC→ETH→USDT  profit=+%.4f%%  rate=%.8f", pct, rateB)
		d.emit(Opportunity{
			Legs:      [3]Leg{{"BTCUSDT", "buy"}, {"ETHBTC", "buy"}, {"ETHUSDT", "sell"}},
			EntryRate: rateB,
			ProfitPct: pct,
		})
	}
}

func (d *TriangleDetector) emit(op Opportunity) {
	select {
	case d.OpChan <- op:
	default:
		logger.Warn("[DETECTOR] OpChan full, dropping opportunity (profit=+%.4f%%)", op.ProfitPct)
	}
}

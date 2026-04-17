package arb

import (
	"sync"
	"time"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

type Leg struct {
	Pair string
	Side string // "buy" or "sell"
}

type Opportunity struct {
	Legs      [3]Leg
	EntryRate float64
	ProfitPct float64
}

type TriangleDetector struct {
	fee       float64
	mu        sync.RWMutex
	books     map[string]*orderBook
	triangles []Triangle
	OpChan    chan Opportunity
	cooldown  time.Duration
}

func NewTriangleDetector(fee float64, pairs []string) *TriangleDetector {
	triangles := GenerateTriangles(pairs)
	logger.Info("[DETECTOR] Generated %d triangles:", len(triangles))
	for _, t := range triangles {
		logger.Info("  %s  legs=%v", t.Name, t.Legs)
	}

	books := make(map[string]*orderBook)
	for _, pair := range pairs {
		books[pair] = newOrderBook()
	}

	return &TriangleDetector{
		fee:       fee,
		books:     books,
		triangles: triangles,
		OpChan:    make(chan Opportunity, 1024),
		cooldown:  5 * time.Second,
	}
}

func (d *TriangleDetector) UpdateOrderBook(ob *market.OrderBookUpdate) {
	d.mu.Lock()
	book, ok := d.books[ob.Pair]
	if !ok {
		d.mu.Unlock()
		return
	}
	book.applyUpdate(ob)
	d.mu.Unlock()

	d.evaluate()
}

func (d *TriangleDetector) evaluate() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	keep := 1.0 - d.fee

	for _, tri := range d.triangles {
		rate := 1.0
		valid := true

		for _, leg := range tri.Legs {
			book, ok := d.books[leg.Pair]
			if !ok {
				valid = false
				break
			}
			var price float64
			if leg.Side == "buy" {
				price = book.bestAsk()
				if price == 0 {
					valid = false
					break
				}
				rate *= (1.0 / price) * keep
			} else {
				price = book.bestBid()
				if price == 0 {
					valid = false
					break
				}
				rate *= price * keep
			}
		}

		if !valid {
			continue
		}

		if rate > 1.0 {
			pct := (rate - 1.0) * 100
			logger.Info("[DETECTOR] %s  profit=+%.4f%%  rate=%.8f", tri.Name, pct, rate)
			d.emit(tri.Name, Opportunity{
				Legs:      tri.Legs,
				EntryRate: rate,
				ProfitPct: pct,
			})
		}
	}
}

func (d *TriangleDetector) emit(cycle string, op Opportunity) {
	select {
	case d.OpChan <- op:
	default:
		logger.Warn("[DETECTOR] OpChan full, dropping opportunity (profit=+%.4f%%)", op.ProfitPct)
	}
}

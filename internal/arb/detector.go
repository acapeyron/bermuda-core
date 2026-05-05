package arb

import (
	"sync"
	"time"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

const tradeSize = 50.0 // USD notional per evaluation

type Leg struct {
	Pair string
	Side string // "buy" or "sell"
}

type Opportunity struct {
	Triangle         string
	Legs             [3]Leg
	EntryRate        float64
	ProfitPct        float64
	HasFullLiquidity bool // false if any leg had insufficient book depth
}

type cycleState struct {
	active    bool
	firstSeen time.Time
	lastRate  float64
}

type TriangleDetector struct {
	fee       float64
	mu        sync.RWMutex
	books     map[string]*orderBook
	triangles []Triangle
	cycles    map[string]*cycleState
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

	cycles := make(map[string]*cycleState)
	for _, t := range triangles {
		cycles[t.Name] = &cycleState{}
	}

	return &TriangleDetector{
		fee:       fee,
		books:     books,
		triangles: triangles,
		cycles:    cycles,
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
		fullLiquidity := true
		currentSize := tradeSize // notional in quote currency, carried through legs

		for _, leg := range tri.Legs {
			book, ok := d.books[leg.Pair]
			if !ok {
				valid = false
				break
			}

			var result LiquidityResult
			if leg.Side == "buy" {
				result = book.bestAskForSize(currentSize)
			} else {
				result = book.bestBidForSize(currentSize)
			}

			if result.AvgPrice == 0 {
				valid = false
				break
			}
			if !result.HasFullLiquidity {
				fullLiquidity = false
			}

			if leg.Side == "buy" {
				// Spending currentSize quote, receiving currentSize/avgPrice base
				rate *= (1.0 / result.AvgPrice) * keep
				currentSize = (currentSize / result.AvgPrice) * keep // now in base units
			} else {
				// Selling currentSize base, receiving currentSize*avgPrice quote
				rate *= result.AvgPrice * keep
				currentSize = currentSize * result.AvgPrice * keep // now in quote units
			}
		}

		if !valid {
			continue
		}
		if rate > 1.0 {
			pct := (rate - 1.0) * 100
			d.onProfitable(tri.Name, Opportunity{
				Triangle:         tri.Name,
				Legs:             tri.Legs,
				EntryRate:        rate,
				ProfitPct:        pct,
				HasFullLiquidity: fullLiquidity,
			})
		} else {
			d.onDead(tri.Name)
		}
	}
}

func (d *TriangleDetector) onProfitable(cycleKey string, op Opportunity) {
	state := d.cycles[cycleKey]
	if !state.active {
		state.active = true
		state.firstSeen = time.Now()
		state.lastRate = op.EntryRate
		liquidityTag := ""
		if !op.HasFullLiquidity {
			liquidityTag = " ⚠️ INSUFFICIENT LIQUIDITY"
		}
		logger.Info("[DETECTOR] [%s] Opportunity OPENED profit=+%.4f%%%s", cycleKey, op.ProfitPct, liquidityTag)
		select {
		case d.OpChan <- op:
		default:
			logger.Warn("[DETECTOR] OpChan full, dropping opportunity (profit=+%.4f%%)", op.ProfitPct)
		}
	} else {
		state.lastRate = op.EntryRate
	}
}

func (d *TriangleDetector) onDead(cycleKey string) {
	state := d.cycles[cycleKey]
	if state.active {
		duration := time.Since(state.firstSeen)
		logger.Info("[DETECTOR] [%s] Opportunity CLOSED after %dms (peak rate=%.8f)", cycleKey, duration.Milliseconds(), state.lastRate)
		state.active = false
		state.lastRate = 0
	}
}

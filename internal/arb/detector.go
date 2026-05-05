package arb

import (
	"sync"
	"time"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

const TradeSize = 50.0 // USD notional per evaluation

type Leg struct {
	Pair string
	Side string // "buy" or "sell"
}

type Opportunity struct {
	Triangle         string
	Legs             [3]Leg
	EntryRate        float64
	ProfitPct        float64
	HasFullLiquidity bool
	DurationMs       int64 // exchange-clock duration of the window, set on close
}

type cycleState struct {
	active          bool
	firstSeenExchTs int64
	lastSeenExchTs  int64
	lastRate        float64
	lastPct         float64
	openOp          Opportunity // snapshot of the opportunity as it opened
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
	exchTs := ob.Timestamp
	d.mu.Unlock()

	d.evaluate(exchTs)
}

func (d *TriangleDetector) evaluate(exchTs int64) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	keep := 1.0 - d.fee

	for _, tri := range d.triangles {
		rate := 1.0
		valid := true
		fullLiquidity := true
		currentSize := TradeSize

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
				rate *= (1.0 / result.AvgPrice) * keep
				currentSize = (currentSize / result.AvgPrice) * keep
			} else {
				rate *= result.AvgPrice * keep
				currentSize = currentSize * result.AvgPrice * keep
			}
		}

		if !valid {
			continue
		}
		if rate > 1.0 {
			pct := (rate - 1.0) * 100
			d.onProfitable(tri.Name, exchTs, Opportunity{
				Triangle:         tri.Name,
				Legs:             tri.Legs,
				EntryRate:        rate,
				ProfitPct:        pct,
				HasFullLiquidity: fullLiquidity,
			})
		} else {
			d.onDead(tri.Name, exchTs)
		}
	}
}

func (d *TriangleDetector) onProfitable(cycleKey string, exchTs int64, op Opportunity) {
	state := d.cycles[cycleKey]
	if !state.active {
		state.active = true
		state.firstSeenExchTs = exchTs
		state.lastSeenExchTs = exchTs
		state.lastRate = op.EntryRate
		state.lastPct = op.ProfitPct
		state.openOp = op
		liquidityTag := ""
		if !op.HasFullLiquidity {
			liquidityTag = " ⚠️ INSUFFICIENT LIQUIDITY"
		}
		logger.Info("[DETECTOR] [%s] Opportunity OPENED profit=+%.4f%%%s", cycleKey, op.ProfitPct, liquidityTag)
	} else {
		state.lastSeenExchTs = exchTs
		state.lastRate = op.EntryRate
		state.lastPct = op.ProfitPct
	}
}

func (d *TriangleDetector) onDead(cycleKey string, exchTs int64) {
	state := d.cycles[cycleKey]
	if state.active {
		durationMs := exchTs - state.firstSeenExchTs
		logger.Info("[DETECTOR] [%s] Opportunity CLOSED — exchange duration: %dms (peak rate=%.8f)",
			cycleKey, durationMs, state.lastRate)

		// Emit on close with full information
		op := state.openOp
		op.DurationMs = durationMs
		op.EntryRate = state.lastRate
		op.ProfitPct = state.lastPct

		select {
		case d.OpChan <- op:
		default:
			logger.Warn("[DETECTOR] OpChan full, dropping opportunity (profit=+%.4f%%)", op.ProfitPct)
		}

		state.active = false
		state.lastRate = 0
		state.lastPct = 0
		state.firstSeenExchTs = 0
		state.lastSeenExchTs = 0
		state.openOp = Opportunity{}
	}
}

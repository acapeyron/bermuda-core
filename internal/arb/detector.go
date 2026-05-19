package arb

import (
	"sync"
	"time"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

const TradeSize = 50.0 // USD notional per evaluation

// TakerFeePerLeg is the Binance non-VIP taker fee (0.1%).
// WS depth updates only show the resting book, so every fill is a taker fill.
const TakerFeePerLeg = 0.001

type Leg struct {
	Pair string
	Side string // "buy" or "sell"
}

type Opportunity struct {
	Triangle               string
	Legs                   [3]Leg
	OpenRate               float64   // rate at the moment the window opened
	PeakRate               float64   // highest rate seen during the window
	DepthAdjustedProfitPct float64   // (PeakRate - 1) * 100
	CloseRate              float64   // last rate seen before window died (≤ 1.0)
	CloseProfitPct         float64   // (CloseRate - 1) * 100  — may be negative
	HasFullLiquidity       bool      // false if any leg had insufficient depth at $50
	DurationMs             int64     // exchange-clock ms from first to last profitable tick
	OpenedAt               time.Time // wall-clock time the window opened
	ClosedAt               time.Time // wall-clock time the window closed
}

type cycleState struct {
	active           bool
	firstSeenExchTs  int64
	firstSeenLocalTs int64
	lastSeenExchTs   int64
	openWallTime     time.Time

	openRate float64
	peakRate float64
	peakPct  float64
	lastRate float64
	lastPct  float64

	openOp Opportunity // snapshot captured when the window opened

	// cooldown: timestamp after which this triangle may re-open
	cooldownUntil time.Time
}

type TriangleDetector struct {
	// fee is kept for potential future maker/taker split; evaluation now uses
	// TakerFeePerLeg directly so the constant is always in sync.
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
	localNow := time.Now().UnixMilli()

	d.mu.Lock()
	book, ok := d.books[ob.Pair]
	if !ok {
		d.mu.Unlock()
		return
	}
	book.applyUpdate(ob)
	exchTs := ob.Timestamp
	d.mu.Unlock()

	if exchTs == 0 {
		return // snapshot ou message sans timestamp, on skip l'évaluation
	}

	lag := localNow - exchTs
	if lag > 50 {
		logger.Warn("[DETECTOR] High lag: %dms for %s", lag, ob.Pair)
	}

	d.evaluate(exchTs, localNow)
}

func (d *TriangleDetector) evaluate(exchTs int64, localTs int64) {
	d.mu.RLock()
	defer d.mu.RUnlock()

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
				rate *= (1.0 / result.AvgPrice)
				currentSize = (currentSize / result.AvgPrice)
			} else {
				rate *= result.AvgPrice
				currentSize = currentSize * result.AvgPrice
			}
		}

		if !valid {
			continue
		}
		pct := (rate - 1.0) * 100
		thresholdPct := d.fee * 100
		if pct > thresholdPct {
			d.onProfitable(tri.Name, exchTs, localTs, Opportunity{
				Triangle:               tri.Name,
				Legs:                   tri.Legs,
				OpenRate:               rate,
				PeakRate:               rate,
				DepthAdjustedProfitPct: pct,
				HasFullLiquidity:       fullLiquidity,
			})
		} else {
			d.onDead(tri.Name, exchTs, rate)
		}
	}
}

func (d *TriangleDetector) onProfitable(cycleKey string, exchTs int64, localTs int64, op Opportunity) {
	state := d.cycles[cycleKey]

	// Respect cooldown: ignore a re-open until the cooldown period has elapsed.
	if !state.active && time.Now().Before(state.cooldownUntil) {
		return
	}

	if !state.active {
		state.active = true
		state.firstSeenExchTs = exchTs
		state.firstSeenLocalTs = localTs
		state.lastSeenExchTs = exchTs
		state.openWallTime = time.Now()
		state.openRate = op.OpenRate
		state.peakRate = op.PeakRate
		state.peakPct = op.DepthAdjustedProfitPct
		state.lastRate = op.OpenRate
		state.lastPct = op.DepthAdjustedProfitPct
		state.openOp = op

		liquidityTag := ""
		if !op.HasFullLiquidity {
			liquidityTag = " ⚠️ INSUFFICIENT LIQUIDITY"
		}
		logger.Info("[DETECTOR] [%s] Opportunity OPENED profit=+%.4f%%%s lag=%dms",
			cycleKey, op.DepthAdjustedProfitPct, liquidityTag, localTs-exchTs)
	} else {
		// Window still alive — update last-seen and track peak.
		state.lastSeenExchTs = exchTs
		state.lastRate = op.OpenRate
		state.lastPct = op.DepthAdjustedProfitPct

		if op.OpenRate > state.peakRate {
			state.peakRate = op.OpenRate
			state.peakPct = op.DepthAdjustedProfitPct
			logger.Info("[DETECTOR] [%s] New peak profit=+%.4f%%", cycleKey, state.peakPct)
		}
	}
}

func (d *TriangleDetector) onDead(cycleKey string, exchTs int64, closeRate float64) {
	state := d.cycles[cycleKey]
	if !state.active {
		return
	}

	durationMs := state.lastSeenExchTs - state.firstSeenExchTs

	// Sanity check : ignore durations > 30s (probable timestamp bug)
	if durationMs < 0 || durationMs > 30_000 {
		logger.Warn("[DETECTOR] [%s] Suspicious duration %dms, discarding cycle", cycleKey, durationMs)
		state.active = false
		state.firstSeenExchTs = 0
		state.lastSeenExchTs = 0
		state.lastRate = 0
		state.lastPct = 0
		state.openOp = Opportunity{}
		return
	}

	closePct := (closeRate - 1.0) * 100

	logger.Info("[DETECTOR] [%s] Opportunity CLOSED — duration: %dms peak=+%.4f%% close=%.4f%%",
		cycleKey, durationMs, state.peakPct, closePct)

	op := state.openOp
	op.OpenRate = state.openRate
	op.PeakRate = state.peakRate
	op.DepthAdjustedProfitPct = state.peakPct
	op.CloseRate = closeRate
	op.CloseProfitPct = closePct
	op.DurationMs = durationMs
	op.OpenedAt = state.openWallTime
	op.ClosedAt = time.Now()

	select {
	case d.OpChan <- op:
	default:
		logger.Warn("[DETECTOR] OpChan full, dropping opportunity (peak=+%.4f%%)", op.DepthAdjustedProfitPct)
	}

	state.active = false
	state.openRate = 0
	state.peakRate = 0
	state.peakPct = 0
	state.lastRate = 0
	state.lastPct = 0
	state.firstSeenExchTs = 0
	state.lastSeenExchTs = 0
	state.openOp = Opportunity{}
	state.cooldownUntil = time.Now().Add(d.cooldown)
}

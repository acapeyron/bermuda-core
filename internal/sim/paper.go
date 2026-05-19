// Package sim contains the paper-trading simulator.
// It takes a detected Opportunity and computes what a real execution would have
// looked like after accounting for taker fees and an estimated slippage cost.
package sim

import "github.com/acapeyron/bermuda-core/internal/arb"

const (
	// TakerFeePerLeg is the Binance non-VIP taker fee rate (0.1 %).
	// Every leg is a market order against the resting book, so all three legs
	// incur the taker fee.
	TakerFeePerLeg = 0.001

	// SlippagePerTriangle is a conservative flat estimate (2 bps) that captures
	// the price impact between the WS tick that triggers detection and the actual
	// fill. On liquid pairs it can be lower; on illiquid pairs it will be higher.
	SlippagePerTriangle = 0.0002

	// NotionalUSD is the trade size used for simulation, matching arb.TradeSize.
	NotionalUSD = arb.TradeSize
)

// PaperTrade holds the simulated P&L for a single closed opportunity.
type PaperTrade struct {
	// Gross profit as reported by the detector (peak, after embedded fee during
	// detection — but we re-add it here so accounting is explicit).
	GrossProfitPct float64

	// Fee charged across all three legs, as a percentage of notional.
	TotalFeePct float64

	// Estimated slippage across the full triangle, as a percentage of notional.
	SlippagePct float64

	// Net profit after fees and slippage.
	NetProfitPct float64

	// Absolute P&L in USD at NotionalUSD size.
	NetProfitUSD float64

	// True if NetProfitPct > 0.
	IsProfitable bool
}

// Simulate computes the paper trade result for a closed Opportunity.
// We use PeakProfitPct as the "best case" gross figure — it represents the
// window where a fast executor could theoretically have entered.
func Simulate(op arb.Opportunity) PaperTrade {
	keep := 1.0 - TakerFeePerLeg
	netRate := op.PeakRate * keep * keep * keep * (1.0 - SlippagePerTriangle)
	netProfitPct := (netRate - 1.0) * 100

	totalFeePct := (1.0 - keep*keep*keep) * 100 // e.g. ~0.2997%
	slippagePct := SlippagePerTriangle * 100    // e.g. 0.02 %

	netProfitUSD := NotionalUSD * (netProfitPct / 100)

	return PaperTrade{
		GrossProfitPct: op.PeakProfitPct,
		TotalFeePct:    totalFeePct,
		SlippagePct:    slippagePct,
		NetProfitPct:   netProfitPct,
		NetProfitUSD:   netProfitUSD,
		IsProfitable:   netProfitPct > 0,
	}
}

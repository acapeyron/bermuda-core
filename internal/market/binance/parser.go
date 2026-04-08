package binance

import (
	"encoding/json"
	"strconv"

	"github.com/acapeyron/bermuda-core/internal/market"
)

type BinanceParser struct{}

func (b *BinanceParser) ParseTrade(msg []byte) (*market.Trade, error) {
	var trade struct {
		Event  string `json:"e"`
		Symbol string `json:"s"`
		Price  string `json:"p"`
		Qty    string `json:"q"`
		T      int64  `json:"T"`
	}
	if err := json.Unmarshal(msg, &trade); err != nil || trade.Event != "trade" {
		return nil, err
	}
	// convert strings to floats
	price, _ := strconv.ParseFloat(trade.Price, 64)
	qty, _ := strconv.ParseFloat(trade.Qty, 64)
	return &market.Trade{
		Pair:      trade.Symbol,
		Price:     price,
		Size:      qty,
		Timestamp: trade.T,
	}, nil
}

func (b *BinanceParser) ParseOrderBook(msg []byte) (*market.OrderBookUpdate, error) {
	var ob struct {
		Symbol       string      `json:"s"`
		Bids         [][2]string `json:"b"`
		Asks         [][2]string `json:"a"`
		E            int64       `json:"E"`
		LastUpdateID int64       `json:"u"`
	}
	if err := json.Unmarshal(msg, &ob); err != nil {
		return nil, err
	}
	parseLevels := func(levels [][2]string) []market.Level {
		out := make([]market.Level, len(levels))
		for i, l := range levels {
			p, _ := strconv.ParseFloat(l[0], 64)
			s, _ := strconv.ParseFloat(l[1], 64)
			out[i] = market.Level{Price: p, Size: s}
		}
		return out
	}
	return &market.OrderBookUpdate{
		Pair:         ob.Symbol,
		Bids:         parseLevels(ob.Bids),
		Asks:         parseLevels(ob.Asks),
		Timestamp:    ob.E,
		LastUpdateID: ob.LastUpdateID,
	}, nil
}

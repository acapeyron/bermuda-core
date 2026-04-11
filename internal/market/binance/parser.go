package binance

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/acapeyron/bermuda-core/internal/market"
)

type BinanceParser struct{}

func (b *BinanceParser) ParseTrade(msg []byte) (*market.Trade, error) {
	var raw struct {
		Event     string `json:"e"`
		Symbol    string `json:"s"`
		Price     string `json:"p"`
		Qty       string `json:"q"`
		Timestamp int64  `json:"T"`
	}
	if err := json.Unmarshal(msg, &raw); err != nil || raw.Event != "trade" {
		return nil, err
	}
	price, _ := strconv.ParseFloat(raw.Price, 64)
	qty, _ := strconv.ParseFloat(raw.Qty, 64)
	return &market.Trade{
		Pair:      raw.Symbol,
		Price:     price,
		Size:      qty,
		Timestamp: raw.Timestamp,
	}, nil
}

func (b *BinanceParser) ParseOrderBook(msg []byte) (*market.OrderBookUpdate, error) {
	var raw struct {
		Event        string      `json:"e"`
		Symbol       string      `json:"s"`
		Bids         [][2]string `json:"b"`
		Asks         [][2]string `json:"a"`
		EventTime    int64       `json:"E"`
		LastUpdateID int64       `json:"u"`
	}
	if err := json.Unmarshal(msg, &raw); err != nil {
		return nil, err
	}
	if raw.Event != "depthUpdate" || raw.Symbol == "" {
		return nil, nil
	}
	return &market.OrderBookUpdate{
		Pair:         raw.Symbol,
		Bids:         parseLevels(raw.Bids),
		Asks:         parseLevels(raw.Asks),
		Timestamp:    raw.EventTime,
		LastUpdateID: raw.LastUpdateID,
	}, nil
}

func (b *BinanceParser) ParseOrderBookSnapshot(msg []byte, pair string) (*market.OrderBookUpdate, error) {
	var raw struct {
		LastUpdateId int64       `json:"lastUpdateId"`
		Bids         [][2]string `json:"bids"`
		Asks         [][2]string `json:"asks"`
	}
	if err := json.Unmarshal(msg, &raw); err != nil {
		return nil, fmt.Errorf("Failed to decode snapshot: %w", err)
	}
	if raw.LastUpdateId == 0 {
		return nil, fmt.Errorf("Snapshot missing lastUpdateId")
	}
	return &market.OrderBookUpdate{
		Pair:         pair,
		Bids:         parseLevels(raw.Bids),
		Asks:         parseLevels(raw.Asks),
		LastUpdateID: raw.LastUpdateId,
	}, nil
}

func parseLevels(levels [][2]string) []market.Level {
	out := make([]market.Level, len(levels))
	for i, l := range levels {
		p, _ := strconv.ParseFloat(l[0], 64)
		s, _ := strconv.ParseFloat(l[1], 64)
		out[i] = market.Level{Price: p, Size: s}
	}
	return out
}

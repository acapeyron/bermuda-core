package binance

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/acapeyron/bermuda-core/internal/market"
)

type BinanceParser struct{}

func (b *BinanceParser) ParseOrderBook(msg []byte) (*market.OrderBookUpdate, error) {
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}

	if json.Unmarshal(msg, &wrapper) == nil && wrapper.Data != nil {
		msg = wrapper.Data
	}

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

	bids, err := parseLevels(raw.Bids)
	if err != nil {
		return nil, fmt.Errorf("parse bids: %w", err)
	}

	asks, err := parseLevels(raw.Asks)
	if err != nil {
		return nil, fmt.Errorf("parse asks: %w", err)
	}
	return &market.OrderBookUpdate{
		Pair:         raw.Symbol,
		Bids:         bids,
		Asks:         asks,
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

	bids, err := parseLevels(raw.Bids)
	if err != nil {
		return nil, fmt.Errorf("parse bids: %w", err)
	}

	asks, err := parseLevels(raw.Asks)
	if err != nil {
		return nil, fmt.Errorf("parse asks: %w", err)
	}

	return &market.OrderBookUpdate{
		Pair:         pair,
		Bids:         bids,
		Asks:         asks,
		LastUpdateID: raw.LastUpdateId,
	}, nil
}

func parseLevels(levels [][2]string) ([]market.Level, error) {
	out := make([]market.Level, len(levels))

	for i, l := range levels {
		if len(l) != 2 {
			return nil, fmt.Errorf("invalid level: %+v", l)
		}

		p, err := strconv.ParseFloat(l[0], 64)
		if err != nil {
			return nil, fmt.Errorf("bad price %q: %w", l[0], err)
		}

		s, err := strconv.ParseFloat(l[1], 64)
		if err != nil {
			return nil, fmt.Errorf("bad size %q: %w", l[1], err)
		}

		out[i] = market.Level{Price: p, Size: s}
	}

	return out, nil
}

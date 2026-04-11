package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
	"github.com/acapeyron/bermuda-core/internal/market/binance"
	"github.com/acapeyron/bermuda-core/internal/storage"
)

type WSClient struct {
	url          string
	conn         *websocket.Conn
	rawChan      chan []byte
	db           storage.Storage
	parser       *binance.BinanceParser
	lastUpdateID int64 // initial snapshot lastupdateId
	pair         string

	preSnapshotChan chan []byte   // buffer WS messages before snapshot is loaded
	snapshotDone    chan struct{} // indicates REST snapshot is loaded
}

func NewClient(url string, db storage.Storage, parser *binance.BinanceParser, pair string) *WSClient {
	return &WSClient{
		url:             url,
		rawChan:         make(chan []byte, 1000),
		db:              db,
		parser:          parser,
		pair:            pair,
		preSnapshotChan: make(chan []byte, 1000),
		snapshotDone:    make(chan struct{}),
	}
}

func (c *WSClient) Connect(ctx context.Context) {
	u, _ := url.Parse(c.url)
	logger.Info("Connecting to %s", u.String())

	// Channels for storage
	tradeChan := make(chan market.Trade, 100)
	obChan := make(chan market.OrderBookUpdate, 100)

	go c.db.Run(ctx, tradeChan, obChan)

	// WS connection
	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		logger.Error("WebSocket connect error: %v", err)
		return
	}
	logger.Info("WebSocket connected to %s", c.url)

	// Start WS read loop and buffer messages
	go c.readLoop(ctx)

	// Initial GET request to get snapshot
	if err := c.fetchInitialOrderBook(obChan); err != nil {
		logger.Error("Failed to fetch initial order book: %v", err)
		panic(err)
	}

	close(c.snapshotDone)
	// Drain buffered messages into rawChan
	c.drainBuffer()

	// Start processing loop
	go c.processLoop(ctx, tradeChan, obChan)
}

// drainBuffer drains preSnapshotChan into rawChan after snapshot

func (c *WSClient) drainBuffer() {
	logger.Info("Draining pre-snapshot buffer...")
	for {
		select {
		case msg := <-c.preSnapshotChan:
			if ob, err := c.parser.ParseOrderBook(msg); err == nil && ob != nil {
				if ob.LastUpdateID <= c.lastUpdateID {
					continue
				}
			}
			c.rawChan <- msg
		default:
			logger.Info("Buffer fully drained")
			return
		}
	}
}

func (c *WSClient) fetchInitialOrderBook(obChan chan<- market.OrderBookUpdate) error {
	body, err := c.fetchSnapshotHTTP()
	if err != nil {
		return err
	}

	snapshot, err := c.parseSnapshot(body)
	if err != nil {
		return err
	}

	// State update
	c.lastUpdateID = snapshot.LastUpdateID

	// Dispatch
	select {
	case obChan <- *snapshot:
	default:
		logger.Warn("Snapshot dropped (channel full)")
	}

	logger.Info(
		"Initial order book snapshot loaded: %s Bids:%d Asks:%d lastUpdateID:%d",
		snapshot.Pair, len(snapshot.Bids), len(snapshot.Asks), snapshot.LastUpdateID,
	)

	return nil
}

func (c *WSClient) fetchSnapshotHTTP() ([]byte, error) {
	endpoint := fmt.Sprintf(
		"https://fapi.binance.com/fapi/v1/depth?symbol=%s&limit=1000",
		c.pair,
	)

	var resp *http.Response
	var err error

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		req, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		resp, err = http.DefaultClient.Do(req)

		cancel()

		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			break
		}

		logger.Warn("Snapshot HTTP attempt %d failed: %v", i+1, err)
		time.Sleep(200 * time.Millisecond)
	}

	if err != nil || resp == nil {
		return nil, fmt.Errorf("snapshot HTTP failed after retries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	return body, nil
}

func (c *WSClient) parseSnapshot(body []byte) (*market.OrderBookUpdate, error) {
	snapshot, err := c.parser.ParseOrderBookSnapshot(body, c.pair)
	if err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}
	return snapshot, nil
}

// readLoop reads WS → pushes raw messages
func (c *WSClient) readLoop(ctx context.Context) {
	firstMessage := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				logger.Error("Read error: %v", err)
				time.Sleep(time.Second)
				continue
			}
			if firstMessage {
				logger.Info("WebSocket is active: first message received")
				firstMessage = false
			}
			select {
			case <-c.snapshotDone:
				c.rawChan <- message
			default:
				c.preSnapshotChan <- message
			}
		}
	}
}

// processLoop consumes rawChan → routes to storage channels
func (c *WSClient) processLoop(ctx context.Context, tradeChan chan<- market.Trade, obChan chan<- market.OrderBookUpdate) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.rawChan:
			c.processMessage(msg, tradeChan, obChan)
		}
	}
}

func (c *WSClient) processMessage(msg []byte, tradeChan chan<- market.Trade, obChan chan<- market.OrderBookUpdate) {
	if trade, err := c.parser.ParseTrade(msg); err == nil && trade != nil {
		tradeChan <- *trade
		logger.Info("Trade routed: %s %f @ %f", trade.Pair, trade.Size, trade.Price)
		return
	}

	if ob, err := c.parser.ParseOrderBook(msg); err == nil && ob != nil {
		if ob.LastUpdateID <= c.lastUpdateID {
			return
		}
		obChan <- *ob
		logger.Info("OrderBook routed: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		return
	}

	logger.Warn("Unknown message: %s", string(msg))
}

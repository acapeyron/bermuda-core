package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
	"github.com/acapeyron/bermuda-core/internal/storage"
)

type WSClient struct {
	wsURL        string
	snapshotURL  string
	conn         *websocket.Conn
	rawChan      chan []byte
	db           storage.Storage
	parser       market.Parser
	lastUpdateID int64 // initial snapshot lastupdateId
	exchange     string
	pair         string

	preSnapshotChan chan []byte   // buffer WS messages before snapshot is loaded
	snapshotDone    chan struct{} // indicates REST snapshot is loaded

}

func NewClient(exchange string, pair config.PairConfig, db storage.Storage, parser market.Parser) *WSClient {
	return &WSClient{
		wsURL:           pair.WSURL,
		snapshotURL:     pair.SnapshotURL,
		rawChan:         make(chan []byte, 1000),
		db:              db,
		parser:          parser,
		exchange:        exchange,
		pair:            pair.Symbol,
		preSnapshotChan: make(chan []byte, 1000),
		snapshotDone:    make(chan struct{}),
	}
}

func (c *WSClient) Connect(ctx context.Context, cancel context.CancelFunc) {
	u, _ := url.Parse(c.wsURL)
	logger.Info("[%s/%s] Connecting to WebSocket %s", c.exchange, c.pair, u.String())

	// Channels for storage
	tradeChan := make(chan market.Trade, 100)
	obChan := make(chan market.OrderBookUpdate, 100)

	go c.db.Run(ctx, tradeChan, obChan)

	// WS connection
	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		logger.Error("[%s/%s] WebSocket connect error: %v", c.exchange, c.pair, err)
		return
	}
	logger.Info("[%s/%s] WebSocket connected to %s", c.exchange, c.pair, u.String())

	// Start WS read loop and buffer messages
	go c.readLoop(ctx)

	// Initial GET request to get snapshot
	if err := c.fetchInitialOrderBook(obChan); err != nil {
		logger.Error("[%s/%s] Failed to fetch initial order book: %v — shutting down client", c.exchange, c.pair, err)
		cancel()
		return
	}

	logger.Info("[%s/%s] Initial order book snapshot fetched successfully", c.exchange, c.pair)

	close(c.snapshotDone)
	// Drain buffered messages into rawChan
	c.drainBuffer()

	// Start processing loop
	go c.processLoop(ctx, tradeChan, obChan)

	<-ctx.Done()
	logger.Warn("[%s/%s] Client shutting down: %v", c.exchange, c.pair, ctx.Err())
	c.conn.Close()
}

// drainBuffer drains preSnapshotChan into rawChan after snapshot

func (c *WSClient) drainBuffer() {
	logger.Info("[%s/%s] Draining pre-snapshot buffer...", c.exchange, c.pair)
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
			logger.Info("[%s/%s] Buffer fully drained", c.exchange, c.pair)
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
		"[%s/%s] Initial order book snapshot loaded: %s Bids:%d Asks:%d lastUpdateID:%d", c.exchange, c.pair,
		snapshot.Pair, len(snapshot.Bids), len(snapshot.Asks), snapshot.LastUpdateID,
	)

	return nil
}

func (c *WSClient) fetchSnapshotHTTP() ([]byte, error) {
	var resp *http.Response
	var err error

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		req, _ := http.NewRequestWithContext(ctx, "GET", c.snapshotURL, nil)
		resp, err = http.DefaultClient.Do(req)

		cancel()

		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			break
		}

		logger.Warn("[%s/%s] Snapshot HTTP attempt %d failed: %v", c.exchange, c.pair, i+1, err)
		time.Sleep(200 * time.Millisecond)
	}

	if err != nil || resp == nil {
		return nil, fmt.Errorf("Snapshot HTTP failed after retries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Bad status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read body: %w", err)
	}

	return body, nil
}

func (c *WSClient) parseSnapshot(body []byte) (*market.OrderBookUpdate, error) {
	snapshot, err := c.parser.ParseOrderBookSnapshot(body, c.pair)
	if err != nil {
		return nil, fmt.Errorf("Failed to decode snapshot: %w", err)
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
				logger.Error("[%s/%s] Read error: %v", c.exchange, c.pair, err)
				time.Sleep(time.Second)
				continue
			}
			if firstMessage {
				logger.Info("[%s/%s] WebSocket is active: first message received", c.exchange, c.pair)
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
		logger.Info("[%s/%s] Trade routed: %s %f @ %f", c.exchange, c.pair, trade.Pair, trade.Size, trade.Price)
		return
	}

	if ob, err := c.parser.ParseOrderBook(msg); err == nil && ob != nil {
		if ob.LastUpdateID <= c.lastUpdateID {
			logger.Warn("[%s/%s] Dropping stale update: %d <= %d", c.exchange, c.pair, ob.LastUpdateID, c.lastUpdateID)
			return
		}
		obChan <- *ob
		logger.Info("[%s/%s] OrderBook routed: %s Bids:%d Asks:%d", c.exchange, c.pair, ob.Pair, len(ob.Bids), len(ob.Asks))
		return
	}

	logger.Warn("[%s/%s] Unknown message: %s", c.exchange, c.pair, string(msg))
}

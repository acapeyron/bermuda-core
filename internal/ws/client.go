package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
	"github.com/acapeyron/bermuda-core/internal/storage"
)

type WSClient struct {
	url            string
	conn           *websocket.Conn
	rawChan        chan []byte
	db             storage.Storage
	lastSnapshotTs int64 // initial snapshot timestamp

	preSnapshotChan chan []byte   // buffer WS messages before snapshot is loaded
	snapshotDone    chan struct{} // indicates REST snapshot is loaded
}

func NewClient(url string, db storage.Storage) *WSClient {
	return &WSClient{
		url:             url,
		rawChan:         make(chan []byte, 1000),
		db:              db,
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
	go c.readLoop()

	// Initial GET request to get snapshot
	if err := c.FetchInitialOrderBook(obChan); err != nil {
		logger.Error("Failed to fetch initial order book: %v", err)
		return
	}

	close(c.snapshotDone)

	// Drain buffered messages into rawChan
	c.processBuffer()

	// Start processing loop
	go c.processLoop(ctx, tradeChan, obChan)
}

// processBuffer drains preSnapshotChan into rawChan after snapshot
func (c *WSClient) processBuffer() {
	logger.Info("Waiting for snapshot to drain buffer...")

	<-c.snapshotDone

	logger.Info("Draining pre-snapshot buffer...")

	for {
		select {
		case msg := <-c.preSnapshotChan:
			c.rawChan <- msg
		default:
			// buffer is empty → we're done
			logger.Info("Buffer fully drained")
			return
		}
	}
}

func (c *WSClient) FetchInitialOrderBook(obChan chan<- market.OrderBookUpdate) error {
	// Exemple simplifié : GET vers l'API REST pour récupérer l'état complet
	// Remplace l'URL par l'endpoint réel de l'orderbook
	resp, err := http.Get("https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read the entire body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println("Response Body:\n", string(bodyBytes))

	var snapshot market.OrderBookUpdate
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return err
	}

	// Save snapshot in DB
	c.lastSnapshotTs = snapshot.Timestamp
	obChan <- snapshot
	logger.Info("Initial order book snapshot loaded: %s Bids:%d Asks:%d",
		snapshot.Pair, len(snapshot.Bids), len(snapshot.Asks))

	return nil
}

// readLoop reads WS → pushes raw messages
func (c *WSClient) readLoop(ctx context.Context) {
	firstMessage := true
	timeout := time.After(5 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			if firstMessage {
				logger.Warn("No WS message received in 5 seconds")
				firstMessage = false
			}
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
			fmt.Println("Message:", string(message))
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
	var trade market.Trade
	if err := json.Unmarshal(msg, &trade); err == nil && trade.Pair != "" {
		tradeChan <- trade
		logger.Info("Trade routed: %s %f @ %f", trade.Pair, trade.Size, trade.Price)
		return
	}

	var ob market.OrderBookUpdate
	if err := json.Unmarshal(msg, &ob); err == nil && ob.Pair != "" {
		if ob.Timestamp <= c.lastSnapshotTs {
			return
		}
		obChan <- ob
		logger.Info("OrderBook routed: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		return
	}

	logger.Info("Unknown message: %s", string(msg))
}

package ws

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

const (
	initialReconnectDelay = 500 * time.Millisecond
	maxReconnectDelay     = 60 * time.Second
)

type WSClient struct {
	wsURL    string
	rawChan  chan []byte
	parser   market.Parser
	exchange string
	manager  *OrderBookManager
}

func NewClient(exchange, baseWSURL string, pairs []config.PairConfig, parser market.Parser) *WSClient {
	streams := make([]string, len(pairs))
	snapshotURLs := make(map[string]string)
	for i, p := range pairs {
		streams[i] = p.WSStream
		snapshotURLs[p.Symbol] = p.SnapshotURL
	}

	return &WSClient{
		wsURL:    baseWSURL + strings.Join(streams, "/"),
		rawChan:  make(chan []byte, 5000),
		parser:   parser,
		exchange: exchange,
		manager:  NewOrderBookManager(exchange, snapshotURLs, parser),
	}
}

// Connect runs the full connect → snapshot → stream lifecycle and reconnects
// automatically with exponential backoff whenever the WebSocket drops.
func (c *WSClient) Connect(ctx context.Context, cancel context.CancelFunc) {
	u, _ := url.Parse(c.wsURL)
	logger.Info("[%s] Connecting to combined stream: %s", c.exchange, u.String())

	delay := initialReconnectDelay

	for {
		if ctx.Err() != nil {
			return
		}

		err := c.connectOnce(ctx)
		if ctx.Err() != nil {
			// Shutdown requested — exit cleanly.
			logger.Info("[%s] Shutting down WS client", c.exchange)
			return
		}

		logger.Warn("[%s] Connection lost (%v), reconnecting in %s...", c.exchange, err, delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		// Exponential backoff, capped at maxReconnectDelay.
		delay *= 2
		if delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
	}
}

// connectOnce dials, fetches snapshots, and pumps messages until the connection
// drops or ctx is cancelled. Returns the error that ended the session.
func (c *WSClient) connectOnce(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		logger.Error("[%s] WebSocket dial error: %v", c.exchange, err)
		return err
	}
	logger.Info("[%s] WebSocket connected", c.exchange)

	defer func() {
		conn.Close()
		logger.Warn("[%s] WebSocket connection closed", c.exchange)
	}()

	// Buffer messages that arrive while snapshots are being fetched.
	preSnapshotChan := make(chan []byte, 5000)
	snapshotDone := make(chan struct{})
	readErr := make(chan error, 1)

	go c.readLoop(ctx, conn, preSnapshotChan, snapshotDone, readErr)

	// Reset snapshot state so the manager treats this as a fresh connection.
	c.manager.ResetSnapshots()

	if err := c.manager.FetchAllSnapshots(); err != nil {
		logger.Error("[%s] Failed to fetch snapshots: %v", c.exchange, err)
		return err
	}

	close(snapshotDone)
	c.drainBuffer(preSnapshotChan)

	// Process live messages until readLoop signals an error or ctx is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		case msg := <-c.rawChan:
			c.manager.Handle(msg)
		}
	}
}

func (c *WSClient) readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	preSnapshotChan chan []byte,
	snapshotDone chan struct{},
	readErr chan<- error,
) {
	firstMessage := true
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
			default:
				logger.Error("[%s] Read error: %v", c.exchange, err)
				readErr <- err
			}
			return
		}
		if firstMessage {
			logger.Info("[%s] WebSocket active: first message received", c.exchange)
			firstMessage = false
		}
		select {
		case <-snapshotDone:
			c.rawChan <- message
		default:
			preSnapshotChan <- message
		}
	}
}

func (c *WSClient) drainBuffer(preSnapshotChan chan []byte) {
	logger.Info("[%s] Draining pre-snapshot buffer...", c.exchange)
	for {
		select {
		case msg := <-preSnapshotChan:
			if ob, err := c.parser.ParseOrderBook(msg); err == nil && ob != nil {
				if c.manager.IsStale(ob.Pair, ob.LastUpdateID) {
					continue
				}
			}
			c.rawChan <- msg
		default:
			logger.Info("[%s] Buffer fully drained", c.exchange)
			return
		}
	}
}

func (c *WSClient) ObChan() chan market.OrderBookUpdate {
	return c.manager.obChan
}

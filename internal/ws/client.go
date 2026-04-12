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

type WSClient struct {
	wsURL    string
	conn     *websocket.Conn
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

func (c *WSClient) Connect(ctx context.Context, cancel context.CancelFunc) {
	u, _ := url.Parse(c.wsURL)
	logger.Info("[%s] Connecting to combined stream: %s", c.exchange, u.String())

	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		logger.Error("[%s] WebSocket connect error: %v", c.exchange, err)
		cancel()
		return
	}
	logger.Info("[%s] WebSocket connected", c.exchange)

	// Buffer WS messages that arrive before snapshots are ready
	preSnapshotChan := make(chan []byte, 5000)
	snapshotDone := make(chan struct{})

	go c.readLoop(ctx, preSnapshotChan, snapshotDone)

	if err := c.manager.FetchAllSnapshots(); err != nil {
		logger.Error("[%s] Failed to fetch snapshots: %v", c.exchange, err)
		cancel()
		return
	}

	close(snapshotDone)
	c.drainBuffer(preSnapshotChan)

	go c.processLoop(ctx)

	<-ctx.Done()
	logger.Warn("[%s] Client shutting down: %v", c.exchange, ctx.Err())
	c.conn.Close()
}

func (c *WSClient) readLoop(ctx context.Context, preSnapshotChan chan []byte, snapshotDone chan struct{}) {
	firstMessage := true
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				logger.Error("[%s] Read error: %v", c.exchange, err)
				time.Sleep(time.Second)
				continue
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

func (c *WSClient) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.rawChan:
			c.manager.Handle(msg)
		}
	}
}

func (c *WSClient) ObChan() chan market.OrderBookUpdate {
	return c.manager.obChan
}

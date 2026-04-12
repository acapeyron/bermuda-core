package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
	"github.com/acapeyron/bermuda-core/internal/storage"
)

type WSClient struct {
	wsURL         string
	snapshotURLs  map[string]string // pair → snapshot URL
	conn          *websocket.Conn
	rawChan       chan []byte
	db            storage.Storage
	ObChan        chan market.OrderBookUpdate
	parser        market.Parser
	lastUpdateIDs map[string]int64 // pair → lastUpdateID
	exchange      string

	preSnapshotChan chan []byte   // buffer WS messages before snapshot is loaded
	snapshotDone    chan struct{} // indicates REST snapshot is loaded
}

func NewClient(exchange string, baseWSURL string, pairs []config.PairConfig, db storage.Storage, parser market.Parser) *WSClient {
	// Build combined URL : btcusdt@depth/ethusdt@depth/...
	streams := make([]string, len(pairs))
	snapshotURLs := make(map[string]string, len(pairs))
	for i, p := range pairs {
		streams[i] = p.WSStream // e.g. "btcusdt@depth"
		snapshotURLs[p.Symbol] = p.SnapshotURL
	}
	wsURL := baseWSURL + strings.Join(streams, "/")

	return &WSClient{
		wsURL:           wsURL,
		snapshotURLs:    snapshotURLs,
		rawChan:         make(chan []byte, 5000),
		db:              db,
		ObChan:          make(chan market.OrderBookUpdate, 5000),
		parser:          parser,
		exchange:        exchange,
		lastUpdateIDs:   make(map[string]int64),
		preSnapshotChan: make(chan []byte, 5000),
		snapshotDone:    make(chan struct{}),
	}
}

func (c *WSClient) Connect(ctx context.Context, cancel context.CancelFunc) {
	u, _ := url.Parse(c.wsURL)
	logger.Info("[%s] Connecting to combined stream: %s", c.exchange, u.String())

	// Channels for storage
	obChan := make(chan market.OrderBookUpdate, 100)

	go c.db.Run(ctx, obChan)

	// WS connection
	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.wsURL, nil)
	if err != nil {
		logger.Error("[%s] WebSocket connect error: %v", c.exchange, err)
		cancel()
		return
	}
	logger.Info("[%s] WebSocket connected", c.exchange)

	// Start WS read loop and buffer messages
	go c.readLoop(ctx)

	// Fetch snapshots for all pairs concurrently
	if err := c.fetchInitialOrderBook(); err != nil {
		logger.Error("[%s] Failed to fetch snapshots: %v", c.exchange, err)
		cancel()
		return
	}

	close(c.snapshotDone)
	c.drainBuffer()

	go c.processLoop(ctx)

	<-ctx.Done()
	logger.Warn("[%s] Client shutting down: %v", c.exchange, ctx.Err())
	c.conn.Close()
}

// drainBuffer drains preSnapshotChan into rawChan after snapshot
func (c *WSClient) drainBuffer() {
	logger.Info("[%s] Draining pre-snapshot buffer...", c.exchange)
	for {
		select {
		case msg := <-c.preSnapshotChan:
			if ob, err := c.parser.ParseOrderBook(msg); err == nil && ob != nil {
				if ob.LastUpdateID <= c.lastUpdateIDs[ob.Pair] {
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

func (c *WSClient) fetchInitialOrderBook() error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	errCh := make(chan error, len(c.snapshotURLs))

	for pair, url := range c.snapshotURLs {
		wg.Add(1)
		go func(pair, url string) {
			defer wg.Done()

			body, err := c.fetchSnapshotHTTP(pair, url)
			if err != nil {
				errCh <- fmt.Errorf("snapshot failed for %s: %w", pair, err)
				return
			}

			snapshot, err := c.parseSnapshot(body, pair)
			if err != nil {
				return
			}

			mu.Lock()
			c.lastUpdateIDs[pair] = snapshot.LastUpdateID
			mu.Unlock()

			c.ObChan <- *snapshot
			logger.Info("[%s/%s] Snapshot loaded: Bids:%d Asks:%d lastUpdateID:%d",
				c.exchange, pair, len(snapshot.Bids), len(snapshot.Asks), snapshot.LastUpdateID)
		}(pair, url)
	}

	wg.Wait()
	close(errCh)

	// return first error if any
	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

func (c *WSClient) fetchSnapshotHTTP(pair, url string) ([]byte, error) {
	var resp *http.Response
	var err error

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err = http.DefaultClient.Do(req)
		cancel()

		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			break
		}
		logger.Warn("[%s/%s] Snapshot HTTP attempt %d failed: %v", c.exchange, pair, i+1, err)
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
		return nil, fmt.Errorf("Failed to read body: %w", err)
	}

	return body, nil
}

func (c *WSClient) parseSnapshot(body []byte, pair string) (*market.OrderBookUpdate, error) {
	snapshot, err := c.parser.ParseOrderBookSnapshot(body, pair)
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
				logger.Error("[%s] Read error: %v", c.exchange, err)
				time.Sleep(time.Second)
				continue
			}
			if firstMessage {
				logger.Info("[%s] WebSocket active: first message received", c.exchange)
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
func (c *WSClient) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.rawChan:
			c.processMessage(msg)
		}
	}
}

func (c *WSClient) processMessage(msg []byte) {
	ob, err := c.parser.ParseOrderBook(msg)
	if err != nil || ob == nil {
		logger.Warn("[%s] Unknown or unparseable message: %s", c.exchange, string(msg))
		return
	}

	lastID, known := c.lastUpdateIDs[ob.Pair]
	if known && ob.LastUpdateID <= lastID {
		logger.Warn("[%s/%s] Dropping stale update: %d <= %d", c.exchange, ob.Pair, ob.LastUpdateID, lastID)
		return
	}

	c.lastUpdateIDs[ob.Pair] = ob.LastUpdateID
	c.ObChan <- *ob
}

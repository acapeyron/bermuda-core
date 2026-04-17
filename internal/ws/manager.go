package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
)

type OrderBookManager struct {
	exchange      string
	snapshotURLs  map[string]string
	lastUpdateIDs map[string]int64
	mu            sync.RWMutex
	obChan        chan market.OrderBookUpdate
	parser        market.Parser
}

func NewOrderBookManager(exchange string, snapshotURLs map[string]string, parser market.Parser) *OrderBookManager {
	return &OrderBookManager{
		exchange:      exchange,
		snapshotURLs:  snapshotURLs,
		lastUpdateIDs: make(map[string]int64),
		obChan:        make(chan market.OrderBookUpdate, 5000),
		parser:        parser,
	}
}

func (m *OrderBookManager) FetchAllSnapshots() error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(m.snapshotURLs))
	sem := make(chan struct{}, 3) // max 3 requêtes en parallèle

	for pair, url := range m.snapshotURLs {
		wg.Add(1)
		go func(pair, url string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()
			body, err := m.fetchSnapshotHTTP(pair, url)
			if err != nil {
				errCh <- fmt.Errorf("snapshot failed for %s: %w", pair, err)
				return
			}

			snapshot, err := m.parser.ParseOrderBookSnapshot(body, pair)
			if err != nil {
				errCh <- fmt.Errorf("parse failed for %s: %w", pair, err)
				return
			}

			m.mu.Lock()
			m.lastUpdateIDs[pair] = snapshot.LastUpdateID
			m.mu.Unlock()

			m.obChan <- *snapshot
			logger.Info("[%s] %s Snapshot loaded: Bids:%d Asks:%d lastUpdateID:%d",
				m.exchange, pair, len(snapshot.Bids), len(snapshot.Asks), snapshot.LastUpdateID)
		}(pair, url)
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

func (m *OrderBookManager) Handle(msg []byte) {
	ob, err := m.parser.ParseOrderBook(msg)
	if err != nil || ob == nil {
		logger.Warn("[%s] Unparseable message", m.exchange)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ob.LastUpdateID <= m.lastUpdateIDs[ob.Pair] {
		logger.Warn("[%s/%s] Dropping stale update: %d <= %d",
			m.exchange, ob.Pair, ob.LastUpdateID, m.lastUpdateIDs[ob.Pair])
		return
	}

	m.lastUpdateIDs[ob.Pair] = ob.LastUpdateID
	m.obChan <- *ob
}

func (m *OrderBookManager) IsStale(pair string, lastUpdateID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return lastUpdateID <= m.lastUpdateIDs[pair]
}

func (m *OrderBookManager) fetchSnapshotHTTP(pair, url string) ([]byte, error) {
	var resp *http.Response
	var err error

	delay := 200 * time.Millisecond

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err = http.DefaultClient.Do(req)
		cancel()

		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			break
		}
		logger.Warn("[%s/%s] Snapshot attempt %d failed: %v", m.exchange, pair, i+1, err)
		time.Sleep(delay)
		delay *= 2
	}

	if err != nil || resp == nil {
		return nil, fmt.Errorf("snapshot HTTP failed after retries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

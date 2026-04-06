package ws

import (
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
	db             storage.Storage
	lastSnapshotTs int64 // timestamp du snapshot initial
}

func NewClient(url string, db storage.Storage) *WSClient {
	return &WSClient{
		url: url,
		db:  db,
	}
}

func (c *WSClient) Connect() {
	u, _ := url.Parse(c.url)
	logger.Info("Connecting to %s", u.String())

	// Étape 1 : GET initial
	if err := c.FetchInitialOrderBook(); err != nil {
		logger.Error("Failed to fetch initial order book: %v", err)
		return
	}

	// Étape 2 : connexion WebSocket
	var err error
	c.conn, _, err = websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		logger.Error("WebSocket connect error: %v", err)
		return
	}

	logger.Info("WebSocket connected to %s", c.url)

	// Étape 3 : lancer la lecture en goroutine
	go c.readLoop()
}

func (c *WSClient) FetchInitialOrderBook() error {
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
	bodyString := string(bodyBytes)
	fmt.Println("Response Body:\n", bodyString)

	// var snapshot market.OrderBookUpdate
	// if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
	// 	return err
	// }

	// // Enregistrer le snapshot complet dans la DB
	// c.db.SaveOrderBook(snapshot)
	// c.lastSnapshotTs = snapshot.Timestamp
	// logger.Info("Initial order book snapshot loaded: %s Bids:%d Asks:%d",
	// 	snapshot.Pair, len(snapshot.Bids), len(snapshot.Asks))

	return nil
}

// Simplified read loop
func (c *WSClient) readLoop() {
	firstMessage := true
	timeout := time.After(5 * time.Second)

	for {
		select {
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
			// c.proccesMessage(message)
		}
	}
}

func (c *WSClient) processMessage(message []byte) {
	// Try parse as Trade
	var trade market.Trade
	if err := json.Unmarshal(message, &trade); err == nil && trade.Pair != "" {
		c.db.SaveTrade(trade)
		logger.Info("Trade logged: %s %f @ %f", trade.Pair, trade.Size, trade.Price)
		return
	}

	// Try parse as OrderBookUpdate
	var ob market.OrderBookUpdate
	if err := json.Unmarshal(message, &ob); err == nil && ob.Pair != "" {
		if ob.Timestamp <= c.lastSnapshotTs {
			return // ignorer les deltas antérieurs au snapshot
		}
		c.db.SaveOrderBook(ob)
		logger.Info("OrderBook logged: %s Bids:%d Asks:%d", ob.Pair, len(ob.Bids), len(ob.Asks))
		return
	}

	logger.Info("Unknown message: %s", string(message))
}

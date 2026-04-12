package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	figure "github.com/common-nighthawk/go-figure"

	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/registry"
	"github.com/acapeyron/bermuda-core/internal/ws"
)

func main() {
	figure.NewFigure("Bermuda Core", "", true).Print()
	// Initialize logger
	logger.Init()

	cfg, err := config.Load("../config/config.yaml")
	if err != nil {
		logger.Error("Failed to load config: %v", err)
		os.Exit(1)
	}

	parser, err := registry.NewParser(cfg.Exchange.Name)
	if err != nil {
		logger.Error("Unknown exchange: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// One client for all pairs
	client := ws.NewClient(cfg.Exchange.Name, cfg.Exchange.BaseWSURL, cfg.Exchange.Pairs, parser)
	go client.Connect(ctx, cancel)

	// Consuming OrderBookUpdates
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ob := <-client.ObChan():
				// TODO: detector.UpdateOrderBook(ob)
				logger.Info("[%s] %s Bids:%d Asks:%d lastUpdateID:%d", cfg.Exchange.Name,
					ob.Pair, len(ob.Bids), len(ob.Asks), ob.LastUpdateID)
			}
		}
	}()

	// Block until CTRL+C or kill signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")
	cancel()
}

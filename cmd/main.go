package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/common-nighthawk/go-figure"

	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market"
	"github.com/acapeyron/bermuda-core/internal/registry"
	"github.com/acapeyron/bermuda-core/internal/storage"
	"github.com/acapeyron/bermuda-core/internal/ws"
)

func main() {
	myFigure := figure.NewFigure("Bermuda Core", "", true)
	myFigure.Print()

	// Initialize logger
	logger.Init()

	// Initialize storage (DB abstraite)
	storage := storage.NewInMemoryStorage()
	logger.Info("Connected to DB")

	cfg, err := config.Load("../config.yaml")
	if err != nil {
		logger.Error("Failed to load config: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, exchange := range cfg.Exchanges {
		parser, err := registry.NewParser(exchange.Name)
		if err != nil {
			logger.Warn("Skipping exchange %s: %v", exchange.Name, err)
			continue
		}
		for _, pair := range exchange.Pairs {
			go func(pair config.PairConfig, parser market.Parser) {
				clientCtx, clientCancel := context.WithCancel(ctx)
				defer clientCancel()

				client := ws.NewClient(exchange.Name, pair, storage, parser)
				client.Connect(clientCtx, clientCancel)

				<-clientCtx.Done()
				logger.Warn("[%s/%s] Client stopped: %v", exchange.Name, pair.Symbol, clientCtx.Err())
			}(pair, parser)
		}
	}

	// Block until CTRL+C or kill signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")
	cancel()
}

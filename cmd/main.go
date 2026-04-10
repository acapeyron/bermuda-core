package main

import (
	"context"

	"github.com/common-nighthawk/go-figure"

	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/market/binance"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parser := &binance.BinanceParser{}
	client := ws.NewClient("wss://fstream.binance.com/ws/btcusdt@depth", storage, parser, "BTCUSDT")
	client.Connect(ctx)

	// Run indefinitely
	select {}
}

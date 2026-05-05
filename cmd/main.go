package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/acapeyron/bermuda-core/internal/arb"
	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/notifier"
	"github.com/acapeyron/bermuda-core/internal/registry"
	"github.com/acapeyron/bermuda-core/internal/ws"
	figure "github.com/common-nighthawk/go-figure"
	"github.com/joho/godotenv"
)

func main() {
	figure.NewFigure("Bermuda Core", "", true).Print()
	logger.Init()

	err := godotenv.Load("../.env")
	if err != nil {
		logger.Error("No .env file found")
	}

	token := os.Getenv("TELEGRAM_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHATID")

	if token == "" || chatID == "" {
		logger.Error("Missing TELEGRAM config")
		os.Exit(1)
	}

	telegramNotifier := notifier.New(token, chatID)

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

	client := ws.NewClient(cfg.Exchange.Name, cfg.Exchange.BaseWSURL, cfg.Exchange.Pairs, parser)
	go client.Connect(ctx, cancel)

	symbols := make([]string, 0, len(cfg.Exchange.Pairs))
	for _, p := range cfg.Exchange.Pairs {
		symbols = append(symbols, p.Symbol)
	}

	det := arb.NewTriangleDetector(0.00011, symbols)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ob := <-client.ObChan():
				det.UpdateOrderBook(&ob)
			case op := <-det.OpChan:
				liquidityWarn := ""
				if !op.HasFullLiquidity {
					liquidityWarn = "\n⚠️ Insufficient liquidity at $50 size"
				}
				msg := fmt.Sprintf(
					"🔺 Arb closed!\n"+
						"Triangle: %s\n"+
						"Profit: +%.4f%%\n"+
						"Duration: %dms\n"+
						"Size: $%.0f%s",
					op.Triangle,
					op.ProfitPct,
					op.DurationMs,
					arb.TradeSize,
					liquidityWarn,
				)
				go telegramNotifier.Send(msg)
				logger.Info("[OPPORTUNITY] triangle=%s profit=+%.4f%% duration=%dms", op.Triangle, op.ProfitPct, op.DurationMs)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")
	cancel()
}

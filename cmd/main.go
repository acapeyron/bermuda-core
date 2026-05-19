package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/acapeyron/bermuda-core/internal/arb"
	"github.com/acapeyron/bermuda-core/internal/config"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/notifier"
	"github.com/acapeyron/bermuda-core/internal/registry"
	"github.com/acapeyron/bermuda-core/internal/sim"
	"github.com/acapeyron/bermuda-core/internal/storage"
	"github.com/acapeyron/bermuda-core/internal/ws"
	figure "github.com/common-nighthawk/go-figure"
	"github.com/joho/godotenv"
)

func main() {
	figure.NewFigure("Bermuda Core", "", true).Print()
	logger.Init()

	if err := godotenv.Load("../.env"); err != nil {
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

	telegramNotifier.Send(fmt.Sprintf("🟢 Bermuda Core started (%d pairs, exchange: %s)", len(cfg.Exchange.Pairs), cfg.Exchange.Name))

	parser, err := registry.NewParser(cfg.Exchange.Name)
	if err != nil {
		logger.Error("Unknown exchange: %v", err)
		os.Exit(1)
	}

	// Open SQLite database. The file is created next to the binary.
	db, err := storage.Open("../data/bermuda.db")
	if err != nil {
		logger.Error("Failed to open database: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := ws.NewClient(cfg.Exchange.Name, cfg.Exchange.BaseWSURL, cfg.Exchange.Pairs, parser, telegramNotifier)
	go client.Connect(ctx, cancel)

	symbols := make([]string, 0, len(cfg.Exchange.Pairs))
	for _, p := range cfg.Exchange.Pairs {
		symbols = append(symbols, p.Symbol)
	}

	// Use the true taker fee (0.1 % per leg) as the detection threshold.
	// Gross profit must exceed 3 × 0.1 % = 0.3 % just to break even, so we
	// set the minimum threshold slightly above that to reduce noise.
	// The old value of 0.00011 (0.011 %) was far too low and would have
	// produced thousands of false-positive "opportunities".
	const detectionFeeThreshold = (3 * arb.TakerFeePerLeg) + sim.SlippagePerTriangle + 0.0005 // 0.05% safety margin
	det := arb.NewTriangleDetector(detectionFeeThreshold, symbols)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		lastUpdate := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(lastUpdate) > 2*time.Minute {
					logger.Error("[HEARTBEAT] No updates for 2min, shutting down")
					cancel() // déclenche l'arrêt propre → systemd relance
				}
			case ob := <-client.ObChan():
				lastUpdate = time.Now()
				det.UpdateOrderBook(&ob)

			case op := <-det.OpChan:
				// 1. Paper-trade simulation.
				pt := sim.Simulate(op)

				// 2. Persist to SQLite (non-blocking for the detection loop).
				go func(op arb.Opportunity, pt sim.PaperTrade) {
					id, err := db.SaveOpportunityAndTrade(op, pt)
					if err != nil {
						logger.Error("[STORAGE] Failed to save opportunity: %v", err)
						return
					}
					logger.Info("[STORAGE] Saved opportunity id=%d triangle=%s", id, op.Triangle)
				}(op, pt)

				// 3. Build Telegram notification.
				liquidityWarn := ""
				if !op.HasFullLiquidity {
					liquidityWarn = "\n⚠️ Insufficient depth at $50"
				}

				profitEmoji := "🟢"
				if !pt.IsProfitable {
					profitEmoji = "🔴"
				}

				msg := fmt.Sprintf(
					"🔺 Arb window closed!\n"+
						"Triangle:   %s\n"+
						"Duration:   %dms\n"+
						"\n"+
						"📊 Gross peak:  +%.4f%%\n"+
						"💸 Fees (3×):   −%.4f%%\n"+
						"📉 Execution drift:    −%.4f%%\n"+
						"%s Net:         %+.4f%%  ($%+.4f)%s",
					op.Triangle,
					op.DurationMs,
					pt.DepthAdjustedProfitPct,
					pt.TotalFeePct,
					pt.ExecutionDriftPct,
					profitEmoji,
					pt.NetProfitPct,
					pt.NetProfitUSD,
					liquidityWarn,
				)
				go telegramNotifier.Send(msg)

				logger.Info(
					"[OPPORTUNITY] triangle=%s peak=+%.4f%% net=%+.4f%% profitable=%v duration=%dms",
					op.Triangle, op.DepthAdjustedProfitPct, pt.NetProfitPct, pt.IsProfitable, op.DurationMs,
				)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	telegramNotifier.Send("🔴 Bermuda Core shutting down — manual intervention may be needed")
	time.Sleep(500 * time.Millisecond)
	logger.Info("Shutting down...")
	cancel()
}

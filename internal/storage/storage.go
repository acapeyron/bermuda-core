// Package storage persists arbitrage opportunities and paper-trade results to
// a local SQLite database. It is intentionally simple: one goroutine owns the
// write path so there is no need for a connection pool.
package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/acapeyron/bermuda-core/internal/arb"
	"github.com/acapeyron/bermuda-core/internal/logger"
	"github.com/acapeyron/bermuda-core/internal/sim"
)

const schema = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS opportunities (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    triangle           TEXT    NOT NULL,
    leg1_pair          TEXT    NOT NULL,
    leg1_side          TEXT    NOT NULL,
    leg2_pair          TEXT    NOT NULL,
    leg2_side          TEXT    NOT NULL,
    leg3_pair          TEXT    NOT NULL,
    leg3_side          TEXT    NOT NULL,
    open_rate          REAL    NOT NULL,
    peak_rate          REAL    NOT NULL,
    peak_profit_pct    REAL    NOT NULL,
    close_rate         REAL    NOT NULL,
    close_profit_pct   REAL    NOT NULL,
    has_full_liquidity INTEGER NOT NULL,   -- 1 / 0
    duration_ms        INTEGER NOT NULL,
    opened_at          TEXT    NOT NULL,   -- ISO-8601 wall-clock
    closed_at          TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id    INTEGER NOT NULL REFERENCES opportunities(id),
    gross_profit_pct  REAL    NOT NULL,
    total_fee_pct     REAL    NOT NULL,
    slippage_pct      REAL    NOT NULL,
    net_profit_pct    REAL    NOT NULL,
    net_profit_usd    REAL    NOT NULL,
    notional_usd      REAL    NOT NULL,
    is_profitable     INTEGER NOT NULL    -- 1 / 0
);

CREATE INDEX IF NOT EXISTS idx_opp_triangle  ON opportunities(triangle);
CREATE INDEX IF NOT EXISTS idx_opp_opened_at ON opportunities(opened_at);
CREATE INDEX IF NOT EXISTS idx_pt_opp_id     ON paper_trades(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_pt_profitable ON paper_trades(is_profitable);
`

// DB wraps a SQLite connection and exposes the write methods used by main.
type DB struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite file at path and applies the schema.
func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Single writer; WAL mode handles concurrent readers safely.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	logger.Info("[STORAGE] SQLite opened at %s", path)
	return &DB{db: db}, nil
}

// Close closes the underlying database connection.
func (s *DB) Close() error {
	return s.db.Close()
}

// SaveOpportunityAndTrade writes the closed opportunity and its paper trade
// result inside a single transaction. Returns the new opportunity row ID.
func (s *DB) SaveOpportunityAndTrade(op arb.Opportunity, pt sim.PaperTrade) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	oppID, err := insertOpportunity(tx, op)
	if err != nil {
		return 0, fmt.Errorf("insert opportunity: %w", err)
	}

	if err = insertPaperTrade(tx, oppID, pt); err != nil {
		return 0, fmt.Errorf("insert paper trade: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}

	return oppID, nil
}

func insertOpportunity(tx *sql.Tx, op arb.Opportunity) (int64, error) {
	const q = `
	INSERT INTO opportunities (
		triangle,
		leg1_pair, leg1_side,
		leg2_pair, leg2_side,
		leg3_pair, leg3_side,
		open_rate, peak_rate, peak_profit_pct,
		close_rate, close_profit_pct,
		has_full_liquidity,
		duration_ms,
		opened_at, closed_at
	) VALUES (?,  ?,?,  ?,?,  ?,?,  ?,?,?,  ?,?,  ?,  ?,  ?,?)`

	fullLiquidity := 0
	if op.HasFullLiquidity {
		fullLiquidity = 1
	}

	res, err := tx.Exec(q,
		op.Triangle,
		op.Legs[0].Pair, op.Legs[0].Side,
		op.Legs[1].Pair, op.Legs[1].Side,
		op.Legs[2].Pair, op.Legs[2].Side,
		op.OpenRate, op.PeakRate, op.PeakProfitPct,
		op.CloseRate, op.CloseProfitPct,
		fullLiquidity,
		op.DurationMs,
		op.OpenedAt.UTC().Format(time.RFC3339Nano),
		op.ClosedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertPaperTrade(tx *sql.Tx, oppID int64, pt sim.PaperTrade) error {
	const q = `
	INSERT INTO paper_trades (
		opportunity_id,
		gross_profit_pct, total_fee_pct, slippage_pct,
		net_profit_pct, net_profit_usd,
		notional_usd, is_profitable
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	isProfitable := 0
	if pt.IsProfitable {
		isProfitable = 1
	}

	_, err := tx.Exec(q,
		oppID,
		pt.GrossProfitPct, pt.TotalFeePct, pt.SlippagePct,
		pt.NetProfitPct, pt.NetProfitUSD,
		sim.NotionalUSD, isProfitable,
	)
	return err
}

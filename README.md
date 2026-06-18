# 🔺 Bermuda Core

A Go application that detects triangular arbitrage opportunities on Binance using live market data.

Built as a personal project to explore:

* Real-time systems
* WebSocket market feeds
* Order book processing
* Financial market mechanics
* Event-driven architecture

---

## What it does

Bermuda Core listens to Binance market streams and continuously evaluates triangular trading paths such as:

```text
USDT → BTC → ETH → USDT
```

For each cycle, it estimates whether the final amount would exceed the initial amount after trading fees.

The project does **not place real orders**.

It only performs paper trading and opportunity detection.

---

## Example

```text
Triangle: BTCUSDT → ETHBTC → ETHUSDT

Starting amount: 1000 USDT
Ending amount:   1004.20 USDT

Profit: +0.42%
```

---

## Tech Stack

* Go
* Binance WebSocket API
* SQLite
* Telegram Bot API

---

## Why I built it

I wanted a project that would force me to work with:

* Concurrent processing
* Streaming data
* State management
* External APIs
* Financial calculations

---

## Architecture

```text
Binance WebSocket
        │
        ▼
 Market Data Stream
        │
        ▼
 Arbitrage Detection
        │
        ▼
 Paper Trading Engine
        │
        ▼
 SQLite + Telegram
```

---

## Run locally

```bash
git clone https://github.com/acapeyron/bermuda-core.git

cd bermuda-core

go run ./cmd
```

---

## Future Ideas

* Multi-exchange support
* Historical backtesting
* Opportunity analytics dashboard
* More realistic execution simulation

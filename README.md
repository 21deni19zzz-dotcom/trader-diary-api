# Trader Diary API

Backend for Trader Diary — live exchange sync via CCXT.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/health | Server status |
| POST | /api/connect | Validate API keys |
| GET | /api/:exchange/balance | Account balance |
| GET | /api/:exchange/positions | Open positions |
| GET | /api/:exchange/trades | Closed trade history |
| GET | /api/:exchange/orders | Open orders |
| GET | /api/:exchange/pnl | P&L summary (custom calc) |

## Auth Headers
```
x-exchange: binance | bybit | bingx
x-api-key: YOUR_KEY
x-api-secret: YOUR_SECRET
```

## Supported Exchanges
- Binance (Futures)
- Bybit (Linear)
- BingX (Swap)

## P&L Logic
All P&L calculations are server-side with custom formulas, not exchange data.

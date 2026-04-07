import express from 'express';
import cors from 'cors';
import ccxt from 'ccxt';
import 'dotenv/config';

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors({ origin: '*' }));
app.use(express.json());

// ── Exchange factory ──────────────────────────────────────────────────────────
function makeExchange(exchangeId, apiKey, secret, passphrase) {
  const cfg = { apiKey, secret, enableRateLimit: true, options: {} };
  if (passphrase) cfg.password = passphrase;

  switch (exchangeId.toLowerCase()) {
    case 'binance':
      return new ccxt.binance({ ...cfg, options: { defaultType: 'future' } });
    case 'bybit':
      return new ccxt.bybit({ ...cfg, options: { defaultType: 'linear' } });
    case 'bingx':
      return new ccxt.bingx({ ...cfg, options: { defaultType: 'swap' } });
    default:
      throw new Error(`Unsupported exchange: ${exchangeId}`);
  }
}

// ── P&L Calculator (твоя логика, не биржевая) ────────────────────────────────
function calcPnL(trade) {
  const { side, entryPrice, exitPrice, amount, fee = 0 } = trade;
  if (!exitPrice || !entryPrice || !amount) return null;
  const gross = side === 'buy'
    ? (exitPrice - entryPrice) * amount
    : (entryPrice - exitPrice) * amount;
  return parseFloat((gross - fee).toFixed(6));
}

function calcPnLPct(trade) {
  const { entryPrice, exitPrice, side } = trade;
  if (!exitPrice || !entryPrice) return null;
  const pct = side === 'buy'
    ? ((exitPrice - entryPrice) / entryPrice) * 100
    : ((entryPrice - exitPrice) / entryPrice) * 100;
  return parseFloat(pct.toFixed(4));
}

function calcRR(trade) {
  const { entryPrice, stopLoss, takeProfit, side } = trade;
  if (!stopLoss || !takeProfit || !entryPrice) return null;
  const risk   = Math.abs(entryPrice - stopLoss);
  const reward = Math.abs(takeProfit - entryPrice);
  return risk > 0 ? parseFloat((reward / risk).toFixed(2)) : null;
}

function enrichTrade(raw) {
  const pnl    = calcPnL(raw);
  const pnlPct = calcPnLPct(raw);
  const rr     = calcRR(raw);
  return { ...raw, pnl, pnlPct, rr };
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function getHeaders(req) {
  return {
    apiKey:      req.headers['x-api-key']      || req.query.apiKey,
    secret:      req.headers['x-api-secret']   || req.query.secret,
    passphrase:  req.headers['x-passphrase']   || req.query.passphrase,
    exchange:    req.headers['x-exchange']     || req.query.exchange || req.params.exchange,
  };
}

function wrap(fn) {
  return async (req, res) => {
    try {
      await fn(req, res);
    } catch (e) {
      const msg = e.message || 'Unknown error';
      const status = msg.includes('AuthenticationError') || msg.includes('Invalid') ? 401
        : msg.includes('Unsupported') ? 400 : 500;
      res.status(status).json({ ok: false, error: msg });
    }
  };
}

// ── Routes ────────────────────────────────────────────────────────────────────

// Health
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString(), version: '1.0.0',
    supported: ['binance', 'bybit', 'bingx'] });
});

// Validate API keys
app.post('/api/connect', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = req.body;
  if (!exchange || !apiKey || !secret) return res.status(400).json({ ok: false, error: 'exchange, apiKey, secret required' });

  const ex = makeExchange(exchange, apiKey, secret, passphrase);
  const bal = await ex.fetchBalance();
  const total = bal.total?.USDT || bal.total?.USD || 0;

  res.json({ ok: true, exchange, balance: parseFloat(total.toFixed(2)),
    currencies: Object.keys(bal.total).filter(k => bal.total[k] > 0).slice(0, 10) });
}));

// Balance
app.get('/api/:exchange/balance', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = getHeaders(req);
  if (!apiKey || !secret) return res.status(400).json({ ok: false, error: 'x-api-key and x-api-secret headers required' });

  const ex = makeExchange(exchange, apiKey, secret, passphrase);
  const bal = await ex.fetchBalance();

  const wallets = {};
  for (const [currency, total] of Object.entries(bal.total)) {
    if (total > 0) {
      wallets[currency] = {
        total: parseFloat(total.toFixed(8)),
        free:  parseFloat((bal.free[currency]  || 0).toFixed(8)),
        used:  parseFloat((bal.used[currency]  || 0).toFixed(8)),
      };
    }
  }

  res.json({ ok: true, exchange, wallets,
    usdtTotal: parseFloat((bal.total?.USDT || bal.total?.USD || 0).toFixed(2)) });
}));

// Open positions
app.get('/api/:exchange/positions', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = getHeaders(req);
  if (!apiKey || !secret) return res.status(400).json({ ok: false, error: 'Headers required' });

  const ex = makeExchange(exchange, apiKey, secret, passphrase);
  const positions = await ex.fetchPositions();

  const active = positions
    .filter(p => p.contracts && parseFloat(p.contracts) > 0)
    .map(p => ({
      id:           p.id,
      symbol:       p.symbol,
      side:         p.side,
      size:         parseFloat(p.contracts || 0),
      entryPrice:   parseFloat(p.entryPrice || 0),
      markPrice:    parseFloat(p.markPrice  || 0),
      notional:     parseFloat(p.notional   || 0),
      leverage:     p.leverage,
      liquidation:  parseFloat(p.liquidationPrice || 0),
      unrealizedPnl: parseFloat((p.unrealizedPnl || 0).toFixed(4)),
      // твой расчёт P&L%
      pnlPct: p.entryPrice && p.markPrice
        ? parseFloat((((p.markPrice - p.entryPrice) / p.entryPrice * 100) * (p.side === 'short' ? -1 : 1)).toFixed(3))
        : null,
      margin:   parseFloat(p.initialMargin || 0),
      exchange,
    }));

  res.json({ ok: true, exchange, count: active.length, positions: active });
}));

// Closed trades history (твои расчёты P&L)
app.get('/api/:exchange/trades', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = getHeaders(req);
  if (!apiKey || !secret) return res.status(400).json({ ok: false, error: 'Headers required' });

  const symbol = req.query.symbol || undefined;
  const since  = req.query.since  ? parseInt(req.query.since)  : Date.now() - 90 * 86400000;
  const limit  = req.query.limit  ? parseInt(req.query.limit)  : 200;

  const ex = makeExchange(exchange, apiKey, secret, passphrase);

  let raw = [];
  if (symbol) {
    raw = await ex.fetchMyTrades(symbol, since, limit);
  } else {
    // Fetch for common pairs if no symbol given
    const markets = await ex.loadMarkets();
    const usdtPairs = Object.keys(markets)
      .filter(s => s.endsWith('/USDT:USDT') || s.endsWith('/USDT'))
      .slice(0, 20);

    for (const sym of usdtPairs) {
      try {
        const trades = await ex.fetchMyTrades(sym, since, limit);
        raw = [...raw, ...trades];
      } catch { /* skip unsupported pair */ }
    }
  }

  // Group buy/sell into round trips, compute our P&L
  const trades = matchTrades(raw, exchange);
  res.json({ ok: true, exchange, count: trades.length, trades });
}));

// Order history (open + closed orders)
app.get('/api/:exchange/orders', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = getHeaders(req);
  if (!apiKey || !secret) return res.status(400).json({ ok: false, error: 'Headers required' });

  const symbol = req.query.symbol || undefined;
  const since  = req.query.since  ? parseInt(req.query.since) : Date.now() - 30 * 86400000;

  const ex = makeExchange(exchange, apiKey, secret, passphrase);
  let orders = [];

  if (symbol) {
    orders = await ex.fetchOrders(symbol, since);
  } else {
    const openOrders = await ex.fetchOpenOrders();
    orders = openOrders;
  }

  const mapped = orders.map(o => ({
    id:       o.id,
    symbol:   o.symbol,
    side:     o.side,
    type:     o.type,
    status:   o.status,
    price:    o.price,
    amount:   o.amount,
    filled:   o.filled,
    remaining:o.remaining,
    cost:     o.cost,
    timestamp: o.timestamp,
    datetime:  o.datetime,
    exchange,
  }));

  res.json({ ok: true, exchange, count: mapped.length, orders: mapped });
}));

// ── P&L summary across all trades ────────────────────────────────────────────
app.get('/api/:exchange/pnl', wrap(async (req, res) => {
  const { exchange, apiKey, secret, passphrase } = getHeaders(req);
  if (!apiKey || !secret) return res.status(400).json({ ok: false, error: 'Headers required' });

  const since = req.query.since ? parseInt(req.query.since) : Date.now() - 30 * 86400000;
  const ex = makeExchange(exchange, apiKey, secret, passphrase);

  const markets = await ex.loadMarkets();
  const pairs = Object.keys(markets).filter(s => s.includes('USDT')).slice(0, 15);

  let allTrades = [];
  for (const sym of pairs) {
    try { allTrades = [...allTrades, ...await ex.fetchMyTrades(sym, since, 100)]; }
    catch {}
  }

  const matched = matchTrades(allTrades, exchange);

  const summary = {
    totalTrades:  matched.length,
    wins:         matched.filter(t => (t.pnl||0) > 0).length,
    losses:       matched.filter(t => (t.pnl||0) < 0).length,
    totalPnl:     parseFloat(matched.reduce((s,t) => s + (t.pnl||0), 0).toFixed(4)),
    bestTrade:    matched.reduce((m,t) => Math.max(m, t.pnl||0), -Infinity),
    worstTrade:   matched.reduce((m,t) => Math.min(m, t.pnl||0), Infinity),
    avgWin:       0, avgLoss: 0, profitFactor: 0, winRate: 0,
  };

  const wins   = matched.filter(t => (t.pnl||0) > 0).map(t => t.pnl||0);
  const losses = matched.filter(t => (t.pnl||0) < 0).map(t => t.pnl||0);
  summary.avgWin       = wins.length   ? parseFloat((wins.reduce((a,b)=>a+b,0)/wins.length).toFixed(4))   : 0;
  summary.avgLoss      = losses.length ? parseFloat((losses.reduce((a,b)=>a+b,0)/losses.length).toFixed(4)): 0;
  summary.profitFactor = summary.avgLoss ? parseFloat((-summary.avgWin / summary.avgLoss).toFixed(3)) : 0;
  summary.winRate      = matched.length  ? parseFloat((summary.wins / matched.length * 100).toFixed(2))    : 0;

  res.json({ ok: true, exchange, period: { since: new Date(since).toISOString(), to: new Date().toISOString() }, summary, trades: matched });
}));

// ── Trade matching logic (buy → sell pairs, твои расчёты) ────────────────────
function matchTrades(rawTrades, exchange) {
  // Sort by time
  const sorted = [...rawTrades].sort((a, b) => a.timestamp - b.timestamp);

  // Group by symbol
  const bySymbol = {};
  for (const t of sorted) {
    if (!bySymbol[t.symbol]) bySymbol[t.symbol] = [];
    bySymbol[t.symbol].push(t);
  }

  const result = [];

  for (const [symbol, trades] of Object.entries(bySymbol)) {
    // Simple FIFO matching
    const buys  = trades.filter(t => t.side === 'buy');
    const sells = trades.filter(t => t.side === 'sell');

    const matched = Math.min(buys.length, sells.length);
    for (let i = 0; i < matched; i++) {
      const buy  = buys[i];
      const sell = sells[i];
      const fee  = (buy.fee?.cost || 0) + (sell.fee?.cost || 0);

      const trade = {
        id:          `${buy.id}_${sell.id}`,
        symbol,
        exchange,
        direction:   'long',
        status:      'closed',
        entryPrice:  parseFloat((buy.price  || buy.average || 0).toFixed(6)),
        exitPrice:   parseFloat((sell.price || sell.average || 0).toFixed(6)),
        amount:      parseFloat((buy.amount || 0).toFixed(6)),
        fee:         parseFloat(fee.toFixed(6)),
        entryDate:   new Date(buy.timestamp).toISOString().slice(0, 10),
        exitDate:    new Date(sell.timestamp).toISOString().slice(0, 10),
        entryTs:     buy.timestamp,
        exitTs:      sell.timestamp,
        side:        'buy',
      };
      result.push(enrichTrade(trade));
    }

    // Unmatched sells (short side)
    for (let i = matched; i < sells.length; i++) {
      const sell = sells[i];
      const correspondingBuy = buys[matched + i] || null;
      if (!correspondingBuy) continue;
      const fee = (sell.fee?.cost || 0) + (correspondingBuy.fee?.cost || 0);
      const trade = {
        id:        `${sell.id}_short`,
        symbol,
        exchange,
        direction: 'short',
        status:    'closed',
        entryPrice: parseFloat((sell.price || sell.average || 0).toFixed(6)),
        exitPrice:  parseFloat((correspondingBuy.price || correspondingBuy.average || 0).toFixed(6)),
        amount:    parseFloat((sell.amount || 0).toFixed(6)),
        fee:       parseFloat(fee.toFixed(6)),
        entryDate: new Date(sell.timestamp).toISOString().slice(0, 10),
        exitDate:  new Date(correspondingBuy.timestamp).toISOString().slice(0, 10),
        entryTs:   sell.timestamp,
        exitTs:    correspondingBuy.timestamp,
        side:      'sell',
      };
      result.push(enrichTrade(trade));
    }
  }

  return result.sort((a, b) => b.entryTs - a.entryTs);
}

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`🚀 Trader Diary API running on port ${PORT}`);
  console.log(`   Supported: Binance | Bybit | BingX`);
  console.log(`   P&L: custom server-side calculations`);
});

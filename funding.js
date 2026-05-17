// ─────────────────────────────────────────────────────────────────────
// funding.js — Sprint 23.B funding-fee integration
//
// Perpetual futures positions pay/receive funding every ~8h. Without
// these, displayed P&L diverges from real wallet balance. This module:
//   1. Fetches funding history (BingX direct REST + CCXT fallback)
//   2. Matches each funding event to its parent closed trade by symbol
//      and timestamp window (entry_date ≤ funding.ts ≤ exit_date ±1h slop)
//   3. Returns a Map<tradeId, summed funding> for the caller to upsert
//
// Decisions:
//   * BingX REST `/openApi/swap/v2/user/income?incomeType=FUNDING_FEE`
//     is reached directly (signed) because CCXT BingX's funding wrapper
//     has the same parsePosition-style flakiness we hit in Sprint 20.2.
//   * For Binance/Bybit we use CCXT's fetchFundingHistory — it works
//     reliably on those adapters.
//   * Timestamp slop ±1h on both sides absorbs timezone fuzz between
//     exchange-side fill timestamps and our `entry_date`/`exit_date`
//     (which are date-truncated YYYY-MM-DD strings).
//   * Symbol must match EXACTLY — funding for BTCUSDT doesn't bleed
//     onto ETHUSDT.
// ─────────────────────────────────────────────────────────────────────

import crypto from 'node:crypto';

/**
 * Fetch BingX funding history via direct REST. Bypasses CCXT for the
 * same reason Sprint 20.2 fetched positionHistory directly — CCXT's
 * BingX adapter throws on the response shape currently in production.
 *
 * Returns: [{ symbol, income, ts }] where income is in quote currency
 * (positive = received, negative = paid), ts is unix milliseconds.
 */
export async function fetchBingXFundingHistory(apiKey, secret, sinceMs) {
  const params = new URLSearchParams();
  params.set('incomeType', 'FUNDING_FEE');
  if (Number.isFinite(+sinceMs)) params.set('startTime', String(Math.floor(+sinceMs)));
  params.set('limit', '1000');
  params.set('timestamp', String(Date.now()));

  const query = params.toString();
  const signature = crypto.createHmac('sha256', secret).update(query).digest('hex');
  const url = `https://open-api.bingx.com/openApi/swap/v2/user/income?${query}&signature=${signature}`;

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 15_000);
  let data;
  try {
    const res = await fetch(url, { method: 'GET', headers: { 'X-BX-APIKEY': apiKey }, signal: ctrl.signal });
    data = await res.json().catch(() => ({}));
  } finally {
    clearTimeout(timer);
  }
  if (data?.code !== 0) {
    throw new Error(`BingX funding: ${data?.msg || `code ${data?.code}`}`);
  }

  // BingX returns array directly under data, or inside data.list — handle both.
  const raw = Array.isArray(data.data) ? data.data : (data.data?.list || []);
  return raw
    .map((r) => ({
      // Normalize "BTC-USDT" → "BTC/USDT:USDT" so it lines up with the
      // symbol shape we store in trades (from rowFromBingXPosition).
      symbol: normaliseBingxSymbol(r.symbol),
      income: parseFloat(r.income),
      ts: Number(r.time),
    }))
    .filter((r) => r.symbol && Number.isFinite(r.income) && Number.isFinite(r.ts));
}

function normaliseBingxSymbol(s) {
  if (!s) return null;
  if (s.includes('/')) return s; // already CCXT-style
  if (s.includes('-')) {
    const [base, quote] = s.split('-');
    return `${base}/${quote}:${quote}`;
  }
  return s;
}

/**
 * Fetch funding history via CCXT (Binance, Bybit). Some CCXT adapters
 * expose fetchFundingHistory directly. Returns the same normalised shape.
 */
export async function fetchFundingHistoryCcxt(ex, sinceMs) {
  if (typeof ex.fetchFundingHistory !== 'function') {
    throw new Error(`Exchange ${ex.id || 'unknown'} doesn't support fetchFundingHistory`);
  }
  const raw = await ex.fetchFundingHistory(undefined, sinceMs, 500);
  return (raw || [])
    .map((r) => ({
      symbol: r.symbol,
      income: parseFloat(r.amount),
      ts: +r.timestamp,
    }))
    .filter((r) => r.symbol && Number.isFinite(r.income) && Number.isFinite(r.ts));
}

/**
 * Match funding events to closed trades by symbol + timestamp window.
 *
 * Match criteria:
 *   - same symbol (exact)
 *   - funding.ts ∈ [entry_ms − 1h, exit_ms + 1h]
 *
 * Trades expected shape: { id, symbol, entry_date, exit_date }
 * Funding events:        { symbol, income, ts }
 *
 * Returns Map<tradeId, summedFunding>. A funding event that falls in
 * multiple trades' windows (rare, but possible if two trades on the
 * same symbol overlap) is attributed to EACH overlapping trade — this
 * is conservative; the realistic case is non-overlapping positions so
 * the sum is correct in practice.
 */
export function matchFundingToTrades(fundingEvents, trades) {
  const SLOP_MS = 3_600_000; // ±1h
  const updates = new Map();

  // Pre-index trades by symbol so we don't do O(funding × trades) scans
  // on every event when symbol counts are large.
  const tradesBySymbol = new Map();
  for (const t of trades || []) {
    if (!t?.symbol) continue;
    if (!tradesBySymbol.has(t.symbol)) tradesBySymbol.set(t.symbol, []);
    tradesBySymbol.get(t.symbol).push(t);
  }

  for (const f of fundingEvents || []) {
    const bucket = tradesBySymbol.get(f.symbol);
    if (!bucket) continue;
    for (const t of bucket) {
      const entryMs = t.entry_date ? new Date(t.entry_date).getTime() : null;
      const exitMs  = t.exit_date  ? new Date(t.exit_date).getTime()  : Date.now();
      if (!Number.isFinite(entryMs)) continue;
      if (f.ts < entryMs - SLOP_MS) continue;
      if (f.ts > exitMs + SLOP_MS)  continue;
      updates.set(t.id, (updates.get(t.id) || 0) + f.income);
    }
  }

  return updates;
}

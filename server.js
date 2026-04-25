import express from 'express';
import cors from 'cors';
import ccxt from 'ccxt';
import cron from 'node-cron';
import crypto from 'node:crypto';
import { createClient } from '@supabase/supabase-js';
import 'dotenv/config';

const app  = express();
const PORT = process.env.PORT || 3001;

// ── Config (no hardcoded fallbacks) ───────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('FATAL: SUPABASE_URL and SUPABASE_SERVICE_KEY required');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_API    = `https://api.telegram.org/bot${BOT_TOKEN}`;
const FRONTEND  = process.env.FRONTEND_URL || 'https://trader-diary-rust.vercel.app';
const ENC_KEY   = process.env.ENCRYPTION_KEY || crypto.randomBytes(32).toString('hex');

// ── Security middleware ───────────────────────────────────────────────────────
const ALLOWED_ORIGINS = [
  FRONTEND,
  'https://trader-diary-rust.vercel.app',
  'http://localhost:5173',
  'http://localhost:3000',
].filter(Boolean);

app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    cb(new Error('CORS blocked'));
  },
  credentials: true,
  methods: ['GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.removeHeader('X-Powered-By');
  next();
});

app.use(express.json({ limit: '100kb' }));

// Rate limiting (in-memory)
const rlMap = new Map();
const RL_WINDOW = 60000, RL_MAX = 60;
function rateLimit(req, res, next) {
  if (req.path === '/api/webhook/telegram') return next();
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip;
  const now = Date.now();
  let e = rlMap.get(ip);
  if (!e || now - e.s > RL_WINDOW) { e = { s: now, c: 0 }; rlMap.set(ip, e); }
  if (++e.c > RL_MAX) return res.status(429).json({ ok: false, error: 'Too many requests' });
  res.setHeader('X-RateLimit-Remaining', Math.max(0, RL_MAX - e.c));
  next();
}
app.use(rateLimit);
setInterval(() => { const c = Date.now() - RL_WINDOW * 2; for (const [k, v] of rlMap) if (v.s < c) rlMap.delete(k); }, 300000);

// ── Helpers ───────────────────────────────────────────────────────────────────
const wrap = fn => async (req, res) => {
  try { await fn(req, res); }
  catch (e) {
    console.error(`[ERR] ${req.method} ${req.path}:`, e.message);
    const code = e.message?.includes('Validation') ? 400 : e.message?.includes('401') || e.message?.includes('Auth') ? 401 : e.message?.includes('Not found') ? 404 : 500;
    res.status(code).json({ ok: false, error: e.message || 'Internal error' });
  }
};

async function tgSend(chat_id, text, extra = {}) {
  if (!BOT_TOKEN) return;
  try { await fetch(`${TG_API}/sendMessage`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id, text, parse_mode: 'HTML', ...extra }) }); }
  catch (e) { console.error('[TG]', e.message); }
}

// ── Encryption ────────────────────────────────────────────────────────────────
function encrypt(text) {
  const key = Buffer.from(ENC_KEY.slice(0, 64), 'hex');
  const iv = crypto.randomBytes(16);
  const c = crypto.createCipheriv('aes-256-cbc', key, iv);
  return iv.toString('hex') + ':' + c.update(text, 'utf8', 'hex') + c.final('hex');
}
function decrypt(data) {
  const key = Buffer.from(ENC_KEY.slice(0, 64), 'hex');
  const [ivH, enc] = data.split(':');
  const d = crypto.createDecipheriv('aes-256-cbc', key, Buffer.from(ivH, 'hex'));
  return d.update(enc, 'hex', 'utf8') + d.final('utf8');
}

// ── Validation ────────────────────────────────────────────────────────────────
function validateFills(fills, errArr) {
  if (fills == null || fills === '') return [];
  if (!Array.isArray(fills)) { errArr.push('fills: must be array'); return []; }
  if (fills.length > 50) { errArr.push('fills: max 50 entries'); return []; }
  const out = [];
  const safeNote = s => s ? String(s).slice(0, 200).replace(/<[^>]*>/g, '') : null;
  for (let i = 0; i < fills.length; i++) {
    const f = fills[i] || {};
    const q = parseFloat(f.qty);
    const p = parseFloat(f.price);
    if (isNaN(q) || q <= 0) { errArr.push(`fills[${i}].qty: positive number`); continue; }
    if (isNaN(p) || p <= 0) { errArr.push(`fills[${i}].price: positive number`); continue; }
    out.push({
      id: typeof f.id === 'string' ? f.id.slice(0, 64) : null,
      qty: q,
      price: p,
      date: f.date && typeof f.date === 'string' ? f.date.slice(0, 10) : new Date().toISOString().slice(0, 10),
      note: safeNote(f.note),
      pnl: f.pnl != null && !isNaN(parseFloat(f.pnl)) ? parseFloat(f.pnl) : null,
      created_at: f.created_at && typeof f.created_at === 'string' ? f.created_at : new Date().toISOString(),
    });
  }
  return out;
}

function validateShots(shots, errArr) {
  if (shots == null || shots === '') return [];
  if (!Array.isArray(shots)) { errArr.push('shots: must be array'); return []; }
  if (shots.length > 8) { errArr.push('shots: max 8 entries'); return []; }
  const out = [];
  for (let i = 0; i < shots.length; i++) {
    const s = shots[i] || {};
    const id  = typeof s.id  === 'string' ? s.id.slice(0, 64)   : null;
    const url = typeof s.url === 'string' ? s.url.slice(0, 1000) : null;
    if (!id)  { errArr.push(`shots[${i}].id: required string`);  continue; }
    if (!url) { errArr.push(`shots[${i}].url: required string`); continue; }
    // Accept https://... and supabase storage paths
    if (!/^https?:\/\//i.test(url) && !url.startsWith('ptj-screenshots/')) {
      errArr.push(`shots[${i}].url: must be http(s) URL or ptj-screenshots/ path`); continue;
    }
    out.push({
      id,
      url,
      uploaded_at: s.uploaded_at && typeof s.uploaded_at === 'string'
        ? s.uploaded_at
        : new Date().toISOString(),
    });
  }
  return out;
}

const sumFillsQty = fs => (fs || []).reduce((s, f) => s + +f.qty, 0);
const fillsWavgPrice = fs => {
  const tq = sumFillsQty(fs);
  if (!tq) return null;
  return fs.reduce((s, f) => s + +f.qty * +f.price, 0) / tq;
};
const fillsRealizedPnL = (fs, dir, ep) => {
  if (!fs || !fs.length) return 0;
  return fs.reduce((s, f) => {
    const fp = f.pnl != null ? +f.pnl : (dir === 'long' ? (+f.price - ep) * +f.qty : (ep - +f.price) * +f.qty);
    return s + fp;
  }, 0);
};

function validateTrade(t) {
  const err = [];
  if (!t.symbol || typeof t.symbol !== 'string' || t.symbol.trim().length < 2 || t.symbol.trim().length > 20)
    err.push('symbol: 2-20 chars');
  if (!['long', 'short'].includes(t.direction)) err.push('direction: long/short');
  if (!['open', 'closed'].includes(t.status || 'open')) err.push('status: open/closed');
  const ep = parseFloat(t.entry_price);
  if (isNaN(ep) || ep <= 0 || ep > 1e12) err.push('entry_price: positive number');
  const qty = parseFloat(t.quantity);
  if (isNaN(qty) || qty <= 0 || qty > 1e15) err.push('quantity: positive number');
  if (t.exit_price != null && t.exit_price !== '') {
    const xp = parseFloat(t.exit_price); if (isNaN(xp) || xp <= 0) err.push('exit_price: positive number');
  }
  if (t.stop_loss != null && t.stop_loss !== '') {
    const sl = parseFloat(t.stop_loss); if (isNaN(sl) || sl <= 0) err.push('stop_loss: positive');
  }
  if (t.take_profit != null && t.take_profit !== '') {
    const tp = parseFloat(t.take_profit); if (isNaN(tp) || tp <= 0) err.push('take_profit: positive');
  }
  const fillsClean = validateFills(t.fills, err);
  const shotsClean = validateShots(t.shots, err);
  const safe = s => s ? String(s).slice(0, 200).replace(/<[^>]*>/g, '') : null;
  return { err, clean: {
    symbol: String(t.symbol || '').toUpperCase().trim().slice(0, 20), direction: t.direction,
    status: t.status || 'open',
    entry_price: ep, exit_price: t.exit_price != null && t.exit_price !== '' ? parseFloat(t.exit_price) : null,
    quantity: qty,
    entry_date: t.entry_date || new Date().toISOString().slice(0, 10), exit_date: t.exit_date || null,
    stop_loss: t.stop_loss != null && t.stop_loss !== '' ? parseFloat(t.stop_loss) : null,
    take_profit: t.take_profit != null && t.take_profit !== '' ? parseFloat(t.take_profit) : null,
    setup: safe(t.setup), emotion: safe(t.emotion), notes: safe(t.notes),
    exchange: safe(t.exchange) || 'manual',
    fills: fillsClean,
    shots: shotsClean,
  }};
}

const VALID_EX = ['binance', 'bybit', 'bingx'];
const valEx = id => { if (!VALID_EX.includes(id)) throw new Error(`Validation: Unsupported exchange "${id}"`); };

// ── P&L ───────────────────────────────────────────────────────────────────────
const calcPnL    = (d, ep, xp, q) => xp ? +((d === 'long' ? xp - ep : ep - xp) * q).toFixed(8) : null;
const calcPnLPct = (d, ep, xp)    => xp && ep ? +((d === 'long' ? xp - ep : ep - xp) / ep * 100).toFixed(4) : null;
const calcRR     = (ep, sl, tp)    => { if (!sl || !tp) return null; const r = Math.abs(ep - sl), w = Math.abs(tp - ep); return r > 0 ? +(w / r).toFixed(2) : null; };

// ── Auth middleware ────────────────────────────────────────────────────────────
async function requireAuth(req, res, next) {
  const token = (req.headers.authorization || '').replace('Bearer ', '').trim();
  if (!token) return res.status(401).json({ ok: false, error: 'No auth token' });
  const { data, error } = await supabase.from('auth_tokens')
    .select('telegram_user_id, expires_at, users(first_name, username)')
    .eq('token', token).eq('revoked', false).gt('expires_at', new Date().toISOString()).single();
  if (error || !data) return res.status(401).json({ ok: false, error: 'Invalid or expired token' });
  const { data: sub } = await supabase.from('subscriptions').select('id,status,expires_at')
    .eq('telegram_user_id', data.telegram_user_id).in('status', ['active', 'trial'])
    .gt('expires_at', new Date().toISOString()).order('expires_at', { ascending: false }).limit(1).single();
  if (!sub) return res.status(403).json({ ok: false, error: 'Subscription expired. Renew at @ParadoxxShop_bot' });
  supabase.from('auth_tokens').update({ last_used_at: new Date().toISOString() }).eq('token', token).then(() => {});
  req.userId = data.telegram_user_id;
  req.user = data.users;
  req.sub = sub;
  next();
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/health', (_, res) => res.json({ ok: true, version: '3.0.0', time: new Date().toISOString() }));

// ── Auth ──────────────────────────────────────────────────────────────────────
app.post('/api/auth/verify', wrap(async (req, res) => {
  const { token } = req.body;
  if (!token || typeof token !== 'string') return res.status(400).json({ ok: false, error: 'token required' });
  const { data, error } = await supabase.from('auth_tokens')
    .select('telegram_user_id, expires_at, users(first_name, username)')
    .eq('token', token).eq('revoked', false).gt('expires_at', new Date().toISOString()).single();
  if (error || !data) return res.status(401).json({ ok: false, error: 'Invalid or expired token' });
  const { data: sub } = await supabase.from('subscriptions').select('expires_at,status')
    .eq('telegram_user_id', data.telegram_user_id).in('status', ['active', 'trial'])
    .gt('expires_at', new Date().toISOString()).order('expires_at', { ascending: false }).limit(1).single();
  if (!sub) return res.status(403).json({ ok: false, error: 'No active subscription' });
  res.json({ ok: true, token,
    user: { telegram_user_id: data.telegram_user_id, name: data.users?.first_name || 'Trader', username: data.users?.username },
    subscription: { expires_at: sub.expires_at, status: sub.status }
  });
}));

// ── Webhook ───────────────────────────────────────────────────────────────────
app.post('/api/webhook/telegram', wrap(async (req, res) => {
  const upd = req.body;
  res.json({ ok: true });
  if (upd.pre_checkout_query) {
    await fetch(`${TG_API}/answerPreCheckoutQuery`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pre_checkout_query_id: upd.pre_checkout_query.id, ok: true }) });
    return;
  }
  if (upd.message?.successful_payment) {
    const msg = upd.message, pay = msg.successful_payment, tg = msg.from;
    const [slug] = pay.invoice_payload.split('|');
    try {
      await supabase.from('users').upsert({ telegram_user_id: tg.id, username: tg.username || null,
        first_name: tg.first_name || null, last_name: tg.last_name || null, updated_at: new Date().toISOString()
      }, { onConflict: 'telegram_user_id' });
      const { data: prod } = await supabase.from('products').select('*').eq('slug', slug).single();
      if (!prod) return;
      const expiresAt = new Date(Date.now() + prod.duration_days * 86400000).toISOString();
      const { data: sub } = await supabase.from('subscriptions').insert({
        telegram_user_id: tg.id, product_id: prod.slug, status: 'active',
        expires_at: expiresAt, payment_method: 'stars', amount_paid: pay.total_amount
      }).select().single();

      // Revoke any existing active tokens before creating new one (renewal cleanup)
      await supabase.from('auth_tokens').update({ revoked: true })
        .eq('telegram_user_id', tg.id).eq('revoked', false);

      const { data: tok } = await supabase.from('auth_tokens').insert({
        telegram_user_id: tg.id, expires_at: expiresAt
      }).select('token').single();
      await supabase.from('telegram_events').insert({
        event_type: 'payment_success', telegram_user_id: tg.id, product_id: prod.slug,
        payment_method: 'stars', payment_tx_id: pay.telegram_payment_charge_id,
        idempotency_key: pay.telegram_payment_charge_id, related_subscription_id: sub?.id,
        metadata: { amount: pay.total_amount, currency: pay.currency }
      });
      const link = `${FRONTEND}?token=${tok?.token}`;
      const exp = new Date(expiresAt).toLocaleDateString('ru-RU');
      // Message 1: confirmation + login link
      await tgSend(tg.id, `✅ <b>Доступ открыт!</b>\n\n📦 <b>${prod.name}</b>\n📅 До: <b>${exp}</b>\n\n🔗 <b>Войти одним кликом:</b>\n${link}\n\n💡 Персональный ключ — не передавай.`);
      // Message 2: token separately for manual copy/paste backup
      await tgSend(tg.id, `🔐 <b>Твой персональный токен доступа</b>\n\n<code>${tok?.token}</code>\n\n💡 Скопируй и сохрани этот токен. Если ссылка не работает — вставь токен вручную на странице входа:\n${FRONTEND}\n\n⚠️ Не передавай токен третьим лицам — это ключ к твоему журналу.`);
    } catch (e) { console.error('Webhook error:', e.message); }
    return;
  }
  if (upd.message?.text?.startsWith('/start')) {
    const tg = upd.message.from;
    await tgSend(tg.id, `👋 <b>Привет, ${tg.first_name}!</b>\n\n📊 <b>Trader Diary</b> — профессиональный торговый журнал.\n\n• Binance / Bybit / BingX\n• P&L с нашими формулами\n• Аналитика + equity curve\n\n🛍 Купить: @ParadoxxShop_bot`);
  }
}));

// ── Trades CRUD ───────────────────────────────────────────────────────────────
app.get('/api/trades', requireAuth, wrap(async (req, res) => {
  const { data, error } = await supabase.from('trades').select('*')
    .eq('telegram_user_id', req.userId).order('entry_date', { ascending: false });
  if (error) throw new Error(error.message);
  res.json({ ok: true, count: data.length, trades: data });
}));

app.post('/api/trades', requireAuth, wrap(async (req, res) => {
  const { err: e, clean: t } = validateTrade(req.body);
  if (e.length) throw new Error(`Validation: ${e.join('; ')}`);
  const { data, error } = await supabase.from('trades').insert({
    telegram_user_id: req.userId, symbol: t.symbol, direction: t.direction, status: t.status,
    entry_price: t.entry_price, exit_price: t.exit_price, quantity: t.quantity,
    entry_date: t.entry_date, exit_date: t.exit_date,
    stop_loss: t.stop_loss, take_profit: t.take_profit,
    pnl: calcPnL(t.direction, t.entry_price, t.exit_price, t.quantity),
    pnl_pct: calcPnLPct(t.direction, t.entry_price, t.exit_price),
    rr: calcRR(t.entry_price, t.stop_loss, t.take_profit),
    setup: t.setup, emotion: t.emotion, notes: t.notes, exchange: t.exchange,
    fills: t.fills,
    shots: t.shots,
  }).select().single();
  if (error) throw new Error(error.message);
  res.json({ ok: true, trade: data });
}));

app.patch('/api/trades/:id', requireAuth, wrap(async (req, res) => {
  const id = req.params.id;
  if (!id) throw new Error('Validation: invalid trade ID');
  const { err: e, clean: t } = validateTrade(req.body);
  if (e.length) throw new Error(`Validation: ${e.join('; ')}`);

  const upd = { symbol: t.symbol, direction: t.direction, status: t.status,
    entry_price: t.entry_price, exit_price: t.exit_price, quantity: t.quantity,
    entry_date: t.entry_date, exit_date: t.exit_date, stop_loss: t.stop_loss, take_profit: t.take_profit,
    setup: t.setup, emotion: t.emotion, notes: t.notes,
    fills: t.fills,
    shots: t.shots,
    updated_at: new Date().toISOString(),
  };

  // Auto-close when partial fills sum up to full quantity
  const closedQty = sumFillsQty(t.fills);
  const fullyFilled = t.fills.length > 0 && Math.abs(closedQty - t.quantity) < 1e-9;
  if (fullyFilled) {
    upd.status = 'closed';
    if (upd.exit_price == null) upd.exit_price = fillsWavgPrice(t.fills);
    if (upd.exit_date == null) upd.exit_date = t.fills[t.fills.length - 1].date;
  }
  // Legacy: single-exit trigger
  if (upd.exit_price) upd.status = 'closed';

  // P&L: prefer fills when present, else classic exit-price calc
  if (t.fills.length > 0) {
    upd.pnl = +fillsRealizedPnL(t.fills, t.direction, t.entry_price).toFixed(8);
    const entryValue = t.entry_price * t.quantity;
    upd.pnl_pct = entryValue > 0 ? +((upd.pnl / entryValue) * 100).toFixed(4) : null;
  } else {
    upd.pnl = calcPnL(t.direction, t.entry_price, upd.exit_price, t.quantity);
    upd.pnl_pct = calcPnLPct(t.direction, t.entry_price, upd.exit_price);
  }
  upd.rr = calcRR(t.entry_price, t.stop_loss, t.take_profit);

  const { data, error } = await supabase.from('trades').update(upd)
    .eq('id', id).eq('telegram_user_id', req.userId).select().single();
  if (error) throw new Error(error.message);
  res.json({ ok: true, trade: data });
}));

app.delete('/api/trades/:id', requireAuth, wrap(async (req, res) => {
  if (!req.params.id) throw new Error('Validation: invalid trade ID');
  const { error } = await supabase.from('trades').delete()
    .eq('id', req.params.id).eq('telegram_user_id', req.userId);
  if (error) throw new Error(error.message);
  res.json({ ok: true });
}));

// ── Exchange (credentials stored encrypted in Supabase) ──────────────────────
function mkEx(id, k, s) {
  valEx(id);
  const cfg = { apiKey: k, secret: s, enableRateLimit: true, timeout: 15000 };
  if (id === 'binance') return new ccxt.binance({ ...cfg, options: { defaultType: 'future' } });
  if (id === 'bybit')   return new ccxt.bybit({ ...cfg, options: { defaultType: 'linear' } });
  if (id === 'bingx')   return new ccxt.bingx({ ...cfg, options: { defaultType: 'swap' } });
}

async function getStoredEx(userId, exId) {
  valEx(exId);
  const { data } = await supabase.from('exchange_connections')
    .select('api_key_enc, secret_enc')
    .eq('telegram_user_id', userId).eq('exchange', exId).single();
  if (!data) throw new Error('Not found: Exchange not connected');
  return mkEx(exId, decrypt(data.api_key_enc), decrypt(data.secret_enc));
}

// Connect + store creds
app.post('/api/exchange/connect', requireAuth, wrap(async (req, res) => {
  const { exchange, apiKey, secret } = req.body;
  if (!exchange || !apiKey || !secret) throw new Error('Validation: exchange, apiKey, secret required');
  valEx(exchange);
  const ex = mkEx(exchange, apiKey.trim(), secret.trim());
  const bal = await ex.fetchBalance();
  await supabase.from('exchange_connections').upsert({
    telegram_user_id: req.userId, exchange,
    api_key_enc: encrypt(apiKey.trim()), secret_enc: encrypt(secret.trim()),
    connected_at: new Date().toISOString(), last_used_at: new Date().toISOString(),
  }, { onConflict: 'telegram_user_id,exchange' });
  res.json({ ok: true, exchange, balance: +(bal.total?.USDT || 0).toFixed(2) });
}));

// Saved connections list
app.get('/api/exchange/connections', requireAuth, wrap(async (req, res) => {
  const { data } = await supabase.from('exchange_connections')
    .select('exchange, connected_at, last_used_at')
    .eq('telegram_user_id', req.userId);
  res.json({ ok: true, connections: data || [] });
}));

// Disconnect
app.delete('/api/exchange/:exchange', requireAuth, wrap(async (req, res) => {
  valEx(req.params.exchange);
  await supabase.from('exchange_connections').delete()
    .eq('telegram_user_id', req.userId).eq('exchange', req.params.exchange);
  res.json({ ok: true });
}));

// Balance (uses stored creds)
app.post('/api/exchange/balance', requireAuth, wrap(async (req, res) => {
  const { exchange } = req.body;
  valEx(exchange);
  const ex = await getStoredEx(req.userId, exchange);
  const bal = await ex.fetchBalance();
  const w = {};
  for (const [k, v] of Object.entries(bal.total)) if (v > 0) w[k] = { total: +v.toFixed(8), free: +(bal.free[k] || 0).toFixed(8) };
  supabase.from('exchange_connections').update({ last_used_at: new Date().toISOString() })
    .eq('telegram_user_id', req.userId).eq('exchange', exchange).then(() => {});
  res.json({ ok: true, exchange, wallets: w, usdtTotal: +(bal.total?.USDT || 0).toFixed(2) });
}));

// Positions (uses stored creds)
app.post('/api/exchange/positions', requireAuth, wrap(async (req, res) => {
  const { exchange } = req.body;
  valEx(exchange);
  const ex = await getStoredEx(req.userId, exchange);
  const pos = (await ex.fetchPositions()).filter(p => +(p.contracts || 0) > 0).map(p => ({
    symbol: p.symbol, side: p.side, size: +p.contracts, entryPrice: +p.entryPrice,
    markPrice: +p.markPrice, unrealizedPnl: +p.unrealizedPnl, leverage: p.leverage,
    pnlPct: p.entryPrice && p.markPrice ? +((p.markPrice - p.entryPrice) / p.entryPrice * 100 * (p.side === 'short' ? -1 : 1)).toFixed(3) : null,
    exchange,
  }));
  res.json({ ok: true, count: pos.length, positions: pos });
}));

// Legacy redirect
app.post('/api/connect', requireAuth, (req, res) => {
  req.url = '/api/exchange/connect';
  app.handle(req, res);
});

// ── Cron ──────────────────────────────────────────────────────────────────────
async function sendReminders() {
  const now = new Date();
  for (const [days, type] of [[5, '5day'], [1, '1day']]) {
    const from = new Date(now.getTime() + days * 86400000).toISOString();
    const to   = new Date(now.getTime() + (days + 1) * 86400000).toISOString();
    const { data: subs } = await supabase.from('subscriptions').select('id,telegram_user_id,expires_at')
      .eq('status', 'active').gt('expires_at', from).lt('expires_at', to);
    for (const sub of subs || []) {
      const { error } = await supabase.from('reminder_log').insert({
        telegram_user_id: sub.telegram_user_id, subscription_id: sub.id, reminder_type: type
      });
      if (!error) {
        const d = new Date(sub.expires_at).toLocaleDateString('ru-RU');
        await tgSend(sub.telegram_user_id, days === 5
          ? `⏰ <b>Подписка истекает через 5 дней</b>\n📅 До: <b>${d}</b>\n\n🔄 Продли: @ParadoxxShop_bot`
          : `🚨 <b>Последний день подписки!</b>\n\nЗавтра доступ закроется.\n⚡ Продли: @ParadoxxShop_bot`);
      }
    }
    console.log(`[CRON] ${type}: ${subs?.length || 0}`);
  }

  // Mark expired subscriptions and revoke their tokens
  const { data: expiredSubs } = await supabase.from('subscriptions')
    .select('telegram_user_id')
    .eq('status', 'active').lt('expires_at', now.toISOString());

  await supabase.from('subscriptions').update({ status: 'expired' })
    .eq('status', 'active').lt('expires_at', now.toISOString());

  // Revoke tokens of users whose subscription just expired
  if (expiredSubs?.length) {
    const userIds = [...new Set(expiredSubs.map(s => s.telegram_user_id))];
    const { count } = await supabase.from('auth_tokens')
      .update({ revoked: true }, { count: 'exact' })
      .in('telegram_user_id', userIds).eq('revoked', false);
    console.log(`[CRON] expired ${expiredSubs.length} subs, revoked ${count || 0} tokens`);
  }

  // Also revoke any tokens that expired themselves (safety net)
  await supabase.from('auth_tokens').update({ revoked: true })
    .eq('revoked', false).lt('expires_at', now.toISOString());
}
cron.schedule('0 10 * * *', sendReminders);

// ── Error handlers ────────────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ ok: false, error: `Not found: ${req.method} ${req.path}` }));
app.use((err, req, res, _next) => {
  if (err.message === 'CORS blocked') return res.status(403).json({ ok: false, error: 'Origin not allowed' });
  console.error(`[FATAL] ${req.method} ${req.path}:`, err.message);
  res.status(500).json({ ok: false, error: 'Internal error' });
});

app.listen(PORT, () => console.log(`🚀 TD API v3.0 on :${PORT}`));

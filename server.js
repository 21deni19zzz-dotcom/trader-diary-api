import express from 'express';
import cors from 'cors';
import ccxt from 'ccxt';
import cron from 'node-cron';
import crypto from 'node:crypto';
import { createClient } from '@supabase/supabase-js';
import multer from 'multer';
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
// ENCRYPTION_KEY decrypts AES-256 rows in exchange_connections.api_key_enc/secret_enc.
// In production we refuse to boot without it — an ephemeral random key would
// silently make all existing encrypted rows un-decryptable on the next restart.
// In dev we still allow ephemeral (warns loudly) so local smoke runs don't need
// the secret stashed.
const ENCRYPTION_KEY_ENV = process.env.ENCRYPTION_KEY;
if (!ENCRYPTION_KEY_ENV) {
  if (process.env.NODE_ENV === 'production') {
    console.error('FATAL: ENCRYPTION_KEY env var required in production (AES key for exchange_connections rows)');
    process.exit(1);
  }
  console.warn('[boot] ENCRYPTION_KEY not set — using ephemeral random key (dev only; encrypted DB rows will NOT survive restart)');
}
const ENC_KEY   = ENCRYPTION_KEY_ENV || crypto.randomBytes(32).toString('hex');
// Telegram webhook signature: when Bot API setWebhook is configured with
// secret_token=<X>, every webhook delivery carries header
// X-Telegram-Bot-Api-Secret-Token: <X>. We compare in constant time. If the
// env var is unset, verification is skipped (compat path) — operator sees
// the warning below and is expected to set env + setWebhook in lockstep.
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
if (!TELEGRAM_WEBHOOK_SECRET) {
  // In production this is a real risk surface: without the signed header,
  // anyone who knows the public webhook URL can forge successful_payment,
  // pre_checkout_query, or /access messages. We still boot (so the
  // operator can stage the env change first) but log loudly enough that
  // the gap can't go unnoticed in a quiet monitoring setup.
  const banner = '!! TELEGRAM_WEBHOOK_SECRET NOT SET — /api/webhook/telegram accepts UNSIGNED requests !!';
  if (process.env.NODE_ENV === 'production') {
    console.error('============================================================');
    console.error(banner);
    console.error('Set TELEGRAM_WEBHOOK_SECRET in Railway env AND call the Bot');
    console.error('API setWebhook with the same secret_token in lockstep.');
    console.error('============================================================');
  } else {
    console.warn('[boot]', banner);
  }
}

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

// SHA256 digest used as opaque fingerprint for auth tokens stored in
// telegram_events.metadata. The plaintext token lives only in auth_tokens
// (the source of truth); metadata keeps just the digest so a read-leak of
// telegram_events can't grant access. Replay-protection compares digests.
function sha256(s) { return crypto.createHash('sha256').update(String(s)).digest('hex'); }

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

  // strategy_id: optional uuid (matches Paradox Coach coach_strategies.id).
  // Reject anything that doesn't look like a v4 UUID to avoid letting a
  // malformed client value reach the trades.strategy_id FK constraint.
  let strategyId = null;
  if (t.strategy_id != null && t.strategy_id !== '') {
    const s = String(t.strategy_id);
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) {
      strategyId = s;
    } else {
      err.push('strategy_id: uuid expected');
    }
  }

  // was_in_plan: boolean tri-state. null = unanswered (legacy rows), true =
  // explicitly "yes, followed the plan", false = "no". Accepting only those
  // three keeps Coach's adherence stats unambiguous.
  let wasInPlan = null;
  if (t.was_in_plan === true || t.was_in_plan === 'true')  wasInPlan = true;
  else if (t.was_in_plan === false || t.was_in_plan === 'false') wasInPlan = false;
  else if (t.was_in_plan != null && t.was_in_plan !== '') err.push('was_in_plan: boolean expected');

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
    strategy_id: strategyId,
    was_in_plan: wasInPlan,
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

// ── Auth: Sprint 9.5 (refresh + bot-driven extend) ────────────────────────────
const INTER_SERVICE_SECRET = process.env.INTER_SERVICE_SECRET || '';

// POST /api/auth/refresh
// Rotates an auth_token for an owner who still has an active subscription.
// Accepts an old token only while it is still non-revoked. Once the daily
// cron has revoked it (server.js:597, runs 10:00), the owner must recover
// via the bot's /access command — which trusts Telegram's signed identity.
// The two paths together prevent a previously-leaked token (e.g. from logs
// or a metadata cache) from being silently rotated into a live one.
app.post('/api/auth/refresh', wrap(async (req, res) => {
  const { token } = req.body || {};
  if (!token || typeof token !== 'string') return res.status(400).json({ ok: false, error: 'token required' });

  const { data: tok, error: tokErr } = await supabase.from('auth_tokens')
    .select('telegram_user_id')
    .eq('token', token).eq('revoked', false).maybeSingle();
  if (tokErr) throw new Error(`auth_tokens lookup: ${tokErr.message}`);
  if (!tok) return res.status(401).json({ ok: false, error: 'Token not found or revoked' });

  const { data: sub } = await supabase.from('subscriptions')
    .select('product_id, expires_at').eq('telegram_user_id', tok.telegram_user_id)
    .in('status', ['active', 'trial']).gt('expires_at', new Date().toISOString())
    .order('expires_at', { ascending: false }).limit(1).maybeSingle();
  if (!sub) return res.status(403).json({ ok: false, error: 'No active subscription' });

  await supabase.from('auth_tokens').update({ revoked: true })
    .eq('telegram_user_id', tok.telegram_user_id).eq('revoked', false);
  const { data: fresh, error: insErr } = await supabase.from('auth_tokens').insert({
    telegram_user_id: tok.telegram_user_id, expires_at: sub.expires_at,
  }).select('token, expires_at').single();
  if (insErr) throw new Error(`auth_tokens insert: ${insErr.message}`);

  // B1 closure — same dual-sync as /extend-from-bot. Without this, web-driven
  // rotation rotates the auth_token but leaves the bot's cached invite_link
  // pointing at the old token, so "🎫 Открыть" in Telegram 401s after the
  // user refreshes from the web. Best-effort: rotation already succeeded.
  const inviteLink = `${FRONTEND}/?token=${fresh.token}`;
  const { error: subErr } = await supabase.from('subscriptions')
    .update({ telegram_invite_link: inviteLink, updated_at: new Date().toISOString() })
    .eq('telegram_user_id', tok.telegram_user_id)
    .in('status', ['active', 'trial']);
  if (subErr) console.error('[refresh] subscription invite_link sync:', subErr.message);

  if (sub.product_id) {
    const { error: uaErr } = await supabase.from('user_accesses')
      .update({ expires_at: sub.expires_at })
      .eq('telegram_user_id', tok.telegram_user_id)
      .eq('product_id', sub.product_id)
      .is('revoked_at', null);
    if (uaErr) console.error('[refresh] user_accesses expires_at sync:', uaErr.message);
  }

  // Audit trail for the silent-401 refresh path. /refresh has no natural
  // idempotency key (frontend retries opportunistically), so we synthesise
  // one with a timestamp — each call gets its own row. Failure to log is
  // non-fatal: rotation already succeeded.
  const { error: evErr } = await supabase.from('telegram_events').insert({
    event_type: 'auth_refresh',
    telegram_user_id: tok.telegram_user_id,
    idempotency_key: `refresh-${tok.telegram_user_id}-${crypto.randomUUID()}`,
    metadata: { source: 'web', token_sha256: sha256(fresh.token), expires_at: fresh.expires_at },
  });
  if (evErr) console.error('[refresh] telegram_events insert:', evErr.message);

  res.json({ ok: true, token: fresh.token, expires_at: fresh.expires_at });
}));

// POST /api/auth/extend-from-bot
// Bot-driven token issuance. Authorised via X-Inter-Service-Secret header
// (timingSafeEqual). Idempotent on body.idempotency_key — telegram_events
// has a UNIQUE constraint on idempotency_key, so a replay returns the cached
// token from metadata.
app.post('/api/auth/extend-from-bot', wrap(async (req, res) => {
  if (!INTER_SERVICE_SECRET) {
    return res.status(503).json({ ok: false, error: 'INTER_SERVICE_SECRET not configured' });
  }
  const provided = String(req.headers['x-inter-service-secret'] || '');
  const expected = Buffer.from(INTER_SERVICE_SECRET);
  const got      = Buffer.from(provided);
  if (expected.length !== got.length || !crypto.timingSafeEqual(expected, got)) {
    return res.status(401).json({ ok: false, error: 'Invalid inter-service secret' });
  }

  const tg_id           = parseInt(req.body?.telegram_user_id, 10);
  const idempotency_key = typeof req.body?.idempotency_key === 'string' ? req.body.idempotency_key.trim() : '';
  if (!tg_id || tg_id < 1)                                return res.status(400).json({ ok: false, error: 'telegram_user_id required' });
  if (!idempotency_key || idempotency_key.length > 256)   return res.status(400).json({ ok: false, error: 'idempotency_key required (1..256 chars)' });

  // Replay: same idempotency_key returns the previously-issued token only if
  // that token is still live. If the cached row points at a token that has
  // since been revoked (e.g. user renewed and rotated), fall through and
  // issue a fresh one — then upsert the event so the cache catches up.
  const { data: prior } = await supabase.from('telegram_events')
    .select('telegram_user_id, metadata')
    .eq('idempotency_key', idempotency_key).maybeSingle();
  // Replay is only honoured when the cached row was originally created for
  // THIS user. Without this, a caller who has the inter-service secret could
  // submit someone else's idempotency_key and — if they happened to own a
  // token whose digest matches the cached one — bait the lookup into
  // returning the victim's plaintext. The tg_id match keeps the auth_tokens
  // lookup pinned to the original owner.
  if (prior?.metadata?.token_sha256 && prior.telegram_user_id === tg_id) {
    // Look up the user's current live token, then compare its digest to the
    // one we cached at first-issue. If they match, this is a true replay and
    // we return the same plaintext (from auth_tokens, the source of truth).
    // If they don't match (token rotated/revoked since), fall through to a
    // fresh issuance — same behaviour as before the hashing change.
    const { data: live } = await supabase.from('auth_tokens')
      .select('token, expires_at')
      .eq('telegram_user_id', tg_id)
      .eq('revoked', false)
      .gt('expires_at', new Date().toISOString())
      .order('expires_at', { ascending: false })
      .limit(1).maybeSingle();
    if (live && sha256(live.token) === prior.metadata.token_sha256) {
      return res.json({ ok: true, token: live.token, expires_at: live.expires_at, replay: true });
    }
  }

  const { data: sub } = await supabase.from('subscriptions')
    .select('id, expires_at, product_id').eq('telegram_user_id', tg_id)
    .in('status', ['active', 'trial']).gt('expires_at', new Date().toISOString())
    .order('expires_at', { ascending: false }).limit(1).maybeSingle();
  if (!sub) return res.status(404).json({ ok: false, error: 'No active subscription for this telegram_user_id' });

  await supabase.from('auth_tokens').update({ revoked: true })
    .eq('telegram_user_id', tg_id).eq('revoked', false);
  const { data: fresh, error: insErr } = await supabase.from('auth_tokens').insert({
    telegram_user_id: tg_id, expires_at: sub.expires_at,
  }).select('token, expires_at').single();
  if (insErr) throw new Error(`auth_tokens insert: ${insErr.message}`);

  // B1 permanent fix — sync subscriptions.telegram_invite_link with the fresh
  // token. Without this, the bot's "🎫 Открыть" cached the original purchase
  // token; once /extend-from-bot rotates the auth_token, the cached link 401s
  // and the bot falls back to "не настроен URL входа" because access_url is
  // checked but the invite_link was stale. Best-effort: failure here is
  // logged, not fatal — the issued token is the authoritative result.
  const inviteLink = `https://trader-diary-rust.vercel.app/?token=${fresh.token}`;
  const { error: subErr } = await supabase.from('subscriptions')
    .update({ telegram_invite_link: inviteLink, updated_at: new Date().toISOString() })
    .eq('telegram_user_id', tg_id)
    .in('status', ['active', 'trial']);
  if (subErr) console.error('[extend-from-bot] subscription invite_link sync:', subErr.message);

  // B1 follow-up — also sync user_accesses.expires_at so the bot's access-grant
  // table reflects the new subscription expiry. user_accesses is the second
  // source of truth the bot checks for "is the user entitled to product X" —
  // if it lags behind subscription.expires_at after a rotation/renewal, the
  // bot can still refuse access even with a valid token.
  const { error: uaErr } = await supabase.from('user_accesses')
    .update({ expires_at: sub.expires_at })
    .eq('telegram_user_id', tg_id)
    .eq('product_id', sub.product_id)
    .is('revoked_at', null);
  if (uaErr) console.error('[extend-from-bot] user_accesses expires_at sync:', uaErr.message);

  const { error: evErr } = await supabase.from('telegram_events').upsert({
    event_type: 'auth_extend',
    telegram_user_id: tg_id,
    idempotency_key,
    related_subscription_id: sub.id,
    metadata: { source: 'bot', token_sha256: sha256(fresh.token), expires_at: fresh.expires_at },
  }, { onConflict: 'idempotency_key' });
  if (evErr) console.error('[extend-from-bot] telegram_events upsert:', evErr.message);

  res.json({ ok: true, token: fresh.token, expires_at: fresh.expires_at });
}));

// POST /api/admin/notify-user
// One-shot outbound push from the bot to a specific telegram_user_id.
// Authorised by X-Inter-Service-Secret. Used for ops hot-fixes (e.g. send
// a fresh login link to a customer whose token was revoked). The bot must
// have spoken to the user previously (Telegram restriction).
app.post('/api/admin/notify-user', wrap(async (req, res) => {
  if (!INTER_SERVICE_SECRET) {
    return res.status(503).json({ ok: false, error: 'INTER_SERVICE_SECRET not configured' });
  }
  const provided = String(req.headers['x-inter-service-secret'] || '');
  const expected = Buffer.from(INTER_SERVICE_SECRET);
  const got      = Buffer.from(provided);
  if (expected.length !== got.length || !crypto.timingSafeEqual(expected, got)) {
    return res.status(401).json({ ok: false, error: 'Invalid inter-service secret' });
  }
  if (!BOT_TOKEN) {
    return res.status(503).json({ ok: false, error: 'TELEGRAM_BOT_TOKEN not configured' });
  }
  const tg_id = parseInt(req.body?.telegram_user_id, 10);
  const text  = typeof req.body?.text === 'string' ? req.body.text : '';
  const url   = typeof req.body?.url  === 'string' ? req.body.url  : null;
  if (!tg_id || tg_id < 1)      return res.status(400).json({ ok: false, error: 'telegram_user_id required' });
  if (text.length < 1 || text.length > 4000) return res.status(400).json({ ok: false, error: 'text required (1..4000 chars)' });
  if (url && !/^https:\/\//i.test(url)) return res.status(400).json({ ok: false, error: 'url must be https' });

  // Strip <a> tags from the message body so a leaked INTER_SERVICE_SECRET
  // can't be turned into a phishing-link delivery channel via the verified
  // bot. Legitimate clickable navigation goes through the `url` button.
  const safeText = text.replace(/<\s*a\b[^>]*>/gi, '').replace(/<\s*\/\s*a\s*>/gi, '');
  const extra = url ? { reply_markup: { inline_keyboard: [[{ text: '🎫 Открыть PTJ', url }]] } } : {};
  await tgSend(tg_id, safeText, extra);
  res.json({ ok: true });
}));

// AI Coach (Sprint 9B Anthropic streaming) — REMOVED in Stage 4 cleanup.
// Replaced by the full rule-based + Bayesian Paradox Coach (PR #10 backend +
// trader-diary PR #24 frontend). The old endpoint always returned 503
// without ANTHROPIC_API_KEY anyway, and recurring Anthropic spend is no
// longer needed — rule engine + cron evaluation runs entirely on Supabase.

// ── Webhook ───────────────────────────────────────────────────────────────────
app.post('/api/webhook/telegram', wrap(async (req, res) => {
  // SH-2/SH-3: verify Telegram's signed webhook secret. Reject unsigned
  // deliveries when the secret is configured; otherwise log-only (boot warning
  // already printed). Covers all webhook subtypes — pre_checkout_query,
  // successful_payment, /access /login /🎫 — in a single gate.
  if (TELEGRAM_WEBHOOK_SECRET) {
    const provided = String(req.headers['x-telegram-bot-api-secret-token'] || '');
    const a = Buffer.from(provided);
    const b = Buffer.from(TELEGRAM_WEBHOOK_SECRET);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      return res.status(401).json({ ok: false, error: 'Invalid webhook secret' });
    }
  }
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
      // product_id stores the product UUID (products.id), not the slug.
      // Existing customer rows already use UUIDs; this normalises new inserts
      // so admin queries and joins on subscriptions don't need OR-branches.
      const { data: sub } = await supabase.from('subscriptions').insert({
        telegram_user_id: tg.id, product_id: prod.id, status: 'active',
        expires_at: expiresAt, payment_method: 'stars', amount_paid: pay.total_amount
      }).select().single();

      // Revoke any existing active tokens before creating new one (renewal cleanup)
      await supabase.from('auth_tokens').update({ revoked: true })
        .eq('telegram_user_id', tg.id).eq('revoked', false);

      const { data: tok } = await supabase.from('auth_tokens').insert({
        telegram_user_id: tg.id, expires_at: expiresAt
      }).select('token').single();

      // B1 closure — same dual-sync as /extend-from-bot, applied at payment time.
      // New purchase / renewal: the freshly inserted subscription needs its
      // telegram_invite_link to track the just-issued token, and user_accesses
      // needs the new expires_at. Best-effort: payment_success is a hot path
      // and the auth_tokens row is already the authoritative entitlement —
      // sync failures are logged, never thrown.
      if (tok?.token) {
        const inviteLink = `${FRONTEND}/?token=${tok.token}`;
        const { error: subSyncErr } = await supabase.from('subscriptions')
          .update({ telegram_invite_link: inviteLink, updated_at: new Date().toISOString() })
          .eq('telegram_user_id', tg.id)
          .in('status', ['active', 'trial']);
        if (subSyncErr) console.error('[payment_success] subscription invite_link sync:', subSyncErr.message);

        const { error: uaErr } = await supabase.from('user_accesses')
          .update({ expires_at: expiresAt })
          .eq('telegram_user_id', tg.id)
          .eq('product_id', prod.id)
          .is('revoked_at', null);
        if (uaErr) console.error('[payment_success] user_accesses expires_at sync:', uaErr.message);
      }

      await supabase.from('telegram_events').insert({
        event_type: 'payment_success', telegram_user_id: tg.id, product_id: prod.id,
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
  // Sprint 18 — "🔑 Мой токен" / /token / /mytoken — show the user's CURRENT
  // live token WITHOUT rotating it. This is the common case after a payment:
  // customer lost the message, switched devices, cleared localStorage and just
  // wants to re-copy their existing token. The /access path below rotates the
  // token (revokes old, issues fresh) — correct for recovery but wrong for
  // re-copy: it invalidates the token the user already saved in their password
  // manager. Two paths, two intents, both supported.
  const tokenText = upd.message?.text || '';
  const isTokenRequest = (
    tokenText === '/token' ||
    tokenText === '/mytoken' ||
    tokenText === '🔑 Мой токен'
  );
  if (isTokenRequest && upd.message?.from) {
    const tg = upd.message.from;
    try {
      const { data: tok } = await supabase.from('auth_tokens')
        .select('token, expires_at')
        .eq('telegram_user_id', tg.id)
        .eq('revoked', false)
        .gt('expires_at', new Date().toISOString())
        .order('expires_at', { ascending: false })
        .limit(1).maybeSingle();
      if (!tok) {
        // No live token. Either the subscription expired (revoke cron 10:00 UTC),
        // or all tokens are revoked from a prior /access cycle without renewal.
        // Point the user at recovery (/access) and renewal (@ParadoxxShop_bot) —
        // never silently issue a fresh token here, as that would conceal that the
        // user has no active subscription.
        await tgSend(tg.id,
          `❌ <b>Активного токена нет</b>\n\nЛибо подписка истекла, либо все токены отозваны.\n\n🎫 <b>Получить новый:</b> отправь /access\n🛍 <b>Продлить подписку:</b> @ParadoxxShop_bot`);
        return;
      }
      // Modern #token= fragment format — auth.jsx accepts both this and the
      // legacy ?token= since Sprint 15 PR #45. Fragments aren't sent in HTTP
      // Referer headers, so the token can't leak via outbound link clicks.
      const link = `${FRONTEND}/#token=${tok.token}`;
      const exp = new Date(tok.expires_at).toLocaleDateString('ru-RU');
      await tgSend(tg.id,
        `🔑 <b>Твой токен доступа</b>\n\n<code>${tok.token}</code>\n\n📅 Действует до: <b>${exp}</b>\n\n💡 <b>Как использовать:</b>\n• Tap на токен выше — скопируется автоматически\n• Или кнопка ниже — войдёт одним кликом\n\n⚠️ Не передавай токен третьим лицам.`,
        { reply_markup: { inline_keyboard: [[{ text: '🚀 Открыть PTJ', url: link }]] } });
    } catch (e) {
      console.error('[/token]', e.message);
      await tgSend(tg.id, `⚠️ Не удалось получить токен. Попробуй через минуту.`);
    }
    return;
  }

  // Sprint 9.5: /access, /login, or "🎫 Мои доступы" — issue fresh login link
  // for a user with an active subscription. Replaces the manual purchase-link
  // recovery path when a token has been revoked by the daily expiry cron.
  const accessText = upd.message?.text || '';
  const isAccessRequest = (
    accessText.startsWith('/access') ||
    accessText.startsWith('/login')  ||
    accessText === '🎫 Мои доступы'  ||  // legacy keyboard label
    accessText === '🎫 Обновить токен'   // Sprint 18 — new label
  );
  if (isAccessRequest && upd.message?.from) {
    const tg = upd.message.from;
    try {
      const { data: sub } = await supabase.from('subscriptions')
        .select('id, product_id, expires_at').eq('telegram_user_id', tg.id)
        .in('status', ['active', 'trial']).gt('expires_at', new Date().toISOString())
        .order('expires_at', { ascending: false }).limit(1).maybeSingle();
      if (!sub) {
        await tgSend(tg.id, `❌ <b>Активной подписки нет</b>\n\nЕсли подписка была — могла истечь.\n🛍 Купить или продлить: @ParadoxxShop_bot`);
        return;
      }
      await supabase.from('auth_tokens').update({ revoked: true })
        .eq('telegram_user_id', tg.id).eq('revoked', false);
      const { data: tok, error: tokErr } = await supabase.from('auth_tokens').insert({
        telegram_user_id: tg.id, expires_at: sub.expires_at,
      }).select('token, expires_at').single();
      if (tokErr) throw new Error(`auth_tokens insert: ${tokErr.message}`);

      // Sprint 16 (B1 closure for /access) — same dual-sync as /refresh,
      // /extend-from-bot, and payment_success. Without this, the bot rotates
      // auth_tokens but leaves subscriptions.telegram_invite_link pointing
      // at the OLD token and user_accesses.expires_at stale, so the next
      // "🎫 Мои доступы" tap or another consumer reading from those tables
      // would land on a 401. Best-effort — rotation already succeeded.
      const inviteLink = `${FRONTEND}/?token=${tok.token}`;
      const { error: subSyncErr } = await supabase.from('subscriptions')
        .update({ telegram_invite_link: inviteLink, updated_at: new Date().toISOString() })
        .eq('telegram_user_id', tg.id)
        .in('status', ['active', 'trial']);
      if (subSyncErr) console.error('[/access] subscription invite_link sync:', subSyncErr.message);

      if (sub.product_id) {
        const { error: uaErr } = await supabase.from('user_accesses')
          .update({ expires_at: sub.expires_at })
          .eq('telegram_user_id', tg.id)
          .eq('product_id', sub.product_id)
          .is('revoked_at', null);
        if (uaErr) console.error('[/access] user_accesses expires_at sync:', uaErr.message);
      }

      // Key on the Telegram update_id so a webhook retry (5xx / network)
      // dedupes against the UNIQUE constraint on telegram_events.idempotency_key.
      const idemp = `access-${upd.update_id || `${tg.id}-${Date.now()}`}`;
      await supabase.from('telegram_events').upsert({
        event_type: 'auth_extend', telegram_user_id: tg.id,
        idempotency_key: idemp,
        related_subscription_id: sub.id,
        metadata: { source: 'bot_command', token_sha256: sha256(tok.token), expires_at: tok.expires_at },
      }, { onConflict: 'idempotency_key' });

      const link = `${FRONTEND}?token=${tok.token}`;
      const exp  = new Date(sub.expires_at).toLocaleDateString('ru-RU');
      await tgSend(tg.id,
        `🎫 <b>Доступ обновлён</b>\n\n📅 Подписка до: <b>${exp}</b>\n\n🔗 <b>Открыть PTJ:</b>\n${link}\n\n💡 Старый токен теперь недействителен.\n\n🔑 Чтобы посмотреть текущий токен без обновления — нажми «🔑 Мой токен» или отправь /token.`,
        { reply_markup: { inline_keyboard: [[{ text: '🎫 Открыть PTJ', url: link }]] } });
    } catch (e) {
      console.error('[/access]', e.message);
      await tgSend(tg.id, `⚠️ Не удалось обновить доступ. Попробуй через минуту или напиши в поддержку.`);
    }
    return;
  }

  if (upd.message?.text?.startsWith('/start')) {
    const tg = upd.message.from;
    await tgSend(tg.id,
      `👋 <b>Привет, ${tg.first_name}!</b>\n\n📊 <b>Trader Diary</b> — профессиональный торговый журнал.\n\n• Binance / Bybit / BingX\n• P&L с нашими формулами\n• Аналитика + equity curve\n\n🛍 Купить: @ParadoxxShop_bot\n\n<b>Кабинет:</b>\n🔑 <b>Мой токен</b> — скопировать текущий токен (без сброса)\n🎫 <b>Обновить токен</b> — выдать новый (старый перестанет работать)`,
      { reply_markup: { keyboard: [[{ text: '🔑 Мой токен' }, { text: '🎫 Обновить токен' }]], resize_keyboard: true } });
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
    strategy_id: t.strategy_id,
    was_in_plan: t.was_in_plan,
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
    strategy_id: t.strategy_id,
    was_in_plan: t.was_in_plan,
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

// ── Uploads (Sprint 5: Journal screenshots) ───────────────────────────────────
// Multer in-memory storage, 5MB limit, image MIME whitelist.
// Owner enforced server-side: path = {telegram_user_id}/{trade_id}/{uuid}.{ext}
// Bucket ptj-screenshots is public → public URL works without signing.
const SHOT_BUCKET = 'ptj-screenshots';
const SHOT_MIMES  = new Set(['image/png', 'image/jpeg', 'image/webp']);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024, files: 1 }, // 5 MB
  fileFilter: (_req, file, cb) => {
    if (!SHOT_MIMES.has(file.mimetype)) {
      return cb(new Error(`Unsupported MIME: ${file.mimetype}`));
    }
    cb(null, true);
  },
});

const extOf = (mime) => ({
  'image/png':  'png',
  'image/jpeg': 'jpg',
  'image/webp': 'webp',
}[mime] || 'bin');

app.post('/api/uploads/screenshot', requireAuth, (req, res) => {
  upload.single('file')(req, res, async (err) => {
    if (err) return res.status(400).json({ ok: false, error: err.message });
    if (!req.file) return res.status(400).json({ ok: false, error: 'No file' });

    const tradeId = String(req.body.trade_id || '').slice(0, 64);
    if (!tradeId) return res.status(400).json({ ok: false, error: 'trade_id required' });

    // Verify trade belongs to user (defence in depth)
    const { data: trade, error: tradeErr } = await supabase.from('trades')
      .select('id').eq('id', tradeId).eq('telegram_user_id', req.userId).single();
    if (tradeErr || !trade) {
      return res.status(404).json({ ok: false, error: 'Trade not found' });
    }

    // Build path: {tg_user_id}/{trade_id}/{uuid}.{ext}
    const id   = crypto.randomUUID();
    const ext  = extOf(req.file.mimetype);
    const path = `${req.userId}/${tradeId}/${id}.${ext}`;

    const { error: upErr } = await supabase.storage.from(SHOT_BUCKET).upload(
      path, req.file.buffer,
      { contentType: req.file.mimetype, upsert: false }
    );
    if (upErr) {
      console.error('[upload]', upErr.message);
      return res.status(500).json({ ok: false, error: 'Upload failed' });
    }

    const { data: pub } = supabase.storage.from(SHOT_BUCKET).getPublicUrl(path);
    const url = pub?.publicUrl;
    if (!url) return res.status(500).json({ ok: false, error: 'Public URL unavailable' });

    res.json({
      ok: true,
      shot: {
        id,
        url,
        path,
        uploaded_at: new Date().toISOString(),
      },
    });
  });
});

app.delete('/api/uploads/screenshot', requireAuth, express.json(), async (req, res) => {
  const path = String(req.body.path || '');
  if (!path) return res.status(400).json({ ok: false, error: 'path required' });

  // Defence in depth: even though Supabase Storage normalises traversal,
  // refuse any path containing the dot-dot escape or double slashes so a
  // single dependency bug can't grant cross-user deletion. Also bound the
  // length and the character set to what our writer produces (uuid/tradeId
  // segments + extension).
  if (path.length > 256 || path.includes('..') || path.includes('//') || path.includes('\\') || /[^a-zA-Z0-9._/-]/.test(path)) {
    return res.status(400).json({ ok: false, error: 'Invalid path' });
  }

  // Verify path starts with user's id (no escape)
  const userPrefix = `${req.userId}/`;
  if (!path.startsWith(userPrefix)) {
    return res.status(403).json({ ok: false, error: 'Forbidden path' });
  }

  const { error } = await supabase.storage.from(SHOT_BUCKET).remove([path]);
  if (error) {
    console.error('[upload-delete]', error.message);
    return res.status(500).json({ ok: false, error: 'Delete failed' });
  }
  res.json({ ok: true });
});

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
  try {
    const pos = (await ex.fetchPositions()).filter(p => +(p.contracts || 0) > 0).map(p => ({
      symbol: p.symbol, side: p.side, size: +p.contracts, entryPrice: +p.entryPrice,
      markPrice: +p.markPrice, unrealizedPnl: +p.unrealizedPnl, leverage: p.leverage,
      pnlPct: p.entryPrice && p.markPrice ? +((p.markPrice - p.entryPrice) / p.entryPrice * 100 * (p.side === 'short' ? -1 : 1)).toFixed(3) : null,
      exchange,
    }));
    res.json({ ok: true, count: pos.length, positions: pos });
  } finally {
    try { await ex.close?.(); } catch { /* close best-effort */ }
  }
}));

// ─────────────────────────────────────────────────────────────────────────────
// Sprint 14 · Exchange history import
// POST /api/exchange/sync-history
//   body: { exchange, symbol?, since?, limit? }
//
// Pulls realized/closed trades from the user's connected exchange and upserts
// them into `trades` keyed on (telegram_user_id, exchange, exchange_order_id),
// so repeat calls are idempotent.
//
// Strategy (try-then-fallback):
//   1) fetchPositionHistory  — CCXT 4.4+, BingX/Bybit. Cleanest: each row is
//                              a paired entry+exit with realized PnL.
//   2) fetchClosedOrders     — every exchange. We treat each closed order as
//                              a discrete closed-trade row; users can refine
//                              entry/exit by editing once it's in the journal.
//
// Per-user-per-exchange cooldown (30s) prevents the user double-clicking the
// import button into a 429 from the exchange. Cooldown is in-memory; the
// Railway dyno is single-instance so a Map is enough.
// ─────────────────────────────────────────────────────────────────────────────
const SYNC_COOLDOWN_MS = 30_000;
const syncCooldown = new Map();

const ymd = (ts) => ts ? new Date(ts).toISOString().slice(0, 10) : null;

function normalizeSide(raw) {
  const s = String(raw || '').toLowerCase();
  if (s === 'long' || s === 'buy')  return 'long';
  if (s === 'short' || s === 'sell') return 'short';
  return s || 'long';
}

// Build a `trades` row from a CCXT position-history record. These records
// already represent a closed position (entry + exit). When fields are missing
// we fall back to the open-orders shape and let the caller decide.
function rowFromPosition(p, userId, exchange) {
  const orderId = p.id || p.info?.positionId || p.info?.posId || p.info?.id || null;
  if (!orderId) return null;
  const side = normalizeSide(p.side || p.info?.positionSide || p.info?.side);
  const qty = +p.contracts || +p.amount || +p.info?.size || 0;
  const entry = +p.entryPrice || +p.info?.entryPrice || +p.info?.avgPrice || 0;
  const exit  = +p.exitPrice  || +p.info?.exitPrice  || +p.info?.closePrice || +p.info?.markPrice || 0;
  const pnl   = +p.realizedPnl ?? +p.info?.realisedPnl ?? +p.info?.realizedPnl ?? null;
  const openTs  = +p.timestamp || +p.info?.openTime || +p.info?.createTime || 0;
  const closeTs = +p.lastUpdateTimestamp || +p.info?.closeTime || +p.info?.updateTime || openTs;
  if (!qty || !entry) return null;
  return {
    telegram_user_id: userId,
    symbol: p.symbol || p.info?.symbol,
    direction: side,
    status: 'closed',
    entry_price: entry,
    exit_price: exit || entry,
    quantity: qty,
    entry_date: ymd(openTs),
    exit_date:  ymd(closeTs),
    pnl: Number.isFinite(pnl) ? pnl : null,
    exchange,
    exchange_order_id: String(orderId),
    exchange_position_id: p.info?.positionId ? String(p.info.positionId) : null,
    source: 'import_' + exchange,
  };
}

// Build a `trades` row from a CCXT closed order. Less rich than a position
// record — we don't know the entry/exit pair, so we record the fill as a
// single-leg closed trade and leave PnL null for the user to edit.
function rowFromOrder(o, userId, exchange) {
  const orderId = o.id || o.info?.orderId || o.info?.id || null;
  if (!orderId) return null;
  const filled = +o.filled || +o.amount || 0;
  if (!filled) return null;
  const avg = +o.average || +o.price || 0;
  if (!avg) return null;
  const side = normalizeSide(o.side);
  const ts = +o.timestamp || +o.lastTradeTimestamp || 0;
  return {
    telegram_user_id: userId,
    symbol: o.symbol,
    direction: side,
    status: 'closed',
    entry_price: avg,
    exit_price: avg,
    quantity: filled,
    entry_date: ymd(ts),
    exit_date:  ymd(ts),
    pnl: null,
    exchange,
    exchange_order_id: String(orderId),
    exchange_position_id: null,
    source: 'import_' + exchange,
  };
}

app.post('/api/exchange/sync-history', requireAuth, wrap(async (req, res) => {
  const { exchange, symbol, since, limit } = req.body || {};
  valEx(exchange);

  // Cooldown — defend the exchange's own rate limiter from over-eager users.
  const key = `${req.userId}:${exchange}`;
  const last = syncCooldown.get(key) || 0;
  const elapsed = Date.now() - last;
  if (elapsed < SYNC_COOLDOWN_MS) {
    return res.status(429).json({
      ok: false,
      error: `cooldown:${Math.ceil((SYNC_COOLDOWN_MS - elapsed) / 1000)}`,
    });
  }
  syncCooldown.set(key, Date.now());

  // Cursor — resume where the last sync stopped, or default to last 90 days.
  let cursorSince = Number.isFinite(+since) ? +since : null;
  if (cursorSince == null) {
    const { data: state } = await supabase.from('exchange_sync_state')
      .select('last_synced_until')
      .eq('telegram_user_id', req.userId).eq('exchange', exchange).maybeSingle();
    cursorSince = state?.last_synced_until || (Date.now() - 90 * 86_400_000);
  }
  const lim = Math.min(500, Math.max(1, +limit || 200));

  const ex = await getStoredEx(req.userId, exchange);
  const rows = [];
  const errors = [];
  let maxTs = cursorSince;
  let method = 'none';

  try {
    // Path 1 — positions (preferred). CCXT 4.4+ supports fetchPositionHistory
    // on BingX/Bybit; binance perp exposes it too on newer builds.
    if (typeof ex.fetchPositionHistory === 'function') {
      try {
        const positions = await ex.fetchPositionHistory(symbol || undefined, cursorSince, lim);
        for (const p of positions || []) {
          const row = rowFromPosition(p, req.userId, exchange);
          if (!row) continue;
          rows.push(row);
          const t = +p.lastUpdateTimestamp || +p.timestamp || 0;
          if (t > maxTs) maxTs = t;
        }
        if (rows.length > 0) method = 'fetchPositionHistory';
      } catch (e) {
        errors.push({ method: 'fetchPositionHistory', message: String(e.message || e).slice(0, 200) });
      }
    }

    // Path 2 — closed orders (fallback). Always available, less rich.
    if (rows.length === 0) {
      try {
        const orders = await ex.fetchClosedOrders(symbol || undefined, cursorSince, lim);
        for (const o of orders || []) {
          if (o.status !== 'closed' && o.status !== 'canceled') continue;
          if (o.status === 'canceled') continue;
          const row = rowFromOrder(o, req.userId, exchange);
          if (!row) continue;
          rows.push(row);
          const t = +o.timestamp || +o.lastTradeTimestamp || 0;
          if (t > maxTs) maxTs = t;
        }
        if (rows.length > 0) method = 'fetchClosedOrders';
      } catch (e) {
        errors.push({ method: 'fetchClosedOrders', message: String(e.message || e).slice(0, 200) });
      }
    }

    let imported = 0;
    if (rows.length > 0) {
      // App-level dedup. Sprint 20 — the historical unique index
      // `uq_trades_exchange_order` is a PARTIAL index with
      // `WHERE exchange_order_id IS NOT NULL`. PostgreSQL's `ON CONFLICT`
      // requires the conflict-target arbiter index's WHERE predicate to
      // match exactly, and Supabase's `.upsert({ onConflict: 'cols' })`
      // cannot pass a WHERE predicate — so the call failed at runtime with
      // `there is no unique or exclusion constraint matching the ON CONFLICT
      // specification`. Instead of dropping/recreating the index (DB
      // migration → STOP-GATE), we dedup app-side: SELECT existing
      // exchange_order_ids for this (user, exchange), filter them out, then
      // plain INSERT the new ones. Safe under the 30s per-(user,exchange)
      // cooldown — no concurrent sync run for the same key.
      const orderIds = rows.map((r) => r.exchange_order_id).filter(Boolean);
      let existing = new Set();
      if (orderIds.length > 0) {
        const { data: existingRows, error: selErr } = await supabase
          .from('trades')
          .select('exchange_order_id')
          .eq('telegram_user_id', req.userId)
          .eq('exchange', exchange)
          .in('exchange_order_id', orderIds);
        if (selErr) throw new Error(`dedup-select: ${selErr.message}`);
        existing = new Set((existingRows || []).map((r) => r.exchange_order_id));
      }
      const newRows = rows.filter((r) => !existing.has(r.exchange_order_id));
      if (newRows.length > 0) {
        const { data: insData, error: insErr } = await supabase
          .from('trades')
          .insert(newRows)
          .select('id');
        if (insErr) throw new Error(`insert: ${insErr.message}`);
        imported = (insData || []).length;
      }
    }

    await supabase.from('exchange_sync_state').upsert({
      telegram_user_id: req.userId,
      exchange,
      last_synced_at: new Date().toISOString(),
      last_synced_until: maxTs,
      total_imported: imported,
      last_error: errors.length > 0 ? errors[0].message : null,
    }, { onConflict: 'telegram_user_id,exchange' });

    res.json({
      ok: true,
      imported,
      skipped: 0,
      errors,
      method,
      lastSync: maxTs,
    });
  } finally {
    try { await ex.close?.(); } catch { /* */ }
  }
}));

// Cron — daily 02:00 UTC sweep across every connected exchange for every user
// whose state row is more than 23h stale. Best-effort, swallow per-user errors
// so one broken key doesn't kill the whole batch.
async function syncAllExchangeHistory() {
  console.log('[CRON sync-history] starting');
  const { data: conns, error } = await supabase.from('exchange_connections')
    .select('telegram_user_id, exchange, api_key_enc, secret_enc');
  if (error || !conns) {
    console.error('[CRON sync-history] connections lookup failed:', error?.message);
    return;
  }
  let processed = 0;
  let imported = 0;
  for (const conn of conns) {
    try {
      const { data: state } = await supabase.from('exchange_sync_state')
        .select('last_synced_at, last_synced_until')
        .eq('telegram_user_id', conn.telegram_user_id)
        .eq('exchange', conn.exchange)
        .maybeSingle();
      // Skip rows synced in the last 23h
      if (state?.last_synced_at) {
        const ageMs = Date.now() - new Date(state.last_synced_at).getTime();
        if (ageMs < 23 * 3_600_000) continue;
      }
      const since = state?.last_synced_until || (Date.now() - 90 * 86_400_000);
      let ex = null;
      const rows = [];
      let maxTs = since;
      try {
        ex = mkEx(conn.exchange, decrypt(conn.api_key_enc), decrypt(conn.secret_enc));
        if (!ex) continue;
        if (typeof ex.fetchPositionHistory === 'function') {
          try {
            const positions = await ex.fetchPositionHistory(undefined, since, 200);
            for (const p of positions || []) {
              const row = rowFromPosition(p, conn.telegram_user_id, conn.exchange);
              if (!row) continue;
              rows.push(row);
              const t = +p.lastUpdateTimestamp || +p.timestamp || 0;
              if (t > maxTs) maxTs = t;
            }
          } catch { /* fall through */ }
        }
        if (rows.length === 0) {
          try {
            const orders = await ex.fetchClosedOrders(undefined, since, 200);
            for (const o of orders || []) {
              if (o.status !== 'closed') continue;
              const row = rowFromOrder(o, conn.telegram_user_id, conn.exchange);
              if (!row) continue;
              rows.push(row);
              const t = +o.timestamp || +o.lastTradeTimestamp || 0;
              if (t > maxTs) maxTs = t;
            }
          } catch { /* */ }
        }
        if (rows.length > 0) {
          await supabase.from('trades').upsert(rows, {
            onConflict: 'telegram_user_id,exchange,exchange_order_id',
            ignoreDuplicates: false,
          });
          imported += rows.length;
        }
        await supabase.from('exchange_sync_state').upsert({
          telegram_user_id: conn.telegram_user_id,
          exchange: conn.exchange,
          last_synced_at: new Date().toISOString(),
          last_synced_until: maxTs,
          total_imported: rows.length,
          last_error: null,
        }, { onConflict: 'telegram_user_id,exchange' });
        processed++;
      } catch (e) {
        console.error('[CRON sync-history]', conn.telegram_user_id, conn.exchange, e?.message);
        await supabase.from('exchange_sync_state').upsert({
          telegram_user_id: conn.telegram_user_id,
          exchange: conn.exchange,
          last_synced_at: new Date().toISOString(),
          last_synced_until: maxTs,
          total_imported: 0,
          last_error: String(e?.message || e).slice(0, 500),
        }, { onConflict: 'telegram_user_id,exchange' });
      } finally {
        try { await ex?.close?.(); } catch { /* */ }
      }
    } catch (outer) {
      console.error('[CRON sync-history outer]', outer?.message);
    }
  }
  console.log(`[CRON sync-history] done — processed ${processed}, imported ${imported}`);
}

// Sprint 9A · Live monitor — single-call snapshot of open positions across ALL
// of the user's connected exchanges. The existing POST /api/exchange/positions
// is per-exchange (frontend would have to N+1 it), this fans out server-side,
// fails partially (one bad exchange doesn't kill the whole response), and
// closes each CCXT instance to avoid the FD-leak audited as a P2.
app.get('/api/positions/live', requireAuth, wrap(async (req, res) => {
  const { data: conns, error: cErr } = await supabase.from('exchange_connections')
    .select('exchange, api_key_enc, secret_enc')
    .eq('telegram_user_id', req.userId);
  if (cErr) throw new Error(`exchange_connections lookup: ${cErr.message}`);
  if (!conns || conns.length === 0) {
    return res.json({ ok: true, positions: [], exchanges: 0, errors: [] });
  }
  const all = [];
  const errors = [];
  for (const conn of conns) {
    let ex = null;
    try {
      ex = mkEx(conn.exchange, decrypt(conn.api_key_enc), decrypt(conn.secret_enc));
      if (!ex) { errors.push({ exchange: conn.exchange, message: 'unsupported' }); continue; }
      const raw = await ex.fetchPositions();
      for (const p of raw) {
        const size = +(p.contracts || 0);
        if (!Number.isFinite(size) || Math.abs(size) === 0) continue;
        const entry = p.entryPrice != null ? +p.entryPrice : null;
        const mark  = p.markPrice  != null ? +p.markPrice  : null;
        const pnlPct = entry && mark
          ? +((mark - entry) / entry * 100 * (p.side === 'short' ? -1 : 1)).toFixed(3)
          : null;
        all.push({
          exchange: conn.exchange,
          symbol: p.symbol,
          side: p.side,
          size,
          entryPrice: entry,
          markPrice: mark,
          unrealizedPnl: p.unrealizedPnl != null ? +p.unrealizedPnl : 0,
          pnlPct,
          leverage: p.leverage != null ? +p.leverage : null,
          ts: Date.now(),
        });
      }
    } catch (e) {
      console.error(`[live] ${conn.exchange}:`, e.message);
      errors.push({ exchange: conn.exchange, message: String(e.message || e).slice(0, 200) });
    } finally {
      try { await ex?.close?.(); } catch { /* best-effort */ }
    }
  }
  // Sort by absolute PnL desc — biggest movers first
  all.sort((a, b) => Math.abs(b.unrealizedPnl) - Math.abs(a.unrealizedPnl));
  res.json({ ok: true, positions: all, exchanges: conns.length, errors });
}));

// Legacy redirect
app.post('/api/connect', requireAuth, (req, res) => {
  req.url = '/api/exchange/connect';
  app.handle(req, res);
});

// ═══════════════════════════════════════════════════════════════════════════════
// PARADOX COACH — profile, strategies, rules catalog, insights, weights, cron
// (Sprint 9B+ — rule-based + Bayesian self-learning, $0/mo. Schema in migration
// `paradox_coach_schema`. Frontend renders user-facing copy via own rules.js;
// backend ONLY persists state + evaluates outcomes for the Bayesian update.)
// ═══════════════════════════════════════════════════════════════════════════════

const COACH_PROFILE_FIELDS = [
  'tone','language','account_balance','base_currency',
  'max_risk_per_trade_pct','max_daily_risk_pct','max_weekly_dd_pct',
  'max_concurrent_positions','use_kelly','kelly_fraction','markets_traded',
  'preferred_sessions','trading_style','avg_hold_time_hours',
  'monthly_goal_pct','monthly_goal_usd','insights_per_session',
  'min_trades_for_pattern','enabled_categories','disabled_rules',
];

const COACH_STRATEGY_FIELDS = [
  'name','description','entry_rules','exit_rules',
  'risk_per_trade_pct','max_positions','expected_winrate','expected_rr',
  'expected_trades_per_week','status',
];

// ── Profile ───────────────────────────────────────────────────────────────────
app.get('/api/coach/profile', requireAuth, wrap(async (req, res) => {
  const { data, error } = await supabase
    .from('coach_user_profile').select('*')
    .eq('telegram_user_id', req.userId).maybeSingle();
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, profile: data });
}));

app.put('/api/coach/profile', requireAuth, wrap(async (req, res) => {
  const updates = { updated_at: new Date().toISOString() };
  for (const k of COACH_PROFILE_FIELDS) if (req.body?.[k] !== undefined) updates[k] = req.body[k];
  const { data, error } = await supabase
    .from('coach_user_profile')
    .upsert({ telegram_user_id: req.userId, ...updates }, { onConflict: 'telegram_user_id' })
    .select().single();
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, profile: data });
}));

// ── Strategies ────────────────────────────────────────────────────────────────
app.get('/api/coach/strategies', requireAuth, wrap(async (req, res) => {
  const { data, error } = await supabase
    .from('coach_strategies')
    .select('*, coach_strategy_performance(*)')
    .eq('telegram_user_id', req.userId)
    .order('created_at', { ascending: false });
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, strategies: data || [] });
}));

app.post('/api/coach/strategies', requireAuth, wrap(async (req, res) => {
  const name = typeof req.body?.name === 'string' ? req.body.name.trim() : '';
  if (!name || name.length > 200) return res.status(400).json({ ok: false, error: 'name required (1..200 chars)' });
  const row = { telegram_user_id: req.userId, name };
  for (const k of COACH_STRATEGY_FIELDS) if (k !== 'name' && req.body?.[k] !== undefined) row[k] = req.body[k];
  const { data, error } = await supabase.from('coach_strategies').insert(row).select().single();
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.status(201).json({ ok: true, strategy: data });
}));

app.put('/api/coach/strategies/:id', requireAuth, wrap(async (req, res) => {
  const { id } = req.params;
  // Verify ownership first (cheap defence-in-depth on top of RLS)
  const { data: existing } = await supabase.from('coach_strategies')
    .select('telegram_user_id').eq('id', id).maybeSingle();
  if (!existing || existing.telegram_user_id !== req.userId) {
    return res.status(404).json({ ok: false, error: 'strategy not found' });
  }
  const updates = { updated_at: new Date().toISOString() };
  for (const k of COACH_STRATEGY_FIELDS) if (req.body?.[k] !== undefined) updates[k] = req.body[k];
  const { data, error } = await supabase.from('coach_strategies')
    .update(updates).eq('id', id).select().single();
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, strategy: data });
}));

app.delete('/api/coach/strategies/:id', requireAuth, wrap(async (req, res) => {
  const { error } = await supabase.from('coach_strategies')
    .delete().eq('id', req.params.id).eq('telegram_user_id', req.userId);
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true });
}));

app.get('/api/coach/strategies/:id/performance', requireAuth, wrap(async (req, res) => {
  const { id } = req.params;
  // Verify ownership first
  const { data: strat } = await supabase.from('coach_strategies')
    .select('telegram_user_id').eq('id', id).maybeSingle();
  if (!strat || strat.telegram_user_id !== req.userId) {
    return res.status(404).json({ ok: false, error: 'strategy not found' });
  }
  const { data } = await supabase.from('coach_strategy_performance')
    .select('*').eq('strategy_id', id).maybeSingle();
  res.json({ ok: true, performance: data });
}));

// ── Rules catalog (read-only) ─────────────────────────────────────────────────
app.get('/api/coach/rules', requireAuth, wrap(async (_req, res) => {
  const { data, error } = await supabase.from('coach_rules')
    .select('id, category, severity, target_metric, target_direction, evaluation_window_trades, min_sample_size, confidence_threshold')
    .eq('is_active', true);
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, rules: data || [] });
}));

// ── Insights — frontend computes, backend persists ───────────────────────────
app.post('/api/coach/insights', requireAuth, wrap(async (req, res) => {
  const { rule_id, strategy_id = null, title, body, action,
          baseline_metrics, trades_count_at_show } = req.body || {};
  if (!rule_id || !title || !body || !action || !baseline_metrics) {
    return res.status(400).json({ ok: false, error: 'rule_id, title, body, action, baseline_metrics required' });
  }
  if (typeof trades_count_at_show !== 'number' || trades_count_at_show < 0) {
    return res.status(400).json({ ok: false, error: 'trades_count_at_show must be non-negative number' });
  }
  const { data, error } = await supabase.from('coach_insights').insert({
    telegram_user_id: req.userId,
    rule_id, strategy_id, title, body, action,
    baseline_metrics, trades_count_at_show,
  }).select().single();
  if (error) return res.status(500).json({ ok: false, error: error.message });

  // Increment total_shown (best-effort)
  supabase.rpc('update_rule_weight', {
    p_rule_id: rule_id, p_user_id: req.userId, p_improved: null,
  }).then(({ error: rpcErr }) => { if (rpcErr) console.error('[coach/insights] update_rule_weight:', rpcErr.message); });

  res.status(201).json({ ok: true, insight: data });
}));

app.get('/api/coach/insights', requireAuth, wrap(async (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit) || 50, 1), 200);
  const { data, error } = await supabase.from('coach_insights')
    .select('*').eq('telegram_user_id', req.userId)
    .order('shown_at', { ascending: false }).limit(limit);
  if (error) return res.status(500).json({ ok: false, error: error.message });
  res.json({ ok: true, insights: data || [] });
}));

app.patch('/api/coach/insights/:id', requireAuth, wrap(async (req, res) => {
  const allowed = ['acknowledged_at','dismissed_at','user_rating','user_comment'];
  const updates = {};
  for (const k of allowed) if (req.body?.[k] !== undefined) updates[k] = req.body[k];
  if (Object.keys(updates).length === 0) {
    return res.status(400).json({ ok: false, error: 'no allowed fields in body' });
  }
  if ('user_rating' in updates && ![-1, 0, 1, null].includes(updates.user_rating)) {
    return res.status(400).json({ ok: false, error: 'user_rating must be -1, 0, or 1' });
  }
  const { data, error } = await supabase.from('coach_insights')
    .update(updates).eq('id', req.params.id).eq('telegram_user_id', req.userId)
    .select().maybeSingle();
  if (error) return res.status(500).json({ ok: false, error: error.message });
  if (!data) return res.status(404).json({ ok: false, error: 'insight not found' });
  res.json({ ok: true, insight: data });
}));

// ── Bayesian weights for frontend ranker ─────────────────────────────────────
app.get('/api/coach/weights', requireAuth, wrap(async (req, res) => {
  const { data: globalRows } = await supabase.from('coach_rule_weights')
    .select('rule_id, weight, alpha, beta, total_evaluated').is('telegram_user_id', null);
  const { data: personalRows } = await supabase.from('coach_rule_weights')
    .select('rule_id, weight, alpha, beta, total_evaluated').eq('telegram_user_id', req.userId);

  const weights = {};
  for (const g of globalRows || []) {
    weights[g.rule_id] = { global: Number(g.weight), personal: null, evaluated: 0 };
  }
  for (const p of personalRows || []) {
    weights[p.rule_id] = weights[p.rule_id] || { global: 0.5, personal: null, evaluated: 0 };
    weights[p.rule_id].personal = Number(p.weight);
    weights[p.rule_id].evaluated = p.total_evaluated || 0;
  }
  res.json({ ok: true, weights });
}));

// ── Cron: evaluate insight outcomes (Bayesian feedback loop) ─────────────────
function _coachComputeMetrics(trades) {
  if (!Array.isArray(trades) || trades.length === 0) return { n_trades: 0 };
  const wins = trades.filter((t) => Number(t.pnl) > 0);
  const losses = trades.filter((t) => Number(t.pnl) < 0);
  const winrate = trades.length ? wins.length / trades.length : 0;
  const totalPnl = trades.reduce((s, t) => s + Number(t.pnl || 0), 0);
  const grossWin = wins.reduce((s, t) => s + Number(t.pnl || 0), 0);
  const grossLoss = Math.abs(losses.reduce((s, t) => s + Number(t.pnl || 0), 0));
  const winRs = wins.map((t) => Number(t.rr || 0)).filter((r) => r > 0);
  const lossRs = losses.map((t) => -Math.abs(Number(t.rr || 0))).filter((r) => r < 0);
  const avgWinR = winRs.length ? winRs.reduce((s, r) => s + r, 0) / winRs.length : 0;
  const avgLossR = lossRs.length ? lossRs.reduce((s, r) => s + r, 0) / lossRs.length : 0;
  const expectancy_r = winrate * avgWinR + (1 - winrate) * avgLossR;
  // Equity curve for drawdown (cumulative pnl since first trade in window)
  let peak = 0, balance = 0, maxDD = 0;
  for (const t of trades) {
    balance += Number(t.pnl || 0);
    if (balance > peak) peak = balance;
    const dd = peak > 0 ? (peak - balance) / peak : 0;
    if (dd > maxDD) maxDD = dd;
  }
  return {
    winrate,
    expectancy_r,
    expectancy_usd: trades.length ? totalPnl / trades.length : 0,
    profit_factor: grossLoss > 0 ? grossWin / grossLoss : (grossWin > 0 ? 99 : 0),
    max_dd_pct: maxDD * 100,
    avg_win_r: avgWinR,
    avg_loss_r: avgLossR,
    n_trades: trades.length,
  };
}

async function evaluateInsightOutcomes() {
  // Insights without outcome shown > 7 days ago, with enough trades after to evaluate.
  const cutoff = new Date(Date.now() - 7 * 86400000).toISOString();
  const { data: pending, error: pErr } = await supabase
    .from('coach_insights')
    .select('id, telegram_user_id, rule_id, shown_at, baseline_metrics, coach_rules!inner(target_metric, target_direction, evaluation_window_trades)')
    .is('outcome_evaluated_at', null)
    .lt('shown_at', cutoff)
    .limit(100);
  if (pErr) { console.error('[coach/cron] pending fetch:', pErr.message); return; }

  for (const ins of pending || []) {
    const target = ins.coach_rules?.target_metric;
    const direction = ins.coach_rules?.target_direction;
    const windowN = ins.coach_rules?.evaluation_window_trades || 10;
    if (!target || !direction) continue;

    const { data: tradesAfter } = await supabase.from('trades')
      .select('pnl, rr')
      .eq('telegram_user_id', ins.telegram_user_id)
      .eq('status', 'closed')
      .gte('exit_date', ins.shown_at)
      .order('exit_date', { ascending: true })
      .limit(windowN);
    if (!tradesAfter || tradesAfter.length < windowN) continue; // wait more data

    const newStats = _coachComputeMetrics(tradesAfter);
    const baseline = ins.baseline_metrics || {};
    const baseVal = baseline[target];
    const newVal  = newStats[target];
    if (baseVal == null || newVal == null) continue;

    const improved = direction === 'increase'
      ? Number(newVal) > Number(baseVal) * 1.05
      : Number(newVal) < Number(baseVal) * 0.95;
    const delta = (Number(newVal) - Number(baseVal)) / (Math.abs(Number(baseVal)) || 1);

    const { error: uErr } = await supabase.from('coach_insights').update({
      outcome_evaluated_at: new Date().toISOString(),
      outcome_metrics: newStats,
      outcome_improved: improved,
      outcome_delta: delta,
    }).eq('id', ins.id);
    if (uErr) { console.error('[coach/cron] insight update:', uErr.message); continue; }

    const { error: rpcErr } = await supabase.rpc('update_rule_weight', {
      p_rule_id: ins.rule_id, p_user_id: ins.telegram_user_id, p_improved: improved,
    });
    if (rpcErr) console.error('[coach/cron] update_rule_weight:', rpcErr.message);
  }
}

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

// Paradox Coach — daily 03:00 UTC outcome evaluation (Bayesian feedback loop).
// Always wrap in try/catch so cron doesn't crash on a single bad row.
cron.schedule('0 3 * * *', () => {
  evaluateInsightOutcomes().catch((e) => console.error('[coach/cron]', e?.message || e));
});

// Sprint 14 — daily 02:00 UTC sweep of exchange history across every
// connected user. Best-effort; per-user errors are logged + persisted in
// exchange_sync_state.last_error but never crash the whole batch.
cron.schedule('0 2 * * *', () => {
  syncAllExchangeHistory().catch((e) => console.error('[sync-history/cron]', e?.message || e));
});

// ── Error handlers ────────────────────────────────────────────────────────────

// ═══ User Profile (editable) ═══════════════════════════════════════════════
app.get('/api/me', requireAuth, wrap(async (req, res) => {
  const { data, error } = await supabase
    .from('users')
    .select('telegram_user_id, username, first_name, last_name, display_name, email, timezone, reporting_currency, start_balance, language_code, points_balance')
    .eq('telegram_user_id', req.userId)
    .maybeSingle();
  if (error) throw new Error(error.message);
  res.json({ ok: true, user: data });
}));

app.put('/api/me', requireAuth, wrap(async (req, res) => {
  const allowed = ['display_name','email','timezone','reporting_currency','start_balance','language_code'];
  const upd = { updated_at: new Date().toISOString() };
  for (const k of allowed) {
    if (req.body[k] !== undefined) {
      if (k === 'email' && req.body.email && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(req.body.email)) {
        return res.status(400).json({ ok: false, error: 'invalid email' });
      }
      if (k === 'timezone' && req.body.timezone) {
        // Basic validation — must be Continent/City format or UTC[+-]N
        const tz = String(req.body.timezone);
        if (!/^([A-Z][a-z]+\/[A-Z][a-zA-Z_]+|UTC[+-]?\d{0,2})$/.test(tz)) {
          return res.status(400).json({ ok: false, error: 'invalid timezone' });
        }
      }
      if (k === 'reporting_currency' && req.body.reporting_currency) {
        if (!/^[A-Z]{3}$/.test(req.body.reporting_currency)) {
          return res.status(400).json({ ok: false, error: 'currency must be 3-letter code (USD, EUR, RUB)' });
        }
      }
      upd[k] = req.body[k];
    }
  }
  const { data, error } = await supabase
    .from('users')
    .update(upd)
    .eq('telegram_user_id', req.userId)
    .select('telegram_user_id, display_name, email, timezone, reporting_currency, start_balance, language_code')
    .single();
  if (error) throw new Error(error.message);
  res.json({ ok: true, user: data });
}));

app.use((req, res) => res.status(404).json({ ok: false, error: `Not found: ${req.method} ${req.path}` }));
app.use((err, req, res, _next) => {
  if (err.message === 'CORS blocked') return res.status(403).json({ ok: false, error: 'Origin not allowed' });
  console.error(`[FATAL] ${req.method} ${req.path}:`, err.message);
  res.status(500).json({ ok: false, error: 'Internal error' });
});

app.listen(PORT, () => console.log(`🚀 TD API v3.0 on :${PORT}`));

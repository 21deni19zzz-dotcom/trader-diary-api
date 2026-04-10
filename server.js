import express from 'express';
import cors from 'cors';
import ccxt from 'ccxt';
import cron from 'node-cron';
import { createClient } from '@supabase/supabase-js';
import 'dotenv/config';

const app  = express();
const PORT = process.env.PORT || 3001;

const supabase = createClient(
  process.env.SUPABASE_URL      || 'https://nqaddvmjvoyxajztvkah.supabase.co',
  process.env.SUPABASE_SERVICE_KEY || '',
  { auth: { persistSession: false } }
);

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_API    = `https://api.telegram.org/bot${BOT_TOKEN}`;
const FRONTEND  = process.env.FRONTEND_URL || 'https://trader-diary-rust.vercel.app';

app.use(cors({ origin: '*' }));
app.use(express.json());

// ── Helpers ───────────────────────────────────────────────────────────────────
const wrap = fn => async (req, res) => {
  try { await fn(req, res); }
  catch (e) {
    const code = e.message?.includes('401')||e.message?.includes('Auth') ? 401 : e.message?.includes('Not found') ? 404 : 500;
    res.status(code).json({ ok: false, error: e.message || 'Unknown error' });
  }
};

async function tgSend(chat_id, text, extra = {}) {
  if (!BOT_TOKEN) return;
  await fetch(`${TG_API}/sendMessage`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ chat_id, text, parse_mode:'HTML', ...extra }),
  });
}

// ── P&L (наши расчёты) ────────────────────────────────────────────────────────
const calcPnL = (dir, ep, xp, qty, fee=0) => {
  if (!xp) return null;
  return parseFloat(((dir==='long'?(xp-ep):(ep-xp))*qty - fee).toFixed(8));
};
const calcPnLPct = (dir, ep, xp) => {
  if (!xp||!ep) return null;
  return parseFloat(((dir==='long'?(xp-ep):(ep-xp))/ep*100).toFixed(4));
};
const calcRR = (ep, sl, tp) => {
  if (!sl||!tp) return null;
  const r=Math.abs(ep-sl), w=Math.abs(tp-ep);
  return r>0 ? parseFloat((w/r).toFixed(2)) : null;
};

// ── Auth middleware ────────────────────────────────────────────────────────────
async function requireAuth(req, res, next) {
  const token = (req.headers.authorization||'').replace('Bearer ','').trim() || req.query.token || '';
  if (!token) return res.status(401).json({ ok:false, error:'No auth token' });

  const { data, error } = await supabase
    .from('auth_tokens')
    .select('telegram_user_id, expires_at, users(first_name, username)')
    .eq('token', token).eq('revoked', false).gt('expires_at', new Date().toISOString())
    .single();

  if (error||!data) return res.status(401).json({ ok:false, error:'Invalid or expired token' });

  const { data: sub } = await supabase
    .from('subscriptions').select('id,status,expires_at')
    .eq('telegram_user_id', data.telegram_user_id)
    .in('status',['active','trial']).gt('expires_at', new Date().toISOString())
    .order('expires_at',{ascending:false}).limit(1).single();

  if (!sub) return res.status(403).json({ ok:false, error:'Subscription expired. Renew at @ParadoxxShop_bot' });

  supabase.from('auth_tokens').update({last_used_at:new Date().toISOString()}).eq('token',token);
  req.userId = data.telegram_user_id;
  req.user   = data.users;
  req.sub    = sub;
  next();
}

// ═══════════════════════════════════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════════════════════════════════

app.get('/api/health', (_,res) => res.json({ ok:true, version:'2.0.0', time:new Date().toISOString() }));

// Шаг 4: Верификация токена с фронтенда
app.post('/api/auth/verify', wrap(async (req,res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ ok:false, error:'token required' });

  const { data, error } = await supabase.from('auth_tokens')
    .select('telegram_user_id, expires_at, users(first_name, username)')
    .eq('token', token).eq('revoked', false).gt('expires_at', new Date().toISOString()).single();
  if (error||!data) return res.status(401).json({ ok:false, error:'Invalid or expired token' });

  const { data: sub } = await supabase.from('subscriptions').select('expires_at,status')
    .eq('telegram_user_id', data.telegram_user_id).in('status',['active','trial'])
    .gt('expires_at', new Date().toISOString()).order('expires_at',{ascending:false}).limit(1).single();
  if (!sub) return res.status(403).json({ ok:false, error:'No active subscription' });

  res.json({ ok:true, token,
    user:{ telegram_user_id:data.telegram_user_id, name:data.users?.first_name||'Trader', username:data.users?.username },
    subscription:{ expires_at:sub.expires_at, status:sub.status }
  });
}));

// Шаг 2: Telegram webhook (Stars payments + bot commands)
app.post('/api/webhook/telegram', wrap(async (req,res) => {
  const upd = req.body;
  res.json({ ok:true });

  // Pre-checkout (обязательно за <10с)
  if (upd.pre_checkout_query) {
    await fetch(`${TG_API}/answerPreCheckoutQuery`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({pre_checkout_query_id:upd.pre_checkout_query.id,ok:true})
    });
    return;
  }

  // Успешная оплата
  if (upd.message?.successful_payment) {
    const msg=upd.message, pay=msg.successful_payment, tg=msg.from;
    const [slug] = pay.invoice_payload.split('|');
    try {
      await supabase.from('users').upsert({
        telegram_user_id:tg.id, username:tg.username||null,
        first_name:tg.first_name||null, last_name:tg.last_name||null,
        updated_at:new Date().toISOString()
      },{ onConflict:'telegram_user_id' });

      const { data: prod } = await supabase.from('products').select('*').eq('slug',slug).single();
      if (!prod) return;

      const expiresAt = new Date(Date.now()+prod.duration_days*86400000).toISOString();

      const { data: sub } = await supabase.from('subscriptions').insert({
        telegram_user_id:tg.id, product_id:prod.slug, status:'active',
        expires_at:expiresAt, payment_method:'stars', amount_paid:pay.total_amount
      }).select().single();

      const { data: tok } = await supabase.from('auth_tokens').insert({
        telegram_user_id:tg.id, expires_at:expiresAt
      }).select('token').single();

      await supabase.from('telegram_events').insert({
        event_type:'payment_success', telegram_user_id:tg.id, product_id:prod.slug,
        payment_method:'stars', payment_tx_id:pay.telegram_payment_charge_id,
        idempotency_key:pay.telegram_payment_charge_id,
        related_subscription_id:sub?.id,
        metadata:{ amount:pay.total_amount, currency:pay.currency }
      });

      const link = `${FRONTEND}?token=${tok?.token}`;
      const exp  = new Date(expiresAt).toLocaleDateString('ru-RU');
      await tgSend(tg.id,
        `✅ <b>Оплата прошла!</b>\n\n📦 <b>${prod.name}</b>\n📅 До: <b>${exp}</b>\n\n`+
        `🔗 <b>Войти в Trader Diary:</b>\n${link}\n\n`+
        `💡 Это твой персональный ключ доступа.`
      );
    } catch(e) { console.error('Webhook payment error:',e.message); }
    return;
  }

  // /start
  if (upd.message?.text?.startsWith('/start')) {
    const tg = upd.message.from;
    await tgSend(tg.id,
      `👋 <b>Привет, ${tg.first_name}!</b>\n\n`+
      `📊 <b>Trader Diary</b> — профессиональный торговый журнал.\n\n`+
      `• Подключение Binance / Bybit / BingX\n`+
      `• P&L с нашими расчётами (не биржевыми)\n`+
      `• Аналитика, equity curve, breakdown по сетапам\n\n`+
      `🛍 Купить доступ: @ParadoxxShop_bot`
    );
  }
}));

// ── Trades API (Supabase) ─────────────────────────────────────────────────────
app.get('/api/trades', requireAuth, wrap(async (req,res) => {
  const { data, error } = await supabase.from('trades').select('*')
    .eq('telegram_user_id', req.userId).order('entry_date',{ascending:false});
  if (error) throw new Error(error.message);
  res.json({ ok:true, count:data.length, trades:data });
}));

app.post('/api/trades', requireAuth, wrap(async (req,res) => {
  const t=req.body;
  const ep=+t.entry_price, xp=t.exit_price?+t.exit_price:null, qty=+t.quantity;
  const { data, error } = await supabase.from('trades').insert({
    telegram_user_id:req.userId,
    symbol:t.symbol?.toUpperCase(), direction:t.direction,
    status:t.status||'open', entry_price:ep, exit_price:xp, quantity:qty,
    entry_date:t.entry_date, exit_date:t.exit_date||null,
    stop_loss:t.stop_loss?+t.stop_loss:null, take_profit:t.take_profit?+t.take_profit:null,
    pnl:calcPnL(t.direction,ep,xp,qty), pnl_pct:calcPnLPct(t.direction,ep,xp),
    rr:calcRR(ep,+t.stop_loss,+t.take_profit),
    setup:t.setup||null, emotion:t.emotion||null, notes:t.notes||null,
    exchange:t.exchange||'manual'
  }).select().single();
  if (error) throw new Error(error.message);
  res.json({ ok:true, trade:data });
}));

app.patch('/api/trades/:id', requireAuth, wrap(async (req,res) => {
  const t=req.body, ep=+t.entry_price, xp=t.exit_price?+t.exit_price:null, qty=+t.quantity;
  const upd = { ...t, updated_at:new Date().toISOString() };
  if (xp) { upd.pnl=calcPnL(t.direction,ep,xp,qty); upd.pnl_pct=calcPnLPct(t.direction,ep,xp); upd.status='closed'; }
  const { data, error } = await supabase.from('trades').update(upd)
    .eq('id',req.params.id).eq('telegram_user_id',req.userId).select().single();
  if (error) throw new Error(error.message);
  res.json({ ok:true, trade:data });
}));

app.delete('/api/trades/:id', requireAuth, wrap(async (req,res) => {
  const { error } = await supabase.from('trades').delete()
    .eq('id',req.params.id).eq('telegram_user_id',req.userId);
  if (error) throw new Error(error.message);
  res.json({ ok:true });
}));

// ── Exchange routes (с auth) ──────────────────────────────────────────────────
function mkEx(id, k, s) {
  const cfg={apiKey:k,secret:s,enableRateLimit:true};
  if(id==='binance') return new ccxt.binance({...cfg,options:{defaultType:'future'}});
  if(id==='bybit')   return new ccxt.bybit({...cfg,options:{defaultType:'linear'}});
  if(id==='bingx')   return new ccxt.bingx({...cfg,options:{defaultType:'swap'}});
  throw new Error(`Unsupported: ${id}`);
}

app.post('/api/connect', requireAuth, wrap(async (req,res) => {
  const {exchange,apiKey,secret}=req.body;
  const ex=mkEx(exchange,apiKey,secret);
  const bal=await ex.fetchBalance();
  res.json({ok:true,exchange,balance:+(bal.total?.USDT||0).toFixed(2)});
}));

app.get('/api/:exchange/balance', requireAuth, wrap(async (req,res) => {
  const {apiKey,secret}=req.query;
  const ex=mkEx(req.params.exchange,apiKey,secret);
  const bal=await ex.fetchBalance();
  const w={};
  for(const[k,v] of Object.entries(bal.total)) if(v>0) w[k]={total:+v.toFixed(8),free:+(bal.free[k]||0).toFixed(8)};
  res.json({ok:true,exchange:req.params.exchange,wallets:w,usdtTotal:+(bal.total?.USDT||0).toFixed(2)});
}));

app.get('/api/:exchange/positions', requireAuth, wrap(async (req,res) => {
  const {apiKey,secret}=req.query;
  const ex=mkEx(req.params.exchange,apiKey,secret);
  const pos=(await ex.fetchPositions()).filter(p=>+(p.contracts||0)>0).map(p=>({
    symbol:p.symbol,side:p.side,size:+p.contracts,entryPrice:+p.entryPrice,
    markPrice:+p.markPrice,unrealizedPnl:+p.unrealizedPnl,leverage:p.leverage,
    pnlPct:p.entryPrice&&p.markPrice?+((p.markPrice-p.entryPrice)/p.entryPrice*100*(p.side==='short'?-1:1)).toFixed(3):null,
    exchange:req.params.exchange
  }));
  res.json({ok:true,count:pos.length,positions:pos});
}));

// Шаг 5: Cron — напоминания ───────────────────────────────────────────────────
async function sendReminders() {
  const now=new Date();
  for(const [days, type] of [[5,'5day'],[1,'1day']]) {
    const from=new Date(now.getTime()+days*86400000).toISOString();
    const to  =new Date(now.getTime()+(days+1)*86400000).toISOString();
    const {data:subs}=await supabase.from('subscriptions').select('id,telegram_user_id,expires_at')
      .eq('status','active').gt('expires_at',from).lt('expires_at',to);
    for(const sub of subs||[]) {
      const {error}=await supabase.from('reminder_log').insert({
        telegram_user_id:sub.telegram_user_id, subscription_id:sub.id, reminder_type:type
      });
      if(!error) {
        const d=new Date(sub.expires_at).toLocaleDateString('ru-RU');
        await tgSend(sub.telegram_user_id, days===5
          ? `⏰ <b>Подписка истекает через 5 дней</b>\n📅 До: <b>${d}</b>\n\n🔄 Продли со скидкой 20%: @ParadoxxShop_bot`
          : `🚨 <b>Последний день подписки!</b>\n\nЗавтра доступ закроется.\n⚡ Продли: @ParadoxxShop_bot`
        );
      }
    }
    console.log(`[CRON] ${type} reminders: ${subs?.length||0}`);
  }
  // Помечаем просроченные
  await supabase.from('subscriptions').update({status:'expired'}).eq('status','active').lt('expires_at',now.toISOString());
}

cron.schedule('0 10 * * *', sendReminders);

app.listen(PORT, () => console.log(`🚀 API v2 on :${PORT} — auth+trades+cron ready`));

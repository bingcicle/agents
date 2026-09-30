#!/usr/bin/env python3
"""A2 PAPER-trader: event-armed post-only liquidity on Hyperliquid after a tracked whale's big sweep.

Strategy (see SPEC.md in the same directory):
  * fair(coin) = Binance USDT-M mid (bookTicker) * (1 + basis), basis = rolling 60-min median of (HL mid / Binance mid - 1),
    sampled once per second, strictly past.
  * TRIGGER: a taker sweep on HL (all prints of one order = same coin+hash+taker) by a TRACKED wallet with
    td = sweep_usd / depth_1% >= --td.  (Live trades feed does not say open/close; history showed this does not matter.)
  * ARM: a virtual ALO order on the side the whale hits (whale sells -> our BID) at L = fair * (1 - X%) (bid) or
    fair * (1 + X%) (ask), for --window seconds, re-pegged to fair when it moves > 0.05%. One order per (coin, direction);
    after a fill, no re-arm for --rearm seconds.
  * FILL (conservative queue rule): only HL prints with the same taker direction and price STRICTLY beyond L count, and only
    their USD volume (px*sz) counts toward our size. Prints at exactly L never fill us (we assume we are last in the queue).
  * EXIT: immediately a virtual ALO exit at A = fair*(1 - d%) (long) / fair*(1 + d%) (short), re-pegged; filled only by
    opposite-taker prints strictly through A (USD counted). If the HL touch is already better than A, take it (IOC).
    At first_fill + --hold seconds the remainder is closed by IOC walking the live HL book. Emergency: IOC if the HL touch
    is --stop % beyond our entry against us, or if Binance fair is stale > 5 s while in a position.
  * Fees: HL maker 0.015%, taker 0.045%.
Every arm / repeg-count / alo_reject / fill / exit / close / expire and a periodic stat line is written as JSON lines.

Run:  python3 a2_paper.py --minutes 180 [--coins VVV,NIL,...] [--X 1.0,1.25] [--size 2000] [--log a2_paper_log.jsonl]
Needs: websocket-client. Honors HTTPS_PROXY (http proxy) and a CA bundle (--ca).
"""
import argparse, json, os, sys, time, threading, statistics, collections
from urllib.parse import urlparse
import websocket

HERE = os.path.dirname(os.path.abspath(__file__))
FEE_MK, FEE_TK = 0.015, 0.045

# ------------------------------------------------------------------ args / config
ap = argparse.ArgumentParser()
ap.add_argument('--minutes', type=float, default=60)
ap.add_argument('--coins', default='', help='comma list of HL coins (default: a2_coins.json)')
ap.add_argument('--coins-file', default=os.path.join(HERE, 'a2_coins.json'), help='json {hl_coin: binance_symbol}')
ap.add_argument('--wallets', default=os.path.join(HERE, 'a2_wallets.txt'))
ap.add_argument('--depth-file', default=os.path.join(HERE, 'a2_depth.json'), help='fallback depth_1%% USD per coin')
ap.add_argument('--X', default='1.0,1.25', help='comma list of distances %% (each = independent virtual strategy); first = primary')
ap.add_argument('--td', type=float, default=0.3)
ap.add_argument('--window', type=float, default=60)
ap.add_argument('--rearm', type=float, default=60)
ap.add_argument('--size', type=float, default=2000, help='USD per order')
ap.add_argument('--hold', type=float, default=60)
ap.add_argument('--d', type=float, default=0.1, help='exit ALO distance from fair, %%')
ap.add_argument('--stop', type=float, default=3.0, help='emergency stop %% vs entry (HL touch)')
ap.add_argument('--max-pos', type=int, default=3)
ap.add_argument('--daily-loss', type=float, default=-150.0, help='USD; stop arming below this realized P&L')
ap.add_argument('--long-only', action='store_true', help='arm only bids (whale sells); SHORT side then logged as shadow')
ap.add_argument('--any-taker-trigger', action='store_true', help='TEST ONLY: trigger on untracked takers too')
ap.add_argument('--log', default=os.path.join(HERE, 'a2_paper_log.jsonl'))
ap.add_argument('--ca', default='/root/.ccr/ca-bundle.crt')
args = ap.parse_args()

COINMAP = json.load(open(args.coins_file))
if args.coins:
    COINMAP = {c: COINMAP[c] for c in args.coins.split(',') if c in COINMAP}
SYM2COIN = {v.upper(): k for k, v in COINMAP.items()}
WALLETS = set(l.strip().lower() for l in open(args.wallets) if l.strip())
DEPTH_FALLBACK = json.load(open(args.depth_file)) if os.path.exists(args.depth_file) else {}
XS = [float(x) for x in args.X.split(',')]
PROXY = urlparse(os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy') or '')
T_END = time.time() + args.minutes * 60

lock = threading.RLock()
logf = open(args.log, 'a')
def now_ms():
    return int(time.time() * 1000)
def log(ev, **kw):
    kw = {'t': now_ms(), 'ev': ev, **kw}
    with lock:
        logf.write(json.dumps(kw, separators=(',', ':'), default=float) + '\n'); logf.flush()
def say(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)

# ------------------------------------------------------------------ market state
class Coin:
    def __init__(self, c):
        self.c = c; self.bn_bid = self.bn_ask = None; self.bn_t = 0
        self.bids = []; self.asks = []; self.book_t = 0          # full-precision HL book [(px, sz)]
        self.agg_bid_1 = self.agg_ask_1 = None                     # USD within 1% from nSigFigs=3 book
        self.bas = collections.deque(); self.bas_last_s = 0
    def bn_mid(self):
        return (self.bn_bid + self.bn_ask) / 2 if self.bn_bid else None
    def hl_mid(self):
        return (self.bids[0][0] + self.asks[0][0]) / 2 if self.bids and self.asks else None
    def basis(self):
        t = time.time()
        while self.bas and self.bas[0][0] < t - 3600:
            self.bas.popleft()
        v = [b for ts, b in self.bas if ts < t - 5]
        return statistics.median(v) if len(v) >= 30 else None
    def fair(self):
        b = self.basis(); m = self.bn_mid()
        if b is None or m is None or time.time() - self.bn_t / 1000 > 5:
            return None
        return m * (1 + b)
    def depth1(self):
        # calibrated to the bot's depth_usd scale: depth_usd ~= 0.69 * (bid+ask USD within 1% of mid on the nSigFigs=3 book)
        if self.agg_bid_1 is not None and self.agg_ask_1 is not None:
            return 0.69 * (self.agg_bid_1 + self.agg_ask_1)
        return DEPTH_FALLBACK.get(self.c)
COINS = {c: Coin(c) for c in COINMAP}

# ------------------------------------------------------------------ strategy state
class Order:
    n = 0
    def __init__(self, X, coin, wd, fair, trig):
        Order.n += 1
        self.id = f'X{X}-{coin}-{Order.n}'; self.X = X; self.coin = coin; self.wd = wd
        self.side = 1 if wd < 0 else -1          # our side: +1 long (bid), -1 short (ask)
        self.L = fair * (1 - self.side * X / 100)
        self.t_arm = now_ms(); self.t_live = self.t_arm + 200; self.t_exp = self.t_arm + args.window * 1000
        self.size = args.size; self.filled = 0.0; self.repegs = 0; self.trig = trig; self.first_fill = None
class Position:
    def __init__(self, o):
        self.o = o; self.coin = o.coin; self.side = o.side; self.entry = o.L; self.usd = 0.0; self.t0 = now_ms()
        self.exit_usd = 0.0; self.exit_val = 0.0; self.exit_mk_usd = 0.0; self.fees = 0.0; self.A = None; self.closed = False
    def remaining(self):
        return self.usd - self.exit_usd
class Strat:
    def __init__(self, X):
        self.X = X; self.orders = {}; self.blocked = {}; self.pos = []; self.pnl = 0.0; self.n_closed = 0; self.n_fills = 0
STRATS = [Strat(X) for X in XS]
STATS = collections.Counter()
pending = {}          # (coin, hash, taker) -> sweep aggregate

def queue_view(c, L, side):
    """Visible resting USD at prices at-or-better than L within 0.1% of L on our side (full book), for logging."""
    lv = c.bids if side > 0 else c.asks
    ahead = sum(p * s for p, s in lv if abs(p / L - 1) < 0.001)
    span = abs(lv[-1][0] / c.hl_mid() - 1) * 100 if lv and c.hl_mid() else None
    return ahead, span

def arm(sw):
    c = COINS[sw['coin']]
    tracked = sw['taker'] in WALLETS
    if not (tracked or args.any_taker_trigger):
        return
    dep = c.depth1()
    if not dep:
        log('skip_trigger', coin=sw['coin'], why='no_depth', usd=sw['usd']); return
    td = sw['usd'] / dep
    if td < args.td:
        return
    f = c.fair()
    if f is None:
        log('skip_trigger', coin=sw['coin'], why='no_fair', usd=round(sw['usd']), td=round(td, 3), taker=sw['taker']); return
    exc_pl = 100 * sw['wd'] * (sw['pl'] / f - 1)
    for s in STRATS:
        key = (sw['coin'], sw['wd'])
        o = s.orders.get(key)
        if o is not None or s.blocked.get(key, 0) > now_ms():
            continue
        shadow = args.long_only and sw['wd'] > 0
        realized = s.pnl
        if realized < args.daily_loss:
            log('skip_trigger', X=s.X, coin=sw['coin'], why='daily_loss'); continue
        if sum(1 for p in s.pos if not p.closed) >= args.max_pos:
            log('skip_trigger', X=s.X, coin=sw['coin'], why='max_pos'); continue
        o = Order(s.X, sw['coin'], sw['wd'], f, dict(taker=sw['taker'], usd=round(sw['usd']), td=round(td, 3), exc_pl=round(exc_pl, 3),
                                                   hash=sw['hash'], tracked=tracked))
        o.shadow = shadow
        s.orders[key] = o
        q, span = queue_view(c, o.L, o.side)
        log('arm', id=o.id, X=s.X, coin=o.coin, side='LONG' if o.side > 0 else 'SHORT', L=o.L, fair=f, basis=c.basis(),
            depth1=round(dep), trig=o.trig, shadow=shadow, visible_at_L_usd=round(q), book_span_pct=span,
            agg_bid_1=c.agg_bid_1, agg_ask_1=c.agg_ask_1)

def on_print(coin, px, sz, wd, t):
    """Every HL print: fills of armed orders (strictly beyond L) and of exit ALOs (strictly through A)."""
    usd = px * sz
    for s in STRATS:
        o = s.orders.get((coin, wd))
        if o is not None and t >= o.t_live and o.filled < o.size:
            beyond = (px < o.L) if o.side > 0 else (px > o.L)
            if beyond:
                q = min(o.size - o.filled, usd)
                o.filled += q; s.n_fills += 1
                if o.first_fill is None:
                    o.first_fill = t
                    p = Position(o); s.pos.append(p); o.posobj = p
                p = o.posobj; p.usd += q; p.fees += q * FEE_MK / 100
                c = COINS[coin]
                log('fill', id=o.id, X=s.X, coin=coin, side='LONG' if o.side > 0 else 'SHORT', L=o.L, print_px=px, usd=round(q, 2),
                    filled=round(o.filled, 2), size=o.size, wait_s=(t - o.t_arm) / 1000, fair=c.fair(), shadow=o.shadow,
                    same_wallet_hint=None)
                if o.filled >= o.size - 1e-6:
                    del s.orders[(coin, wd)]; s.blocked[(coin, wd)] = t + args.rearm * 1000
        # exit ALO fills: opposite taker direction prints strictly through A
        for p in s.pos:
            if p.closed or p.coin != coin or p.A is None or p.remaining() <= 1e-9:
                continue
            if wd == p.side:        # long exit is hit by BUY takers (wd=+1)
                through = (px > p.A) if p.side > 0 else (px < p.A)
                if through:
                    q = min(p.remaining(), usd)
                    p.exit_usd += q; p.exit_val += q * (p.A / p.entry); p.exit_mk_usd += q; p.fees += q * FEE_MK / 100
                    log('exit_maker', id=p.o.id, coin=coin, A=p.A, print_px=px, usd=round(q, 2), remaining=round(p.remaining(), 2))
                    if p.remaining() <= 1e-6:
                        close(s, p, 'maker')

def taker_exit(s, p, why):
    """IOC through the live HL book for the remaining USD; VWAP by walking levels."""
    c = COINS[p.coin]
    lv = c.bids if p.side > 0 else c.asks
    rem = p.remaining(); val = 0.0; got = 0.0
    for px, sz in lv:
        u = px * sz; q = min(rem - got, u)
        val += q * (px / p.entry); got += q
        if got >= rem - 1e-9:
            break
    if got < rem - 1e-9:                          # book too thin: assume the rest 1% worse than the last level
        lastpx = lv[-1][0] if lv else p.entry
        val += (rem - got) * (lastpx * (1 - p.side * 0.01) / p.entry); got = rem
    p.exit_usd += rem; p.exit_val += val; p.fees += rem * FEE_TK / 100
    log('exit_taker', id=p.o.id, coin=p.coin, why=why, usd=round(rem, 2), vwap_rel=val / rem if rem else None,
        touch=lv[0][0] if lv else None, fair=c.fair())
    close(s, p, why)

def close(s, p, how):
    p.closed = True
    # P&L: long: sum(q * (exit/entry - 1)); short: sum(q * (1 - exit/entry))
    gross = p.side * (p.exit_val - p.exit_usd)
    net = gross - p.fees
    if not p.o.shadow:
        s.pnl += net; s.n_closed += 1
    log('close', id=p.o.id, X=s.X, coin=p.coin, side='LONG' if p.side > 0 else 'SHORT', how=how, usd=round(p.usd, 2),
        maker_exit_share=round(p.exit_mk_usd / p.usd, 3) if p.usd else None, gross_usd=round(gross, 3), fees_usd=round(p.fees, 3),
        net_usd=round(net, 3), net_pct=round(100 * net / p.usd, 4) if p.usd else None, hold_s=(now_ms() - p.t0) / 1000,
        shadow=p.o.shadow, cum_pnl_usd=round(s.pnl, 3))

def tick():
    """200 ms housekeeping: expire/re-peg orders, manage exits, flush sweeps."""
    t = now_ms()
    # basis samples: once per second per coin (HL book is pushed on change, so the last book is current)
    sec = int(t / 1000)
    for c in COINS.values():
        hm, bm = c.hl_mid(), c.bn_mid()
        if hm and bm and sec != c.bas_last_s and t / 1000 - c.bn_t / 1000 < 2:
            c.bas.append((t / 1000, hm / bm - 1)); c.bas_last_s = sec
    # flush sweeps idle > 300 ms
    for k in [k for k, v in pending.items() if t - v['last_r'] > 300]:
        sw = pending.pop(k)
        STATS['sweeps'] += 1
        arm(sw)
    for s in STRATS:
        for key, o in list(s.orders.items()):
            c = COINS[o.coin]; f = c.fair()
            if t > o.t_exp:
                del s.orders[key]
                if o.first_fill is not None:
                    s.blocked[key] = o.first_fill + args.rearm * 1000
                log('expire', id=o.id, X=s.X, coin=o.coin, filled=round(o.filled, 2), repegs=o.repegs)
                continue
            if f is not None:
                L2 = f * (1 - o.side * o.X / 100)
                if abs(L2 / o.L - 1) > 0.0005 and o.filled < o.size:
                    o.L = L2; o.repegs += 1
                # ALO sanity: a bid at/above the best ask would be rejected
                if o.side > 0 and c.asks and o.L >= c.asks[0][0] or o.side < 0 and c.bids and o.L <= c.bids[0][0]:
                    log('alo_reject', id=o.id, L=o.L); STATS['alo_reject'] += 1
        for p in s.pos:
            if p.closed or p.usd <= 0:
                continue
            c = COINS[p.coin]; f = c.fair()
            touch = (c.bids[0][0] if p.side > 0 else c.asks[0][0]) if (c.bids and c.asks) else None
            if f is not None:
                p.A = f * (1 - p.side * args.d / 100)
            # keep adding size while the entry order is still filling; exit logic starts at first fill
            if touch is not None and p.side * (touch / p.entry - 1) * 100 <= -args.stop:
                taker_exit(s, p, 'stop'); continue
            if f is None and time.time() - c.bn_t / 1000 > 5:
                taker_exit(s, p, 'stale_fair'); continue
            if touch is not None and p.A is not None and p.side * (touch - p.A) >= 0 and p.remaining() > 0:
                taker_exit(s, p, 'touch_better_than_A'); continue
            if t - p.t0 >= args.hold * 1000:
                o = p.o
                if (o.coin, o.wd) in s.orders and s.orders[(o.coin, o.wd)] is o:     # stop entry before final exit
                    del s.orders[(o.coin, o.wd)]; s.blocked[(o.coin, o.wd)] = t + args.rearm * 1000
                taker_exit(s, p, 'hold')

# ------------------------------------------------------------------ websocket handlers
def hl_msg(m):
    ch = m.get('channel')
    if ch == 'trades':
        r = now_ms()
        with lock:
            for tr in m['data']:
                coin = tr['coin']
                if coin not in COINS:
                    continue
                px = float(tr['px']); sz = float(tr['sz']); wd = 1 if tr['side'] == 'B' else -1
                taker = (tr['users'][0] if wd > 0 else tr['users'][1]).lower()
                STATS['prints'] += 1
                on_print(coin, px, sz, wd, r)
                k = (coin, tr['hash'], taker)
                sw = pending.get(k)
                if sw is None:
                    pending[k] = dict(coin=coin, hash=tr['hash'], taker=taker, wd=wd, usd=px * sz, pf=px, pl=px, t=tr['time'], last_r=r)
                else:
                    sw['usd'] += px * sz; sw['last_r'] = r
                    sw['pl'] = min(sw['pl'], px) if wd < 0 else max(sw['pl'], px)
    elif ch == 'l2Book':
        d = m['data']; c = COINS.get(d['coin'])
        if c is None:
            return
        b, a = d['levels']
        with lock:
            c.bids = [(float(x['px']), float(x['sz'])) for x in b]; c.asks = [(float(x['px']), float(x['sz'])) for x in a]
            c.book_t = d['time']
        STATS['books'] += 1

def hl_agg_msg(m):
    if m.get('channel') != 'l2Book':
        return
    d = m['data']; c = COINS.get(d['coin'])
    if c is None:
        return
    b, a = d['levels']
    if not b or not a:
        return
    bp = [(float(x['px']), float(x['sz'])) for x in b]; ap_ = [(float(x['px']), float(x['sz'])) for x in a]
    mid = (bp[0][0] + ap_[0][0]) / 2
    with lock:
        c.agg_bid_1 = sum(p * s for p, s in bp if p >= mid * 0.99)
        c.agg_ask_1 = sum(p * s for p, s in ap_ if p <= mid * 1.01)
    STATS['agg_books'] += 1

def bn_msg(m):
    d = m.get('data') or {}
    if d.get('e') != 'bookTicker':
        return
    c = COINS.get(SYM2COIN.get(d['s']))
    if c is None:
        return
    with lock:
        c.bn_bid = float(d['b']); c.bn_ask = float(d['a']); c.bn_t = d.get('T') or d.get('E') or now_ms()
    STATS['bn'] += 1

def run_ws(name, url, subs, handler, ping=None):
    while time.time() < T_END:
        try:
            ws = websocket.WebSocket(sslopt={'ca_certs': args.ca} if os.path.exists(args.ca) else {})
            kw = dict(timeout=30)
            if PROXY.hostname:
                kw.update(http_proxy_host=PROXY.hostname, http_proxy_port=PROXY.port, proxy_type='http')
            ws.connect(url, **kw)
            for s in subs:
                ws.send(json.dumps(s))
            say(name, 'connected', len(subs), 'subs'); log('ws_connected', name=name, subs=len(subs))
            last_ping = time.time()
            while time.time() < T_END:
                raw = ws.recv()
                if not raw:
                    continue
                STATS['msg_' + name] += 1
                try:
                    handler(json.loads(raw))
                except Exception as e:
                    STATS['handler_err'] += 1
                    if STATS['handler_err'] < 5:
                        log('handler_error', name=name, err=repr(e)[:300])
                if ping and time.time() - last_ping > 30:
                    ws.send(json.dumps({'method': 'ping'})); last_ping = time.time()
            ws.close()
        except Exception as e:
            say(name, 'error', repr(e)[:200]); log('ws_error', name=name, err=repr(e)[:300]); time.sleep(3)

def main():
    log('start', args=vars(args), coins=list(COINMAP), wallets=len(WALLETS))
    say('coins', len(COINMAP), 'wallets', len(WALLETS), 'X', XS, 'minutes', args.minutes)
    hl_subs = []
    for c in COINMAP:
        hl_subs.append({'method': 'subscribe', 'subscription': {'type': 'trades', 'coin': c}})
        hl_subs.append({'method': 'subscribe', 'subscription': {'type': 'l2Book', 'coin': c}})
    agg_subs = [{'method': 'subscribe', 'subscription': {'type': 'l2Book', 'coin': c, 'nSigFigs': 3}} for c in COINMAP]
    bn_url = 'wss://fstream.binance.com/stream?streams=' + '/'.join(f'{s.lower()}@bookTicker' for s in COINMAP.values())
    th = [threading.Thread(target=run_ws, args=('hl', 'wss://api.hyperliquid.xyz/ws', hl_subs, hl_msg, True), daemon=True),
          threading.Thread(target=run_ws, args=('hl_agg', 'wss://api.hyperliquid.xyz/ws', agg_subs, hl_agg_msg, True), daemon=True),
          threading.Thread(target=run_ws, args=('bn', bn_url, [], bn_msg, False), daemon=True)]
    for t in th:
        t.start()
    last_stat = time.time()
    while time.time() < T_END:
        time.sleep(0.2)
        with lock:
            tick()
            if time.time() - last_stat >= 30:
                last_stat = time.time()
                ready = sum(1 for c in COINS.values() if c.fair() is not None)
                st = dict(STATS); st.update(fair_ready=ready, armed=sum(len(s.orders) for s in STRATS),
                                            open_pos=sum(1 for s in STRATS for p in s.pos if not p.closed),
                                            pnl={str(s.X): round(s.pnl, 3) for s in STRATS}, closed={str(s.X): s.n_closed for s in STRATS})
                log('stat', **st); say('stat', json.dumps(st))
    with lock:                                    # end of session: cancel orders, flatten positions
        for s in STRATS:
            for key, o in list(s.orders.items()):
                log('cancel_eos', id=o.id, filled=o.filled); del s.orders[key]
            for p in s.pos:
                if not p.closed and p.usd > 0:
                    taker_exit(s, p, 'end_of_session')
        summ = {str(s.X): dict(pnl_usd=round(s.pnl, 3), closed=s.n_closed, fill_prints=s.n_fills) for s in STRATS}
        log('end', summary=summ, stats=dict(STATS)); say('end', json.dumps(summ), json.dumps(dict(STATS)))

if __name__ == '__main__':
    main()

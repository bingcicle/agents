"""Shared research library for the HL whale-closure study.

Import:  import sys; sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib'); from hl import *

Conventions
- All timestamps are UTC epoch milliseconds (ms) unless the name ends in _s.
- `side` is OUR side: 'LONG' (+1) or 'SHORT' (-1). Returns are signed so that + means profit for our side.
- Binance USDT-M futures prices. 1-second bars built from aggTrades (only for (symbol, day) with events),
  1-minute klines for every symbol 2026-08-20..2026-09-29. Binance archive has no 30.09 yet (published next day).
- OFFICIAL methodology of the bot (what prior research used, conservative):
    entry price = WORST for us among 1s bars [s, s+2] after the decision second (LONG: max high, SHORT: min low),
    fallback window [s, s+9]; exit price the same rule; net = gross - COSTS_OFFICIAL - funding.
  COSTS_OFFICIAL ~ 0.146% per round trip (the bot's costs_pct median). Use the row's costs_pct when available.
- Alternative cost scenarios for sensitivity (per round trip, on top of the price rule used):
    TAKER_FEES = 0.10 (Binance VIP0 taker 0.05% x2), MAKER_FEES = 0.04 (0.02% x2), HL_TAKER=0.09 (0.045 x2), HL_MAKER=0.03.
"""
import os, json, functools, math
import numpy as np, pandas as pd

SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
DATA = SP + 'data/'
BOT = SP + 'hl_whale_export_20260930/02_bot_data/'
RES = SP + 'hl_whale_export_20260930/03_research_datasets/'
SYM = json.load(open(SP + 'infra/symmap.json'))          # HL coin -> Binance symbol (or None)
COSTS_OFFICIAL = 0.146
TAKER_FEES, MAKER_FEES, HL_TAKER, HL_MAKER = 0.10, 0.04, 0.09, 0.03

def sgn(side):
    return 1 if side in ('LONG', 1, 'B', 'buy') else -1

def day_of(ms):
    return pd.Timestamp(int(ms), unit='ms').strftime('%Y-%m-%d')

# ---------------------------------------------------------------- 1-second bars
@functools.lru_cache(maxsize=256)
def _bars_day(symbol, day):
    fn = f'{DATA}bars1s/{symbol}/{day}.npz'
    if not os.path.exists(fn):
        return None
    z = np.load(fn)
    return {k: z[k] for k in z.files}

def bars(symbol, t0_ms, t1_ms):
    """Dense per-second frame for [t0, t1] (seconds). Columns: o,h,l,c (NaN where no trade), cf (ffilled close),
    quote, bq (taker-buy quote), n. Index = epoch second. Returns None if no data for any needed day."""
    s0, s1 = int(t0_ms // 1000), int(t1_ms // 1000)
    days = pd.date_range(pd.Timestamp(s0 - 1, unit='s').normalize(), pd.Timestamp(s1, unit='s').normalize(), freq='D')
    parts = []
    for d in days:
        b = _bars_day(symbol, d.strftime('%Y-%m-%d'))
        if b is not None:
            parts.append(pd.DataFrame({k: b[k] for k in ('o', 'h', 'l', 'c', 'quote', 'bq', 'n')}, index=b['sec']))
    if not parts:
        return None
    df = pd.concat(parts)
    df = df[~df.index.duplicated()]
    # include previous trades to ffill the first seconds
    prev = df[df.index < s0]
    idx = np.arange(s0, s1 + 1)
    out = df.reindex(idx)
    out['cf'] = out.c.ffill()
    if len(prev) and np.isnan(out.cf.iloc[0]):
        out['cf'] = out.cf.fillna(prev.c.iloc[-1])
    out[['quote', 'bq', 'n']] = out[['quote', 'bq', 'n']].fillna(0)
    return out

def has_bars(symbol, ms):
    return os.path.exists(f'{DATA}bars1s/{symbol}/{day_of(ms)}.npz')

def worst_px(b, sec, side, win=3, fallback=10):
    """Worst price for OUR trade in bars [sec, sec+win-1]; side=+1 buying (max high), -1 selling (min low).
    b = frame from bars(). Returns NaN if no trade in fallback window."""
    for w in (win, fallback):
        sl = b.loc[sec:sec + w - 1]
        if len(sl) and sl.n.sum() > 0:
            return float(sl.h.max()) if side > 0 else float(sl.l.min())
    return float('nan')

def last_px(b, sec):
    """Last traded price at or before the end of second `sec` (ffilled close)."""
    try:
        return float(b.cf.loc[sec])
    except KeyError:
        return float('nan')

# ---------------------------------------------------------------- 1-minute klines
@functools.lru_cache(maxsize=512)
def _k_sym(symbol):
    d = f'{DATA}k1m/{symbol}/'
    if not os.path.isdir(d):
        return None
    parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn)
        parts.append(pd.DataFrame({k: z[k] for k in ('o', 'h', 'l', 'c', 'qv', 'tbqv', 'n')}, index=z['ot']))
    df = pd.concat(parts).sort_index()
    return df[~df.index.duplicated()]

def kl(symbol):
    """Full 1m kline frame for symbol (index = open time ms). Columns o,h,l,c,qv (quote vol),tbqv (taker-buy quote),n."""
    return _k_sym(symbol)

def px_at_min(symbol, ms, field='c'):
    """Close of the 1m candle containing ms-1 (i.e. last price at ms, minute resolution); NaN if missing."""
    k = kl(symbol)
    if k is None:
        return float('nan')
    i = k.index.searchsorted(ms - 60000, side='right') - 1
    if i < 0 or i >= len(k):
        return float('nan')
    return float(k[field].iloc[i])

def ret_min(symbol, t0_ms, t1_ms):
    """% return from t0 to t1 using 1m closes (last price of minute before each time)."""
    a, b = px_at_min(symbol, t0_ms), px_at_min(symbol, t1_ms)
    return 100 * (b / a - 1) if a and b and not (np.isnan(a) or np.isnan(b)) else float('nan')

# ---------------------------------------------------------------- statistics (honesty standard of prior research)
def boot_ci(values, clusters=None, stat=np.median, n=2000, q=(5, 95), seed=0):
    """Cluster bootstrap CI (resample clusters with replacement). Returns (lo, hi, P(stat<=0))."""
    v = np.asarray(values, float)
    rng = np.random.default_rng(seed)
    if clusters is None:
        clusters = np.arange(len(v))
    cl = pd.Series(v).groupby(np.asarray(clusters)).apply(list).tolist()
    k = len(cl)
    stats = []
    for _ in range(n):
        pick = rng.integers(0, k, k)
        vv = np.concatenate([cl[i] for i in pick])
        stats.append(stat(vv))
    stats = np.array(stats)
    return float(np.percentile(stats, q[0])), float(np.percentile(stats, q[1])), float((stats <= 0).mean())

def summary(x, label=''):
    x = np.asarray(x, float); x = x[~np.isnan(x)]
    if len(x) == 0:
        return f'{label} n=0'
    top = np.sort(x)[::-1]; k = max(1, int(round(len(x) * 0.1)))
    return (f'{label} n={len(x)} med={np.median(x):+.3f} mean={x.mean():+.3f} win={100*(x>0).mean():.0f}% '
            f'sum={x.sum():+.2f} sum_wo_top10%={top[k:].sum():+.2f}')

def honesty(df, col, wallet='whale', day='day', coin='coin'):
    """Standard battery: n, median, mean, win, cluster-CI by wallet/day/coin, halves, sum without top10%, concentration."""
    d = df[~df[col].isna()].copy()
    if len(d) == 0:
        return {'n': 0}
    x = d[col].values
    out = {'n': len(d), 'median': float(np.median(x)), 'mean': float(x.mean()), 'win': float((x > 0).mean())}
    for c in (wallet, day, coin):
        if c in d:
            lo, hi, p = boot_ci(x, d[c].values, np.mean)
            out[f'mean_ci90_{c}'] = (round(lo, 3), round(hi, 3)); out[f'P(mean<=0)_{c}'] = round(p, 3)
            lo, hi, p = boot_ci(x, d[c].values, np.median)
            out[f'med_ci90_{c}'] = (round(lo, 3), round(hi, 3))
    if day in d:
        days = sorted(d[day].unique()); h = len(days) // 2
        out['half1_mean'] = float(d[d[day].isin(days[:h])][col].mean()) if h else float('nan')
        out['half2_mean'] = float(d[d[day].isin(days[h:])][col].mean())
    xs = np.sort(x)[::-1]; k = max(1, int(round(len(x) * 0.1)))
    out['sum'] = float(x.sum()); out['sum_wo_top10'] = float(xs[k:].sum())
    for c in (wallet, day, coin):
        if c in d:
            pos = d[d[col] > 0].groupby(c)[col].sum()
            out[f'max_share_{c}'] = float(pos.max() / pos.sum()) if pos.sum() > 0 else float('nan')
    return out

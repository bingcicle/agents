"""Market-drift helper: our-direction returns of coin, BTC and an equal-weight alt index over [t0, t1] (UTC ms),
from Binance 1m klines (<= 29.09) or HL 1m candles (27.09 08:26 .. 30.09 19:46 UTC) when Binance is missing."""
import os, json, functools
import numpy as np, pandas as pd

SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
SYM = json.load(open(SP + 'infra/symmap.json'))
K1 = SP + 'data/k1m/'
HL = SP + 'data/hl/'


@functools.lru_cache(maxsize=None)
def bn_close(symbol):
    d = K1 + symbol + '/'
    if not os.path.isdir(d):
        return None
    parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn)
        parts.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(parts).sort_index()
    return s[~s.index.duplicated()]


@functools.lru_cache(maxsize=None)
def hl_close(coin):
    fn = HL + coin + '_1m.json'
    if not os.path.exists(fn):
        return None
    d = json.load(open(fn))
    if not d:
        return None
    s = pd.Series([float(x['c']) for x in d], index=[int(x['t']) for x in d]).sort_index()
    return s[~s.index.duplicated()]


def px(s, ms):
    """close of the last complete minute before ms."""
    if s is None or len(s) == 0:
        return np.nan
    i = s.index.searchsorted(ms - 60000, side='right') - 1
    if i < 0:
        return np.nan
    if ms - 60000 - s.index[i] > 5 * 60000:   # gap > 5 min -> unreliable
        return np.nan
    if s.index[-1] < ms - 120000:
        return np.nan
    return float(s.iloc[i])


def ret(coin, t0, t1, sign):
    """our-direction % return of coin over [t0,t1]; returns (ret, src)."""
    sym = SYM.get(coin)
    b = bn_close(sym) if sym else None
    a0, a1 = px(b, t0), px(b, t1)
    if np.isfinite(a0) and np.isfinite(a1):
        return sign * 100 * (a1 / a0 - 1), 'bn'
    h = hl_close(coin)
    a0, a1 = px(h, t0), px(h, t1)
    if np.isfinite(a0) and np.isfinite(a1):
        return sign * 100 * (a1 / a0 - 1), 'hl'
    return np.nan, None


@functools.lru_cache(maxsize=1)
def alt_universe():
    syms = sorted(os.listdir(K1))
    hlc = sorted({f.split('_')[0] for f in os.listdir(HL) if f.endswith('_1m.json')})
    return syms, hlc


def alt_index_ret(t0, t1, sign, src):
    """equal-weight mean our-direction return of all alts (excl. BTC) over [t0, t1]."""
    syms, hlc = alt_universe()
    rs = []
    if src == 'bn':
        for s in syms:
            if s == 'BTCUSDT':
                continue
            c = bn_close(s)
            a0, a1 = px(c, t0), px(c, t1)
            if np.isfinite(a0) and np.isfinite(a1) and a0 > 0:
                rs.append(100 * (a1 / a0 - 1))
    else:
        for coin in hlc:
            if coin == 'BTC':
                continue
            c = hl_close(coin)
            a0, a1 = px(c, t0), px(c, t1)
            if np.isfinite(a0) and np.isfinite(a1) and a0 > 0:
                rs.append(100 * (a1 / a0 - 1))
    return sign * float(np.mean(rs)) if len(rs) >= 20 else np.nan


def btc_ret(t0, t1, sign, src):
    if src == 'bn':
        c = bn_close('BTCUSDT')
    else:
        c = hl_close('BTC')
    a0, a1 = px(c, t0), px(c, t1)
    return sign * 100 * (a1 / a0 - 1) if np.isfinite(a0) and np.isfinite(a1) else np.nan


def same_day_placebo(coin, t0, hold_ms, sign, src):
    """mean and percentile rank of our-direction coin return over all windows of the same length starting at every minute
    of the same UTC day (placebo = same coin, same day, random time)."""
    sym = SYM.get(coin)
    c = bn_close(sym) if (src == 'bn' and sym) else hl_close(coin)
    if c is None:
        return np.nan, np.nan
    d0 = (t0 // 86400000) * 86400000
    idx = c.index
    m = (idx >= d0) & (idx < d0 + 86400000)
    starts = idx[m]
    if len(starts) < 200:
        return np.nan, np.nan
    s = c
    r = []
    vals = s.reindex(np.arange(d0 - 60000, d0 + 86400000 + hold_ms + 60000, 60000)).ffill()
    base = vals.values
    step = hold_ms // 60000
    rr = sign * 100 * (base[step:] / base[:-step] - 1)
    rr = rr[np.isfinite(rr)]
    return float(np.mean(rr)), rr

"""Independent price engine (skeptic). 1s Binance bars (worst-in-3s, last price) with k1m fallback; own alt index and BTC.
All times: UTC epoch seconds (float ok) unless *_ms."""
import os, json, functools
import numpy as np, pandas as pd

SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
DATA = SP + 'data/'
BOT = SP + 'hl_whale_export_20260930/02_bot_data/'
W = SP + 'work/skeptic-twap-flow/'
SYM = json.load(open(SP + 'infra/symmap.json'))
END_S = int(pd.Timestamp('2026-09-30').value // 10**9)  # Binance data ends 29.09 23:59:59 UTC


class Sym1s:
    """All available 1s bars of one symbol concatenated."""
    def __init__(self, sym):
        d = DATA + f'bars1s/{sym}/'
        parts = []
        if os.path.isdir(d):
            for fn in sorted(os.listdir(d)):
                z = np.load(d + fn)
                parts.append((z['sec'], z['h'], z['l'], z['c']))
        if parts:
            self.sec = np.concatenate([p[0] for p in parts]).astype(np.int64)
            self.h = np.concatenate([p[1] for p in parts]); self.l = np.concatenate([p[2] for p in parts])
            self.c = np.concatenate([p[3] for p in parts])
            o = np.argsort(self.sec, kind='stable'); self.sec, self.h, self.l, self.c = self.sec[o], self.h[o], self.l[o], self.c[o]
            self.days = set(pd.to_datetime(self.sec[::3600], unit='s').strftime('%Y-%m-%d'))
            self.days |= set(fn[:10] for fn in os.listdir(d))
        else:
            self.sec = np.zeros(0, np.int64); self.days = set()
        k = DATA + f'k1m/{sym}/'
        kp = []
        if os.path.isdir(k):
            for fn in sorted(os.listdir(k)):
                z = np.load(k + fn); kp.append((z['ot'], z['c'], z['qv']))
        if kp:
            self.kot = np.concatenate([p[0] for p in kp]).astype(np.int64) // 1000
            self.kc = np.concatenate([p[1] for p in kp]); self.kqv = np.concatenate([p[2] for p in kp])
            o = np.argsort(self.kot); self.kot, self.kc, self.kqv = self.kot[o], self.kc[o], self.kqv[o]
        else:
            self.kot = np.zeros(0, np.int64)

    def has1s(self, t):
        return pd.Timestamp(int(t), unit='s').strftime('%Y-%m-%d') in self.days

    def last(self, t, stale=300):
        """last traded price at or before second t (1s bars if that day exists, else k1m close of last complete minute)."""
        t = int(t)
        if t >= END_S or t <= 0:
            return np.nan
        if self.has1s(t) and len(self.sec):
            i = np.searchsorted(self.sec, t, 'right') - 1
            if i >= 0 and self.sec[i] >= t - stale:
                return float(self.c[i])
        return self.klast(t)

    def klast(self, t, stale=300):
        """close of last complete 1m candle before t (candle open <= t-60)."""
        if not len(self.kot) or t >= END_S:
            return np.nan
        i = np.searchsorted(self.kot, t - 60, 'right') - 1
        if i < 0 or self.kot[i] < t - 60 - stale:
            return np.nan
        return float(self.kc[i])

    def worst(self, t, side, win=3, fb=10):
        """worst price for our trade: side +1 buy -> max high in [t, t+win-1]; -1 sell -> min low. Fallback 10 s, then NaN."""
        t = int(t)
        if t + fb >= END_S or not len(self.sec) or not self.has1s(t):
            return np.nan
        for w in (win, fb):
            i0 = np.searchsorted(self.sec, t, 'left'); i1 = np.searchsorted(self.sec, t + w, 'left')
            if i1 > i0:
                return float(self.h[i0:i1].max()) if side > 0 else float(self.l[i0:i1].min())
        return np.nan

    def vol24(self, t):
        """average quote volume per minute over the 24 h before t (k1m), None if < 600 minutes present"""
        a = np.searchsorted(self.kot, t - 86400, 'left'); b = np.searchsorted(self.kot, t - 60, 'right')
        n = b - a
        return float(self.kqv[a:b].sum() / n) if n > 600 else np.nan


@functools.lru_cache(maxsize=1)
def market():
    """own equal-weight alt index (mean of 1m log returns across all k1m symbols except BTC, clipped) + BTC closes.
    value at minute grid g = after candle opened at g closes."""
    syms = sorted(os.listdir(DATA + 'k1m'))
    g0 = int(pd.Timestamp('2026-08-20').value // 10**9); g1 = END_S
    grid = np.arange(g0, g1, 60)
    acc = np.zeros(len(grid)); cnt = np.zeros(len(grid))
    btc = None
    for s in syms:
        kp = []
        for fn in sorted(os.listdir(DATA + f'k1m/{s}/')):
            z = np.load(DATA + f'k1m/{s}/{fn}'); kp.append((z['ot'] // 1000, z['c']))
        ot = np.concatenate([p[0] for p in kp]); c = np.concatenate([p[1] for p in kp])
        ser = pd.Series(c, index=ot); ser = ser[~ser.index.duplicated()].sort_index()
        cc = ser.reindex(grid).values
        if s == 'BTCUSDT':
            btc = pd.Series(cc).ffill().values
            continue
        lr = np.log(cc[1:] / cc[:-1])
        ok = np.isfinite(lr) & (np.abs(lr) < 0.2)
        acc[1:][ok] += lr[ok]; cnt[1:][ok] += 1
    alt = np.cumsum(np.where(cnt > 0, acc / np.maximum(cnt, 1), 0))
    return grid, alt, btc


def mkt_at(t):
    """(alt index level, btc) at time t using last complete minute (candle opened at floor(t/60)*60-60)."""
    grid, alt, btc = market()
    t = np.asarray(t, float)
    i = ((np.floor(t / 60) * 60 - 60) - grid[0]) // 60
    i = i.astype(np.int64)
    ok = (i >= 0) & (i < len(grid)) & (t < END_S)
    ii = np.clip(i, 0, len(grid) - 1)
    return np.where(ok, np.exp(alt[ii]), np.nan), np.where(ok, btc[ii], np.nan)


def load_twaps():
    d = pd.read_csv(BOT + 'twap_signals.csv')
    d = d[d.eol == '^'].copy()
    for c in ('date', 'start', 'end'):
        d[c + '_s'] = (pd.to_datetime(d[c]) - pd.Timedelta(hours=2)).astype('datetime64[s]').astype('int64')
    # sanity: twap_id carries unix start seconds
    idt = d.twap_id.str.split('-').str[1].astype('int64')
    d['id_minus_start'] = idt - d.start_s
    d['sym'] = d.coin.map(SYM)
    d['tdir'] = np.where(d.twap_side == 'buy', 1, -1)
    d['lag_s'] = d.date_s - d.start_s
    d['dday'] = pd.to_datetime(d.date_s, unit='s').dt.strftime('%Y-%m-%d')
    d['per'] = np.where(d.dday <= '2026-09-21', 'disc', np.where(d.dday <= '2026-09-29', 'test', 'none'))
    d['cd'] = d.coin + '|' + d.dday
    d['dur_min'] = d.dur_s / 60
    return d.reset_index(drop=True)


def cboot(v, cl, stat=np.mean, n=2000, seed=0):
    v = np.asarray(v, float); cl = np.asarray(cl)
    m = np.isfinite(v); v = v[m]; cl = cl[m]
    u, inv = np.unique(cl, return_inverse=True)
    groups = [v[inv == k] for k in range(len(u))]
    rng = np.random.default_rng(seed); k = len(groups)
    st = np.empty(n)
    for b in range(n):
        pick = rng.integers(0, k, k)
        st[b] = stat(np.concatenate([groups[i] for i in pick]))
    return np.percentile(st, 5), np.percentile(st, 95)


def bat(x, col, label=''):
    v = x[col].values.astype(float); m = np.isfinite(v); x = x[m]; v = v[m]
    if len(v) < 5:
        return f'{label} n={len(v)}'
    s = f'{label} n={len(v)} med={np.median(v):+.3f} mean={v.mean():+.3f} win={100*(v>0).mean():.0f}%'
    for c in ('cd', 'coin', 'dday', 'addr'):
        lo, hi = cboot(v, x[c].values, n=1000)
        s += f' CI[{c}]=[{lo:+.3f},{hi:+.3f}]'
    days = sorted(x.dday.unique()); h = len(days) // 2
    s += f' halves={x[x.dday.isin(days[:h])][col].mean():+.3f}/{x[x.dday.isin(days[h:])][col].mean():+.3f}'
    srt = np.sort(v)[::-1]; k = max(1, int(round(len(v) * .1)))
    s += f' sum={v.sum():+.2f} wo_top10={srt[k:].sum():+.2f}'
    for c in ('coin', 'dday', 'addr'):
        pos = x[x[col] > 0].groupby(c)[col].sum()
        s += f' mx_{c}={pos.max()/pos.sum():.2f}' if pos.sum() > 0 else ''
    return s

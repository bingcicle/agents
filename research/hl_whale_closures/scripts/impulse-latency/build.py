"""Build per-tx impulse / latency / exit matrix on Binance 1s bars for all whale close txs >= $1k (tx_matrix).
Output: txm.parquet (one row per tx) with signed LOG returns x100 in WHALE direction (wdir), relative to
p0 = last Binance trade price in the second BEFORE the tx second.
  m{k}    : last-trade (ffilled close) at end of second sec+k   (k in OFFS)
  ew{L}   : worst-in-3s ENTRY (buy in whale dir: max high; fallback 10s) with decision time ts+L s
  el{L}   : last-trade price at end of decision second (optimistic)
  xw{H}   : worst-in-3s EXIT of a follow position at sec+H (selling if wdir=+1: min low)
  xr{H}   : worst-in-3s EXIT of a FADE position at sec+H (buying back: max high if wdir=+1) -> for reversal
  gross follow (L,H) = xw{H} - ew{L};  fade entered at sec+K exit H = -(xr{H} - fw{K}) where fw{K} = worst fade entry
Placebo rows (same coin/day/wdir, random second >=120 s from any >=$1k tx of that coin) have prefix 'P_'.
Also: sig1m (std of 1m log returns prior 24h, %), qv24 (Binance quote vol prior 24h), qv60 (prior 60 s, from 1s bars),
alt index (equal-weight 1m log-return index of all k1m symbols) and BTC returns over key windows.
"""
import sys, os, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
from hl import DATA, SP
OUT = SP + 'work/impulse-latency/'
OFFS = [-300, -60, -10, -3, -2, -1, 0, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300, 600, 900, 1800, 3600]
LATS = [1, 2, 3, 5, 10, 20, 30, 60, 300]
HORS = [30, 60, 120, 300, 600, 900, 1800, 3600]
PAD0, PAD1 = 3700, 3800

def load_day(sym, day):
    fn = f'{DATA}bars1s/{sym}/{day}.npz'
    if not os.path.exists(fn):
        return None
    z = np.load(fn)
    return z['sec'], z['h'], z['l'], z['c'], z['n'], z['quote']

def dense(sym, day):
    D0 = int(pd.Timestamp(day).timestamp())
    base = D0 - PAD0
    N = PAD0 + 86400 + PAD1
    h = np.full(N, -np.inf); l = np.full(N, np.inf); c = np.full(N, np.nan); n = np.zeros(N); q = np.zeros(N)
    have_next = False
    for dd in (-1, 0, 1):
        d2 = (pd.Timestamp(day) + pd.Timedelta(days=dd)).strftime('%Y-%m-%d')
        r = load_day(sym, d2)
        if r is None:
            continue
        if dd == 1: have_next = True
        s, hh, ll, cc, nn, qq = r
        i = s - base
        m = (i >= 0) & (i < N)
        i = i[m]
        h[i] = hh[m]; l[i] = ll[m]; c[i] = cc[m]; n[i] = nn[m]; q[i] = qq[m]
    cf = pd.Series(c).ffill().values.copy()
    if not have_next:  # no data after day end -> invalidate beyond
        cut = PAD0 + 86400
        cf[cut:] = np.nan
        h[cut:] = -np.inf; l[cut:] = np.inf; n[cut:] = 0
    from numpy.lib.stride_tricks import sliding_window_view as sw
    def fwd(a, w, fn, fill):
        ap = np.concatenate([a, np.full(w - 1, fill)])
        return fn(sw(ap, w), axis=1)
    h3 = fwd(h, 3, np.max, -np.inf); h10 = fwd(h, 10, np.max, -np.inf)
    l3 = fwd(l, 3, np.min, np.inf); l10 = fwd(l, 10, np.min, np.inf)
    hw = np.where(np.isfinite(h3), h3, np.where(np.isfinite(h10), h10, np.nan))
    lw = np.where(np.isfinite(l3), l3, np.where(np.isfinite(l10), l10, np.nan))
    qc = np.concatenate([[0], np.cumsum(q)])
    return dict(base=base, cf=cf, hw=hw, lw=lw, qc=qc, n=n, N=N)

def metrics(dd, secs, tsms, wdir):
    """Vectorized metrics for events (secs: tx second, tsms: ms, wdir +-1). Returns dict of arrays."""
    base, cf, hw, lw, N = dd['base'], dd['cf'], dd['hw'], dd['lw'], dd['N']
    i0 = secs - base
    def g(a, idx):
        idx = np.asarray(idx)
        ok = (idx >= 0) & (idx < N)
        out = np.full(len(idx), np.nan)
        out[ok] = a[idx[ok]]
        return out
    p0 = g(cf, i0 - 1)
    lp0 = np.log(p0)
    out = {'p0': p0}
    def sl(px):
        return wdir * (np.log(px) - lp0) * 100
    for k in OFFS:
        out[f'm{k}'] = sl(g(cf, i0 + k))
    buyw = lambda idx: np.where(wdir > 0, g(hw, idx), g(lw, idx))   # worst price when trading IN whale dir
    sellw = lambda idx: np.where(wdir > 0, g(lw, idx), g(hw, idx))  # worst price when trading AGAINST whale dir
    for L in LATS:
        s = (tsms + L * 1000) // 1000 - base
        out[f'ew{L}'] = sl(buyw(s))
        out[f'el{L}'] = sl(g(cf, s))
        out[f'fw{L}'] = sl(sellw(s))   # fade entry (sell into whale buy) worst
    for H in HORS:
        out[f'xw{H}'] = sl(sellw(i0 + H))
        out[f'xr{H}'] = sl(buyw(i0 + H))
    qc = dd['qc']
    ii = np.clip(i0, 60, N - 1)
    out['qv60b'] = qc[ii] - qc[ii - 60]
    iip = np.clip(i0 + 1, 0, N - 61)
    out['qv60a'] = qc[iip + 60] - qc[iip]
    return out

def main():
    tm = pd.read_parquet(DATA + 'tx_matrix.parquet')
    tm = tm[tm.day <= '2026-09-29'].reset_index(drop=True)
    tm = tm[tm.symbol.notna()].reset_index(drop=True)
    tm['row'] = np.arange(len(tm))
    rng = np.random.default_rng(42)
    res, pres = [], []
    for (sym, day), grp in tm.groupby(['symbol', 'day']):
        if not os.path.exists(f'{DATA}bars1s/{sym}/{day}.npz'):
            continue
        dd = dense(sym, day)
        secs = grp.sec.values.astype(np.int64); tsms = grp.ts.values.astype(np.int64); wd = grp.wdir.values
        m = metrics(dd, secs, tsms, wd)
        m['row'] = grp.row.values
        res.append(pd.DataFrame(m))
        # placebo: random seconds in same day, >=120 s from any tx of this coin that day, with trades nearby
        D0 = int(pd.Timestamp(day).timestamp())
        allowed = np.ones(86400, bool)
        for s in np.unique(secs):
            a = max(0, s - D0 - 120); b = min(86400, s - D0 + 121)
            allowed[a:b] = False
        cand = np.where(allowed)[0]
        if len(cand) < 100:
            cand = np.arange(86400)
        for rep in range(2):
            ps = D0 + rng.choice(cand, len(grp))
            pms = ps * 1000 + (tsms % 1000)
            pm = metrics(dd, ps, pms, wd)
            pm['row'] = grp.row.values; pm['rep'] = rep; pm['psec'] = ps
            pres.append(pd.DataFrame(pm))
    R = pd.concat(res); P = pd.concat(pres)
    # vol & 24h volume & alt index / BTC from k1m
    from hl import kl
    feats = []
    for sym, grp in tm.groupby('symbol'):
        k = kl(sym)
        if k is None:
            continue
        lc = np.log(k.c.values); t = k.index.values
        r1 = np.diff(lc, prepend=np.nan) * 100
        s_r = pd.Series(r1, index=t)
        sig = s_r.rolling(1440, min_periods=600).std().values
        qv24 = pd.Series(k.qv.values).rolling(1440, min_periods=600).sum().values
        # value at minute fully before tx: last completed minute index
        idx = np.searchsorted(t, grp.ts.values - 60000, side='right') - 1
        ok = idx >= 0
        f = pd.DataFrame({'row': grp.row.values})
        f['sig1m'] = np.where(ok, sig[np.clip(idx, 0, None)], np.nan)
        f['qv24'] = np.where(ok, qv24[np.clip(idx, 0, None)], np.nan)
        feats.append(f)
    F = pd.concat(feats)
    # alt index: equal-weight mean of 1m log returns across all k1m symbols
    rets = {}
    for sym in os.listdir(DATA + 'k1m'):
        k = kl(sym)
        if k is None or len(k) < 1000:
            continue
        rets[sym] = pd.Series(np.diff(np.log(k.c.values), prepend=np.nan), index=k.index.values)
    RR = pd.DataFrame(rets)
    btc = RR['BTCUSDT'].fillna(0).cumsum()
    alt = RR.drop(columns=['BTCUSDT', 'ETHUSDT'], errors='ignore').clip(-0.2, 0.2).mean(axis=1).fillna(0).cumsum()
    idx_df = pd.DataFrame({'btc': btc, 'alt': alt})
    idx_df.to_parquet(OUT + 'index_1m.parquet')
    def at(series, ms):
        t = series.index.values
        i = np.searchsorted(t, ms - 60000, side='right') - 1   # close of minute containing ms-1ms approx
        v = series.values[np.clip(i, 0, len(t) - 1)]
        return np.where(i >= 0, v, np.nan)
    ts = tm.ts.values; wd = tm.wdir.values
    C = pd.DataFrame({'row': tm.row.values})
    for nm, ser in (('btc', btc), ('alt', alt)):
        b0 = at(ser, ts)
        for H in (60, 300, 900, 1800, 3600):
            C[f'{nm}{H}'] = wd * (at(ser, ts + H * 1000) - b0) * 100
        for K, H in ((60, 900), (60, 1800), (60, 3600), (300, 900), (300, 1800), (300, 3600)):
            C[f'{nm}{K}_{H}'] = wd * (at(ser, ts + H * 1000) - at(ser, ts + K * 1000)) * 100
    keep = ['row', 'addr', 'coin', 'symbol', 'wside', 'ts', 'sec', 'day', 'wdir', 'dir', 'liq', 'detect_src', 'bot_ts',
            'detect_lag_s', 'batch_ratio', 'batch_old_val', 'depth_usd', 'batch_full', 'tx_usd', 'tx_pct', 'sp', 'px',
            'pf', 'pl', 'qv_pre60', 'r1', 'r60', 'wentry1']
    X = tm[keep].merge(R, on='row', how='left').merge(F, on='row', how='left').merge(C, on='row', how='left')
    X.to_parquet(OUT + 'txm.parquet')
    P = P.merge(tm[['row', 'addr', 'coin', 'symbol', 'day', 'wdir', 'tx_usd']], on='row', how='left')
    P.to_parquet(OUT + 'placebo.parquet')
    print(X.shape, P.shape)
    # consistency check vs tx_matrix r1/r60/wentry1 (tx_matrix uses simple % returns)
    for a, b in (('m1', 'r1'), ('m60', 'r60'), ('ew1', 'wentry1')):
        d = (X[a] - X[b]).abs()
        print(a, b, 'median abs diff', d.median(), 'q99', d.quantile(.99), 'n', d.notna().sum())

if __name__ == '__main__':
    main()

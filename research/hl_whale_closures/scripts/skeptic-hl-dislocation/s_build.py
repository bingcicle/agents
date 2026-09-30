# Independent build: every whale tx (all dirs, all liq) 12.09-29.09 with Binance 1s refs and two independent basis estimates.
import os, json, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/skeptic-hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
w = pd.read_parquet(SP + 'data/whale_txs.parquet')
w = w[w.ts < pd.Timestamp('2026-09-30').value // 10**6].copy()
w['sym'] = w.coin.map(SYM)
# whale TRADE direction: +1 = whale buys
buy_dirs = {'Close Short', 'Open Long', 'Short > Long'}
w['wd'] = np.where(w.dir.isin(buy_dirs), 1, -1)
w['sec'] = (w.ts // 1000).astype(np.int64)
T0 = int(pd.Timestamp('2026-09-11').value // 10**9); T1 = int(pd.Timestamp('2026-09-30').value // 10**9); N = T1 - T0
OFF = [-5, -2, -1, 0, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300, 600]
res = []
for s, g in w.groupby('sym'):
    d = f'{SP}data/bars1s/{s}/'
    c = np.full(N, np.nan); h = np.full(N, np.nan); l = np.full(N, np.nan)
    avail = np.zeros(N, bool)
    for fn in sorted(os.listdir(d)):
        day0 = int(pd.Timestamp(fn[:10]).value // 10**9) - T0
        if day0 + 86400 <= 0 or day0 >= N:
            continue
        z = np.load(d + fn)
        i = z['sec'] - T0; m = (i >= 0) & (i < N)
        c[i[m]] = z['c'][m]; h[i[m]] = z['h'][m]; l[i[m]] = z['l'][m]
        avail[max(day0, 0):min(day0 + 86400, N)] = True
    # ffill close only within contiguous available spans; age since last trade
    idx = np.where(~np.isnan(c), np.arange(N), -1)
    # break ffill at unavailable seconds
    idx[~avail] = -2
    last = np.maximum.accumulate(np.where(idx >= 0, idx, -1))
    # detect a reset: if an unavailable second lies between last trade and now, invalidate
    unav_cum = np.cumsum(~avail)
    ok = last >= 0
    cf = np.full(N, np.nan); age = np.full(N, np.nan)
    ok2 = ok & (unav_cum - unav_cum[np.clip(last, 0, None)] == 0) & avail
    cf[ok2] = c[last[ok2]]; age[ok2] = (np.arange(N) - last)[ok2]
    i0 = (g.sec.values - T0).astype(np.int64)
    r = {}
    for o in OFF:
        j = i0 + o; jj = np.clip(j, 0, N - 1)
        v = cf[jj].copy(); v[(j < 0) | (j >= N)] = np.nan
        r[f'b{o}'] = v
    r['age_m1'] = age[np.clip(i0 - 1, 0, N - 1)]
    # worst price for a Binance trade IN whale direction within [sec+1, sec+3] (hedge after fill), fallback cf
    wd = g.wd.values
    Hs = np.vstack([h[np.clip(i0 + k, 0, N - 1)] for k in (1, 2, 3)]); Ls = np.vstack([l[np.clip(i0 + k, 0, N - 1)] for k in (1, 2, 3)])
    with np.errstate(all='ignore'):
        wh = np.where(wd > 0, np.nanmax(Hs, 0), np.nanmin(Ls, 0))
    r['hedge13'] = np.where(np.isnan(wh), cf[np.clip(i0 + 3, 0, N - 1)], wh)
    df = pd.DataFrame(r, index=g.index)
    res.append(df)
w = w.join(pd.concat(res))
print('rows', len(w), 'b-1 notna', w['b-1'].notna().mean())

# ---- basis estimate 1: HL 15m candle close vs Binance 1m close at the same instant; rolling median of previous 4 bars (1h), only bars ending <= ts
# ---- basis estimate 2: same with HL 5m, previous 12 bars
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'
    parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn); parts.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(parts).sort_index(); return s[~s.index.duplicated()]
for iv, nb, col in (('15m', 4, 'bas15'), ('5m', 12, 'bas5')):
    w[col] = np.nan
    for coin, g in w.groupby('coin'):
        fn = f'{SP}data/hl/{coin}_{iv}.json'
        if not os.path.exists(fn):
            continue
        dd = json.load(open(fn))
        if not dd:
            continue
        hdf = pd.DataFrame(dd)
        hdf = hdf[hdf.n.astype(int) > 0]           # skip empty candles (stale close)
        end = hdf['T'].astype(np.int64).values + 1    # instant after the last ms of the bar
        k = kline(SYM[coin])
        bc = k.reindex(end - 60000).values            # Binance close of the minute ending at the same instant
        b = pd.Series(hdf.c.astype(float).values / bc - 1, index=end).dropna()
        base = b.rolling(nb, min_periods=max(2, nb // 2)).median()
        pos = np.searchsorted(base.index.values, g.ts.values, side='right') - 1
        v = np.where(pos >= 0, base.values[np.clip(pos, 0, None)], np.nan)
        agev = g.ts.values - np.where(pos >= 0, base.index.values[np.clip(pos, 0, None)], 0)
        v[agev > 3 * 3600 * 1000] = np.nan
        w.loc[g.index, col] = v
print(w[['bas15', 'bas5']].describe())
print('corr', w[['bas15', 'bas5']].corr().iloc[0, 1], 'med|diff|', (w.bas15 - w.bas5).abs().median())
w.to_parquet(OUT + 's_tx.parquet')

# Independent rebuild from raw: whale_txs + bars1s + k1m + HL candles. Own fair, two own basis estimates.
import os, json, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/p2-skeptic-a2/'
SYM = json.load(open(SP + 'infra/symmap.json'))
w = pd.read_parquet(SP + 'data/whale_txs.parquet')
w = w[(w.ts < pd.Timestamp('2026-09-30').value // 10**6)].copy()
w['sym'] = w.coin.map(SYM)
w = w[w.sym.notna()].copy()
w['wd'] = np.where(w.dir.isin(['Close Short', 'Open Long', 'Short > Long']), 1, -1)
w['sec'] = (w.ts // 1000).astype(np.int64)
OFF = [-3, -1, 0, 60, 90, 120, 300, 600]
parts = []
for s, g in w.groupby('sym'):
    d = f'{SP}data/bars1s/{s}/'
    if not os.path.isdir(d):
        continue
    secs, cl, lo, hi = [], [], [], []
    for fn in sorted(os.listdir(d)):
        if fn[:10] < '2026-09-11':
            continue
        z = np.load(d + fn); secs.append(z['sec']); cl.append(z['c']); lo.append(z['l']); hi.append(z['h'])
    if not secs:
        continue
    secs = np.concatenate(secs); cl = np.concatenate(cl); lo = np.concatenate(lo); hi = np.concatenate(hi)
    o = np.argsort(secs); secs, cl, lo, hi = secs[o], cl[o], lo[o], hi[o]
    days = set(fn[:10] for fn in os.listdir(d))
    r = {}
    for off in OFF:
        q = g.sec.values + off                      # last close at or before second q-? : value "at sec+off" = last trade with sec <= q
        if off == -1:
            q = g.sec.values - 1
        pos = np.searchsorted(secs, q, side='right') - 1
        v = np.where(pos >= 0, cl[np.clip(pos, 0, None)], np.nan)
        age = q - np.where(pos >= 0, secs[np.clip(pos, 0, None)], -10**9)
        v[age > 600] = np.nan                        # stale / missing day -> NaN
        r[f'c{off}'] = v
    # min low / max high over (sec, sec+60] for MAE
    for k in ('mae60',):
        vals = np.full(len(g), np.nan)
        a = np.searchsorted(secs, g.sec.values, side='right'); b = np.searchsorted(secs, g.sec.values + 60, side='right')
        for i in range(len(g)):
            if b[i] > a[i]:
                vals[i] = lo[a[i]:b[i]].min() if g.wd.values[i] < 0 else hi[a[i]:b[i]].max()
        r[k] = vals
    df = pd.DataFrame(r, index=g.index)
    df['bn_day_ok'] = pd.to_datetime(g.ts, unit='ms').dt.strftime('%Y-%m-%d').isin(days).values
    parts.append(df)
w = w.join(pd.concat(parts), how='inner')

# ---- basis A: HL 15m close vs Binance 1m close of the minute ending at the same instant; median of last 8 bars ended <= ts
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'
    ps = []
    for fn in sorted(os.listdir(d)):
        if fn[:10] < '2026-09-05':
            continue
        z = np.load(d + fn); ps.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(ps).sort_index(); return s[~s.index.duplicated()]
w['basA'] = np.nan
for coin, g in w.groupby('coin'):
    fn = f'{SP}data/hl/{coin}_15m.json'
    if not os.path.exists(fn):
        continue
    h = pd.DataFrame(json.load(open(fn)))
    if h.empty:
        continue
    h = h[h.n.astype(int) > 5]
    end = h['T'].astype(np.int64).values + 1
    k = kline(SYM[coin])
    b = pd.Series(h.c.astype(float).values / k.reindex(end - 60000).values - 1, index=end).dropna()
    base = b.rolling(8, min_periods=4).median()
    pos = np.searchsorted(base.index.values, g.ts.values, side='right') - 1
    v = np.where(pos >= 0, base.values[np.clip(pos, 0, None)], np.nan)
    v[(g.ts.values - base.index.values[np.clip(pos, 0, None)]) > 3 * 3600e3] = np.nan
    w.loc[g.index, 'basA'] = v
# ---- basis B: from whale prints themselves (HL px vs Binance close of previous second), balanced buy/sell medians, past 60 min,
# only small txs (tx_usd < 5% of depth) and strictly before ts - 5 s
w = w.sort_values('ts').reset_index(drop=True)
w['rawb'] = w.px / w['c-1'] - 1
w['basB'] = np.nan
for coin, g in w.groupby('coin'):
    small = g[(g.tx_usd < 0.05 * g.depth_usd) & g.rawb.notna()]
    out = np.full(len(g), np.nan)
    tsb = {sd: small[small.wd == sd] for sd in (1, -1)}
    for i, t in enumerate(g.ts.values):
        m = []
        for sd in (1, -1):
            x = tsb[sd]
            a = np.searchsorted(x.ts.values, t - 3600e3); b = np.searchsorted(x.ts.values, t - 5000)
            if b - a >= 3:
                m.append(np.median(x.rawb.values[a:b]))
        if len(m) == 2:
            out[i] = np.mean(m)
    w.loc[g.index, 'basB'] = out
print(w[['basA', 'basB']].describe(), w[['basA', 'basB']].corr())
w.to_parquet(OUT + 'tx.parquet')

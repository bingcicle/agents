# HL-vs-Binance baseline basis per coin, two independent estimators, attached to each tx (no look-ahead: only data before tx).
#  A) HL 5m candle close / Binance last price at the same instant (1m kline close) - 1, rolling median of the previous 12 bars (1 h)
#  B) whale prints of the same coin in [ts-30min, ts-5s]: median(px/bn_prev_sec-1) separately for buy & sell prints, averaged
import json, os, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
import sys; sys.path.insert(0, SP + 'lib'); from hl import kl

w = pd.read_parquet(W + 'q1_tx_raw.parquet')
w['g_print'] = w.px / w['bn-1'] - 1          # raw HL print vs Binance last price before the tx second

# ---- A: candle basis
rows = []
bas5 = {}
for coin in w.coin.unique():
    fn = f'{SP}data/hl/{coin}_5m.json'
    if not os.path.exists(fn):
        continue
    d = json.load(open(fn))
    if not d:
        continue
    h = pd.DataFrame(d); h['T'] = h['t'] + 300000; h['c'] = h.c.astype(float)
    k = kl(SYM[coin])
    if k is None:
        continue
    kc = k.c.reindex(h['T'] - 60000).values       # Binance close of the 1m kline ending at the same instant
    b = pd.Series(h.c.values / kc - 1, index=h['T'].values)
    b = b[~np.isnan(b)]
    base = b.rolling(12, min_periods=6).median()   # value at index T uses bars ending <= T
    bas5[coin] = base
    rows.append(dict(coin=coin, n=len(b), basis_med=100 * b.median(), basis_iqr=100 * (b.quantile(.75) - b.quantile(.25)),
                     basis_absdev_1h=100 * (b - base).abs().median()))
pd.DataFrame(rows).sort_values('n').to_csv(W + 'q1_basis_by_coin.csv', index=False)

w = w.sort_values('ts')
w['basisA'] = np.nan
for coin, g in w.groupby('coin'):
    if coin not in bas5:
        continue
    base = bas5[coin]
    i = np.searchsorted(base.index.values, g.ts.values, side='right') - 1   # last bar end <= ts
    v = np.where(i >= 0, base.values[np.clip(i, 0, None)], np.nan)
    age = g.ts.values - np.where(i >= 0, base.index.values[np.clip(i, 0, None)], 0)
    v[age > 15 * 60000] = np.nan
    w.loc[g.index, 'basisA'] = v

# ---- B: print basis (prior 30 min, excluding the last 5 s), per side then averaged
w['basisB'] = np.nan
for coin, g in w.groupby('coin'):
    g = g[g['bn-1'].notna()]
    t = g.ts.values; x = g.g_print.values; side = g.wdir.values
    res = np.full(len(g), np.nan)
    tb = t[side > 0]; xb = x[side > 0]; ts_ = t[side < 0]; xs = x[side < 0]
    lo_b = np.searchsorted(tb, t - 1800000); hi_b = np.searchsorted(tb, t - 5000)
    lo_s = np.searchsorted(ts_, t - 1800000); hi_s = np.searchsorted(ts_, t - 5000)
    for j in range(len(g)):
        nb = hi_b[j] - lo_b[j]; ns = hi_s[j] - lo_s[j]
        if nb >= 3 and ns >= 3:
            res[j] = 0.5 * (np.median(xb[lo_b[j]:hi_b[j]][-200:]) + np.median(xs[lo_s[j]:hi_s[j]][-200:]))
    w.loc[g.index, 'basisB'] = res
    print(coin, len(g), flush=True)
w['basis'] = w.basisA.fillna(w.basisB)
w.to_parquet(W + 'q1_tx.parquet')
print(w[['basisA', 'basisB']].describe())
print('corr A,B', w[['basisA', 'basisB']].corr().iloc[0, 1], 'med |A-B|', (w.basisA - w.basisB).abs().median())

# Q1 build: for every whale tx (12.09-29.09) attach Binance 1s prices around the tx + HL-vs-Binance basis baselines.
# Output: q1_tx.parquet  (one row per unique tx; all wallets; used both as "event" and as "HL print")
import sys, os, json, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
w = pd.read_parquet(SP + 'data/whale_txs.parquet')
w = w[w.ts < pd.Timestamp('2026-09-30').value // 10**6].copy()
w['sym'] = w.coin.map(SYM)
w['wdir'] = np.where(w.dir.isin(['Close Short', 'Open Long', 'Short > Long']), 1, -1)   # whale trade direction (+1 buys)
w['sec'] = w.ts // 1000
OFFS = [-300, -60, -10, -5, -2, -1, 0, 1, 2, 3, 5, 10, 30, 60, 120, 300, 600, 1800]
T0 = int(pd.Timestamp('2026-08-25').value // 10**9); T1 = int(pd.Timestamp('2026-09-30').value // 10**9)
N = T1 - T0

def dense(symbol):
    d = f'{SP}data/bars1s/{symbol}/'
    c = np.full(N, np.nan); h = np.full(N, np.nan); l = np.full(N, np.nan); q = np.zeros(N)
    have = np.zeros(N, bool)
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn)
        i = z['sec'] - T0
        m = (i >= 0) & (i < N)
        c[i[m]] = z['c'][m]; h[i[m]] = z['h'][m]; l[i[m]] = z['l'][m]; q[i[m]] = z['quote'][m]
        day0 = int(pd.Timestamp(fn[:10]).value // 10**9) - T0
        have[max(day0, 0):min(day0 + 86400, N)] = True
    cf = pd.Series(c).ffill().to_numpy(copy=True)
    cf[~have] = np.nan
    return cf, h, l, q, have

out = []
for s, g in w.groupby('sym'):
    if not os.path.isdir(f'{SP}data/bars1s/{s}'):
        continue
    cf, h, l, q, have = dense(s)
    i0 = (g.sec.values - T0).astype(int)
    r = {}
    for o in OFFS:
        j = np.clip(i0 + o, 0, N - 1)
        r[f'bn{o}'] = cf[j]
    # Binance range around the tx second [s-1, s+1]
    hh = np.vstack([h[np.clip(i0 + k, 0, N - 1)] for k in (-1, 0, 1)]); ll = np.vstack([l[np.clip(i0 + k, 0, N - 1)] for k in (-1, 0, 1)])
    r['bn_hi'] = np.nanmax(hh, 0); r['bn_lo'] = np.nanmin(ll, 0)
    # worst price for a hedge trade IN THE WHALE DIRECTION (we are opposite on HL, so hedge = whale dir) in [s+lat, s+lat+2]
    wd = g.wdir.values
    for lat in (0, 1, 2):
        H = np.vstack([h[np.clip(i0 + lat + k, 0, N - 1)] for k in range(3)]); L = np.vstack([l[np.clip(i0 + lat + k, 0, N - 1)] for k in range(3)])
        with np.errstate(all='ignore'):
            worst_hedge_buy = np.nanmax(H, 0); worst_hedge_sell = np.nanmin(L, 0)
        wp = np.where(wd > 0, worst_hedge_buy, worst_hedge_sell)   # hedge buys if whale buys (we sold on HL)
        wp = np.where(np.isnan(wp), cf[np.clip(i0 + lat + 2, 0, N - 1)], wp)
        r[f'hedge_w{lat}'] = wp
    cq = np.concatenate([[0], np.cumsum(q)]); r['bn_q_pre60'] = cq[np.clip(i0, 0, N)] - cq[np.clip(i0 - 60, 0, N)]
    df = pd.DataFrame(r, index=g.index)
    out.append(df)
    print(s, len(g), flush=True)
feat = pd.concat(out)
w = w.join(feat)
w.to_parquet(W + 'q1_tx_raw.parquet')
print('rows', len(w), 'with bn', w['bn-1'].notna().sum())

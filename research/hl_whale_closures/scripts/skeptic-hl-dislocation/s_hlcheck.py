# Check A2 fills against HL's OWN candles: (a) did HL trade at/through our level in the fill bar? (b) where was HL (candle close)
# 60-360 s later vs our level, vs the Binance-fair assumption at the same instant.
import os, json, numpy as np, pandas as pd, sys
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/skeptic-hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
fn_in = sys.argv[1] if len(sys.argv) > 1 else 's_a2_base.parquet'
f = pd.read_parquet(OUT + fn_in)
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'; parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn); parts.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(parts).sort_index(); return s[~s.index.duplicated()]
cache = {}
def hl(coin, iv):
    k = (coin, iv)
    if k not in cache:
        d = pd.DataFrame(json.load(open(f'{SP}data/hl/{coin}_{iv}.json')))
        for c in 'ohlcv':
            d[c] = d[c].astype(float)
        cache[k] = d.set_index('t')
    return cache[k]
rows = []
for r in f.itertuples():
    out = {}
    k = kline(SYM[r.coin])
    for iv, ms in (('1m', 60000), ('5m', 300000)):
        d = hl(r.coin, iv)
        t0 = (r.ts // ms) * ms
        if t0 not in d.index or d.index.min() > t0 - ms:
            continue
        bar = d.loc[t0]
        # (a) fill validation: whale sells (wd=-1) -> our bid L must be >= bar low
        out[f'val_{iv}'] = bool(bar.l <= r.L * (1 + 1e-9)) if r.wd < 0 else bool(bar.h >= r.L * (1 - 1e-9))
        out[f'depth_{iv}'] = 100 * (-r.wd) * (r.L / (bar.l if r.wd < 0 else bar.h) - 1)   # how far beyond L HL traded in that bar
        # (b) exit at the first bar close whose end >= fill + 60 s
        te = ((r.ts + 60000) // ms + 1) * ms            # end of the bar containing ts+60s
        tb = te - ms
        if tb not in d.index:
            continue
        c = d.loc[tb, 'c']
        bn = k.get(te - 60000, np.nan)                 # Binance close of the minute ending at te
        out[f'hor_{iv}'] = (te - r.ts) / 1000
        out[f'HLx_{iv}'] = 100 * (-r.wd) * (c / r.L - 1) - 0.06       # sell at HL last price, maker in 0.015 + taker out 0.045
        out[f'BNx_{iv}'] = 100 * (-r.wd) * (bn * (1 + r.bas) / r.L - 1) - 0.03   # passive exit at Binance fair at the same instant
        out[f'res_{iv}'] = 100 * r.wd * (c / (bn * (1 + r.bas)) - 1)             # HL below fair in whale dir (+ = HL still dislocated)
        out[f'nv_{iv}'] = d.loc[tb, 'n']
    rows.append(out)
X = pd.concat([f.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
X.to_parquet(OUT + fn_in.replace('.parquet', '_hl.parquet'))
for iv in ('1m', '5m'):
    g = X[X[f'val_{iv}'].notna()]
    print(f'--- {iv}: fills with HL candle coverage {len(g)} (DISC {(~g.test).sum()}, TEST {g.test.sum()})')
    print('  fill validated (HL bar traded through our level):', g[f'val_{iv}'].mean().round(3), '| median depth beyond L in bar %.3f' % g[f'depth_{iv}'].median())
    h = g[g[f'HLx_{iv}'].notna()]
    for part in (False, True):
        hh = h[h.test == part]
        if len(hh):
            print(f'  {"TEST" if part else "DISC"} n={len(hh)} horizon med {hh[f"hor_{iv}"].median():.0f}s | HL-close exit (taker-ish) mean {hh[f"HLx_{iv}"].mean():+.3f} med {hh[f"HLx_{iv}"].median():+.3f} '
                  f'| Binance-fair exit same instant mean {hh[f"BNx_{iv}"].mean():+.3f} med {hh[f"BNx_{iv}"].median():+.3f} | HL residual below fair mean {hh[f"res_{iv}"].mean():+.3f} med {hh[f"res_{iv}"].median():+.3f}')

"""Honesty battery for the D-selected ms touch-price follow candidate: rel24>=4 (tx_usd >= 4x Binance avg 1-min quote volume
over prior 24h), enter touch at t0+L ms, exit touch at t0+H s. Plus neighbours, latency curve, direction split, placebo,
index-neutral, one-position-per-coin dedupe, fee scenarios."""
from ms_eval import *
from fade_eval import battery
def prep(d, L, H):
    d = d.copy(); d['g'] = d[f'x{H}'] - d[f'a{L}']
    d['g_alt'] = d.g - d[f'alt{H}'] if H in (60, 300) else d.g
    d['g_btc'] = d.g - d[f'btc{H}'] if H in (60, 300) else d.g
    d['wdir'] = d.wd
    d = d[d.g.notna()].sort_values('t0')
    keep, busy = [], {}
    for r in d.itertuples():
        if busy.get(r.coin, 0) > r.t0: continue
        keep.append(r.Index); busy[r.coin] = r.t0 + H * 1000
    return d.loc[keep]
import sys
L, H, thr = 300, 300, 4
for Lx in (300, 1000, 2000):
    d = prep(EV[EV.rel24 >= thr], Lx, H)
    for per in ('D', 'T', None):
        x = d if per is None else d[d.per == per]
        print(battery(x, f'rel24>={thr} L={Lx}ms H={H}s touch, period={per or "ALL"} (fee 0.146 in net)'))
        px = PL[PL.row.isin(x.row)]; pg = (px[f'x{H}'] - px[f'a{Lx}'])
        print(f'   placebo same coin/day/dir random time: n={pg.notna().sum()} mean={pg.mean():+.3f} med={pg.median():+.3f}; excess over placebo={x.g.mean()-pg.mean():+.3f}')
print('=== latency curve (touch->touch, H=300 and H=60), rel24>=4, D / T means')
for H2 in (60, 300):
    for per in ('D', 'T'):
        x = EV[(EV.rel24 >= 4) & (EV.per == per)]
        print(H2, per, ' '.join(f'L{L2}:{(x[f"x{H2}"] - x[f"a{L2}"]).mean():+.3f}' for L2 in (0, 100, 150, 200, 250, 300, 500, 1000, 1500, 2000, 3000, 5000, 10000)))
print('=== neighbours on T (touch L=500 H=300): rel24 thresholds and td thresholds')
for f in ('rel24>=1', 'rel24>=2', 'rel24>=4', 'td>=0.3', 'td>=0.5', 'td>=1.0', 'usd>=100k', 'usd>=300k', 'rel60>=1', 'rel60>=3'):
    x = EV[FIL[f](EV)]
    s = ' | '.join(f'{p}: n={len(x[x.per==p])} mean={(x[x.per==p].x300 - x[x.per==p].a500).mean():+.3f} med={(x[x.per==p].x300 - x[x.per==p].a500).median():+.3f}' for p in ('D', 'T'))
    print(f'{f:10s} {s}')

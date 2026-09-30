"""Null for the grid: flip trade direction at random per coin-day cluster (same clustering, same vol), rerun the full grid,
record max lower-CI and max mean (n>=100) over the K variants. 16 reps."""
import sys; sys.path.insert(0, '.')
from rules import *
import itertools
D = base(); disc = D[D.per == 'disc']
from grid_defs import FILT, SHAPES
res = []
rng = np.random.default_rng(1)
for rep in range(16):
    cds = disc.cd.unique(); flip = dict(zip(cds, rng.choice([-1, 1], len(cds))))
    dd = disc.copy(); dd['tdir'] = dd.tdir * dd.cd.map(flip)
    best_lo, best_mean = -9, -9
    for (fn, f), (dr, ek, xk) in itertools.product(FILT.items(), SHAPES):
        x = trades(dd[f(dd)], dr, ek, xk)
        if len(x) < 30: continue
        v = x.net.values
        lo, hi, p = boot_ci(v, x.cd.values, np.mean, n=200)
        best_lo = max(best_lo, lo)
        if len(x) >= 100: best_mean = max(best_mean, v.mean())
    res.append((best_lo, best_mean)); print(rep, round(best_lo, 3), round(best_mean, 3), flush=True)
r = np.array(res)
print('NULL max lo: median', np.median(r[:, 0]).round(3), 'q90', np.percentile(r[:, 0], 90).round(3), '| max mean(n>=100): median', np.median(r[:, 1]).round(3), 'q90', np.percentile(r[:, 1], 90).round(3))

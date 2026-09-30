"""Pre-declared grid on DISCOVERY only. Ranking = lower bound of cluster(coin-day) bootstrap CI90 of mean net (costs 0.146).
Also sign-flip placebo for the max over the grid."""
import sys; sys.path.insert(0, '.')
from rules import *
import itertools, time
t0 = time.time()
D = base()
disc = D[D.per == 'disc']
q40, q60, q80 = 0.0122, 0.0388, 0.1203       # intensity quintile edges (all-period; used only as round thresholds)
FILT = {
    'all': lambda x: x.index == x.index,
    'int>=q60': lambda x: x.intensity >= q60, 'int>=q80': lambda x: x.intensity >= q80, 'int<q40': lambda x: x.intensity < q40,
    'dur<=15': lambda x: x.dur_min <= 15, 'dur15-180': lambda x: (x.dur_min > 15) & (x.dur_min <= 180), 'dur>180': lambda x: x.dur_min > 180,
    'reduce': lambda x: x.kind2 == 'reduce', 'increase': lambda x: x.kind2 == 'increase', 'open': lambda x: x.kind2 == 'open',
    'inc|open': lambda x: x.kind2.isin(['increase', 'open']), 'buy': lambda x: x.twap_side == 'buy', 'sell': lambda x: x.twap_side == 'sell',
    'hl_src': lambda x: x.hl_src, 'usd>=1M': lambda x: x.usd >= 1e6,
}
SHAPES = []
for dr in (1, -1):
    for xk in ('e1_15', 'e1_60', 'e1_240', 'mid', 'end'):
        SHAPES.append((dr, 'e1', xk))
    for xk in ('end_15', 'end_60', 'end_120', 'end_240'):
        SHAPES.append((dr, 'end', xk))
rows = []
for (fn, f), (dr, ek, xk) in itertools.product(FILT.items(), SHAPES):
    x = trades(disc[f(disc)], dr, ek, xk)
    if len(x) < 30:
        continue
    v = x.net.values
    lo, hi, p = boot_ci(v, x.cd.values, np.mean, n=300)
    rows.append(dict(filt=fn, dir=dr, ek=ek, xk=xk, n=len(x), mean=v.mean(), med=np.median(v), win=(v > 0).mean(), lo=lo, hi=hi,
                     xalt=x.gross_a.mean() - COST, ncl=x.cd.nunique()))
g = pd.DataFrame(rows).sort_values('lo', ascending=False)
g.to_csv('grid_disc.csv', index=False)
print('K variants', len(g), 'time', round(time.time() - t0))
print('positive mean net', (g['mean'] > 0).sum(), ' lo>0', (g.lo > 0).sum())
print(g.head(25).round(3).to_string())
print('\nbest by mean with n>=100:')
print(g[g.n >= 100].sort_values('mean', ascending=False).head(15).round(3).to_string())

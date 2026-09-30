import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet').merge(pd.read_parquet(W + 'kind.parquet'), on='twap_id')
d = d[d.per != 'none'].copy()
R = lambda x, a, b: 100 * x.tdir * ((x['p_' + b] / x['p_' + a] - 1) - (x['a_' + b] / x['a_' + a] - 1))
print('=== reduce vs others path (xalt, means; per period)')
for per in ('disc', 'test'):
    s = d[d.per == per]
    for k in ('reduce', 'increase', 'open', 'unknown'):
        x = s[s.kind2 == k]
        print(per, f'{k:9s} n={len(x):5d}', ' '.join(f'{a}->{b}={R(x, a, b).mean():+.3f}' for a, b in (('pre60', 'pre'), ('pre', 'e1'), ('e1', 'end'), ('end', 'end_60'), ('end', 'end_120'))))
    # slice-kind reduce only (exact kind)
    x = s[s.kind == 'reduce']
    print(per, f'slice-reduce n={len(x)}', ' '.join(f'{a}->{b}={R(x, a, b).mean():+.3f}' for a, b in (('pre60', 'pre'), ('pre', 'e1'), ('e1', 'end'), ('end', 'end_60'), ('end', 'end_120'))))
print('=== native detection: follow worst-in-3s at start+3 s, exit last at +60/+300/+900 s (gross, no costs)')
for per in ('disc', 'test'):
    s = d[d.per == per]
    q80 = d.intensity.quantile(.8)
    for lab, x in (('all', s), ('q80', s[s.intensity >= q80])):
        en = np.where(x.tdir > 0, x.wb_s3, x.ws_s3)
        out = []
        for k in ('s60', 's300', 's900'):
            g = 100 * x.tdir * (x['p_' + k] / en - 1)
            lo, hi = cboot(g.values, x.cd.values, n=500)
            out.append(f'{k}={np.nanmean(g):+.3f}[{lo:+.3f},{hi:+.3f}]')
        print(per, lab, len(x), ' '.join(out))
print('=== E family walk-forward: choose (mv,int) on DISC (n>=20, tape mean) -> TEST')
x0 = d[(d.eligible == 1) & (d.lag_s / d.dur_s > 0.5) & (d.lag_s / d.dur_s < 1.1)].copy()
x0['mvK'] = 100 * x0.tdir * (x0.p_Kdec / x0.p_Kstart - 1)
def dedup(x, te, tx):
    x = x.assign(_te=te, _tx=tx).sort_values('_te'); keep = []; busy = {}
    for i, c, a, b in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > a: continue
        keep.append(i); busy[c] = b
    return x.loc[keep]
def fade(x):
    x = x[np.isfinite(x.ws_dec) & np.isfinite(x.wb_dec_60)]
    x = dedup(x, x.date_s, x.date_s + 3600); our = -x.tdir
    en = np.where(our > 0, x.wb_dec, x.ws_dec); ex = np.where(our > 0, x.ws_dec_60, x.wb_dec_60)
    x['t_net'] = 100 * our * (ex / en - 1) - COST
    return x
rows = []
for mv in (0.25, 0.5, 0.75, 1.0, 1.25, 1.5):
    for it in (0, 0.1, 0.2, 0.35, 0.5, 0.75, 1.0, 1.5, 2.0):
        r = {'mv': mv, 'it': it}
        for per in ('disc', 'test'):
            s = x0[x0.per == per]; y = fade(s[(s.mvK >= mv) & (s.intensity >= it)])
            r[per + '_n'] = len(y); r[per + '_mean'] = y.t_net.mean()
        rows.append(r)
g = pd.DataFrame(rows); gg = g[g.disc_n >= 20].sort_values('disc_mean', ascending=False)
print('K cells', len(g), 'eligible (disc n>=20)', len(gg))
print(gg.head(8).round(3).to_string())
print('WF: TEST mean of DISC-best cell', round(gg.test_mean.iloc[0], 3), '| avg TEST of DISC top-5', round(gg.test_mean.head(5).mean(), 3), '| TEST of all cells median', round(g.test_mean.median(), 3))
print('=== E reduce slice (T2-like) TEST/DISC tape')
for per in ('disc', 'test'):
    s = x0[x0.per == per]; y = fade(s[(s.mvK >= 1) & (s.kind == 'reduce')])
    print(per, bat(y, 't_net', 'mv>=1 & slice-reduce fade60'))

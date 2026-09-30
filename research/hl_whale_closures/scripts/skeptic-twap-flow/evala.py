import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet').merge(pd.read_parquet(W + 'kind.parquet'), on='twap_id')
d = d[d.per != 'none'].copy()
def dedup(x, te, tx):
    x = x.assign(_te=te, _tx=tx).sort_values('_te'); keep = []; busy = {}
    for i, c, a, b in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > a: continue
        keep.append(i); busy[c] = b
    return x.loc[keep]
def follow(x, xk, xs):
    x = x[np.isfinite(x.wb_e1) & np.isfinite(x['p_' + xk]) & (xs.loc[x.index] > x.seen_s + 60)]
    x = dedup(x, x.seen_s + 60, xs.loc[x.index])
    our = x.tdir
    x['our'] = np.where(our > 0, 'LONG', 'SHORT')
    en = np.where(our > 0, x.wb_e1, x.ws_e1)
    exw = np.where(our > 0, x.get('ws_' + xk, np.nan), x.get('wb_' + xk, np.nan)) if ('ws_' + xk) in x else x['p_' + xk].values
    x['t_net'] = 100 * our * (exw / en - 1) - COST
    x['alt'] = 100 * our * (x['a_' + xk] / x.a_e1 - 1); x['btc'] = 100 * our * (x['b_' + xk] / x.b_e1 - 1)
    x['xalt'] = x.t_net - x.alt; x['xbtc'] = x.t_net - x.btc
    x['hold_h'] = (xs.loc[x.index] - x.seen_s - 60) / 3600
    return x
for lab, filt, xk in (('A dur>180 e1->end', lambda s: s.dur_min > 180, 'end'), ('B dur>180 e1->+240', lambda s: s.dur_min > 180, 'e1_240'),
                      ('dur 60-180 e1->end', lambda s: (s.dur_min > 60) & (s.dur_min <= 180), 'end')):
    for per in ('disc', 'test'):
        s = d[d.per == per]; s = s[filt(s)]
        xs = s.end_s if xk == 'end' else s.seen_s + 60 + 14400
        x = follow(s, xk, xs)
        print(f'## {lab} {per} hold_h median {x.hold_h.median():.1f}')
        print(bat(x, 't_net', ' tape net'))
        print(bat(x, 'xalt', ' tape net xalt'))
        for sd in ('LONG', 'SHORT'):
            y = x[x.our == sd]
            print(f'   {sd} n={len(y)} net={y.t_net.mean():+.3f} med={y.t_net.median():+.3f} xalt={y.xalt.mean():+.3f} xalt_med={y.xalt.median():+.3f} xbtc={y.xbtc.mean():+.3f}')
        # sign-flip null for xalt mean (random direction per coin-day)
        rng = np.random.default_rng(3); cds = x.cd.values; u, inv = np.unique(cds, return_inverse=True)
        gross_x = x.xalt + COST
        nul = []
        for r in range(2000):
            f = rng.choice([-1, 1], len(u))[inv]
            nul.append((f * gross_x).mean() - COST)
        nul = np.array(nul)
        print(f'   sign-flip(coin-day) null for xalt-net mean: q95={np.percentile(nul,95):+.3f} real={x.xalt.mean():+.3f} p={(nul>=x.xalt.mean()).mean():.3f}')

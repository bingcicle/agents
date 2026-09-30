import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
import statsmodels.formula.api as smf
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet').merge(pd.read_parquet(W + 'kind.parquet'), on='twap_id')
d = d[d.per != 'none'].copy()
def dedup(x, te, tx):
    x = x.assign(_te=te, _tx=tx).sort_values('_te'); keep = []; busy = {}
    for i, c, a, b in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > a: continue
        keep.append(i); busy[c] = b
    return x.loc[keep]
def fade_e1(x):
    x = x[np.isfinite(x.ws_e1) & np.isfinite(x.wb_e1_60)]
    x = dedup(x, x.seen_s + 60, x.seen_s + 3660)
    our = -x.tdir
    en = np.where(our > 0, x.wb_e1, x.ws_e1); ex = np.where(our > 0, x.ws_e1_60, x.wb_e1_60)
    x['t_net'] = 100 * our * (ex / en - 1) - COST
    x['alt'] = 100 * our * (x.a_e1_60 / x.a_e1 - 1)
    x['pre_mv'] = 100 * x.tdir * (x.p_e1 / x.p_pre60 - 1)   # move in TWAP dir from 1h before start to entry
    x['pre_mv_x'] = x.pre_mv - 100 * x.tdir * (x.a_e1 / x.a_pre60 - 1)
    return x
print('=== same fade rule (tape) per kind, dedup within kind')
allx = []
for per in ('disc', 'test'):
    s = d[d.per == per]
    for k in ('increase', 'reduce', 'open', 'unknown'):
        x = fade_e1(s[s.kind2 == k]); x['per'] = per
        allx.append(x)
        print(per, bat(x, 't_net', f'fade {k:9s}'), f' xalt={(x.t_net - x.alt).mean():+.3f} pre_mv={x.pre_mv.mean():+.3f}')
A = pd.concat(allx)
A['inc'] = (A.kind2 == 'increase').astype(int)
A['net_x'] = A.t_net - A.alt
for per in ('disc', 'test'):
    y = A[(A.per == per) & A.pre_mv_x.notna()]
    m = smf.ols('net_x ~ inc + pre_mv_x', data=y).fit(cov_type='cluster', cov_kwds={'groups': pd.factorize(y.cd)[0]})
    print(per, 'OLS net_xalt ~ inc + pre_mv_x (cluster coin-day):', {k: (round(m.params[k], 3), round(m.bse[k], 3)) for k in m.params.index})
print('=== pre-start profile (vs 1 s before start), all and intensity>=q80')
q80 = d.intensity.quantile(.8)
pts = {'-3600': 'pre60', '-900': 'pre15', 's0': 's0', 's3': 's3'}
for lab, m in (('all', d.intensity.notna()), ('q80', d.intensity >= q80)):
    x = d[m]
    print(lab, ' '.join(f'{k}={(100 * x.tdir * (x["p_pre"] / x["p_" + v] - 1)).mean() if v.startswith("pre") else (100 * x.tdir * (x["p_" + v] / x["p_pre"] - 1)).mean():+.3f}' for k, v in pts.items()),
          '(pre60/pre15 = move from that time TO 1 s before start)')

import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet')
d = d[d.per != 'none'].copy()
# extra fixed-horizon exits via k1m (8h, 24h) + market
S_cache = {}
for h in (8, 24):
    col = f'p_e1_{h}h'; d[col] = np.nan
    for s, idx in d.groupby('sym').groups.items():
        S = Sym1s(s)
        d.loc[idx, col] = [S.last(t) for t in (d.loc[idx, 'seen_s'] + 60 + h * 3600).values]
    a, b = mkt_at((d.seen_s + 60 + h * 3600).values); d[f'a_e1_{h}h'] = a; d[f'b_e1_{h}h'] = b
def dedup(x, te, tx):
    x = x.assign(_te=te, _tx=tx).sort_values('_te'); keep = []; busy = {}
    for i, c, a, b in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > a: continue
        keep.append(i); busy[c] = b
    return x.loc[keep]
def run(x, xk, texit):
    x = x[np.isfinite(x.p_e1) & np.isfinite(x['p_' + xk]) & (texit.loc[x.index] > x.seen_s + 60)]
    x = dedup(x, x.seen_s + 60, texit.loc[x.index])
    en = np.where(x.tdir > 0, x.wb_e1, x.ws_e1)
    x['net'] = 100 * x.tdir * (x['p_' + xk] / en - 1) - COST
    x['xalt'] = x.net - 100 * x.tdir * (x['a_' + xk] / x.a_e1 - 1)
    return x
def signflip(x, col, cl, reps=2000):
    g = x[col] + COST; u, inv = np.unique(x[cl].values, return_inverse=True); rng = np.random.default_rng(5)
    nul = np.array([(rng.choice([-1, 1], len(u))[inv] * g).mean() - COST for _ in range(reps)])
    return (nul >= x[col].mean()).mean()
rows = []
for lab, f in (('dur180-360', lambda s: (s.dur_min > 180) & (s.dur_min <= 360)), ('dur360-720', lambda s: (s.dur_min > 360) & (s.dur_min <= 720)),
               ('dur>720', lambda s: s.dur_min > 720), ('dur>180', lambda s: s.dur_min > 180), ('dur60-180', lambda s: (s.dur_min > 60) & (s.dur_min <= 180))):
    for xk in ('end', 'e1_8h', 'e1_24h'):
        for per in ('disc', 'test'):
            s = d[d.per == per]; s = s[f(s)]
            te = s.end_s if xk == 'end' else s.seen_s + 60 + int(xk[3:-1]) * 3600
            x = run(s, xk, te)
            if len(x) < 10: continue
            lo, hi = cboot(x.xalt.values, x.cd.values, n=800)
            rows.append(dict(f=lab, xk=xk, per=per, n=len(x), net=x.net.mean(), net_med=x.net.median(), xalt=x.xalt.mean(), xalt_med=x.xalt.median(),
                             lo=lo, hi=hi, p_cd=signflip(x, 'xalt', 'cd'), p_coin=signflip(x, 'xalt', 'coin'),
                             long_share=(x.tdir > 0).mean()))
r = pd.DataFrame(rows)
print(r.round(3).to_string())

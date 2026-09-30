import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
COST = 0.146
d = pd.read_parquet(W + 'tw.parquet').merge(pd.read_parquet(W + 'kind.parquet'), on='twap_id')
d = d[d.per != 'none'].copy()
def dedup(x, tent, tex):
    x = x.assign(_te=tent, _tx=tex).sort_values('_te')
    keep = []; busy = {}
    for i, c, te, tx in zip(x.index, x.coin, x._te.values, x._tx.values):
        if busy.get(c, -1) > te: continue
        keep.append(i); busy[c] = tx
    return x.loc[keep]
def ruleC(x, dr=-1, ek='e1', xk='e1_60', kek='Ke1', kxk='Ke1_60'):
    """dr=-1 fade. Returns deduped trades with k1m net (lens-style), tape net (worst in 3 s), xalt, xbtc."""
    x = x[np.isfinite(x['p_' + kek]) & np.isfinite(x['p_' + kxk])]
    x = dedup(x, x[kek.replace('K', 'p_K')].index.map(lambda i: 0) if False else x['seen_s'] + 60, x['seen_s'] + 60 + 3600)
    our = dr * x.tdir
    x['our'] = np.where(our > 0, 'LONG', 'SHORT')
    x['k_gross'] = 100 * our * (x['p_' + kxk] / x['p_' + kek] - 1)
    x['k_net'] = x.k_gross - COST
    en = np.where(our > 0, x['wb_' + ek], x['ws_' + ek]); ex = np.where(our > 0, x['ws_' + xk], x['wb_' + xk])
    x['t_net'] = 100 * our * (ex / en - 1) - COST
    x['l_gross'] = 100 * our * (x['p_' + xk] / x['p_' + ek] - 1)
    x['alt'] = 100 * our * (x['a_' + xk] / x['a_' + ek] - 1); x['btc'] = 100 * our * (x['b_' + xk] / x['b_' + ek] - 1)
    x['t_net_xalt'] = x.t_net - x.alt; x['k_net_xalt'] = x.k_net - x.alt
    return x
for per in ('disc', 'test'):
    s = d[d.per == per]
    x = ruleC(s[s.kind2 == 'increase'])
    print(f'##### {per} C increase fade e1->+60')
    print(bat(x, 'k_net', 'k1m net (lens-style)'))
    print(bat(x, 't_net', 'tape worst3s net'))
    print(bat(x, 't_net_xalt', 'tape net xalt'))
    print(f'   gross k1m={x.k_gross.mean():+.3f} last1s={x.l_gross.mean():+.3f} alt(our side)={x.alt.mean():+.3f} btc={x.btc.mean():+.3f}')
    for sd in ('LONG', 'SHORT'):
        y = x[x.our == sd]; print(f'   {sd} n={len(y)} k_net={y.k_net.mean():+.3f} t_net={y.t_net.mean():+.3f} alt={y.alt.mean():+.3f}')
    for lab, f in (('slice-kind', s.kind == 'increase'), ('inferred-only', s.kind.isna() & (s.kind_inf == 'increase')),
                   ('inferred tx-only', s.kind.isna() & (s.kind_inf_tx == 'increase')),
                   ('slice|inf with seen=date', (s.kind == 'increase') | (s.kind.isna() & (s.kind_inf_date == 'increase')))):
        y = ruleC(s[f]); print('  ', bat(y, 't_net', f'{lab:22s} tape'))
    x.to_parquet(W + f'C_{per}.parquet')

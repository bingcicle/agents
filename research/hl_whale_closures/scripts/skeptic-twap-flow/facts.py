import sys; sys.path.insert(0, '.')
from px import *
import warnings; warnings.filterwarnings('ignore')
d = pd.read_parquet(W + 'tw.parquet')
print('id_minus_start range', d.id_minus_start.min(), d.id_minus_start.max())
d = d[d.per != 'none']
def R(a, b, ctrl=None, x=None):
    x = d if x is None else x
    r = 100 * x.tdir * (x['p_' + b] / x['p_' + a] - 1)
    if ctrl == 'a': r = r - 100 * x.tdir * (x['a_' + b] / x['a_' + a] - 1)
    if ctrl == 'b': r = r - 100 * x.tdir * (x['b_' + b] / x['b_' + a] - 1)
    return r
print('=== F1 drift start->end (pre->end), per period; raw / xalt / xbtc; CI by coin-day')
for per in ('disc', 'test'):
    x = d[d.per == per].copy()
    for c in (None, 'a', 'b'):
        x['v'] = R('pre', 'end', c, x); x.loc[x.p_end.isna(), 'v'] = np.nan
        print(per, c, bat(x, 'v', 'pre->end'))
    x['v'] = R('Kstart', 'Kend', 'a', x)
    print(per, 'k1m-lens-style xalt', bat(x, 'v', 'Kstart->Kend'))
print('=== F2 front-loading: profile vs pre (1 s before start), intensity>=q80 and all; xalt not applied (seconds)')
q80 = d.intensity.quantile(.8); print('q80 intensity', round(q80, 4), 'q60', round(d.intensity.quantile(.6), 4))
for lab, m in (('all', d.intensity.notna()), ('int>=q80', d.intensity >= q80)):
    x = d[m]
    s = ' '.join(f'{k}={R("pre", k, None, x).mean():+.3f}' for k in ('s0', 's3', 's30', 's60', 's120', 's300', 's900'))
    print(lab, len(x), s)
    for per in ('disc', 'test'):
        xx = x[x.per == per]
        print('  ', per, ' '.join(f'{k}={R("pre", k, None, xx).mean():+.3f}' for k in ('s0', 's3', 's60', 's300', 's900')))
print('=== F2b start->e1 and e1->end/+15/+60 xalt (lens seen assumption)')
for per in ('disc', 'test'):
    x = d[d.per == per].copy()
    for a, b in (('pre', 'e1'), ('e1', 'e1_15'), ('e1', 'e1_60'), ('e1', 'end')):
        x['v'] = R(a, b, 'a', x)
        if b == 'end': x.loc[x.end_s <= x.e1_s if 'e1_s' in x else x.end_s <= x.seen_s + 60, 'v'] = np.nan
        print(per, bat(x, 'v', f'{a}->{b} xalt'))
print('=== F3 after end xalt')
for per in ('disc', 'test'):
    x = d[d.per == per].copy()
    for b in ('end_15', 'end_60', 'end_120'):
        x['v'] = R('end', b, 'a', x)
        print(per, bat(x, 'v', f'end->{b} xalt'))
print('=== intensity deciles: z of pre->end move; and pre->e1 xalt')
x = d[d.intensity.notna() & d.p_end.notna()].copy()
x['mv'] = R('pre', 'end', 'a', x)
x['dec'] = pd.qcut(x.intensity, 10, labels=False)
print(x.groupby('dec').agg(n=('mv', 'size'), int_med=('intensity', 'median'), mv=('mv', 'mean'), mvmed=('mv', 'median')).round(3).T.to_string())
from scipy.stats import spearmanr
x['m1'] = R('pre', 'e1', 'a', x)
print('spearman pre->e1 vs intensity', spearmanr(x.m1, x.intensity, nan_policy='omit'))
print('spearman pre->e1 vs usd', spearmanr(x.m1, x.usd, nan_policy='omit'))

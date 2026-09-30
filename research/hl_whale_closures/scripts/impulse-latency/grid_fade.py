"""Discovery grid for FADE (reverse) rules after whale close anchors: enter AGAINST whale dir at ts+K (worst-in-3s),
exit at sec+H (worst-in-3s). Conditions: anchor size filters, direction, pre-move (60 s before, in whale dir),
post-impact m{K'} known before entry (K'=30 for K=60, K'=120 for K=300). Select on DISCOVERY, report TEST."""
from common import *
import itertools
from grid import FILT, DIRF, E, COST
E['pre60'] = -E['m-60']
PRE = {'any': lambda d: np.ones(len(d), bool)}
for t in (0.2, 0.5, 1.0): PRE[f'pre>={t}'] = (lambda t: lambda d: d.pre60 >= t)(t)
IMP = {'any': (None, None)}
rows = []
for (fn, ff), (dn, df), (pn, pf) in itertools.product(FILT.items(), DIRF.items(), PRE.items()):
    base = ff(E) & df(E) & pf(E)
    for K in (2, 60, 300):
        imps = [('any', np.ones(len(E), bool))]
        if K == 60: imps += [(f'm30>={t}', E.m30 >= t) for t in (0.3, 0.6, 1.0)]
        if K == 300: imps += [(f'm120>={t}', E.m120 >= t) for t in (0.3, 0.6, 1.0)]
        for inm, im in imps:
            d = E[base & im]
            if len(d) < 30: continue
            for H in (300, 900, 1800, 3600):
                if H <= K: continue
                g = d[f'fw{K}'] - d[f'xr{H}']
                an = g + d[f'alt{H}']
                for per in ('D', 'T'):
                    s = (d.per == per).values
                    rows.append(dict(filt=fn, dir=dn, pre=pn, K=K, imp=inm, H=H, per=per, n=int(s.sum()), nw=d[s].addr.nunique(),
                                     mean=g[s].mean(), med=g[s].median(), net=g[s].mean() - COST, altneut=an[s].mean() - COST))
R = pd.DataFrame(rows)
W = R.pivot_table(index=['filt', 'dir', 'pre', 'K', 'imp', 'H'], columns='per', values=['n', 'nw', 'mean', 'med', 'net', 'altneut']).reset_index()
W.columns = [a if not b else f'{a}_{b}' for a, b in W.columns]
W.to_csv(OUT + 'grid_fade.csv', index=False)
ok = W[(W.n_D >= 30) & (W.nw_D >= 8)]
pd.set_option('display.width', 250)
print('variants', len(W), 'eligible', len(ok), 'share net_D>0', round((ok.net_D > 0).mean(), 3), 'share net_T>0', round((ok.net_T > 0).mean(), 3),
      'corr D/T', round(ok[['mean_D', 'mean_T']].corr().iloc[0, 1], 3))
print('both>0', ((ok.net_D > 0) & (ok.net_T > 0)).sum(), 'expected indep', round((ok.net_D > 0).mean() * (ok.net_T > 0).mean() * len(ok), 1))
cols = ['filt', 'dir', 'pre', 'K', 'imp', 'H', 'n_D', 'nw_D', 'mean_D', 'med_D', 'net_D', 'altneut_D', 'n_T', 'mean_T', 'med_T', 'net_T', 'altneut_T']
print(ok.sort_values('net_D', ascending=False).head(25)[cols].round(3).to_string())
print('--- dir=both best altneut D'); print(ok[ok.dir == 'both'].sort_values('altneut_D', ascending=False).head(15)[cols].round(3).to_string())

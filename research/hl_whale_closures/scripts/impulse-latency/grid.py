"""Discovery grid for FOLLOW rules on episode anchors (1s bars, worst-in-3s entry at ts+L and exit at sec+H).
Only decision-time info: anchor tx size/ratios, sig1m, qv (all measured before ts), m-60 (pre-move), direction.
Select on DISCOVERY (<=21.09) only; print top variants and their TEST numbers (evaluated once, reported for all top-20).
Also FADE grid (entry at t0+60 / t0+300 against whale, conditions known before entry)."""
from common import *
import itertools
X, P = load()
E = X[X.ep_first].copy()
E['pre60'] = -E['m-60']          # + = price already moved in whale dir during the 60 s before tx
FILT = {'all': lambda d: np.ones(len(d), bool)}
for t in (0.05, 0.1, 0.2, 0.3, 0.5, 1.0): FILT[f'td>={t}'] = (lambda t: lambda d: d.td >= t)(t)
for t in (0.5, 1, 2, 4): FILT[f'rel24>={t}'] = (lambda t: lambda d: d.rel24 >= t)(t)
for t in (3e4, 1e5, 3e5): FILT[f'usd>={int(t/1e3)}k'] = (lambda t: lambda d: d.tx_usd >= t)(t)
for t in (1, 2, 4): FILT[f'rel60>={t}'] = (lambda t: lambda d: d.rel60 >= t)(t)
for t in (5, 10, 20): FILT[f'pct>={t}'] = (lambda t: lambda d: d.tx_pct >= t)(t)
DIRF = {'both': lambda d: np.ones(len(d), bool), 'LONG': lambda d: d.wdir > 0, 'SHORT': lambda d: d.wdir < 0}
PREF = {'any': lambda d: np.ones(len(d), bool), 'pre+': lambda d: d.pre60 > 0, 'pre-': lambda d: d.pre60 <= 0}
COST = 0.146

def run(kind='follow'):
    rows = []
    D = E
    for (fn, ff), (dn, df), (pn, pf) in itertools.product(FILT.items(), DIRF.items(), PREF.items()):
        m = ff(D) & df(D) & pf(D)
        d = D[m]
        if len(d) < 30:
            continue
        for L in (1, 2):
            for H in (30, 60, 120, 300, 600, 900, 1800, 3600):
                if kind == 'follow':
                    g = d[f'xw{H}'] - d[f'ew{L}']
                else:
                    continue
                an = g - d[f'alt{H}'] if f'alt{H}' in d else g
                for per in ('D', 'T'):
                    s = d.per == per
                    rows.append(dict(filt=fn, dir=dn, pre=pn, L=L, H=H, per=per, n=int(s.sum()), nw=d[s].addr.nunique(),
                                     ncoin=d[s].coin.nunique(), mean=g[s].mean(), med=g[s].median(),
                                     net=g[s].mean() - COST, altneut=an[s].mean() - COST))
    R = pd.DataFrame(rows)
    W = R.pivot_table(index=['filt', 'dir', 'pre', 'L', 'H'], columns='per', values=['n', 'nw', 'mean', 'med', 'net', 'altneut'])
    W.columns = [f'{a}_{b}' for a, b in W.columns]
    return W.reset_index()

if __name__ == '__main__':
    W = run()
    W.to_csv(OUT + 'grid_follow.csv', index=False)
    K = len(W)
    ok = W[(W.n_D >= 30) & (W.nw_D >= 8)]
    print('variants K =', K, 'eligible', len(ok))
    print('share of eligible variants with net_D>0:', (ok.net_D > 0).mean().round(3), ' mean_D>0:', (ok.mean_D > 0).mean().round(3))
    pd.set_option('display.width', 250)
    top = ok.sort_values('net_D', ascending=False).head(25)
    print(top[['filt', 'dir', 'pre', 'L', 'H', 'n_D', 'nw_D', 'mean_D', 'med_D', 'net_D', 'altneut_D', 'n_T', 'mean_T', 'med_T', 'net_T', 'altneut_T']].round(3).to_string())
    top2 = ok[(ok.dir == 'both')].sort_values('altneut_D', ascending=False).head(15)
    print('--- both directions, best alt-neutral on D')
    print(top2[['filt', 'dir', 'pre', 'L', 'H', 'n_D', 'nw_D', 'mean_D', 'med_D', 'net_D', 'altneut_D', 'n_T', 'mean_T', 'med_T', 'net_T', 'altneut_T']].round(3).to_string())
    print('--- correlation of D and T mean across eligible variants:', ok[['mean_D', 'mean_T']].corr().iloc[0, 1].round(3))
    print('--- share of eligible with net_T>0:', (ok.net_T > 0).mean().round(3))
    print('--- best by H, all events (filt=all,dir=both,pre=any):')
    print(ok[(ok.filt == 'all') & (ok.dir == 'both') & (ok.pre == 'any')][['L', 'H', 'n_D', 'mean_D', 'net_D', 'altneut_D', 'n_T', 'mean_T', 'net_T', 'altneut_T']].round(3).to_string())

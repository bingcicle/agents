"""Tradable FOLLOW on ms data (touch prices): enter at first our-side aggressor trade at/after t0+L ms, exit at first
opposite-aggressor trade at/after t0+H s. Size filter chosen on DISCOVERY only. Also placebo (random time same coin/day/dir)
and index-neutral versions. Costs: 0.146 official, 0.10 taker both legs, 0.07 taker+maker, 0.04 maker both legs."""
from common import *
import itertools
M = pd.read_parquet(OUT + 'ms_events.parquet')
X, P = load()
E = X[X.ep_first]
M = M.merge(E[['row', 'tx_usd', 'td', 'rel24', 'rel60', 'tx_pct', 'per', 'addr', 'day', 'coin', 'alt60', 'alt300', 'btc60', 'btc300',
               'detect_src', 'm-60', 'wdir']].rename(columns={'wdir': 'wd'}), on='row')
EV = M[M.kind == 'ev'].copy(); PL = M[M.kind != 'ev'].copy()
FIL = {}
for t in (0.1, 0.2, 0.3, 0.5, 1.0): FIL[f'td>={t}'] = lambda d, t=t: d.td >= t
for t in (1, 2, 4): FIL[f'rel24>={t}'] = lambda d, t=t: d.rel24 >= t
for t in (1e5, 3e5): FIL[f'usd>={int(t/1e3)}k'] = lambda d, t=t: d.tx_usd >= t
for t in (1, 3): FIL[f'rel60>={t}'] = lambda d, t=t: d.rel60 >= t

def g(d, L, H):
    return d[f'x{H}'] - d[f'a{L}']

if __name__ == '__main__':
    pd.set_option('display.width', 250)
    out = []
    rows = []
    for (fn, ff), L, H in itertools.product(FIL.items(), (300, 500, 1000), (10, 30, 60, 300)):
        d = EV[ff(EV)]; pl = PL[PL.row.isin(d.row)]
        for per in ('D', 'T'):
            x = d[d.per == per]; px = pl[pl.row.isin(x.row)]
            gg = g(x, L, H); pg = g(px, L, H)
            rows.append(dict(filt=fn, L=L, H=H, per=per, n=len(x), nw=x.addr.nunique(), mean=gg.mean(), med=gg.median(),
                             P_mean=pg.mean(), P_med=pg.median(), excess=gg.mean() - pg.mean()))
    R = pd.DataFrame(rows)
    W = R.pivot_table(index=['filt', 'L', 'H'], columns='per', values=['n', 'nw', 'mean', 'med', 'P_mean', 'excess']).reset_index()
    W.columns = [a if not b else f'{a}_{b}' for a, b in W.columns]
    W.to_csv(OUT + 'ms_grid.csv', index=False)
    print('K variants', len(W))
    print(W.sort_values('mean_D', ascending=False)[['filt', 'L', 'H', 'n_D', 'nw_D', 'mean_D', 'med_D', 'P_mean_D', 'n_T', 'nw_T', 'mean_T', 'med_T', 'P_mean_T']].round(3).head(30).to_string())
    print('corr D/T means', W[['mean_D', 'mean_T']].corr().iloc[0, 1].round(3))
    print('share mean_D>0.10', (W.mean_D > 0.10).mean().round(3), 'share mean_T>0.10', (W.mean_T > 0.10).mean().round(3))

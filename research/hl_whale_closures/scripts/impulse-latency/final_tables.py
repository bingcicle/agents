"""Consolidated key tables -> RESULTS.txt (all numbers quoted in the final answer come from here or the *_txt files)."""
from common import *
import warnings; warnings.filterwarnings('ignore')
pd.set_option('display.width', 260); pd.set_option('display.max_columns', 60)
X, P = load()
E = X[X.ep_first].copy()
M = pd.read_parquet(OUT + 'ms_events.parquet').merge(E[['row', 'td', 'rel24', 'per', 'addr', 'coin', 'day']], on='row')
out = []
def w(s): out.append(str(s))
TDB = [0, 0.01, 0.05, 0.1, 0.3, 1, 20]
R24B = [0, 0.1, 0.5, 1, 2, 4, 1000]
w('# impulse-latency lens: key tables. Returns = signed log-ret x100 in WHALE direction vs last Binance trade in the second before the tx.')
w(f'txs>=1k with Binance bars (12.09-29.09): {len(X)}; episodes (same wallet+coin+dir, chain gap<=10s): {X.ep.nunique()} '
  f'(dup factor {len(X)/X.ep.nunique():.2f}); 60s-chains: {X.ep60.nunique()}; multi-wallet bursts(30s): {(X.drop_duplicates("burst").b_nadr>=2).sum()}')
w(f'episodes D (<=21.09): {(E.per=="D").sum()}, T (22-29.09): {(E.per=="T").sum()}; wdir=+1 (whale buys = Close Short) share {(E.wdir>0).mean():.2f}')

w('\n## A. Impulse response by td = tx_usd / HL depth(1%) (episode anchors, 1s bars); P_ = placebo same coin/day/dir random time')
cols = ['m-300', 'm-60', 'm-10', 'm-2', 'm0', 'm1', 'm2', 'm5', 'm10', 'm30', 'm60', 'm300', 'm1800', 'm3600']
for v, bins in (('td', TDB), ('rel24', R24B), ('tx_usd', [1e3, 1e4, 3e4, 1e5, 3e5, 1e6, 1e8])):
    E['b'] = pd.cut(E[v], bins)
    t = E.groupby('b', observed=True)[cols].mean()
    t['n'] = E.groupby('b', observed=True).size(); t['nw'] = E.groupby('b', observed=True).addr.nunique()
    pp = P.loc[E.row]; pp['b'] = E.b.values
    for c in ('m60', 'm300', 'm1800'):
        t['P_' + c] = pp.groupby('b', observed=True)[c].mean()
    t['med_m1'] = E.groupby('b', observed=True).m1.median()
    w(f'-- by {v}'); w(t.round(3).to_string())
w('-- liq flag (system fills): ' + E.groupby('ep_liq')[['m0', 'm1', 'm60', 'm300']].mean().round(3).assign(n=E.groupby('ep_liq').size()).to_string())
w('-- tx-level (no dedupe) vs episode, td>=0.3: tx n=%d m1=%.3f m60=%.3f | episodes n=%d m1=%.3f m60=%.3f' % (
    (X.td >= 0.3).sum(), X[X.td >= 0.3].m1.mean(), X[X.td >= 0.3].m60.mean(), (E.td >= 0.3).sum(), E[E.td >= 0.3].m1.mean(), E[E.td >= 0.3].m60.mean()))

w('\n## B. Share of the eventual move already done before any tradable entry (ratio of means, placebo-subtracted), by td')
E['b'] = pd.cut(E.td, TDB)
rows = []
for b, d in E.groupby('b', observed=True):
    pp = P.loc[d.row]
    ex = {c: d[c].mean() - pp[c].mean() for c in ('m0', 'm1', 'm2', 'm60', 'm300', 'm1800')}
    rows.append(dict(b=str(b), n=len(d), m1=ex['m1'], m60=ex['m60'], m300=ex['m300'], m1800=ex['m1800'],
                     share_m1_of_m60=ex['m1'] / ex['m60'] if ex['m60'] > 0.02 else np.nan,
                     share_m2_of_m300=ex['m2'] / ex['m300'] if ex['m300'] > 0.02 else np.nan,
                     share_m2_of_m1800=ex['m2'] / ex['m1800'] if ex['m1800'] > 0.02 else np.nan,
                     pre60=-(d['m-60'].mean() - pp['m-60'].mean())))
w(pd.DataFrame(rows).round(3).to_string())
ev = M[M.kind == 'ev'].copy(); ev['b'] = pd.cut(ev.td, TDB)
path = ['o-2000', 'o-1000', 'o-500', 'o-250', 'o-100', 'o-50', 'o0', 'o50', 'o100', 'o150', 'o200', 'o250', 'o300', 'o500', 'o1000', 'o2000', 'o5000', 'o60000', 'o300000']
w('-- ms path (aggTrades sample, 363 symbol-days, all episode anchors in them), mean by td; base = last trade before HL fill ts')
t = ev.groupby('b', observed=True)[path].mean(); t['n'] = ev.groupby('b', observed=True).size(); w(t.round(3).to_string())
pl = M[M.kind != 'ev'].copy(); pl['b'] = pd.cut(pl.td, TDB)
t = pl.groupby('b', observed=True)[['o-1000', 'o-250', 'o200', 'o1000', 'o60000', 'o300000']].mean(); w('-- placebo ms path'); w(t.round(3).to_string())
big = ev[ev.td >= 0.3]
w(f'-- timing td>=0.3 (m(+2s)>=0.05: {big.t50.notna().sum()} of {len(big)}): t50 quantiles ms ' +
  str(big.t50.quantile([.1, .25, .5, .75, .9]).round(0).to_dict()) + f'; share t50 in (0,150]: {((big.t50 > 0) & (big.t50 <= 150)).mean():.2f}, (150,300]: {((big.t50 > 150) & (big.t50 <= 300)).mean():.2f}, <=0: {(big.t50 <= 0).mean():.2f}')

w('\n## C. Latency value, 1s bars, OFFICIAL worst-in-3s entry at ts+L s and worst-in-3s exit at sec+H (gross, mean), by td; P = placebo (L=2)')
for H in (30, 60, 300, 1800):
    rows = []
    for b, d in E.groupby('b', observed=True):
        pp = P.loc[d.row]
        r = {'b': str(b), 'n': len(d)}
        for L in (1, 2, 3, 5, 10, 20, 30, 60):
            r[f'L{L}'] = (d[f'xw{H}'] - d[f'ew{L}']).mean()
        r['P_L2'] = (pp[f'xw{H}'] - pp['ew2']).mean()
        r['mid_L2'] = (d[f'm{H}'] - d['el2']).mean(); r['P_mid_L2'] = (pp[f'm{H}'] - pp['el2']).mean()
        rows.append(r)
    w(f'-- H={H}s'); w(pd.DataFrame(rows).round(3).to_string())

w('\n## D. Latency value, ms (touch entry = first our-side aggressor trade at/after t0+L; exit = first opposite aggressor at t0+H), by td; P = placebo')
ev['b'] = pd.cut(ev.td, TDB)
for H in (10, 60, 300):
    rows = []
    for b, d in ev.groupby('b', observed=True):
        p_ = pl[pl.row.isin(d.row)]
        r = {'b': str(b), 'n': len(d)}
        for L in (0, 100, 150, 200, 250, 300, 500, 1000, 2000, 5000, 10000):
            r[f'{L}'] = (d[f'x{H}'] - d[f'a{L}']).mean()
        r['P_300'] = (p_[f'x{H}'] - p_['a300']).median()
        r['med_300'] = (d[f'x{H}'] - d['a300']).median()
        rows.append(r)
    w(f'-- H={H}s (gross %, P_300 and med_300 are medians)'); w(pd.DataFrame(rows).round(3).to_string())
w('-- break-even: gross(touch, L) - placebo must exceed fee: taker both legs 0.10, taker+maker 0.07, maker both 0.04, official 0.146')

w('\n## E. Reversal after large post-impact move (fade entered at t0+60 s cond. m30, or t0+300 s cond. m120; worst-in-3s both legs)')
for K, mc in ((60, 'm30'), (300, 'm120')):
    for thr in (0.3, 0.5, 1.0):
        d = E[E[mc] >= thr]
        for H in (900, 1800, 3600):
            f = d[f'fw{K}'] - d[f'xr{H}']
            w(f'K={K} {mc}>={thr} H={H}: n={len(d)} fade mean={f.mean():+.3f} med={f.median():+.3f} | D {f[d.per=="D"].mean():+.3f} T {f[d.per=="T"].mean():+.3f}')

w('\n## F. Multi-wallet bursts (>=2 distinct wallets same coin+dir within 30 s): follow from the tx where the 2nd wallet appears')
S = X[(X.b_rank_addr == 2)].drop_duplicates('burst')
pp = P.loc[S.row]
w(f'n={len(S)} m0={S.m0.mean():+.3f} m1={S.m1.mean():+.3f} m60={S.m60.mean():+.3f} post(el2->m300)={(S.m300-S.el2).mean():+.3f} (placebo {(pp.m300-pp.el2).mean():+.3f}) '
  f'worst3s L2->H60 {(S.xw60-S.ew2).mean():+.3f} H300 {(S.xw300-S.ew2).mean():+.3f} H1800 {(S.xw1800-S.ew2).mean():+.3f}')
open(OUT + 'RESULTS.txt', 'w').write('\n'.join(out))
print('\n'.join(out))

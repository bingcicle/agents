# (2) Robustness of A2: grid X/W/td, breakdowns, tails and emergency stop.
from core import *
from hl import bars
out = open(W + 'a2_robust_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s, flush=True); out.write(s + '\n')
def real(f, **kw):
    return exit_sim(f, S=5000, Hs=60, d=0.1, **kw).pnl.values
# ---------------- grid
rows = []
for X in (0.75, 1.0, 1.25, 1.5):
    for Wsec in (30, 60, 120):
        for td in (0.2, 0.3, 0.5, 1.0):
            f = run(A.td.values >= td, X=X, Wsec=Wsec)
            if len(f) == 0:
                continue
            f['P60'] = g(f, 60); f['R'] = real(f)
            for part in (False, True):
                x = f[f.test == part]
                rows.append(dict(X=X, W=Wsec, td=td, part='TEST' if part else 'DISC', n=len(x), per_day=len(x) / (8 if part else 10),
                                 P60=x.P60.mean(), R=x.R.mean(), R_med=x.R.median(), R_ciw_lo=cboot(x.R, x.whale)[0] if len(x) >= 5 else np.nan,
                                 LONG=(x.side == 'LONG').mean()))
G = pd.DataFrame(rows)
G.to_csv(W + 'a2_grid.csv', index=False)
pv = G.pivot_table(index=['X', 'W', 'td'], columns='part', values=['n', 'R', 'R_ciw_lo', 'P60'])
P('GRID (R = realistic HL exit S=$5k H=60 d=0.1; R_ciw_lo = CI90 lower by wallet):'); P(pv.round(3).to_string())
K = len(pv)
P(f'\nK={K} variants. DISC R>0: {(pv[("R","DISC")]>0).sum()}; TEST R>0: {(pv[("R","TEST")]>0).sum()}; both>0: {((pv[("R","DISC")]>0)&(pv[("R","TEST")]>0)).sum()}; '
  f'corr(DISC,TEST) R = {pv[("R","DISC")].corr(pv[("R","TEST")]):.2f}; TEST R median over variants {pv[("R","TEST")].median():+.3f}')
for dim in ('X', 'W', 'td'):
    P(f'by {dim} (mean over other params): ' + G.groupby([dim, 'part']).R.mean().unstack().round(3).to_string().replace('\n', ' ; '))
# walk-forward choice: pick the best DISC variant with n>=20 and report its TEST
dsc = pv[pv[('n', 'DISC')] >= 20][('R', 'DISC')].sort_values(ascending=False)
best = dsc.index[0]
P('walk-forward: best DISC variant (n>=20)', best, 'DISC R %.3f -> TEST R %.3f (n %d)' % (dsc.iloc[0], pv.loc[best, ('R', 'TEST')], pv.loc[best, ('n', 'TEST')]))
# ---------------- base breakdowns
f = add_epi(run(A.td.values >= 0.3))
f['P60'] = g(f, 60); f['R'] = real(f); f['R_rsd35'] = real(f, resid=0.35); f['R_rsd50'] = real(f, resid=0.50)
f['R_slip15'] = exit_sim(f, S=5000, Hs=60, d=0.1, tk_slip=0.15).pnl.values
P('\nBASE R (S=5k,H=60,d=0.1):', rep(f, 'R'))
P('  if HL taker discount at exit = 0.35%:', rep(f, 'R_rsd35'))
P('  if HL taker discount at exit = 0.50%:', rep(f, 'R_rsd50'))
P('  taker slip 0.15% instead of 0.05%:', rep(f, 'R_slip15'))
for c in ('week', 'side'):
    P(f'\nby {c}:'); P(f.groupby(c).agg(n=('R', 'size'), R=('R', 'mean'), R_med=('R', 'median'), P60=('P60', 'mean'), win=('R', lambda x: (x > 0).mean())).round(3).to_string())
P('\nby coin (n>=2):'); t = f.groupby('coin').agg(n=('R', 'size'), R=('R', 'mean'), sum=('R', 'sum')).sort_values('n', ascending=False); P(t[t.n >= 2].round(3).to_string())
P('coins with R<0 (mean):', (t.R < 0).sum(), 'of', len(t), '| max share of positive sum by coin %.2f' % (t['sum'].clip(lower=0).max() / t['sum'].clip(lower=0).sum()))
tw = f.groupby('whale').agg(n=('R', 'size'), R=('R', 'mean'), sum=('R', 'sum')).sort_values('n', ascending=False)
P('\nby wallet (top 10 by n):'); P(tw.head(10).round(3).to_string())
P('wallets', len(tw), 'with R<0:', (tw.R < 0).sum(), '| max share of positive sum by wallet %.2f' % (tw['sum'].clip(lower=0).max() / tw['sum'].clip(lower=0).sum()))
td_ = f.groupby('day').R.agg(['size', 'sum'])
P('days', len(td_), 'days>0', (td_['sum'] > 0).sum(), '| max share of positive sum by day %.2f' % (td_['sum'].clip(lower=0).max() / td_['sum'].clip(lower=0).sum()))
for nm, m in [('w/o 09-18 & 09-23', ~f.day.isin(['09-18', '09-23'])), ('w/o top-3 wallets', ~f.whale.isin(tw.index[:3])), ('LONG only', f.side == 'LONG')]:
    P(f'{nm}:', rep(f[m], 'R'))
# ---------------- BTC-neutral
# ---------------- tails and emergency stop (Binance 1s path, HL taker exit at fair*(1-0.5%) during stress, fee 0.045)
def path_stats(r, T=600):
    b = bars(r.sym, r.ts - 2000, r.ts + T * 1000)
    if b is None:
        return None
    s0 = r.ts // 1000
    fair = b.cf.loc[s0 + 1:s0 + T].values * (1 + r.bas)
    lo = (b.l.fillna(b.cf) if r.wd < 0 else b.h.fillna(b.cf)).loc[s0 + 1:s0 + T].values * (1 + r.bas)
    return fair, lo
tails = []
for r in f.itertuples():
    ps = path_stats(r)
    if ps is None:
        tails.append({}); continue
    fair, adv = ps
    side = -r.wd
    ret_adv = 100 * side * (adv / r.L - 1)        # worst tick in each second, our side
    d = dict(mae60=np.nanmin(ret_adv[:60]), mae300=np.nanmin(ret_adv[:300]), mae600=np.nanmin(ret_adv))
    for SL in (0.75, 1.0, 1.5, 2.0):
        for Hs in (60, 300):
            hit = np.where(ret_adv[:Hs] <= -SL)[0]
            if len(hit):
                k = hit[0]
                wpx = adv[k:k + 3].min() if side > 0 else adv[k:k + 3].max()     # worst in 3 s
                d[f'stop{SL}_{Hs}'] = 100 * side * (wpx * (1 - side * 0.005) / r.L - 1) - FEE_MK - FEE_TK
                d[f'hit{SL}_{Hs}'] = 1
            else:
                d[f'hit{SL}_{Hs}'] = 0
    tails.append(d)
Tl = pd.DataFrame(tails, index=f.index)
f = f.join(Tl)
for SL in (0.75, 1.0, 1.5, 2.0):
    f[f'Rstop{SL}'] = f[f'stop{SL}_60'].where(f[f'hit{SL}_60'] == 1, f.R)
P('\nMAE (worst adverse Binance tick x basis vs our level, %%): 60 s p5 %.2f min %.2f | 300 s p5 %.2f min %.2f | 600 s p5 %.2f min %.2f' %
  (f.mae60.quantile(.05), f.mae60.min(), f.mae300.quantile(.05), f.mae300.min(), f.mae600.quantile(.05), f.mae600.min()))
for SL in (0.75, 1.0, 1.5, 2.0):
    P(f'stop {SL}% within 60 s hold: hit share {f[f"hit{SL}_60"].mean():.3f}, R with stop mean DISC {f[~f.test][f"Rstop{SL}"].mean():+.3f} TEST {f[f.test][f"Rstop{SL}"].mean():+.3f}, min {f[f"Rstop{SL}"].min():+.2f}')
P('\nR distribution: p1 %.2f p5 %.2f p10 %.2f min %.2f max %.2f; P300 min %.2f' % tuple(list(f.R.quantile([.01, .05, .1])) + [f.R.min(), f.R.max(), g(f, 300).min()]))
f['P300'] = g(f, 300); f['P600'] = g(f, 600)
P('\n8 worst fills by R:'); P(f.sort_values('R')[['day', 'coin', 'side', 'whale', 'exc_pl', 'td', 'cap', 'P60', 'R', 'P300', 'P600', 'mae60', 'mae600']].head(8).round(3).to_string())
P('SAGA fills in A2:', f[f.coin == 'SAGA'][['day', 'side', 'exc_pl', 'R', 'P300', 'P600', 'mae600']].round(2).to_string())
f.to_parquet(W + 'a2_robust_fills.parquet')
out.close()

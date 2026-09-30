"""Coin x 5-min panel: known ongoing TWAP pressure S(t) = sum over TWAPs with seen<=t<end of tdir*intensity
(intensity = TWAP usd per min / Binance 24h avg quote vol per min). Forward returns t->t+15/60/240 min (raw and minus alt index).
Entries only at grid times (minute boundaries), price = last k1m close. One observation per coin per 5 min (overlap handled by
aggregation; forward windows overlap -> day-clustered inference)."""
import sys; sys.path.insert(0, '.')
from rules import *
from build2 import pfun, palt
D = base()
t0 = pd.Timestamp('2026-09-09 12:00').value // 10**6; t1 = pd.Timestamp('2026-09-29 19:55').value // 10**6
G = np.arange(t0, t1, 5 * 60000)
rows = []
for s, x in D.groupby('sym'):
    if len(x) < 20: continue
    k = kl(s); ot = k.index.values.astype('int64'); c = k.c.values
    S = np.zeros(len(G)); Sn = np.zeros(len(G)); Sfresh = np.zeros(len(G))
    for r in x.itertuples():
        a = np.searchsorted(G, r.seen_ms); b = np.searchsorted(G, r.end_ms)
        if b > a:
            S[a:b] += r.tdir * r.intensity; Sn[a:b] += 1
            Sfresh[a:min(b, a + 3)] += r.tdir * r.intensity
    m = Sn > 0
    if m.sum() == 0: continue
    p0 = pfun(ot, c, G[m]); a0 = palt(G[m])
    rec = pd.DataFrame({'sym': s, 'T': G[m], 'S': S[m], 'Sfresh': Sfresh[m], 'n': Sn[m]})
    for h in (15, 60, 240):
        rec[f'f{h}'] = 100 * (pfun(ot, c, G[m] + h * 60000) / p0 - 1)
        rec[f'fa{h}'] = rec[f'f{h}'] - 100 * (palt(G[m] + h * 60000) / a0 - 1)
    rows.append(rec)
P = pd.concat(rows, ignore_index=True)
P['dday'] = pd.to_datetime(P['T'], unit='ms').dt.strftime('%Y-%m-%d')
P['per'] = np.where(P.dday <= '2026-09-21', 'disc', 'test')
P.to_parquet('panel.parquet')
print('panel rows', len(P), 'syms', P.sym.nunique())
from scipy.stats import spearmanr
for per in ('disc', 'test'):
    x = P[P.per == per]
    for sc in ('S', 'Sfresh'):
        xx = x[x[sc] != 0]
        out = [f'{per} {sc} n={len(xx)}']
        for h in (15, 60, 240):
            ic = xx.groupby('dday').apply(lambda g: spearmanr(g[sc], g[f'fa{h}'], nan_policy='omit')[0] if len(g) > 20 else np.nan)
            out.append(f'IC_fa{h}: mean_daily={ic.mean():+.3f} pos_days={int((ic>0).sum())}/{ic.notna().sum()}')
        print(' '.join(out))
    # sign-trading: follow sign(S) when |S| >= thr, hold 60 min, non-overlapping per coin (take every 12th 5-min slot)
    for thr in (0.05, 0.2, 0.5):
        xx = x[(x.S.abs() >= thr)].copy()
        xx = xx.sort_values(['sym', 'T'])
        keep = []; last = {}
        for i, s_, T in zip(xx.index, xx.sym, xx['T']):
            if last.get(s_, -1) > T: continue
            keep.append(i); last[s_] = T + 60 * 60000
        y = xx.loc[keep]; y['cd'] = y.sym + '|' + y.dday
        g = np.sign(y.S) * y.f60; ga = np.sign(y.S) * y.fa60
        print(f'  {per} follow sign(S) |S|>={thr} 60m non-overlap:', line(g - COST, y.cd, 'net raw'), '|', line(ga - COST, y.cd, 'net xalt'))

import json, os, sys, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/p2-skeptic-a2/'
TEST0 = pd.Timestamp('2026-09-22').value // 10**6
w = pd.read_parquet(OUT + 'tx.parquet').sort_values('ts').reset_index(drop=True)
w['day'] = pd.to_datetime(w.ts, unit='ms').dt.strftime('%m-%d')
w['taker_close'] = (w.liq == 0) & w.dir.isin(['Close Long', 'Close Short'])
# strictly-past depth: median depth_usd of the coin on the previous calendar day(s) (fallback: expanding median of past rows)
w['depth_past'] = np.nan
for c, g in w.groupby('coin'):
    s = g.depth_usd.where(g.depth_usd > 0)
    dm = s.groupby(g.day).median()
    prev = dm.shift(1).reindex(g.day).values
    exp = s.expanding(20).median().shift(1).values
    w.loc[g.index, 'depth_past'] = np.where(np.isnan(prev), exp, prev)
out = open(OUT + 'sim_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s, flush=True); out.write(s + '\n')

def cboot(x, cl, n=2000, seed=1):
    x = np.asarray(x, float); cl = np.asarray(cl); ok = ~np.isnan(x); x, cl = x[ok], cl[ok]
    if len(x) < 4: return (np.nan, np.nan)
    u, inv = np.unique(cl, return_inverse=True); s = np.bincount(inv, weights=x); k = np.bincount(inv)
    p = np.random.default_rng(seed).integers(0, len(u), (n, len(u)))
    return tuple(np.round(np.percentile(s[p].sum(1) / k[p].sum(1), [5, 95]), 3))

def sim(bas='basA', X=1.0, Wsec=60, lat=1500, td=0.3, depth='depth_usd', fillset='close', trig_shift_ms=0, H=60,
        disc=0.20, slip=0.05, rearm=60, margin=0.0):
    F = w['c-1'].values * (1 + w[bas].values)
    wd = w.wd.values; ts = w.ts.values
    exc = 100 * wd * (w.pl.values / F - 1); excf = 100 * wd * (w.pf.values / F - 1)
    tdv = w.tx_usd.values / w[depth].values
    trig = np.where(w.taker_close.values & (tdv >= td) & ~np.isnan(F))[0]
    okfill = w.taker_close.values if fillset == 'close' else np.ones(len(w), bool)
    coins = w.coin.values
    byc = {c: g.index.values for c, g in w.groupby('coin')}
    busy = {}; rows = []
    for i in trig:
        tdet = ts[i] + lat + trig_shift_ms; key = (coins[i], wd[i])
        if busy.get(key, 0) > tdet: continue
        idx = byc[coins[i]]; tsv = ts[idx]
        a = np.searchsorted(tsv, tdet, side='right'); b = np.searchsorted(tsv, tdet + Wsec * 1000, side='right')
        c = idx[a:b]; c = c[(wd[c] == wd[i]) & okfill[c] & (exc[c] > X + margin) & ~np.isnan(exc[c])]
        busy[key] = tdet + Wsec * 1000
        if not len(c): continue
        j = c[0]; busy[key] = ts[j] + rearm * 1000; rows.append((i, j))
    if not rows: return None
    R = pd.DataFrame(rows, columns=['ti', 'fi'])
    f = w.loc[R.fi, ['addr', 'coin', 'day', 'ts', 'wd', 'tx_usd', 'pf', 'pl', 'c-1', 'c60', 'c120', 'c300', 'mae60', 'liq', 'dir', 'sym']].reset_index(drop=True)
    f['ti'] = R.ti.values; f['trig_addr'] = w.addr.values[R.ti.values]
    f['bas'] = w[bas].values[R.fi.values]; f['F'] = F[R.fi.values]; f['exc'] = exc[R.fi.values]; f['excf'] = excf[R.fi.values]
    f['L'] = f.F * (1 + f.wd * X / 100); side = -f.wd
    fr = ((f.exc - X) / (f.exc - f.excf).clip(lower=1e-9)).clip(0, 1); f['cap'] = f.tx_usd * fr
    fx = f[f'c{H}'] * (1 + f.bas) * (1 - side * disc / 100)
    f['R'] = 100 * side * (fx / f.L - 1) - slip - 0.015 - 0.045
    f['P'] = 100 * side * (f[f'c{H}'] * (1 + f.bas) / f.L - 1) - 0.03       # optimistic passive at fair
    f['MAE'] = 100 * side * (f.mae60 * (1 + f.bas) / f.L - 1)
    f['test'] = f.ts >= TEST0; f['side'] = np.where(side > 0, 'LONG', 'SHORT')
    f['same'] = f.addr == f.trig_addr
    f = f.sort_values(['addr', 'coin', 'ts']); f['epi'] = ((f.addr != f.addr.shift()) | (f.coin != f.coin.shift()) | (f.ts.diff() > 300000)).cumsum()
    return f.sort_values('ts')

def rep(f, col='R'):
    s = []
    for part in (False, True):
        x = f[(f.test == part) & f[col].notna()]
        if len(x) < 4: s.append(f'{"TEST" if part else "DISC"} n={len(x)} mean={x[col].mean():+.3f}'); continue
        top = x[col].sort_values(ascending=False); wo = x[col].sum() - top.iloc[:max(1, round(len(x) * .1))].sum()
        s.append(f'{"TEST" if part else "DISC"} n={len(x)} ep={x.epi.nunique()} mean={x[col].mean():+.3f} med={x[col].median():+.3f} '
                 f'CIw{cboot(x[col], x.addr)} CId{cboot(x[col], x.day)} CIc{cboot(x[col], x.coin)} wo_top10={wo:+.2f}')
    return ' | '.join(s)

if __name__ == '__main__':
    f = sim()
    f.to_parquet(OUT + 'fills_base.parquet')
    P('BASE own rebuild (basA, X=1, W60, lat1.5, td>=0.3 bot depth, fills=taker closes): n', len(f), 'same wallet', round(f.same.mean(), 2), 'LONG', round((f.side == 'LONG').mean(), 2))
    P(' P60 fair-passive:', rep(f, 'P'))
    P(' R taker exit fair-0.20%:', rep(f, 'R'))
    old = pd.read_parquet(SP + 'work/p2-a2/a2_base_fills.parquet')
    m = f.merge(old[['ts', 'addr', 'coin']].assign(inold=1), on=['ts', 'addr', 'coin'], how='left')
    P(' overlap with lens fills:', int(m.inold.sum()), 'of mine', len(f), 'lens', len(old))
    for name, kw in [('basB (print-based basis)', dict(bas='basB')), ('depth strictly past (prev day median)', dict(depth='depth_past')),
                     ('fills from ALL tracked txs (opens, liq, TWAP)', dict(fillset='all')), ('lat 3 s', dict(lat=3000)), ('lat 0.5 s', dict(lat=500)),
                     ('disc 0.35', dict(disc=0.35)), ('disc 0.50', dict(disc=0.5)), ('H=120', dict(H=120)), ('H=300', dict(H=300)),
                     ('margin 0.1 (price must be 0.1% beyond L)', dict(margin=0.1)), ('X=1.25', dict(X=1.25)), ('X=0.75', dict(X=0.75)),
                     ('PLACEBO trigger shifted +20 min', dict(trig_shift_ms=1200000)), ('PLACEBO trigger shifted -20 min', dict(trig_shift_ms=-1200000))]:
        g = sim(**kw)
        if g is None: P(name, 'no fills'); continue
        P(f'{name}: n={len(g)}', rep(g, 'R'))
    # USD-weighted and size-capped
    for S in (2000, 5000):
        sz = f.cap.clip(upper=S)
        for part in (False, True):
            x = f.test == part
            P(f'USD-weighted R at S={S} {"TEST" if part else "DISC"}: {np.average(f.R[x], weights=sz[x]):+.3f}  $/fill {np.mean(f.R[x] / 100 * sz[x]):+.2f}')
    P('by side:', f.groupby(['side', 'test']).R.agg(['count', 'mean']).round(3).to_dict())
    P('worst 8 fills:', f.nsmallest(8, 'R')[['day', 'coin', 'addr', 'side', 'R', 'MAE', 'cap']].round(2).to_string())
    P('MAE60 p5/min:', f.MAE.quantile(.05).round(2), f.MAE.min().round(2))
    P('by day:', f.groupby('day').R.agg(['count', 'mean']).round(2).T.to_string())
    out.close()

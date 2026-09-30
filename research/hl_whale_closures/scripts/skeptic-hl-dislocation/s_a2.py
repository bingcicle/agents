# Independent A2 re-implementation + attacks.
import os, json, sys, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT = SP + 'work/skeptic-hl-dislocation/'
SYM = json.load(open(SP + 'infra/symmap.json'))
TEST0 = pd.Timestamp('2026-09-22').value // 10**6
A = pd.read_parquet(OUT + 's_tx.parquet')
A['bas'] = A.bas15.fillna(A.bas5)
A = A[A['b-1'].notna() & A.bas.notna()].copy()
A['td'] = A.tx_usd / A.depth_usd
A['day'] = pd.to_datetime(A.ts, unit='ms').dt.strftime('%m-%d')
A['test'] = A.ts >= TEST0
A['taker_close'] = (A.liq == 0) & A.dir.isin(['Close Long', 'Close Short'])
A = A.sort_values('ts').reset_index(drop=True)
BYC = {c: g for c, g in A.groupby('coin')}

def run(trig_mask, X=1.0, Wsec=60, lat_ms=1500, ref='b-1', fillset='lens', margin=0.0, use_bot=False, rearm_s=60, bas_col='bas'):
    trig = A[trig_mask & A.taker_close]
    busy = {}; rows = []
    for t in trig.itertuples():
        tdet = t.ts + lat_ms
        if use_bot:
            tdet = max(tdet, int(t.bot_ts * 1000))
        key = (t.coin, t.wd)
        if busy.get(key, 0) > tdet:
            continue
        g = BYC[t.coin]; tsv = g.ts.values
        lo = np.searchsorted(tsv, tdet, side='right'); hi = np.searchsorted(tsv, tdet + Wsec * 1000, side='right')
        c = g.iloc[lo:hi]
        c = c[c.wd == t.wd]
        if fillset == 'lens':
            c = c[c.taker_close]
        F = c[ref] * (1 + c[bas_col])
        exc = 100 * c.wd * (c.pl / F - 1)
        c = c[exc > X + margin]
        busy[key] = tdet + Wsec * 1000
        if not len(c):
            continue
        f = c.iloc[0]; busy[key] = f.ts + rearm_s * 1000
        Fv = f[ref] * (1 + f[bas_col]); L = Fv * (1 + f.wd * X / 100)
        r = dict(trig_ts=t.ts, trig_addr=t.addr, trig_h=t.h, whale=f.addr, coin=f.coin, day=f.day, test=f.test, ts=f.ts, wd=f.wd, L=L,
                 exc=100 * f.wd * (f.pl / Fv - 1), td=f.td, tx_usd=f.tx_usd, same_wallet=f.addr == t.addr, liq=f.liq, fdir=f.dir,
                 wait=(f.ts - tdet) / 1000, bas=f[bas_col], bm1=f['b-1'], age=f.age_m1, sym=f.sym)
        for dt in (5, 10, 30, 60, 120, 300):
            r[f'P{dt}'] = 100 * (-f.wd) * (f[f'b{dt}'] * (1 + f[bas_col]) / L - 1) - 0.03
        r['H'] = 100 * (-f.wd) * (f['hedge13'] * (1 + f[bas_col]) / L - 1) - 0.16
        rows.append(r)
    return pd.DataFrame(rows)

def cboot(x, cl, n=2000, seed=0):
    x = np.asarray(x, float); cl = np.asarray(cl)
    u, inv = np.unique(cl, return_inverse=True)
    s = np.bincount(inv, weights=x); k = np.bincount(inv)
    rng = np.random.default_rng(seed); m = []
    for _ in range(n):
        p = rng.integers(0, len(u), len(u))
        m.append(s[p].sum() / k[p].sum())
    return np.percentile(m, [5, 95])

def rep(f, col, label=''):
    out = []
    for part in (False, True):
        g = f[(f.test == part) & f[col].notna()]
        if len(g) < 3:
            out.append(f'{"TEST" if part else "DISC"} n={len(g)}'); continue
        ciw = cboot(g[col], g.whale); cid = cboot(g[col], g.day); cic = cboot(g[col], g.coin); cie = cboot(g[col], g.epi) if 'epi' in g else (np.nan, np.nan)
        out.append(f'{"TEST" if part else "DISC"} n={len(g)} mean={g[col].mean():+.3f} med={g[col].median():+.3f} win={(g[col]>0).mean():.2f} '
                   f'CIw[{ciw[0]:+.2f},{ciw[1]:+.2f}] CId[{cid[0]:+.2f},{cid[1]:+.2f}] CIc[{cic[0]:+.2f},{cic[1]:+.2f}] CIep[{cie[0]:+.2f},{cie[1]:+.2f}]')
    print(f'{label:28s} {col:5s} | ' + ' | '.join(out))

def add_epi(f, gap=300):
    f = f.sort_values(['whale', 'coin', 'ts']).copy()
    f['epi'] = ((f.whale != f.whale.shift()) | (f.coin != f.coin.shift()) | (f.ts.diff() > gap * 1000)).cumsum()
    return f.sort_values('ts')

if __name__ == '__main__':
    base = add_epi(run(A.td >= 0.3))
    base.to_parquet(OUT + 's_a2_base.parquet')
    print('BASE A2 td>=0.3 X=1 W=60 lat1.5: fills', len(base), 'DISC', (~base.test).sum(), 'TEST', base.test.sum(), 'episodes(5min)', base.epi.nunique(),
          'same wallet', base.same_wallet.mean().round(2), 'LONG share', (base.wd < 0).mean().round(2))
    for col in ('P5', 'P30', 'P60', 'P120', 'P300', 'H'):
        rep(base, col, 'base')

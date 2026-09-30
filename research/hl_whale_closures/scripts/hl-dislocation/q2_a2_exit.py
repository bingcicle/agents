# A2 with an explicit exit simulation on real HL prints: after our fill, post a passive exit on HL at Binance fair shifted by delta
# toward the whale side; it is filled when a later OPPOSITE-side tracked-wallet print trades strictly through it (lower bound:
# untracked traders are invisible). If not filled within 60 s -> taker exit (fair at 60 s minus residual same-side discount, fee).
from strat import *
out = open(W + 'q2_a2_exit_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
R = pd.read_parquet(W + 'q1_rev_prints.parquet')
R = R[pd.to_datetime('2026-' + R.day) < pd.Timestamp('2026-09-22')]
EB = [0.3, 0.75, 1.25, 2, 100]
R['e0b'] = pd.cut(R.exc0, EB, labels=False)
RS = R[R.same_side].groupby(['ep', 'e0b', 'bk']).exc.median().groupby(['e0b', 'bk']).median().unstack().ffill(axis=1).bfill(axis=1)
def resid_same(exc, dt=60):
    b = int(np.clip(pd.cut([exc], EB, labels=False)[0] if exc > 0.3 else 0, 0, len(EB) - 2))
    return float(RS.loc[b, max(k for k in RS.columns if k <= dt)])

allp = load_tx(all_rows=True); allp = allp[allp['bn-1'].notna()].sort_values('ts')
P_by = {c: g for c, g in allp.groupby('coin')}
w = w.sort_values('ts').reset_index(drop=True)
by_coin = {c: g for c, g in w.groupby('coin')}
def a2(trig, X=1.0, Wsec=60, deltas=(0.0, 0.1, 0.2), hold=60):
    rows = []; busy = {}
    for t in trig.itertuples():
        tdet = t.ts + 1500
        key = (t.coin, t.wdir)
        if busy.get(key, 0) > tdet:
            continue
        g = by_coin[t.coin]; tsv = g.ts.values
        lo = np.searchsorted(tsv, tdet, side='right'); hi = np.searchsorted(tsv, tdet + Wsec * 1000, side='right')
        c = g.iloc[lo:hi]; c = c[(c.wdir == t.wdir) & (c.exc_pl > X)]
        busy[key] = tdet + Wsec * 1000
        if not len(c):
            continue
        f = c.iloc[0]; busy[key] = f.ts + 60000
        L = f['bn-1'] * (1 + f.basis) * (1 + f.wdir * X / 100)
        pc = P_by[f.coin]; pt = pc.ts.values
        a, b = np.searchsorted(pt, f.ts, side='right'), np.searchsorted(pt, f.ts + hold * 1000, side='right')
        seg = pc.iloc[a:b]; seg = seg[(seg.wdir == -f.wdir) & (seg.h != f.h)]
        r = dict(whale=f.addr, coin=f.coin, day=f.day, test=f.test, exc_pl=f.exc_pl)
        fair60 = f[f'bn{hold}'] * (1 + f.basis)
        taker = 100 * (-f.wdir) * (fair60 / L - 1) - resid_same(f.exc_pl, hold) - 0.06
        for d in deltas:
            F_t = seg['bn-1'] * (1 + f.basis)
            A_t = F_t * (1 + f.wdir * d / 100)                  # our exit quote, re-pegged to Binance at each print
            hit = (-f.wdir) * (seg.px - A_t) > 0                # buyer print strictly through our ask (whale-sell case)
            if hit.any():
                j = np.argmax(hit.values)
                r[f'E{d}'] = 100 * (-f.wdir) * (A_t.iloc[j] / L - 1) - 0.03
                r[f'filled{d}'] = True; r[f'tfill{d}'] = (seg.ts.iloc[j] - f.ts) / 1000
            else:
                r[f'E{d}'] = taker; r[f'filled{d}'] = False
        r['T60'] = taker
        rows.append(r)
    return pd.DataFrame(rows)

f = a2(w[w.td >= 0.3]); f.to_parquet(W + 'q2_a2_exit_fills.parquet')
for part in (False, True):
    g = f[f.test == part]
    P(f'\n{"TEST" if part else "DISC"}: fills {len(g)}')
    for d in (0.0, 0.1, 0.2):
        P(f'  exit maker at fair-{d}%: maker-filled within 60 s by visible prints {g[f"filled{d}"].mean():.2f}; ' + fmt_h(honesty(g, f'E{d}', wallet='whale', day='day', coin='coin')))
    P('  all-taker T60: ' + fmt_h(honesty(g, 'T60', wallet='whale', day='day', coin='coin')))
out.close()

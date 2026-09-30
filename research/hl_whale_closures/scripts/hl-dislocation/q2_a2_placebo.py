# Robustness + placebo for A2 (trigger: tracked-whale taker close with tx_usd/depth >= 0.3; arm at ts+1.5 s; rest X=1% beyond
# Binance fair for W=60 s; fill by later tracked-whale tx beyond our level; exit passive at Binance fair after 60 s).
from strat import *
out = open(W + 'q2_a2_placebo_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n')
tm = pd.read_parquet(SP + 'data/tx_matrix.parquet', columns=['addr', 'coin', 'h', 'BTCr60', 'BTCr300'])
R = pd.read_parquet(W + 'q1_rev_prints.parquet')
R = R[pd.to_datetime('2026-' + R.day) < pd.Timestamp('2026-09-22')]
EB = [0.3, 0.75, 1.25, 2, 100]
R['e0b'] = pd.cut(R.exc0, EB, labels=False)
def table(same):
    t = R[R.same_side == same].groupby(['ep', 'e0b', 'bk']).exc.median().groupby(['e0b', 'bk']).median().unstack()
    return t.ffill(axis=1).bfill(axis=1)
RS, RO = table(True), table(False)
def lookup(T, exc, dt):
    b = np.clip(pd.cut(exc, EB, labels=False).fillna(0).astype(int), 0, len(EB) - 2)
    col = max(k for k in T.columns if k <= dt)
    return T[col].reindex(b).values

w = w.sort_values('ts').reset_index(drop=True)
by_coin = {c: g for c, g in w.groupby('coin')}
def run(trig, X=1.0, Wsec=60, ref='bn-1', lat_ms=1500, use_bot=False):
    rows = []; busy = {}
    for t in trig.itertuples():
        tdet = t.ts + lat_ms
        if use_bot:
            tdet = max(tdet, int(t.bot_ts * 1000))
        key = (t.coin, t.wdir)
        if busy.get(key, 0) > tdet:
            continue
        g = by_coin[t.coin]; tsv = g.ts.values
        lo = np.searchsorted(tsv, tdet, side='right'); hi = np.searchsorted(tsv, tdet + Wsec * 1000, side='right')
        c = g.iloc[lo:hi]
        F = c[ref] * (1 + c.basis)
        exc = 100 * c.wdir * (c.pl / F - 1)
        c = c[(c.wdir == t.wdir) & (exc > X)]
        busy[key] = tdet + Wsec * 1000
        if not len(c):
            continue
        f = c.iloc[0]; busy[key] = f.ts + 60000
        L = f[ref] * (1 + f.basis) * (1 + f.wdir * X / 100)
        r = dict(trig_addr=t.addr, whale=f.addr, coin=f.coin, day=f.day, test=f.test, h=f.h, addr=f.addr, ts=f.ts, wdir=f.wdir,
                 exc_pl=f.exc_pl, td=f.td, tx_usd=f.tx_usd, exc_pf=f.exc_pf, same_wallet=f.addr == t.addr)
        for dt in (5, 60, 300):
            fair = f[f'bn{dt}'] * (1 + f.basis)
            g0 = 100 * (-f.wdir) * (fair / L - 1)
            r[f'P{dt}'] = g0 - 0.03
            r[f'M{dt}'] = g0 - lookup(RO, pd.Series([f.exc_pl]), dt)[0] - 0.03
            r[f'T{dt}'] = g0 - lookup(RS, pd.Series([f.exc_pl]), dt)[0] - 0.06
        r['H1'] = 100 * (-f.wdir) * (f['hedge_w1'] * (1 + f.basis) / L - 1) - 0.16
        fr = min(max((f.exc_pl - X) / max(f.exc_pl - f.exc_pf, 1e-9), 0), 1)
        r['cap_usd'] = f.tx_usd * fr
        rows.append(r)
    return pd.DataFrame(rows)

trig = w[w.td >= 0.3]
base = pd.read_parquet(W + 'q2_a2_fills.parquet')
P('\n===== PLACEBO: same triggers moved to random times in the same coin & day (not within 10 min of the real one), 20 draws')
rng = np.random.default_rng(1)
pl = []
for k in range(20):
    tr = trig.copy()
    day0 = (tr.ts // 86400000) * 86400000
    off = rng.integers(0, 86400000, len(tr))
    newts = day0 + off
    bad = np.abs(newts - tr.ts) < 600000
    newts[bad] = day0[bad] + (off[bad] + 43200000) % 86400000
    tr['ts'] = newts
    tr = tr[tr.ts < END].sort_values('ts')
    f = run(tr)
    if not len(f):
        f = pd.DataFrame(columns=['test', 'P60', 'P5', 'P300'])
    for part in (False, True):
        g = f[f.test == part]
        pl.append(dict(draw=k, part=part, n=len(g), P60=g.P60.mean(), P5=g.P5.mean(), P300=g.P300.mean()))
PL = pd.DataFrame(pl)
for part in (False, True):
    g = PL[PL.part == part]
    real = base[base.test == part].P60.mean()
    P(f'  {"TEST" if part else "DISC"}: placebo fills/draw {g.n.mean():.1f} (real {len(base[base.test==part])}); placebo P60 mean over draws {g.P60.mean():+.3f} '
      f'[min {g.P60.min():+.3f}, max {g.P60.max():+.3f}]; real {real:+.3f}; share of draws >= real: {(g.P60 >= real).mean():.2f}')
out.close()

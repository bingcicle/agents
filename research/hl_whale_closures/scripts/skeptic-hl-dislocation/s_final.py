from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
X = pd.read_parquet(OUT + 's_a2_exit.parquet')
X['Rreal'] = X['R1000_0.1_px_fb_bn']
top = X.groupby('whale').size().sort_values(ascending=False)
for nm, m in [('all', X.index == X.index), ('w/o 0x0871de', X.whale != top.index[0]), ('w/o top3 wallets', ~X.whale.isin(top.index[:3])),
              ('w/o 09-18,09-23', ~X.day.isin(['09-18', '09-23'])), ('LONG only (whale sells)', X.wd < 0), ('SHORT only', X.wd > 0)]:
    g = X[m]
    print(f'{nm:26s} n={len(g):3d} | P60 DISC {g[~g.test].P60.mean():+.3f} TEST {g[g.test].P60.mean():+.3f} | real-exit DISC {g[~g.test].Rreal.mean():+.3f} TEST {g[g.test].Rreal.mean():+.3f} | HL-close DISC {g[~g.test].fb_hl.mean():+.3f} TEST {g[g.test].fb_hl.mean():+.3f}')
# BTC 1-min drift over the fill minute -> minute+1 (market beta control)
def kline(sym):
    d = f'{SP}data/k1m/{sym}/'; parts = []
    for fn in sorted(os.listdir(d)):
        z = np.load(d + fn); parts.append(pd.Series(z['c'], index=z['ot']))
    s = pd.concat(parts).sort_index(); return s[~s.index.duplicated()]
btc = kline('BTCUSDT')
m0 = (X.ts // 60000) * 60000
X['btc60'] = 100 * (btc.reindex(m0 + 60000).values / btc.reindex(m0).values - 1)   # our side is LONG when wd<0
X['P60_btc'] = X.P60 - (-X.wd) * X.btc60
print('BTC-adjusted P60 (1m approx): DISC %+.3f TEST %+.3f' % (X[~X.test].P60_btc.mean(), X[X.test].P60_btc.mean()))

# Structural fact: opposite-side HL prints 1-5 s after a big sweep vs fair (reversion of the other side)
c = A[A.taker_close].copy()
c['exc'] = 100 * c.wd * (c.pl / (c['b-1'] * (1 + c.bas)) - 1)
ev = c[(c.exc > 0.3) & (c.td >= 0.03)].sort_values(['addr', 'coin', 'ts'])
ev['ep'] = ((ev.addr != ev.addr.shift()) | (ev.coin != ev.coin.shift()) | (ev.ts.diff() > 60000)).cumsum()
ev = ev.sort_values('exc', ascending=False).drop_duplicates('ep')
rec = []
for e in ev.itertuples():
    g = BYC[e.coin]; tsv = g.ts.values
    for a, b in ((1, 5), (5, 30), (30, 120), (120, 300)):
        lo = np.searchsorted(tsv, e.ts + a * 1000, side='right'); hi = np.searchsorted(tsv, e.ts + b * 1000, side='right')
        s = g.iloc[lo:hi]; s = s[s.h != e.h]
        if not len(s): continue
        ex = 100 * e.wd * (s.px / (s['b-1'] * (1 + e.bas)) - 1)      # + = still dislocated in whale direction
        for side in (1, -1):
            m = (s.wd == e.wd) if side == 1 else (s.wd != e.wd)
            if m.any(): rec.append((e.ep, f'{a}-{b}s', 'same' if side == 1 else 'opp', ex[m].median(), e.exc))
R = pd.DataFrame(rec, columns=['ep', 'win', 'side', 'exc', 'exc0'])
print('\nHL prints after big sweeps (deepest tx per 60s-episode, exc>0.3, n_ep=%d): median over episodes of per-episode median dislocation vs fair' % len(ev))
print(R.groupby(['win', 'side']).exc.agg(['median', 'size']).unstack().round(3))

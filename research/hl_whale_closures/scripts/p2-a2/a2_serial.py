# (4) Serial-sweeper score (walk-forward) and pre-emptive arming.
from core import *
out = open(W + 'a2_serial_results.txt', 'w')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s, flush=True); out.write(s + '\n')
ts, wd, exc, tc, td = COL['ts'], COL['wd'], COL['exc_pl'], COL['taker_close'], COL['td']
# big sweeps of each wallet: td>=0.3 taker closes; "first of burst" = no big sweep of same wallet/coin/dir in previous 120 s
big = A[(td >= 0.3) & tc].copy()
big = big.sort_values(['addr', 'coin', 'wd', 'ts'])
prev_gap = big.groupby(['addr', 'coin', 'wd']).ts.diff()
big['first'] = prev_gap.isna() | (prev_gap > 120000)
# continuation: a later tx of the SAME wallet/coin/dir within (1.5, 61.5] s that prints >1% beyond fair (what would fill A2)
cont = []
for r in big.itertuples():
    idx = BYC[r.coin]; tsv = ts[idx]
    lo = np.searchsorted(tsv, r.ts + 1500, side='right'); hi = np.searchsorted(tsv, r.ts + 61500, side='right')
    s = idx[lo:hi]
    cont.append(bool(((ADDR[s] == r.addr) & (wd[s] == r.wd) & (exc[s] > 1.0)).any()))
big['cont'] = cont
ep = big[big['first']].copy()
P('first-of-burst big sweeps: %d, wallets %d; share followed by a >1%% continuation within 60 s: %.3f' % (len(ep), ep.addr.nunique(), ep.cont.mean()))
# walk-forward score per (wallet, day) from strictly earlier days
days = sorted(A.day.unique())
cnt = ep.groupby(['addr', 'day']).agg(n=('cont', 'size'), k=('cont', 'sum')).reset_index()
score = {}
for d in days:
    past = cnt[cnt.day < d].groupby('addr')[['n', 'k']].sum()
    for a, r in past.iterrows():
        score[(a, d)] = (r.k + 0.5) / (r.n + 2), r.n        # shrunk continuation rate, n past bursts
A['ser'] = [score.get((a, d), (np.nan, 0))[0] for a, d in zip(A.addr.values, A.day.values)]
A['ser_n'] = [score.get((a, d), (np.nan, 0))[1] for a, d in zip(A.addr.values, A.day.values)]
ep['ser'] = A.loc[ep.index, 'ser']; ep['ser_n'] = A.loc[ep.index, 'ser_n']
# persistence: does the past score predict the continuation of today's bursts?
e2 = ep[ep.ser_n >= 2]
P('bursts with a past score (n_past>=2): %d; continuation rate by past-score tercile:' % len(e2))
e2['q'] = pd.qcut(e2.ser, 3, labels=['low', 'mid', 'high'], duplicates='drop')
P(e2.groupby('q').agg(n=('cont', 'size'), cont=('cont', 'mean'), ser=('ser', 'mean')).round(3).to_string())
P('corr(score, cont) = %.3f' % np.corrcoef(e2.ser, e2.cont)[0, 1])
real = lambda f: exit_sim(f, S=5000, Hs=60, d=0.1).pnl.values
ser = A.ser.values; sn = A.ser_n.values
variants = {
  'A2 base (td>=0.3, any wallet)': (td >= 0.3),
  'A2, trigger wallet score>=0.4': (td >= 0.3) & (ser >= 0.4) & (sn >= 2),
  'A2, trigger wallet score<0.4 or unknown': (td >= 0.3) & ~((ser >= 0.4) & (sn >= 2)),
  'serial wallets: td>=0.1 trigger': (td >= 0.1) & (ser >= 0.4) & (sn >= 2),
  'serial: pre-arm on ANY tx td>=0.02': (td >= 0.02) & (ser >= 0.4) & (sn >= 2),
  'base + serial pre-arm (union)': (td >= 0.3) | ((td >= 0.02) & (ser >= 0.4) & (sn >= 2)),
  'serial(>=0.3) pre-arm td>=0.02': (td >= 0.02) & (ser >= 0.3) & (sn >= 2),
}
res = {}
for nm, m in variants.items():
    f = run(m)
    if len(f) < 3:
        P(nm, 'n', len(f)); continue
    f = add_epi(f); f['R'] = real(f); f['P60'] = g(f, 60)
    # was the fill a FIRST hit (no big sweep by anyone in this coin/dir in the 61.5 s before the fill)?
    fh = []
    for r in f.itertuples():
        idx = BYC[r.coin]; tsv = ts[idx]
        lo = np.searchsorted(tsv, r.ts - 61500); hi = np.searchsorted(tsv, r.ts, side='left')
        s = idx[lo:hi]
        fh.append(not ((wd[s] == r.wd) & (td[s] >= 0.3) & tc[s]).any())
    f['first_hit'] = fh
    res[nm] = f
    P(f'\n{nm}: fills {len(f)} ({len(f) / 18:.1f}/day), first-hit share {f.first_hit.mean():.2f}')
    P('   R   :', rep(f, 'R'))
    if f.first_hit.sum() >= 3:
        x = f[f.first_hit]; P('   R first-hit fills only: n=%d mean %+.3f med %+.3f CIw %s' % (len(x), x.R.mean(), x.R.median(), cboot(x.R, x.whale)))
        y = f[~f.first_hit]; P('   R continuation fills only: n=%d mean %+.3f med %+.3f CIw %s' % (len(y), y.R.mean(), y.R.median(), cboot(y.R, y.whale)))
out.close()

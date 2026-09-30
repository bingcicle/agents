# (4b) Always-on pre-arming for serial wallets: P&L of FIRST hits (first big sweep of a burst printing >1% beyond fair)
# by wallets with a walk-forward serial score, vs all other first hits. This is what a standing order would collect.
from core import *
exec(open(W + 'a2_serial.py').read().split("real = lambda")[0].replace("out = open(W + 'a2_serial_results.txt', 'w')", "out = open(W + 'a2_serial2_results.txt', 'w')"))
def fills_from(ix, X=1.0):
    f = A.loc[ix, ['addr', 'coin', 'day', 'week', 'test', 'ts', 'wd', 'F', 'bas', 'exc_pl', 'exc_pf', 'td', 'tx_usd', 'pf', 'pl', 'b-1', 'b5', 'b10', 'b30', 'b60', 'b120', 'b300', 'b600', 'sym', 'liq', 'ser', 'ser_n']].reset_index(drop=True)
    f['L'] = f.F * (1 + f.wd * X / 100)
    fr = ((f.exc_pl - X) / (f.exc_pl - f.exc_pf).clip(lower=1e-9)).clip(0, 1); fr[f.exc_pl - f.exc_pf <= 1e-9] = 1.0
    f['cap'] = f.tx_usd * fr; f['whale'] = f.addr; f['side'] = np.where(f.wd < 0, 'LONG', 'SHORT')
    return add_epi(f)
fh = ep[ep.exc_pl > 1.0]
f = fills_from(fh.index)
f['R'] = exit_sim(f, S=5000, Hs=60, d=0.1).pnl.values; f['P60'] = g(f, 60)
P('\nFIRST-HIT fills (first big sweep of a burst beyond 1%%): n=%d' % len(f))
P('  all:', rep(f, 'R'))
for thr in (0.2, 0.3, 0.4):
    m = (f.ser >= thr) & (f.ser_n >= 2)
    P(f'  wallet score>={thr} (walk-forward, n_past>=2): n={m.sum()}', rep(f[m], 'R') if m.sum() >= 3 else '')
    P(f'  others:', rep(f[~m], 'R'))
out.close()

# Non-parametric generic-reversal control for S_DISTRESS L24 fad: for every trade find the 100 nearest (symbol, hour) points
# of an all-symbol hourly panel (same period, any Binance symbol ex BTC) in (past 24h, 72h, 7d market-neutral return, OUR-side sign)
# and compare the trade's 24h mn outcome with the neighbours' 24h mn outcome (same side convention). Output appended to out_distress_robust.txt
import sys, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
import ws
from ws import *
P_ = np.load(W + 'pxmat.npz'); mins = P_['mins']; C = P_['C']; syms = list(P_['syms']); A = P_['altidx']
S = pd.read_parquet(W + 'sim_events_out.parquet'); S['cd'] = S.coin + '|' + S.day
ws.STATES['S_DISTRESS'] = lambda d: ((d.wpnl < 0) & (d.liq_dist < 5), np.ones(len(d), bool))
out = open(W + 'out_distress_robust.txt', 'a')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n'); out.flush()
# panel: every 60 min, every symbol except BTC; returns vs alt index
hrs = np.arange(60 * 24 * 8, C.shape[1] - 60 * 24 - 2, 60)
rows = []
for i, s in enumerate(syms):
    if s == 'BTCUSDT': continue
    c = C[i]
    def mn(j0, j1): return 100 * ((c[j1] / c[j0] - 1) - (A[j1 + 1] / A[j0 + 1] - 1))
    rows.append(pd.DataFrame({'sym': s, 'j': hrs, 'p24': mn(hrs - 1440, hrs), 'p72': mn(hrs - 4320, hrs), 'p7d': mn(hrs - 10080, hrs),
                              'f24': mn(hrs + 1, hrs + 1 + 1440)}))
PN = pd.concat(rows).dropna(); PN['t'] = mins[PN.j.values]
P(f'\n==== NN generic-reversal control (panel {len(PN)} symbol-hours, {PN.sym.nunique()} symbols)')
for per, d in (('DISC', S[S.day <= DISC_END]), ('TEST', S[(S.day >= TEST_START) & (S.day <= TEST_END)])):
    d = d.reset_index(drop=True)
    x = cell_frame(d, 'S_DISTRESS', 'L24', 'fad')
    side = -x.wdir.values
    # past_mn_* are in whale dir; our side = -wdir -> our-side past = side * wdir * past = -past
    q = np.c_[-x.past_mn_24h.values, -x.past_mn_72h.values, -x.past_mn_7d.values]
    pn = PN[(PN.t >= d.dec_ms.min() - 86400e3) & (PN.t <= d.dec_ms.max())]
    ctrl = []
    for k in range(len(x)):
        # neighbour in "long" and "short" orientation: for our side s, the panel point's our-side past = s * p ; outcome = s * f24
        s_ = side[k]
        Z = np.c_[s_ * pn.p24.values, s_ * pn.p72.values, s_ * pn.p7d.values]
        sc = np.array([3.0, 6.0, 12.0])
        dist = (((Z - q[k]) / sc) ** 2).sum(1)
        nn = np.argsort(dist)[:100]
        ctrl.append(np.mean(s_ * pn.f24.values[nn]) - 0.146)
    ctrl = np.array(ctrl)
    ex = x.y.values - ctrl
    x2 = x.assign(y=ex)
    o = battery(x2)
    P(f'  {per}: trade mean {x.y.mean():+.3f} | NN-control mean {ctrl.mean():+.3f} | excess mean {o["mean"]:+.3f} med {o["median"]:+.3f} ciC={o["ci_c"]} ciD={o["ci_d"]} ciW={o["ci_w"]} h1/h2={o["half1"]:+.2f}/{o["half2"]:+.2f}')

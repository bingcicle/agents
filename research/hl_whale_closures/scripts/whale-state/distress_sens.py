# Post-test SENSITIVITY (not selection) of S_DISTRESS thresholds; each row reported for DISC and TEST. K += 8.
import sys, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
import ws
from ws import *
S = pd.read_parquet(W + 'sim_events_out.parquet'); S['cd'] = S.coin + '|' + S.day
out = open(W + 'out_distress_robust.txt', 'a')
def P(*a):
    s = ' '.join(str(x) for x in a); print(s); out.write(s + '\n'); out.flush()
P('\n==== threshold sensitivity (L24 fad, mn net) DISC | TEST')
for lab, f in [('pnl<0 & liq<5 (declared)', lambda d: (d.wpnl < 0) & (d.liq_dist < 5)),
               ('pnl<0 & liq<3', lambda d: (d.wpnl < 0) & (d.liq_dist < 3)),
               ('pnl<0 & liq<10', lambda d: (d.wpnl < 0) & (d.liq_dist < 10)),
               ('pnl<-3 & liq<5', lambda d: (d.wpnl < -3) & (d.liq_dist < 5)),
               ('pnl<-10 & liq<5', lambda d: (d.wpnl < -10) & (d.liq_dist < 5)),
               ('pnl>=0 & liq<5', lambda d: (d.wpnl >= 0) & (d.liq_dist < 5)),
               ('pnl<0 & liq>=5', lambda d: (d.wpnl < 0) & (d.liq_dist >= 5)),
               ('pnl<0 & liq nan (no liq px)', lambda d: (d.wpnl < 0) & d.liq_dist.isna()),
               ('pnl<0 & liq<5, whale buys only', lambda d: (d.wpnl < 0) & (d.liq_dist < 5) & (d.wdir == 1)),
               ('pnl<0 & liq<5, whale sells only', lambda d: (d.wpnl < 0) & (d.liq_dist < 5) & (d.wdir == -1))]:
    ws.STATES['X'] = (lambda f: (lambda d: (f(d).values, np.ones(len(d), bool))))(f)
    s = []
    for d in (S[S.day <= DISC_END], S[(S.day >= TEST_START) & (S.day <= TEST_END)]):
        x = cell_frame(d.reset_index(drop=True), 'X', 'L24', 'fad'); o = battery(x)
        s.append(f"n={o['n']:4d} mean={o.get('mean',np.nan):+.2f} med={o.get('median',np.nan):+.2f} ciC={o.get('ci_c')} ciD={o.get('ci_d')}")
    P(f'  {lab:32s} DISC {s[0]} | TEST {s[1]}')

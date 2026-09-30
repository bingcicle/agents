"""Sensitivity: HEAD vs verified&non-null (ALL), costs 0.146 (official) vs 0.10 taker vs 0.04 maker, tape-only gross-costs."""
import sys
import numpy as np, pandas as pd
W = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle/'
sys.path.insert(0, W)
from st import END_LOCAL
a = pd.read_parquet(W + 'trades.parquet')
a['vnn'] = a.vf & a.net.notna()
out = []
def P(*s):
    line = ' '.join(str(x) for x in s); print(line); out.append(line)

D = lambda s: pd.Timestamp(s)
REG17 = D('2026-09-17 10:32')
T = a[a.family == 'T']
F = a[a.family == 'F']
R = a[a.family == 'R']
f1 = F[F.card == 'F1_1хв']
twd = pd.read_parquet(W + 'twap_with_drift.parquet')[['key', 'frac']]
T = T.merge(twd, on='key', how='left')
H = {
    'T2 OOS (>=17.09 10:32)': (T[(T.card == 'T2_твап_скорочення') & (T.dt >= REG17)], REG17),
    'T2 EVAL (>=13.09)': (T[(T.card == 'T2_твап_скорочення') & (T.dt >= D('2026-09-13'))], D('2026-09-13')),
    'T2_15 OOS': (T[(T.card == 'T2_твап_скорочення_15') & (T.dt >= REG17)], REG17),
    'T2_20 OOS': (T[(T.card == 'T2_твап_скорочення_20') & (T.dt >= REG17)], REG17),
    'T2 >=90% OOS': (T[(T.card == 'T2_твап_скорочення') & (T.dt >= REG17) & (T.frac >= 0.9)], REG17),
    'T1 OOS': (T[(T.card == 'T1_твап_відкриття') & (T.dt >= REG17)], REG17),
    'T1_15 OOS': (T[(T.card == 'T1_твап_відкриття_15') & (T.dt >= REG17)], REG17),
    'T1_20 OOS': (T[(T.card == 'T1_твап_відкриття_20') & (T.dt >= REG17)], REG17),
    'F4 >=18.09': (F[(F.card == 'F4_розумний') & (F.dt >= D('2026-09-18'))], D('2026-09-18')),
    'F1-PROF >=18.09': (f1[(f1.dt >= D('2026-09-18')) & (f1.prof_status == 'ok') & (f1.tx_pct < 20) & (f1.lag_s < 2)], D('2026-09-18')),
    'V9 >=18.09': (f1[(f1.dt >= D('2026-09-18')) & (f1.prof_status == 'ok') & (f1.tx_pct < 20) & (f1.lag_s < 2) & (f1.prof_cont_pct >= 30)], D('2026-09-18')),
    'ok&lag<=1.8 >=17.09 10:32': (f1[(f1.dt >= REG17) & (f1.prof_status == 'ok') & (f1.lag_s <= 1.8)], REG17),
    'F1 base >=20.09': (f1[f1.dt >= D('2026-09-20')], D('2026-09-20')),
    'pair 0x0871deb3/VVV F1 >=20.09': (f1[(f1.dt >= D('2026-09-20')) & f1.wallet.str.startswith('0x0871deb3') & (f1.coin == 'VVV')], D('2026-09-20')),
    'pair 0x523852be/CHIP F1 >=20.09': (f1[(f1.dt >= D('2026-09-20')) & f1.wallet.str.startswith('0x523852be') & (f1.coin == 'CHIP')], D('2026-09-20')),
    'R1 >=19.09': (R[(R.card == 'R1_загальний') & (R.dt >= D('2026-09-19'))], D('2026-09-19')),
    'R4 >=19.09': (R[(R.card == 'R4_великий') & (R.dt >= D('2026-09-19'))], D('2026-09-19')),
    'F9 >=20.09': (F[(F.card == 'F9_без_ратіо_90') & (F.dt >= D('2026-09-20'))], D('2026-09-20')),
}
P('hyp | set | n | median/mean official | cost0.10 | maker0.04 | tape-only(gross-costs) | $1000/month official | $/month @0.10')
for k, (d, start) in H.items():
    days = (END_LOCAL - start).total_seconds() / 86400
    for nm, dd in [('HEAD', d[d.hd]), ('VERIFIED', d[d.vnn])]:
        x = dd.net
        if len(x) == 0:
            P(f'{k} | {nm} | 0'); continue
        c = dd.costs.fillna(0.146)
        x10 = x + c - 0.10; x04 = x + c - 0.04; xt = dd.gross_tape - c
        P(f'{k} | {nm} | {len(x)} | {x.median():+.3f}/{x.mean():+.3f} | {x10.median():+.3f}/{x10.mean():+.3f} | {x04.median():+.3f}/{x04.mean():+.3f} | '
          f'{xt.median():+.3f}/{xt.mean():+.3f} | {x.sum()*10/days*30:+.0f} | {x10.sum()*10/days*30:+.0f}')
open(W + 'out_h4_sens.txt', 'w').write('\n'.join(out))

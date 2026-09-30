import numpy as np, pandas as pd
from ev import *
from scipy import stats
BT=pd.read_parquet(MY+'ev_tx.parquet'); BG=pd.read_parquet(MY+'ev_trig.parquet'); BB=pd.read_parquet(MY+'ev_bat.parquet')
cols=['addr','coin','dec_ms','side','maxval','src']
sets={'tx_only':union([BT[cols]]),'trig_only':union([BG[cols]]),'bat_only':union([BB[cols]]),
      'union_tx+trig':union([BT[cols],BG[cols]]),'union_all3':union([BT[cols],BG[cols],BB[cols]]),
      'union_maxval>=300k':union([BT[BT.maxval>=300e3][cols],BG[BG.maxval>=300e3][cols]]),
      'union_dedupe_coin24h':union([BT[cols],BG[cols]],gap_h=24,key=('coin',))}
out=open(MY+'out_b_defs.txt','w')
def pr(s): print(s); out.write(s+'\n')
for nm,S in sets.items():
    E=attach(S,hs=(4,8,12,24,48),beta=False)
    pr(f'\n### {nm}: n={len(E)} {E.split.value_counts().to_dict()}')
    for h in (8,12,24):
        s=f' h={h:2d}'
        for sp in ('early','disc','test'):
            v=E[E.split==sp].dropna(subset=[f'mnA{h}'])
            if len(v)<8: s+=f' | {sp}: n={len(v)}'; continue
            x=v[f'mnA{h}'].values; nm_=placebo(v,h,'mnA',nd=150)
            s+=f' | {sp}: n={len(x)} mean={x.mean():+.2f} med={np.median(x):+.2f} P={((nm_>=x.mean()).mean()):.2f}'
        pr(s)
# delay and horizon profile for union
E=pd.read_parquet(MY+'U.parquet')[cols]
pr('\n### union: entry delay sensitivity (mnA mean by split) h=12 / h=24')
for d in (60,300,900,1800,3600,4*3600):
    F=attach(E,hs=(12,24),delay_s=d,beta=False)
    pr(f' delay {d/60:5.0f} min: '+' '.join(f'{sp}:{F[F.split==sp].mnA12.mean():+.2f}/{F[F.split==sp].mnA24.mean():+.2f}' for sp in ('early','disc','test')))
pr('\n### union horizon profile (mean / median mnA)')
F=attach(E,hs=(1,2,4,6,8,12,16,24,36,48,72),beta=False)
for sp in ('early','disc','test'):
    v=F[F.split==sp]
    pr(f' {sp}: '+' '.join(f'{h}h:{v[f"mnA{h}"].mean():+.2f}/{v[f"mnA{h}"].median():+.2f}' for h in (1,2,4,6,8,12,16,24,36,48,72)))
out.close()

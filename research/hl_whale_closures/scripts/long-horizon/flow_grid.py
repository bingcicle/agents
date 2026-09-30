import numpy as np, pandas as pd, sys
from lh import *
P=pd.read_parquet(W+'flow_panel.parquet')
P['fd']=P.f24/P.dep; P['fq']=P.f24/P.qv24.clip(lower=1)
out=open(W+'out_flow_grid.txt','w')
def pr(s): print(s,flush=True); out.write(s+'\n')
pr('FLOW-FOLLOW grid (all 24 decision hours pooled, overlapping; per coin-decision mean of side*mnA; CI by day cluster). K counted.')
K=0
for norm,thrs in (('fd',(0,0.1,0.3,1,3)),('fq',(0,0.001,0.003,0.01,0.03))):
    for thr in thrs:
        for H in (8,24,48):
            K+=1
            line=f'{norm} thr={thr:<6} H={H:2d} |'
            for split in ('disc','test'):
                v=P[(P.split==split)&(P[norm].abs()>thr)&(P.f24!=0)].dropna(subset=[f'mnA{H}',norm])
                x=np.sign(v[norm])*v[f'mnA{H}']
                lo,hi,_=cluster_ci(x.values,v.day.values,n=400)
                lo2,hi2,_=cluster_ci(x.values,v.coin.values,n=400)
                buy=x[v[norm]>0]; sell=x[v[norm]<0]
                line+=f' {split}: n={len(v)} evd/day={len(v)/v.day.nunique()/24:.1f} mean={x.mean():+.3f} med={x.median():+.3f} CIday[{lo:+.2f},{hi:+.2f}] CIcoin[{lo2:+.2f},{hi2:+.2f}] buy={buy.mean():+.2f}({len(buy)}) sell={sell.mean():+.2f}({len(sell)}) |'
            pr(line)
pr(f'K={K}')

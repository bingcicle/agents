"""Simple portfolio view of B12 (fade after whale full close, hold 12h, hedged vs EW alt index), equal notional per event.
Daily PnL (by entry day) in units of '% of one trade notional'; concurrency; drawdown. Also without hedge (raw)."""
import numpy as np, pandas as pd
from lh import *
B=pd.read_parquet(W+'b_union_final.parquet').dropna(subset=['mnA12']).sort_values('entry_ms')
B['net']=B.mnA12-0.146-B.fund12.fillna(0); B['netraw']=B.raw12-0.146-B.fund12.fillna(0)
out=open(W+'out_B_sim.txt','w')
def pr(x): print(x); out.write(x+'\n')
for col in ('net','netraw'):
    d=B.groupby('day')[col].sum()
    alld=pd.Series(0.0,index=sorted(set(B.day)))
    cum=d.cumsum(); dd=(cum-cum.cummax()).min()
    pr(f'{col}: trades={len(B)} days={len(d)} trades/day={len(B)/len(d):.1f} | total={d.sum():+.1f} (units=% of 1 trade notional) | mean/day={d.mean():+.2f} | positive days={np.mean(d>0):.2f} | maxDD={dd:+.1f} | worst day={d.min():+.1f} best day={d.max():+.1f}')
    pr('   per split total: '+str(B.groupby('split')[col].sum().round(1).to_dict())+' per-trade mean: '+str(B.groupby('split')[col].mean().round(3).to_dict()))
# concurrency
t=np.sort(np.concatenate([B.entry_ms.values,B.entry_ms.values+12*3600e3])); s=np.concatenate([np.ones(len(B)),-np.ones(len(B))])[np.argsort(np.concatenate([B.entry_ms.values,B.entry_ms.values+12*3600e3]))]
pr(f'max concurrent positions={int(np.cumsum(s).max())}, median concurrent={np.median(np.cumsum(s)):.0f}')
out.close()

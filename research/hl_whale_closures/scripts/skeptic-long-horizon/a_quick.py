import numpy as np, pandas as pd
from ev import *
t=pd.read_parquet(SP+'data/whale_txs.parquet')
t=t[t.dir.isin(['Close Long','Close Short'])].sort_values(['addr','coin','ts']).reset_index(drop=True)
D0=t.ts.min(); GAP=6*3600e3; rows=[]
for (a,c),d in t.groupby(['addr','coin'],sort=False):
    ts=d.ts.values; usd=d.tx_usd.values; pct=d.tx_pct.values; sigts=ts[usd>=1000]
    for i in np.where((pct>=5)&(usd>=5000))[0]:
        if ts[i]<D0+GAP: continue
        k=np.searchsorted(sigts,ts[i])
        if k>0 and ts[i]-sigts[k-1]<GAP: continue
        rows.append(dict(addr=a,coin=c,dec_ms=int(d.bot_ts.values[i]*1000),side=(1 if d.wside.values[i]=='SHORT' else -1),ratio=d.batch_ratio.values[i]))
A=attach(pd.DataFrame(rows),hs=(8,24,48),beta=False)
for h in (8,24,48):
    s=f'h={h}'
    for sp in ('disc','test'):
        v=A[A.split==sp].dropna(subset=[f'mnA{h}'])
        s+=f' | {sp} n={len(v)} mnA={v[f"mnA{h}"].mean():+.2f} med={v[f"mnA{h}"].median():+.2f} L={v[v.side==1][f"mnA{h}"].mean():+.2f}(n{(v.side==1).sum()}) S={v[v.side==-1][f"mnA{h}"].mean():+.2f}'
        w=v[v.ratio>=5]; s+=f' r>=5:{w[f"mnA{h}"].mean():+.2f}(n{len(w)})'
    print(s)

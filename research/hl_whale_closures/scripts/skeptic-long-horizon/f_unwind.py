"""Structural fact: unwind speed vs ratio. Episode start = close tx with tx_pct>=5 and tx_usd>=5k, no close tx >=$1k of the same
(addr,coin) in the previous 6h, start >= data start + 6h. closed share at H = 1 - (position after last close tx <= t0+H)/sp0,
clipped to [0,1] - does NOT stop at 6h gaps (unlike the lens). Right-censoring: only starts with t0+24h <= data end."""
import numpy as np, pandas as pd
from px import *
t=pd.read_parquet(SP+'data/whale_txs.parquet')
t=t[t.dir.isin(['Close Long','Close Short','Long > Short','Short > Long'])].copy()
t['pa']=np.where(t.dir.str.contains('>'),0.0,np.maximum(t.sp-t.sz,0))
t=t.sort_values(['addr','coin','ts']).reset_index(drop=True)
D0=t.ts.min(); D1=t.ts.max(); GAP=6*3600e3
rows=[]
for (a,c),d in t.groupby(['addr','coin'],sort=False):
    ts=d.ts.values; usd=d.tx_usd.values; pct=d.tx_pct.values; pa=d.pa.values; sp=d.sp.values; rat=d.batch_ratio.values
    sigts=ts[usd>=1000]
    for i in np.where((pct>=5)&(usd>=5000))[0]:
        if ts[i]<D0+GAP: continue
        k=np.searchsorted(sigts,ts[i])   # sig txs strictly before
        if k>0 and ts[i]-sigts[k-1]<GAP: continue
        r=dict(addr=a,coin=c,ts0=ts[i],ratio=rat[i],sp0=sp[i],val0=sp[i]*d.px.values[i])
        for H in (1,4,24):
            e=np.searchsorted(ts,ts[i]+H*3600e3,side='right')-1
            r[f'cl{H}']=float(np.clip(1-pa[e]/sp[i],0,1))
            r[f'ok{H}']=ts[i]+H*3600e3<=D1
        rows.append(r)
E=pd.DataFrame(rows); print('episodes',len(E),'censored24',(~E.ok24).sum())
E=E[E.ts0<pd.Timestamp('2026-09-30').value//10**6]
E['rb']=pd.cut(E.ratio,[0,2,3,5,10,1e9],labels=['<=2','2-3','3-5','5-10','>10'])
g=E[E.ok24].groupby('rb',observed=True).agg(n=('cl1','size'),cl1_med=('cl1','median'),cl1_mean=('cl1','mean'),cl24_med=('cl24','median'),cl24_mean=('cl24','mean'),val_med=('val0','median'))
print(g.round(2).to_string())
# control for position size: within val0 terciles
E['vb']=pd.qcut(E.val0,3,labels=['small','mid','big'])
print(E[E.ok24].groupby(['vb','rb'],observed=True).cl24.agg(['size','median','mean']).round(2).unstack(0).to_string())
from scipy.stats import spearmanr
v=E[E.ok24]; print('spearman ratio vs cl24',spearmanr(v.ratio,v.cl24)); print('spearman val0 vs cl24',spearmanr(v.val0,v.cl24))

"""Tradable C rule chosen on discovery IC: FLOW4 - every hour, coins with net whale close flow in last 4h (bot_ts-known) with
|flow4/BinanceQV4| >= thr: trade in flow direction (whale buying -> LONG), hold 24h, market-neutral vs EW alt index.
Non-overlap version: per coin, no new entry while a position in that coin is open (24h)."""
import numpy as np, pandas as pd
from lh import *
P=pd.read_parquet(W+'flow_panel.parquet')
P['fq4']=P.f4/P.qv4.clip(lower=1)
out=open(W+'out_flow4_rule.txt','w')
def pr(x): print(x); out.write(x+'\n')
for thr in (0,0.003,0.01,0.03):
    v=P[(P.f4!=0)&(P.fq4.abs()>=thr)].dropna(subset=['mnA24']).copy()
    v['side']=np.sign(v.f4); v['x']=v.side*v.mnA24; v['xr']=v.side*v.raw24
    # non-overlapping per coin
    v=v.sort_values('Dm'); keep=[]; last={}
    for i,r in zip(v.index,v.itertuples()):
        if r.coin in last and r.Dm<last[r.coin]+24*3.6e6: continue
        last[r.coin]=r.Dm; keep.append(i)
    no=v.loc[keep]
    for split in ('disc','test'):
        a=v[v.split==split]; b=no[no.split==split]
        lo,hi,_=cluster_ci(b.x.values,b.coin.values,n=500); lo2,hi2,_=cluster_ci(b.x.values,b.day.values,n=500)
        c=b.groupby('coin').x.sum().sort_values(); 
        pr(f'thr={thr:<6} [{split}] all-hours n={len(a)} mean={a.x.mean():+.3f} | NON-OVERLAP n={len(b)} mean mnA={b.x.mean():+.3f} med={b.x.median():+.3f} win={100*(b.x>0).mean():.0f}% CIcoin[{lo:+.2f},{hi:+.2f}] CIday[{lo2:+.2f},{hi2:+.2f}] '
           f'net(mnA-0.146)={b.x.mean()-0.146:+.3f} raw={b.xr.mean():+.3f} | L n={int((b.side>0).sum())} {b[b.side>0].x.mean():+.2f} S n={int((b.side<0).sum())} {b[b.side<0].x.mean():+.2f} | w/o top3 coins={b[~b.coin.isin(c.index[-3:])].x.mean():+.3f}')
out.close()

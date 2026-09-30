"""Mechanism A: unwind pressure. Event = episode start (6h-gap, material). Side = whale trade direction."""
import sys; import numpy as np, pandas as pd
from lh import *; from evalx import *
E=pd.read_parquet(W+'episodes.parquet')
t=pd.read_parquet(W+'txs_slim.parquet')
# forward closing regardless of gaps: fraction of sp0 closed by same (addr,coin) within X h after start (incl start tx)
t=t.sort_values('ts'); grp={k:(d.ts.values,np.cumsum(d.tx_usd.values/d.px.values*0+d.sz.values)) for k,d in t.groupby(['addr','coin'])}
for H in (1,4,24,72):
    v=[]
    for r in E.itertuples():
        ts,cs=grp[(r.addr,r.coin)]; a=np.searchsorted(ts,r.ts0,'left'); b=np.searchsorted(ts,r.ts0+H*3.6e6,'right')
        v.append((cs[b-1]-(cs[a-1] if a>0 else 0))/r.sp0)
    E[f'fwd_closed_{H}h']=np.minimum(v,5)
E['rbin']=pd.cut(E.ratio0,[0,2,3,5,10,1000])
print(E.groupby('rbin',observed=True)[['tx_pct0','fwd_closed_1h','fwd_closed_4h','fwd_closed_24h','fwd_closed_72h','n_tx','dur_h']].agg(['median','mean']).round(2).to_string())
print(E.groupby('rbin',observed=True).size())
print('side mix', E.wdir.value_counts().to_dict())
E['dec_ms']=E.bot0; E['side']=E.wdir
ev=attach(E[~E.censored])
ev.drop(columns=['rbin']).to_parquet(W+'a_events.parquet')
out=W+'out_A.txt'; open(out,'w').write('Mechanism A: follow whale trade direction from episode start (bot_ts+1min), 6h-gap episodes 12.09-29.09\n')
report(ev,'A all episodes',out=out)
report(ev[ev.ratio0>=5],'A ratio0>=5',out=out)
report(ev[(ev.ratio0>=3)&(ev.tx_pct0<50)],'A ratio0>=3 & tx_pct0<50 (partial start, big overhang)',out=out)

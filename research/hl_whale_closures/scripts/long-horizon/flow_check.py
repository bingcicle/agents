import numpy as np, pandas as pd
from lh import *
P=pd.read_parquet(W+'flow_panel.parquet')
P['fd']=P.f24/P.dep; P['fq']=P.f24/P.qv24.clip(lower=1)
for norm,thr,H in (('fq',0.01,24),('fd',3,24),('fd',0.3,24)):
    print(f'\n##### {norm}>={thr} H={H}')
    for split in ('disc','test'):
        v=P[(P.split==split)&(P[norm].abs()>thr)&(P.f24!=0)].dropna(subset=[f'mnA{H}']).copy()
        v['x']=np.sign(v[norm])*v[f'mnA{H}']
        c=v.groupby('coin').x.agg(['size','mean','sum']).sort_values('sum')
        pos=c['sum'][c['sum']>0]
        print(f'[{split}] coins={len(c)} top contributors:', c.tail(4).round(1).to_dict('index'), '| worst:', c.head(3).round(1).to_dict('index'))
        print(f'   sum={v.x.sum():.0f} max_share_coin={pos.max()/pos.sum():.2f}; mean w/o top coin={v[v.coin!=c.index[-1]].x.mean():+.3f}; w/o top3 coins={v[~v.coin.isin(c.index[-3:])].x.mean():+.3f}; coin-level mean of means={c["mean"].mean():+.3f}, share coins>0={np.mean(c["mean"]>0):.2f}')
        d=v.groupby('day').x.mean(); print('   per-day means:', d.round(2).to_dict())
        # per decision-hour-of-day
        hh=v.groupby('hod').x.mean(); print('   by hour-of-day decision: pos share', np.mean(hh>0).round(2), 'min/max', hh.min().round(2), hh.max().round(2))

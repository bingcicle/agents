import pandas as pd, numpy as np
from stats import *
T=prep(pd.read_parquet('trades_grid.parquet'))
rows=[]
for (f,L,H),d in T.groupby(['fam','L','H'],sort=False):
    for per in ('TRAIN','TEST'):
        x=d[d.per==per]; s=summ(x)
        s.update(fam=f,L=L,H=H,per=per,btc=round(x.y_btc.mean(),2),raw=round(x.y_raw.mean(),2),nf=round(x.y_nf.mean(),2),
                 fund=round(x.f0.mean(),3),fnan=round(x.fund.isna().mean(),2))
        mm=d.groupby('month').y.mean(); s['months']=' '.join(f'{m}:{v:+.1f}' for m,v in mm.items()) if per=='TEST' else ''
        s['mpos']=int((mm>0).sum())
        rows.append(s)
R=pd.DataFrame(rows); R.to_csv('grid_results.csv',index=False)
pd.set_option('display.width',250); pd.set_option('display.max_columns',30); pd.set_option('display.max_rows',200)
tr=R[R.per=='TRAIN'].set_index(['fam','L','H']); te=R[R.per=='TEST'].set_index(['fam','L','H'])
J=tr[['n','mean','med','ciC','ciW','btc','raw','fund']].join(te[['n','mean','med','ciC','ciW','wo10','btc','raw','months','mpos']],lsuffix='_tr',rsuffix='_te')
print(J.to_string())

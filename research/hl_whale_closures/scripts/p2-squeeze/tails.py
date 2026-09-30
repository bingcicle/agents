import pandas as pd, numpy as np
from stats import *
T=prep(pd.read_parquet('trades_grid.parquet'))
S=T[T.fam=='SQZ']
rows=[]
for (L,H),d in S.groupby(['L','H']):
    for per in ('TRAIN','TEST','ALL'):
        x=d if per=='ALL' else d[d.per==per]
        wk=x.groupby('week').y.sum()
        rows.append(dict(L=L,H=H,per=per,n=len(x),mean=x.y.mean().round(2),sl10=x.y_sl10.mean().round(2),sl20=x.y_sl20.mean().round(2),
            hit10=x.hit10.mean().round(2),hit20=x.hit20.mean().round(2),worst=x.y.min().round(1),q01=np.quantile(x.y,.01).round(1),
            worst_sl20=x.y_sl20.min().round(1),mae_q90=np.quantile(x.mae,.9).round(1),mae_max=x.mae.max().round(0),
            worst_week=wk.min().round(1),worst_week_mean=x.groupby('week').y.mean().min().round(1),
            slip=x.slip.mean().round(2),qv24_med=round(x.qv24.median()/1e6,1),qv24_q10=round(x.qv24.quantile(.1)/1e6,1),
            fnan=x.fund.isna().mean().round(2),fund=x.f0.mean().round(2),topcoin=summ(x)['topcoin_share'] if len(x)>3 else np.nan,
            ncoin=x.sym.nunique(), mean_wo_fund=x.y_nf.mean().round(2)))
R=pd.DataFrame(rows); pd.set_option('display.width',250); pd.set_option('display.max_columns',40)
print(R.to_string()); R.to_csv('sqz_tails.csv',index=False)
# which coins dominate
for per in ('TRAIN','TEST'):
    x=S[(S.L==12)&(S.H==24)&(S.per==per)]
    print(per,'L12H24 by coin top/bottom', x.groupby('sym').y.agg(['count','sum']).sort_values('sum').iloc[[0,1,2,-3,-2,-1]].round(1).to_dict('index'))

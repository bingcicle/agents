import pandas as pd,numpy as np
from st2 import *
D=pd.read_parquet('liqdump_L24H24.parquet')
for p,x in D.groupby('per'):
    print(p,'n',len(x),'worst',x.y.min().round(1),'maeq90',x.mae.quantile(.9).round(1),'maemax',x.mae.max().round(1),
      'sl10',x.sl10.mean().round(2)-0.246,'sl20',x.sl20.mean().round(2)-0.246,'slipL mean',x.slipL.mean().round(2),'qv24 med $M',(x.qv24.median()/1e6).round(1),'q10',(x.qv24.quantile(.1)/1e6).round(1))
    ds=x.groupby('day').y.sum().sort_values(); print('  top days',ds.tail(4).round(1).to_dict(),' worst',ds.head(3).round(1).to_dict(),'ndays',len(ds))
    cs=x.groupby('sym').y.sum().sort_values(); print('  top coins',cs.tail(4).round(1).to_dict(),'ncoins',len(cs))
    w=x.groupby('week').y.sum(); print('  worst week',w.min().round(1),'weeks pos',(w>0).mean().round(2))
    print('  per day-count: trades per day max',x.groupby('day').size().max())
    # slippage-adjusted: entry at worst of entry minute for short (low)
    print('  y with entry slippage',(x.y-x.slipL).mean().round(2),' y with extra 0.2 cost thin (qv24<20M)', (x.y-0.2*(x.qv24<20e6)).mean().round(2))
    # remove top day
    top=ds.index[-1]; z=x[x.day!=top]; print('  without top day',round(z.y.mean(),2),cb(z.y,z.sym))

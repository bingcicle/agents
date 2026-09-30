from mybt import *; from st2 import *
import sys
D=add(run()); D.to_parquet(W+'liqdump_L24H24.parquet')
print('missing exit',D.miss.sum())
for p,x in D.groupby('per'): print(p,sm(x), 'btc',sm(x,'y_btc')['mean'],'raw',sm(x,'y_raw')['mean'],'md',sm(x,'y_md')['mean'],'nf',sm(x,'y_nf')['mean'],'fundpaid',round((x.fl).mean(),3))
print(D.groupby('month').y.agg(['count','mean']).round(2).T)

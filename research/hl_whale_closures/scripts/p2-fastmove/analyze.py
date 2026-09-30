import sys; sys.path.insert(0,'.')
from common import *
pd.set_option('display.width',250); pd.set_option('display.max_rows',200)
d=load()
rows=[]
for (c,p),g in d.groupby(['cell','per']):
    s=stats(g); s.update(cell=c,per=p,gross=g.gross.mean(),neutral_gross=g.neutral.mean(),nnet=g.nnet.mean()); rows.append(s)
T=pd.DataFrame(rows)
piv=T.pivot(index='cell',columns='per',values=['n','gross','mean','ci_coin','neutral_gross'])
print(piv.to_string(float_format=lambda v:f'{v:+.3f}'))
T.to_csv(W+'cells_train_test.csv',index=False)
tr=T[(T.per=='TRAIN')&(T.n>=200)]
tr=tr[tr.ci_coin.apply(lambda z:z[0])>0].sort_values('mean',ascending=False)
print('\nSELECTION (train n>=200 & coin CI lower>0):'); print(tr[['cell','n','mean','ci_coin','ci_week']].to_string())
# gross by month for A cells
print('\nGROSS mean by month (A cells, H60):')
print(d[d.cell.str.startswith('A_')].pivot_table(index='cell',columns='month',values='gross',aggfunc='mean').to_string(float_format=lambda v:f'{v:+.2f}'))
print('\nNEUTRAL gross by month:')
print(d[d.cell.str.startswith('A_')].pivot_table(index='cell',columns='month',values='neutral',aggfunc='mean').to_string(float_format=lambda v:f'{v:+.2f}'))

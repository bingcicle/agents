import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from mystats import *
f=pd.read_pickle('f.pkl')
d=f[(f.strategy=='F1_1хв')&(f['loc']>='2026-09-13')&(f['loc']<'2026-09-17 19:14')&f.hd]
print('F1 13-17.09 19:14 head n',len(d),'med',round(d.net.median(),4), ' verified n',((f.strategy=='F1_1хв')&(f['loc']>='2026-09-13')&(f['loc']<'2026-09-17 19:14')&f.vf).sum())
S='2026-09-20'; END=pd.Timestamp('2026-09-30 21:07:06'); days=(END-pd.Timestamp(S)).total_seconds()/86400
for c in ['F1_1хв','F2_2хв','F3_3хв','F6_1хв_перший','F8_ratio35','F10_розумний_60','F4_розумний','F7_без_ратіо','F5_перший']:
    x=f[(f.strategy==c)&(f['loc']>=S)&f.hd]
    lo,hi,_=cboot(x.net.values,x.day.values); lo2,hi2,_=cboot(x.net.values,x.whale_addr.values)
    xv=f[(f.strategy==c)&(f['loc']>=S)&f.vf]
    print(f'{c}: head n={len(x)} mean={x.net.mean():+.3f} med={x.net.median():+.3f} gross mean={x.gross.mean():+.3f} $/mo={x.net.sum()*10/days*30:+.0f} CIday [{lo:+.3f},{hi:+.3f}] CIwallet [{lo2:+.3f},{hi2:+.3f}] | verified n={len(xv)} mean={xv.net.mean():+.3f}')
# unique events
x=f[f.strategy.str.startswith('F')&(f['loc']>=S)&f.hd]
print('F head rows',len(x),'unique (wallet,coin,open_ts):',x[['whale_addr','coin','open_ts_ms']].drop_duplicates().shape[0])
# F1 dedup by episode: one per wallet x coin within 10 min
d=f[(f.strategy=='F1_1хв')&(f['loc']>=S)&f.hd].sort_values('open_ts_ms')
d['ep']=(d.groupby(['whale_addr','coin']).open_ts_ms.diff().fillna(1e12)>600000).astype(int).groupby([d.whale_addr,d.coin]).cumsum()
e=d.groupby(['whale_addr','coin','ep']).net.mean()
print('F1 episodes(10-min gap):',len(e),'mean of episode-mean',round(e.mean(),3),'median',round(e.median(),3))
# A_any: first F1 per wallet x coin x local day
d['dd']=d['loc'].dt.date
k=d.groupby(['whale_addr','coin','dd']).head(1)
rm=d.drop(k.index)
print('A_any: base n',len(d),'mean',round(d.net.mean(),3),' kept',len(k),round(k.net.mean(),3),' removed',len(rm),round(rm.net.mean(),3))
# paired by day delta
g=pd.DataFrame({'b':d.groupby('day').net.mean(),'k':k.groupby('day').net.mean()})
print('per-day delta kept-base mean',round((g.k-g.b).mean(),3), ' days kept better:',int(((g.k-g.b)>0).sum()),'/',len(g))

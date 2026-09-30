import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from mystats import *
pd.set_option('display.width',250)
f=pd.read_pickle('f.pkl')
END=pd.Timestamp('2026-09-30 21:07:06')
def rep(d,name,since,full=True):
    days=(END-pd.Timestamp(since)).total_seconds()/86400
    d=d.sort_values('open_ts_ms'); x=d.net.values; n=len(x)
    if n==0: print(name,0); return
    k=max(1,int(round(n*.1)))
    s=f'{name}: n={n} w={d.whale_addr.nunique()} coins={d.coin.nunique()} days={d.day.nunique()} med={np.median(x):+.3f} mean={x.mean():+.3f} win={np.mean(x>0):.2f} sum={x.sum():+.2f} $/mo={x.sum()*10/days*30:+.0f} woTop10={np.sort(x)[::-1][k:].sum():+.2f}'
    if n>=4 and full:
        h=n//2; s+=f' halves {x[:h].mean():+.3f}/{x[h:].mean():+.3f}'
        s+=f' CIday mean {np.round(cboot(x,d.day.values)[:2],3)} CIwallet mean {np.round(cboot(x,d.whale_addr.values)[:2],3)}'
    print(s)
W='0x0871deb3'
S='2026-09-20'
p=f[f.whale_addr.str.startswith(W)&(f.coin=='VVV')]
for c in ['F1_1хв','F2_2хв','F3_3хв','F6_1хв_перший','F4_розумний','F10_розумний_60']:
    rep(p[(p.strategy==c)&(p['loc']>=S)&p.hd],'VVV pair '+c+' HEAD since 20.09',S)
    rep(p[(p.strategy==c)&(p['loc']>=S)&p.vf],'VVV pair '+c+' VERIF since 20.09',S,False)
d=p[(p.strategy=='F1_1хв')&(p['loc']>=S)&p.hd].sort_values('open_ts_ms')
d['gap_min']=d.open_ts_ms.diff()/60000
print(d[['date_open','our_side','tx_pct_of_pos','lag_s','hold_s','exit_reason','net','gross','gap_min']].round(3).to_string())
# F1 pair history by period
for a,b in [('2026-09-01','2026-09-13'),('2026-09-13','2026-09-20'),('2026-09-20','2026-10-01')]:
    x=p[(p.strategy=='F1_1хв')&(p['loc']>=a)&(p['loc']<b)&p.hd]
    print(a,b,'F1 pair n',len(x),'mean',round(x.net.mean(),3),'med',round(x.net.median(),3))
# the pair's gross vs same wallet other coins
o=f[f.whale_addr.str.startswith(W)&(f.coin!='VVV')&(f.strategy=='F1_1хв')&(f['loc']>=S)&f.hd]
print('same wallet other coins F1 since 20.09: n',len(o),'mean',round(o.net.mean(),3), o.coin.value_counts().to_dict())
# all VVV F1 other wallets since 20.09
o=f[~f.whale_addr.str.startswith(W)&(f.coin=='VVV')&(f.strategy=='F1_1хв')&(f['loc']>=S)&f.hd]
print('VVV other wallets F1 since 20.09: n',len(o),'mean',round(o.net.mean(),3),'med',round(o.net.median(),3))
# null: how many (wallet,coin) pairs with >=8 F1 head trades since 20.09 have mean>= observed?
g=f[(f.strategy=='F1_1хв')&(f['loc']>=S)&f.hd].groupby(['whale_addr','coin']).net.agg(['size','mean','median'])
g=g[g['size']>=8]
print('pairs with >=8 F1 head trades since 20.09:',len(g),' with mean>=+0.187:',(g['mean']>=0.187).sum(),' mean>0:',(g['mean']>0).sum())
print(g.sort_values('mean',ascending=False).head(8).round(3))
# F9
for S2 in ['2026-09-20','2026-09-13']:
    rep(f[(f.strategy=='F9_без_ратіо_90')&(f['loc']>=S2)&f.hd],'F9 HEAD since '+S2,S2)
    rep(f[(f.strategy=='F9_без_ратіо_90')&(f['loc']>=S2)&f.vf],'F9 VERIF since '+S2,S2)
d=f[(f.strategy=='F9_без_ратіо_90')&(f['loc']>='2026-09-20')&f.hd]
print(d[['date_open','coin','our_side','whale_addr','net','prof_status','exit_reason']].assign(whale_addr=d.whale_addr.str[:10]).round(3).to_string())
# F9 events also in F1? paired difference
k=['whale_addr','coin','open_ts_ms']
m=d.merge(f[(f.strategy=='F1_1хв')&f.hd][k+['net']],on=k,how='left',suffixes=('','_f1'))
print('F9 events matched to F1 head:',m.net_f1.notna().sum(),' F9 mean',round(m.net.mean(),3),' F1 on same events mean',round(m.net_f1.mean(),3))

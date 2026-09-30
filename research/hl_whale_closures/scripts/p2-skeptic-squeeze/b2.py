import pandas as pd,numpy as np
from st2 import *
E=pd.read_parquet('simE.parquet'); E=E[E.dec<=pd.Timestamp('2026-09-28 23:57').value//10**6]
def dd(x,h=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+h*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
S=E[E.side=='LONG']
def line(tag,x):
    x=dd(x); s=sm(x)
    print(f"{tag:28s} n={s['n']:4d} mean={s['mean']:+.2f} med={s['med']:+.2f} ciC={s['ciC']} ciD={s['ciD']} wo10={s['wo10']} btc={x.y_btc.mean():+.2f} md={x.y_md.mean():+.2f} ncoin={x.sym.nunique()}")
    return x
print('--- threshold sweep (short whale in loss, liq<thr)')
for thr in (0.3,0.5,1,2,3,4,5,6,8,10,20,1e9):
    line(f'loss liq<{thr}',S[(S.wpnl<0)&(S.liq.fillna(1e9)<thr)])
line('loss liq NaN',S[(S.wpnl<0)&S.liq.isna()])
print('--- whale in PROFIT (placebo on whale state)')
for thr in (1,5,1e9): line(f'profit liq<{thr}',S[(S.wpnl>=0)&(S.liq.fillna(1e9)<thr)])
print('--- any short-closer, and any long-closer (short coin after whale sells)')
line('any short closer',S); line('long closer (SHORT dir)',E[E.side=='SHORT'])
D=line('D_ORIG',S[(S.wpnl<0)&(S.liq<5)])
D['half']=np.where(D.day<'0913','A','B')
for h,x in D.groupby('half'): print(' half',h,len(x),round(x.y.mean(),2),cb(x.y,x.sym))
for ex in (['NILUSDT'],['NILUSDT','CHIPUSDT']):
    x=D[~D.sym.isin(ex)]; print(' w/o',ex,len(x),round(x.y.mean(),2),cb(x.y,x.sym),cb(x.y,x.day))
x=D[D.day!='0928']; print(' w/o 28.09',len(x),round(x.y.mean(),2),cb(x.y,x.sym))
print(' by day top',D.groupby('day').y.sum().sort_values().tail(4).round(1).to_dict())
print(' ex24 median',D.ex24.median().round(2),' worst trade',D.y.min().round(1))
# pre-event state control: generic coin-hours with ex24 in same band, same period
E.to_csv('/dev/null')

import sys; sys.path.insert(0,'.')
from sk import *
ev=pd.read_parquet('u_events.parquet')
for lab,st,col,hours,sm in (('WDISTRESS fad24',ev.wpnl2<-3,'fad24',24,-1),('WPROFIT fol8',ev.wpnl2>3,'fol8',8,1),('WPROFIT fad24',ev.wpnl2>3,'fad24',24,-1),('ALL(known pnl) fad24',ev.wpnl2.notna(),'fad24',24,-1),('ALL(known pnl) fol8',ev.wpnl2.notna(),'fol8',8,1),('ALL fad24',ev.wdir==ev.wdir,'fad24',24,-1),('ALL fol8',ev.wdir==ev.wdir,'fol8',8,1)):
    print('==',lab)
    for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
        d=ev[st&(ev.day>=a)&(ev.day<=b)].copy()
        d['side']=sm*d.wdir; d['y']=d[col+'_ew']-COST-d[col+'_fund'].fillna(0)
        d=d[d.y.notna()]
        x=dedupe(d,hours)
        print(f'  {per} ', bat(x))
        print(f'       btc-hedged', bat(x.assign(y=x[col+'_btc']-COST-x[col+'_fund'].fillna(0)),short=True), '| raw', bat(x.assign(y=x[col+'_raw']-COST-x[col+'_fund'].fillna(0)),short=True))

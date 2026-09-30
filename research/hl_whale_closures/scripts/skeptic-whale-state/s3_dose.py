import sys; sys.path.insert(0,'.')
from sk import *
ep=pd.read_parquet('sim_ep_L24b.parquet')
ep['beta']=ep.beta.fillna(1.0); ep['yb']=ep.raw-ep.side*np.clip(ep.beta,0,3)*ep.ewret-COST-ep.fund.fillna(0)
bins=[0,0.5,1,2,3,5,10,1e9]
ep['lb']=pd.cut(ep.liq_dist,bins,right=False).astype(str)
for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
    d=ep[(ep.day>=a)&(ep.day<=b)&ep.y.notna()]
    print(f'== {per}: whale SHORT positions (our fade = SHORT), whale in LOSS, by liq_dist bin (FRACTION units: 1.0 = liq 100% away)')
    for lb,g in d[(d.sw==-1)&(d.wpnl<0)].groupby('lb'):
        x=dedupe(g,24); print(f'   liq {lb:14s}', bat(x,short=True), f' wallets {x.addr.nunique()}  top wallet share {x.addr.value_counts(normalize=True).iloc[0]:.2f}')
    print(f'   whale SHORT, in PROFIT, by liq bin')
    for lb,g in d[(d.sw==-1)&(d.wpnl>=0)].groupby('lb'):
        x=dedupe(g,24); print(f'   liq {lb:14s}', bat(x,short=True))
    print('   whale SHORT & liq<5, by pnl bin')
    for lab,m in (('<-10',d.wpnl<-10),('-10..-3',(d.wpnl>=-10)&(d.wpnl<-3)),('-3..0',(d.wpnl>=-3)&(d.wpnl<0)),('0..3',(d.wpnl>=0)&(d.wpnl<3)),('>=3',d.wpnl>=3)):
        x=dedupe(d[(d.sw==-1)&(d.liq_dist<5)&m],24); print(f'   pnl {lab:8s}', bat(x,short=True))
    print('   whale LONG positions (fade=LONG), liq known vs nan, loss vs profit')
    for lab,m in (('loss, liq known',(d.wpnl<0)&d.liq_dist.notna()),('loss, liq nan',(d.wpnl<0)&d.liq_dist.isna()),('profit, liq known',(d.wpnl>=0)&d.liq_dist.notna()),('profit, liq nan',(d.wpnl>=0)&d.liq_dist.isna())):
        x=dedupe(d[(d.sw==1)&m],24); print(f'   {lab:18s}', bat(x,short=True))

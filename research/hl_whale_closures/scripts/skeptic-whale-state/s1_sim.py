import sys; sys.path.insert(0,'.')
from s0_load import load_sim
from sk import *
s=load_sim()
s['symbol']=s.coin.map(SYM); print('no binance symbol', s.symbol.isna().sum()); s=s[s.symbol.notna()].copy()
s['addr']=s.whale_addr; s['dec_ms']=s.open_ms
s=s.sort_values(['addr','coin','wtd','dec_ms'])
gap=s.groupby(['addr','coin','wtd']).dec_ms.diff()
s['ep_first']=gap.isna()|(gap>30*60000)
s['day']=pd.to_datetime(s.dec_ms,unit='ms').dt.strftime('%Y-%m-%d')
s['distress']=(s.wpnl<0)&(s.liq_dist<5)
ep=s[s.ep_first].copy()
print('rows',len(s),'episodes',len(ep))
# our side = fade = -whale trade dir
ep['side']=-ep.wtd
O=outcomes(ep, ep.side.values, 24)
ep=ep.join(O)
ep['y']=ep.ew-COST-ep.fund.fillna(0)
ep.to_parquet('sim_ep_L24.parquet')
for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
    d=ep[(ep.day>=a)&(ep.day<=b)&ep.y.notna()]
    print(f'== {per}: episodes with L24 {len(d)}  distress share {d.distress.mean():.3f}')
    x=dedupe(d[d.distress],24)
    print(' S_DISTRESS L24 fade (mn EW, -0.146, -fund):', bat(x))
    for lab,col in (('raw',x.raw-COST-x.fund.fillna(0)),('btc-hedged',x.btc-COST-x.fund.fillna(0)),('no funding',x.ew-COST)):
        print(f'   {lab:12s}', bat(x.assign(y=col)))
    print('   mean funding paid', round(x.fund.mean(),3), 'fund nan',x.fund.isna().sum(), ' entry-minute worst slip mean',round(x.slip_in.mean(),3))
    for lab,m in (('ALL',d.index==d.index),('pnl>=0&liq<5',(d.wpnl>=0)&(d.liq_dist<5)),('pnl<0&liq>=5',(d.wpnl<0)&(d.liq_dist>=5)),('pnl<0&liq nan',(d.wpnl<0)&d.liq_dist.isna())):
        z=dedupe(d[m],24); print(f'   ctrl {lab:14s}', bat(z,short=True), ' L/S', f"{z[z.side>0].y.mean():+.2f}/{z[z.side<0].y.mean():+.2f}")

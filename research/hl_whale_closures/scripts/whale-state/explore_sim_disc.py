# DISCOVERY-only (<=21.09) exploration of the sim-trigger universe by whale PnL and liquidation distance.
import numpy as np, pandas as pd
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state/'
d=pd.read_parquet(W+'sim_events_out.parquet'); d=d[d.day<='2026-09-21'].copy()
d['mn8']=d.L8-d.alt8; d['mn24']=d.L24-d.alt24
d['f300n']=d.fol_300-0.146; d['d300n']=d.fad_300-0.146; d['f1800n']=d.fol_1800-0.146; d['d1800n']=d.fad_1800-0.146
outs=['mid_60','mid_300','mid_1800','f300n','d300n','f1800n','d1800n','mn8','mn24']
print('n',len(d),'wallets',d.addr.nunique(),'coins',d.coin.nunique(),'days',d.day.nunique())
print(f"wpnl quantiles {d.wpnl.quantile([.05,.25,.5,.75,.95]).round(2).to_dict()} ; liq_dist known {d.liq_dist.notna().mean():.2f} q {d.liq_dist.quantile([.1,.25,.5,.75]).round(2).to_dict()}")
def show(name,cats):
    print('\n==',name)
    for lab,m in cats:
        x=d[m]
        print(f'  {lab:22s} n={len(x):4d} w={x.addr.nunique():3d} c={x.coin.nunique():3d} cd={(x.coin+x.day).nunique():4d} | '+' '.join(f'{o}={x[o].mean():+.2f}/{x[o].median():+.2f}' for o in outs))
p=d.wpnl
show('whale PnL at trigger (%)',[('< -10',p<-10),('-10..-3',(p>=-10)&(p<-3)),('-3..0',(p>=-3)&(p<0)),('0..3',(p>=0)&(p<3)),('3..10',(p>=3)&(p<10)),('>=10',p>=10)])
l=d.liq_dist
show('liq distance %',[('<2',l<2),('2-5',(l>=2)&(l<5)),('5-15',(l>=5)&(l<15)),('15-50',(l>=15)&(l<50)),('>=50',l>=50),('none (no liq px)',l.isna())])
show('distress combos',[('loss & liq<5',(p<0)&(l<5)),('loss<-3 & liq<10',(p<-3)&(l<10)),('profit>3 & liq>=15|nan',(p>3)&((l>=15)|l.isna()))])
show('side',[('whale buys',d.wdir==1),('whale sells',d.wdir==-1)])

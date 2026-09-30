# DISCOVERY-ONLY exploration of state variables (events <= 21.09). Prints per-bin means/medians of outcomes.
import numpy as np, pandas as pd
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state/'
ev=pd.read_parquet(W+'events_out.parquet'); d=ev[ev.day<='2026-09-21'].copy()
d['herd5']=d.nw_same_5; d['herd30']=d.nw_same_30; d['herd120']=d.nw_same_120
for Wm in (5,30,120):
    d[f'hs{Wm}']=d[f'usd_same_{Wm}']/d.depth_usd; d[f'ho{Wm}']=d[f'usd_opp_{Wm}']/d.depth_usd
d['mn8']=d.L8-d.alt8; d['mn24']=d.L24-d.alt24
d['f300n']=d.fol_300-0.146; d['d300n']=d.fad_300-0.146; d['f1800n']=d.fol_1800-0.146; d['d1800n']=d.fad_1800-0.146
outs=['mid_60','mid_300','mid_1800','f300n','d300n','f1800n','d1800n','mn8','mn24']
print('n disc events',len(d),'days',d.day.nunique(),'wallets',d.addr.nunique(),'coins',d.coin.nunique())
print('ALL', ' '.join(f'{o}={d[o].mean():+.3f}/{d[o].median():+.3f}' for o in outs))
def show(name, cats):
    print('\n==',name)
    for lab,m in cats:
        x=d[m]
        print(f'  {lab:28s} n={len(x):4d} w={x.addr.nunique():3d} c={x.coin.nunique():3d} | '+' '.join(f'{o}={x[o].mean():+.2f}/{x[o].median():+.2f}' for o in outs))
for Wm in (5,30,120):
    h=d[f'nw_same_{Wm}']
    show(f'herd distinct other wallets same dir {Wm}m',[('0',h==0),('1',h==1),('2+',h>=2),('3+',h>=3)])
    hs=d[f'hs{Wm}']
    show(f'herd other USD/depth same dir {Wm}m',[('0',hs==0),('(0,0.1]',(hs>0)&(hs<=0.1)),('(0.1,0.5]',(hs>0.1)&(hs<=0.5)),('>0.5',hs>0.5)])
    ho=d[f'ho{Wm}']
    show(f'opp dir other USD/depth {Wm}m',[('0',ho==0),('>0',ho>0),('>0.1',ho>0.1)])
show('stage fresh6',[('fresh6=1',d.fresh6==1),('fresh6=0 (cont)',d.fresh6==0),('unknown',d.fresh6_known==0)])
show('stage fresh24',[('fresh24=1',(d.fresh24==1)),('fresh24=0',d.fresh24==0),('unknown',d.fresh24_known==0)])
show('gap_prev_h',[('<0.5h',d.gap_prev_h<0.5),('0.5-6',(d.gap_prev_h>=0.5)&(d.gap_prev_h<6)),('6-24',(d.gap_prev_h>=6)&(d.gap_prev_h<24)),('>=24',d.gap_prev_h>=24),('nan',d.gap_prev_h.isna())])
show('near full (tx_pct>=80)',[('>=80',d.tx_pct>=80),('r_pct>=50',d.r_pct>=50),('<50',d.r_pct<50)])
show('wallet ncoins_past',[('<=1',d.ncoins_past<=1),('2',d.ncoins_past==2),('3+',d.ncoins_past>=3)])
show('vault',[('vault',d.vault==1),('wallet',d.vault==0)])
show('activity tx/day past',[('<5',d.tx_per_day_past<5),('5-50',(d.tx_per_day_past>=5)&(d.tx_per_day_past<50)),('>=50',d.tx_per_day_past>=50)])
show('whale side (wdir)',[('whale buys (close short)',d.wdir==1),('whale sells (close long)',d.wdir==-1)])
show('ratio',[('1-2',d.batch_ratio<2),('2-5',(d.batch_ratio>=2)&(d.batch_ratio<5)),('>=5',d.batch_ratio>=5)])
show('rtd (roll60 usd/depth)',[('<0.1',d.rtd<0.1),('0.1-0.3',(d.rtd>=0.1)&(d.rtd<0.3)),('>=0.3',d.rtd>=0.3)])
show('detect',[('ws',d.detect_src=='ws'),('sweep/scan',d.detect_src!='ws')])

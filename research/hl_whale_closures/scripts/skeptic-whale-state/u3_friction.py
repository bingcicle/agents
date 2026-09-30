# Fact check: taker friction around U events on Binance 1s bars (own code via lib.bars/worst_px)
import sys; sys.path.insert(0,'.'); sys.path.insert(0,'/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
import numpy as np, pandas as pd
from hl import bars, worst_px, last_px, has_bars
ev=pd.read_parquet('u_events.parquet')
rows=[]
for i,r in ev.sort_values(['symbol','ts']).iterrows():
    if not has_bars(r.symbol, r.ts): continue
    stx=int(r.ts//1000); dec=int(np.ceil(r.dec_ms/1000))
    b=bars(r.symbol, (stx-5)*1000, (dec+70)*1000)
    if b is None: continue
    p0=last_px(b,stx-1); pd_=last_px(b,dec-1)
    eb=worst_px(b,dec,+1); es=worst_px(b,dec,-1); xb=worst_px(b,dec+60,+1); xs=worst_px(b,dec+60,-1)
    w=r.wdir
    fol=100*((xs/eb-1) if w>0 else (1-xb/es)); fad=100*((1-xb/es) if w>0 else (xs/eb-1))
    rows.append(dict(day=r.day,imp=100*w*(pd_/p0-1),spread=100*(eb/es-1),rt=fol+fad,lat=(r.dec_ms-r.ts)/1000))
F=pd.DataFrame(rows); F['per']=np.where(F.day<='2026-09-21','DISC','TEST')
print(F.groupby('per')[['imp','spread','rt','lat']].median().round(3)); print(F.groupby('per')[['imp','spread','rt']].mean().round(3)); print(F.groupby('per').size())

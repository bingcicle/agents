import numpy as np, pandas as pd, sys
sys.path.insert(0,'/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib'); from hl import bars
d=pd.read_parquet('ev.parquet'); d=d[d.ts>=pd.Timestamp('2026-08-25').value//10**6]
rows=[]
for r in d.itertuples():
    b=bars(r.sym,r.ts-5000,r.ts+10000)
    if b is None: continue
    s=r.ts//1000+1  # decision 1 s after candle close
    w=b.loc[s:s+2]
    if w.h.notna().sum()==0: w=b.loc[s:s+9]
    if w.h.notna().sum()==0: continue
    ent=w.h.max(); last=b.loc[:s-1].cf.iloc[-1] if 'cf' in b else np.nan
    bx=bars(r.sym,r.ts+59*60000,r.ts+61*60000+10000)
    xw=None
    if bx is not None:
        se=(r.ts+60*60000)//1000; xw=bx.loc[se:se+2]
        if xw.l.notna().sum()==0: xw=bx.loc[se:se+9]
    xl=xw.l.min() if xw is not None and xw.l.notna().sum() else np.nan
    rows.append(dict(sym=r.sym,X=r.X,N=r.N,ts=r.ts,e1=r.e1,c0=r.c0,x=r.x,went=ent,wx=xl,oi5=r.oi5))
e=pd.DataFrame(rows); e['slip_ent']=100*(e.went/e.e1-1); e['slip_x']=100*(e.x/e.wx-1)
e['g_kl']=100*(e.x/e.e1-1); e['g_tape']=100*(e.wx/e.went-1)
print(len(e)); print(e.groupby(['X','N'])[['slip_ent','slip_x','g_kl','g_tape']].agg(['mean','median','size']).round(3).to_string())
e.to_parquet('tape.parquet')

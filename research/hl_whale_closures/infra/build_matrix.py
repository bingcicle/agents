# Event-return matrix for whale close txs (tx_usd >= 1000): Binance returns in the WHALE TRADE direction.
# whale trade dir: 'Close Short'/'Short > Long'/'Open Long' = buy (+1); 'Close Long'/'Long > Short'/'Open Short' = sell (-1)
import sys, numpy as np, pandas as pd, time
sys.path.insert(0,'/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/lib')
from hl import *
tx=pd.read_parquet(DATA+'whale_txs.parquet')
tx=tx[tx.tx_usd>=1000].copy()
tx['wdir']=np.where(tx.dir.isin(['Close Short','Short > Long','Open Long']),1,-1)
tx['symbol']=tx.coin.map(SYM)
tx=tx[tx.symbol.notna()].copy()
tx['sec']=(tx.ts//1000).astype(np.int64)
tx['day']=pd.to_datetime(tx.ts,unit='ms').dt.strftime('%Y-%m-%d')
OFF=[-300,-60,-10,-3,-1,0,1,2,3,5,10,20,30,60,120,300,600,1800,3600]
LONGH=[2,4,8,24,48,72]   # hours, via 1m klines
out=[]
t0=time.time()
for (sym,day),g in tx.groupby(['symbol','day']):
    lo=g.sec.min()-400; hi=g.sec.max()+3700
    b=bars(sym, lo*1000, hi*1000)
    res=pd.DataFrame(index=g.index)
    if b is not None:
        cf=b.cf; qu=b.quote; bq=b.bq
        base_s=g.sec.values-1
        base=cf.reindex(base_s).values
        res['px_base']=base
        for o in OFF:
            v=cf.reindex(g.sec.values+o).values
            res[f'r{o}']=100*(v/base-1)*g.wdir.values
        # worst-in-3s entry at t+lat (bot style) for lat 1,2 s  (follow direction = whale dir)
        for lat in (1,2):
            wp=[]
            for s,dirn in zip(g.sec.values,g.wdir.values):
                wp.append(worst_px(b, s+lat, dirn))
            res[f'wentry{lat}']=100*(np.array(wp)/base-1)*g.wdir.values   # cost of entering vs base (positive = paid more)
        # flow around: quote volume and taker-buy share 60s before / after
        cq=qu.cumsum(); cb=bq.cumsum()
        def win(a,bnd):
            x=cq.reindex(g.sec.values+bnd).values-cq.reindex(g.sec.values+a).values
            y=cb.reindex(g.sec.values+bnd).values-cb.reindex(g.sec.values+a).values
            return x,y
        q1,b1=win(-61,-1); q2,b2=win(-1,59)
        res['qv_pre60']=q1; res['qv_post60']=q2
        res['tbuy_pre60']=b1/np.where(q1>0,q1,np.nan); res['tbuy_post60']=b2/np.where(q2>0,q2,np.nan)
    # long horizons from 1m klines
    k=kl(sym)
    if k is not None:
        idx=k.index.values; c=k.c.values
        def pxm(ms):
            i=np.searchsorted(idx, ms-60000, side='right')-1
            ok=(i>=0)&(i<len(idx))
            v=np.where(ok, c[np.clip(i,0,len(c)-1)], np.nan)
            # stale guard: candle must be within 2 minutes
            st=np.where(ok, idx[np.clip(i,0,len(idx)-1)], 0)
            return np.where(ms-st<=180000, v, np.nan)
        m0=pxm(g.ts.values)
        res['pxm0']=m0
        for h in LONGH:
            res[f'R{h}h']=100*(pxm(g.ts.values+h*3600000)/m0-1)*g.wdir.values
    out.append(res)
M=pd.concat(out)
M=tx.join(M)
# market factors (BTC and equal-weight alt index) over the same long windows, sign = whale dir
kb=kl('BTCUSDT'); 
def mret(ms0, ms1, k):
    idx=k.index.values; c=k.c.values
    i0=np.clip(np.searchsorted(idx, ms0-60000, side='right')-1,0,len(c)-1); i1=np.clip(np.searchsorted(idx, ms1-60000, side='right')-1,0,len(c)-1)
    return 100*(c[i1]/c[i0]-1)
for h in LONGH:
    M[f'BTC{h}h']=mret(M.ts.values, M.ts.values+h*3600000, kb)*M.wdir.values
for o in (60,300,600,1800,3600):
    M[f'BTCr{o}']=mret(M.ts.values, M.ts.values+o*1000, kb)*M.wdir.values
M.to_parquet(DATA+'tx_matrix.parquet')
print('rows',len(M),'with bars',M.px_base.notna().sum(),'with R24h',M.R24h.notna().sum(), round(time.time()-t0),'s')

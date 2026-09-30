"""Build unwind episodes per (wallet, coin) from whale_txs.parquet.
start = material close (tx_pct>=5%, tx_usd>=5k) with no close tx >= $1k by the same (addr,coin) in the previous 6 h,
        and ts >= data start + 6 h (otherwise left-censored, flagged).
episode = all txs from start until the first 6 h gap in txs >= $1k (or data end).
"""
import numpy as np, pandas as pd
from lh import *
t=pd.read_parquet(SP+'data/whale_txs.parquet')
t=t[t.dir.isin(['Close Short','Close Long','Short > Long','Long > Short'])].copy()
t['wdir']=np.where(t.wside=='SHORT',1,-1)          # whale TRADE direction: closing short = buy (+1)
t['flip']=t.dir.isin(['Short > Long','Long > Short'])
t=t.sort_values(['addr','coin','ts']).reset_index(drop=True)
DSTART=t.ts.min(); GAP=6*3600*1000
t['sig']=t.tx_usd>=1000
t['mat']=(t.tx_pct>=5)&(t.tx_usd>=5000)
t['pos_after']=np.where(t.flip,0,np.clip(t.sp-t.sz,0,None))
t['bot_ms']=t.bot_ts*1000
# previous significant tx time within group
g=t.groupby(['addr','coin'],sort=False)
t['sig_ts']=np.where(t.sig,t.ts,np.nan)
t['prev_sig']=g.sig_ts.transform(lambda s: s.shift(1).ffill())
t['gap_prev']=t.ts-t.prev_sig
t['start']=t.mat & ((t.gap_prev>=GAP)|t.prev_sig.isna())
t['censored']=t.start & (t.prev_sig.isna()) & (t.ts<DSTART+GAP)
eps=[]
for (a,c),d in g:
    ts=d.ts.values; sig=d.sig.values; st=np.where(d.start.values)[0]
    if len(st)==0: continue
    sigts=ts[sig]; sig_idx=np.where(sig)[0]
    for k in st:
        # episode end: last sig tx before a gap >= 6h after start
        j=np.searchsorted(sig_idx,k)   # position of k among sig idx (k is sig since mat implies tx_usd>=5k)
        end=k
        for m in range(j+1,len(sig_idx)):
            if ts[sig_idx[m]]-ts[sig_idx[m-1]]>=GAP: break
            end=sig_idx[m]
        # include non-sig txs up to end
        e=d.iloc[k:end+1]
        r0=d.iloc[k]
        sp0=r0.sp; pa=e.pos_after.values; ets=e.ts.values; ebot=e.bot_ms.values
        closed_cum=np.cumsum(e.sz.values)
        rem=pa/sp0
        def t_first(mask):
            i=np.where(mask)[0]; return (ets[i[0]],ebot[i[0]]) if len(i) else (np.nan,np.nan)
        full_ts,full_bot=t_first(rem<=0.10)
        h50_ts,_=t_first(closed_cum>=0.5*sp0)
        eps.append(dict(addr=a,coin=c,wside=r0.wside,wdir=r0.wdir,ts0=r0.ts,bot0=r0.bot_ms,src0=r0.detect_src,
            lag0=(r0.bot_ms-r0.ts)/1000,tx_pct0=r0.tx_pct,tx_usd0=r0.tx_usd,sp0=sp0,px0=r0.px,val0=sp0*r0.px,
            ratio0=r0.batch_ratio,depth0=r0.depth_usd,censored=r0.censored,n_tx=len(e),n_mat=int(e.mat.sum()),
            ts_end=ets[-1],dur_h=(ets[-1]-r0.ts)/3.6e6,closed_frac=closed_cum[-1]/sp0,rem_end=rem[-1],
            full_ts=full_ts,full_bot=full_bot,t_full_h=(full_ts-r0.ts)/3.6e6,t50_h=(h50_ts-r0.ts)/3.6e6,
            closed_1h=closed_cum[ets<=r0.ts+3.6e6][-1]/sp0,closed_4h=closed_cum[ets<=r0.ts+4*3.6e6][-1]/sp0,
            closed_24h=closed_cum[ets<=r0.ts+24*3.6e6][-1]/sp0,
            usd_closed=(e.sz.values*e.px.values).sum()))
E=pd.DataFrame(eps)
E['day']=pd.to_datetime(E.ts0,unit='ms').dt.strftime('%m-%d')
E['sym']=E.coin.map(coin_sym)
E.to_parquet(W+'episodes.parquet')
t[['addr','coin','ts','bot_ms','wdir','wside','sz','sp','px','tx_usd','tx_pct','batch_ratio','depth_usd','pos_after','mat','start','flip','detect_src']].to_parquet(W+'txs_slim.parquet')
print(len(E), E.censored.sum(), E.sym.notna().sum())
print(E.describe(percentiles=[.1,.25,.5,.75,.9]).T.to_string())

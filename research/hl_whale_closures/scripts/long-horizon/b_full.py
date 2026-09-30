"""Mechanism B from raw whale txs: (near-)full close of a position. Event at tx where pos_after <= 5% of the max position (size)
seen for this (addr,coin) in the previous 72h; overhang = max position value / depth (ratio_max). Decision = bot_ts of that tx.
Side = AGAINST whale trade (fade): whale closed long (sold) -> we LONG. Dedupe: one event per (addr,coin) per 24h.
Also require the first tx of the pair in the data to be >= 6h before (so max is meaningful) - not required, flagged."""
import numpy as np, pandas as pd
from lh import *; from evalx import *
t=pd.read_parquet(W+'txs_slim.parquet').sort_values(['addr','coin','ts'])
ev=[]
for (a,c),d in t.groupby(['addr','coin'],sort=False):
    ts=d.ts.values; sp=d.sp.values; pa=d.pos_after.values; px=d.px.values; bot=d.bot_ms.values; wd=d.wdir.values; dep=d.depth_usd.values; rat=d.batch_ratio.values
    last=-1e18
    for i in np.where(pa<=0.05*sp)[0]:
        lo=np.searchsorted(ts,ts[i]-72*3.6e6)
        k=lo+np.argmax(sp[lo:i+1]); mx=sp[k]
        if pa[i]>0.05*mx: continue
        if ts[i]<last+24*3.6e6: continue
        last=ts[i]
        # closing duration: time from first tx after max to this one; share closed in the last 1h
        ev.append(dict(addr=a,coin=c,ts=ts[i],dec_ms=bot[i],side=-wd[i],wdir=wd[i],max_val=mx*px[k],depth=dep[k],ratio_max=rat[k],
                       close_dur_h=(ts[i]-ts[k])/3.6e6,n_tx_window=i-lo+1,tx_usd=sp[i]*px[i]))
B=pd.DataFrame(ev); B['day']=pd.to_datetime(B.dec_ms,unit='ms').dt.strftime('%m-%d')
print(len(B)); print(B.describe().T.round(2).to_string())
B=attach(B); B.to_parquet(W+'b_full_ev.parquet')
out=W+'out_B.txt'; open(out,'w').write('Mechanism B (fade whale dir after (near) full close) from whale_txs 12.09-29.09\n')
compact(B[B.max_val>=100e3],'B-tx all, max_val>=100k',out=out,splits=('disc','test'))
compact(B[(B.max_val>=100e3)&(B.ratio_max>=3)],'B-tx max_val>=100k & ratio_max>=3',out=out,splits=('disc','test'))
compact(B[(B.max_val>=100e3)&(B.close_dur_h>=1)],'B-tx max_val>=100k & unwind took >=1h (slow unwind done)',out=out,splits=('disc','test'))

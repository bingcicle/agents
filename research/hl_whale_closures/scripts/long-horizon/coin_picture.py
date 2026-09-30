"""Coin-level picture: per coin per UTC day - whale close flow (buy = close short, sell = close long), aggregate known whale
short/long position (last known pos_after per wallet, value at day-end price) / median depth, day return raw & minus EW alt index,
next-day return. For coins with the biggest positions relative to depth."""
import numpy as np, pandas as pd
from lh import *
from scipy.stats import spearmanr
t=pd.read_parquet(W+'txs_slim.parquet').sort_values('ts')
t['sym']=t.coin.map(coin_sym); t=t[t.sym.notna()]
t['d']=pd.to_datetime(t.ts,unit='ms').dt.strftime('%m-%d')
coins=['PONS','NEAR','LIT','CHIP','VVV','STRK','XPL','NIL','USELESS','MON','INJ','GRASS','XMR','ZRO','CC','HYPE','PUMP','FARTCOIN','MINA','SKR','MET']
days=pd.date_range('2026-09-13','2026-09-29',freq='D')
out=open(W+'out_coin_picture.txt','w')
rows=[]
for c in coins:
    d=t[t.coin==c]; j=SIDX[coin_sym(c)]
    dep=d.depth_usd.median()
    lines=[f'\n=== {c}  median HL depth1% ${dep/1e3:.0f}k; wallets={d.addr.nunique()}']
    lines.append('day   | buy$k(closeS) sell$k(closeL) net/depth | shortPos/depth longPos/depth (known, end of day) | ret raw% | ret-alt% | next-day ret-alt%')
    for dd in days:
        ds=dd.strftime('%m-%d'); e=d[d.d==ds]
        end=(dd+pd.Timedelta(days=1)).value//10**6
        # known positions: last tx per wallet up to day end
        k=d[d.ts<end].groupby('addr').last()
        px_end=np.exp(LC[minute_of(end)-1,j])
        sh=(k[k.wside=='SHORT'].pos_after*px_end).sum(); lo=(k[k.wside=='LONG'].pos_after*px_end).sum()
        buy=e[e.wdir==1].tx_usd.sum(); sell=e[e.wdir==-1].tx_usd.sum()
        i0=minute_of(dd.value//10**6)-1; i1=minute_of(end)-1; i2=i1+1440
        r=logret(np.array([j]),np.array([i0]),np.array([i1]))[0]; a=idxret(np.array([i0]),np.array([i1]))[0]
        rn=logret(np.array([j]),np.array([i1]),np.array([i2]))[0]; an=idxret(np.array([i1]),np.array([i2]))[0]
        rows.append(dict(coin=c,day=ds,buy=buy,sell=sell,net_dep=(buy-sell)/dep,short_dep=sh/dep,long_dep=lo/dep,ret=r,mn=r-a,next_mn=rn-an))
        lines.append(f'{ds} | {buy/1e3:8.0f} {sell/1e3:8.0f} {(buy-sell)/dep:+6.2f} | {sh/dep:6.1f} {lo/dep:6.1f} | {r:+6.2f} | {r-a:+6.2f} | {rn-an:+6.2f}')
    txt='\n'.join(lines); print(txt); out.write(txt+'\n')
R=pd.DataFrame(rows); R.to_csv(W+'coin_day_panel.csv',index=False)
# pooled relationships on this panel
R['dshort']=R.groupby('coin').short_dep.diff()
for split,msk in (('disc',R.day<='09-21'),('test',R.day>='09-22')):
    v=R[msk].dropna(subset=['next_mn'])
    s1=spearmanr(v.net_dep,v.mn).statistic; s2=spearmanr(v.net_dep,v.next_mn).statistic
    s3=spearmanr(v.short_dep,v.next_mn).statistic
    txt=f'[{split}] coin-days={len(v)} spearman(net flow/depth, same-day ret-alt)={s1:+.3f}; (net flow/depth, NEXT-day ret-alt)={s2:+.3f}; (short overhang/depth, next-day ret-alt)={s3:+.3f}'
    print(txt); out.write(txt+'\n')

"""Rolling hourly panel for flow-following: decision times every hour from 13.09 00:00 to 29.09 23:00 UTC.
For each (decision hour D, coin): trailing-L-hour net whale close flow (wdir*tx_usd, by bot_ms < D), normalized by
trailing HL depth (median of depth_usd of that coin's txs over the previous 7 days, bot_ms<D) and by Binance quote volume
over the same L hours. Forward returns from price at D+1min (close of minute D..D+1) to +H h: raw, minus EW-alt, minus BTC; funding for LONG."""
import numpy as np, pandas as pd
from lh import *
t=pd.read_parquet(W+'txs_slim.parquet')
t['sym']=t.coin.map(coin_sym); t=t[t.sym.notna()].copy()
coins=sorted(t.coin.unique()); ci={c:i for i,c in enumerate(coins)}
H0=pd.Timestamp('2026-09-12 00:00').value//10**6
NH=18*24
t['hb']=((t.bot_ms-H0)//3.6e6).astype(int)
t=t[(t.hb>=0)&(t.hb<NH)]
F=np.zeros((NH,len(coins))); G=np.zeros((NH,len(coins))); NTX=np.zeros((NH,len(coins)))
np.add.at(F,(t.hb.values,t.coin.map(ci).values),t.wdir.values*t.tx_usd.values)
np.add.at(G,(t.hb.values,t.coin.map(ci).values),t.tx_usd.values)
np.add.at(NTX,(t.hb.values,t.coin.map(ci).values),1)
Fc=np.vstack([np.zeros((1,len(coins))),np.cumsum(F,0)]); Gc=np.vstack([np.zeros((1,len(coins))),np.cumsum(G,0)])
# depth: per coin per hour-bin median, then trailing 7d median approximated by expanding daily medians
t['dday']=t.hb//24
dd=t.groupby(['coin','dday']).depth_usd.median().unstack()   # coin x day
rows=[]
jj=np.array([SIDX[coin_sym(c)] for c in coins])
QVc=np.vstack([np.zeros((1,QV.shape[1])),np.cumsum(QV,axis=0,dtype=np.float64)])
for h in range(24,NH):   # decision at H0 + h hours (from 13.09 00:00)
    Dm=H0+h*3600000
    if Dm>=T0+NMIN*60000: break
    iD=minute_of(Dm)            # close of minute starting at D -> price at D+1min (entry)
    day=h//24
    dep=dd.loc[:,[x for x in dd.columns if day-7<=x<day]].median(axis=1).reindex(coins).values if day>0 else np.full(len(coins),np.nan)
    rec={'h':h,'Dm':Dm}
    for L in (4,24,72):
        a=max(h-L,0)
        f=Fc[h]-Fc[a]; g=Gc[h]-Gc[a]
        qv=QVc[minute_of(Dm),jj]-QVc[max(minute_of(Dm)-L*60,0),jj]
        rec[f'f{L}']=f; rec[f'g{L}']=g; rec[f'qv{L}']=qv
    for Hh in (4,8,24,48,72):
        i1=iD+Hh*60
        ok=i1<NMIN
        r=100*(np.exp(LC[i1,jj]-LC[iD,jj])-1) if ok else np.full(len(coins),np.nan)
        a_=100*(np.exp(IDX[i1]-IDX[iD])-1) if ok else np.nan
        b_=100*(np.exp(BTC[i1]-BTC[iD])-1) if ok else np.nan
        rec[f'raw{Hh}']=r; rec[f'mnA{Hh}']=r-a_; rec[f'mnB{Hh}']=r-b_
    pre=100*(np.exp(LC[iD,jj]-LC[iD-1440,jj])-1)-100*(np.exp(IDX[iD]-IDX[iD-1440])-1)
    df=pd.DataFrame({'coin':coins,'dep':dep,'pre24':pre})
    for k,v in rec.items():
        if k in ('h','Dm'): df[k]=v
        else: df[k]=v
    rows.append(df)
P=pd.concat(rows,ignore_index=True)
P['day']=pd.to_datetime(P.Dm,unit='ms').dt.strftime('%m-%d'); P['hod']=((P.Dm//3600000)%24).astype(int)
P['split']=np.where(P.Dm<DISC_END,'disc','test')
P.to_parquet(W+'flow_panel.parquet')
print(P.shape, P.day.min(), P.day.max()); print(P[['f24','dep','qv24','mnA24']].describe().T)

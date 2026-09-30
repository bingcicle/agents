"""Independent rebuild of B events (fade after whale (near-)full close).
Sources:
 (tx)   whale_txs 12.09-30.09: tx after which whale position <= 5% of its max startPosition over the prior 72h,
        max value >= $100k. Decision = bot_ts of that tx. One event per (addr,coin) per 24h.
 (bat)  close_batches with full_close flag (bot's own flag) - alternative independent definition.
 (trig) follow_trades (legacy 30.08-12.09 + current) exit_reason full_close*: decision = bot close time.
side = -whale trade direction (whale sold to close long -> we LONG)."""
import numpy as np, pandas as pd
from px import *
BOTD=SP+'hl_whale_export_20260930/02_bot_data/'

t=pd.read_parquet(SP+'data/whale_txs.parquet')
t=t[t.dir.isin(['Close Long','Close Short','Long > Short','Short > Long'])].copy()
t['wdir']=np.where(t.wside=='SHORT',1,-1)
t['pa']=np.where(t.dir.str.contains('>'),0.0,np.maximum(t.sp-t.sz,0))
t=t.sort_values(['addr','coin','ts']).reset_index(drop=True)
ev=[]
for (a,c),d in t.groupby(['addr','coin'],sort=False):
    ts=d.ts.values; sp=d.sp.values; pa=d.pa.values; px=d.px.values; bt=d.bot_ts.values; wd=d.wdir.values
    rat=d.batch_ratio.values; src=d.detect_src.values
    last=-1e18
    cand=np.where(pa<=0.05*sp)[0]
    for i in cand:
        lo=np.searchsorted(ts,ts[i]-72*3600*1000,side='left')
        k=lo+int(np.argmax(sp[lo:i+1])); mx=sp[k]
        if pa[i]>0.05*mx: continue
        if mx*px[k]<100e3: continue
        if ts[i]<last+24*3600*1000: continue
        last=ts[i]
        ev.append(dict(addr=a,coin=c,ts=int(ts[i]),dec_ms=int(round(bt[i]*1000)),side=-int(wd[i]),maxval=mx*px[k],
                       ratio_max=rat[k],src_det=src[i],first_ts_pair=int(ts[0])))
BT=pd.DataFrame(ev); BT['src']='tx'
print('B-tx events',len(BT), 'lag s median',((BT.dec_ms-BT.ts)/1000).median(), 'q90',((BT.dec_ms-BT.ts)/1000).quantile(.9))
# how many events are the first tx seen of that pair (max only from that tx: pair appears first time closing fully)
print('share of B-tx where pair first seen < 6h before event:',((BT.ts-BT.first_ts_pair)<6*3600e3).mean())

# (bat) close_batches full_close flag
cb=pd.read_parquet(SP+'data/close_batches.parquet').sort_values(['addr','coin','bot_ts'])
cb['dec_ms']=(cb.bot_ts*1000).round().astype('int64')
out=[]
for (a,c),d in cb.groupby(['addr','coin'],sort=False):
    bt=d.dec_ms.values; ov=d.old_val.values; fc=d.full_close.values; ws=d.wside.values; last=-1e18
    for i in np.where(fc)[0]:
        lo=np.searchsorted(bt,bt[i]-72*3600*1000); mx=np.nanmax(ov[lo:i+1])
        if mx<100e3 or bt[i]<last+24*3600*1000: continue
        last=bt[i]; out.append(dict(addr=a,coin=c,dec_ms=int(bt[i]),side=(1 if ws[i]=='LONG' else -1),maxval=mx,src='bat'))
BB=pd.DataFrame(out); print('B-bat events',len(BB))

# (trig) follow trades full_close exits
L=pd.read_csv(BOTD+'follow_trades.csv.legacy-1789241806.csv'); L=L[L.eol=='^'].copy()
L['close_ms']=((pd.to_datetime(L.date_close)-pd.Timedelta(hours=2))-pd.Timestamp('1970-01-01'))//pd.Timedelta(milliseconds=1)
Cc=pd.read_csv(BOTD+'follow_trades.csv',low_memory=False); Cc=Cc[Cc.eol=='^'].copy()
Cc['close_ms']=Cc.exit_decision_ms.fillna(Cc.close_ts_ms).astype('int64')
print('current: exit_decision vs close_ts diff s median',((Cc.close_ts_ms-Cc.exit_decision_ms)/1000).median())
F=pd.concat([L.assign(fsrc='legacy'),Cc.assign(fsrc='current')])
F=F[F.exit_reason.astype(str).str.startswith('full_close')]
F=F.sort_values('close_ms').drop_duplicates(['whale_addr','coin'],keep='first') if False else F.sort_values('close_ms')
F=F.drop_duplicates(['whale_addr','coin','close_ms'])
rows=[]; last={}
for r in F.itertuples():
    k=(r.whale_addr,r.coin)
    if k in last and r.close_ms-last[k]<6*3600e3: continue
    last[k]=r.close_ms
    rows.append(dict(addr=r.whale_addr,coin=r.coin,dec_ms=int(r.close_ms),side=-(1 if r.our_side=='LONG' else -1),
                     maxval=r.pos_usd,src='trig',fsrc=r.fsrc,ratio_max=r.ratio))
BG=pd.DataFrame(rows); print('B-trig events',len(BG),BG.fsrc.value_counts().to_dict())
for nm,d in (('tx',BT),('bat',BB),('trig',BG)): d.to_parquet(MY+f'ev_{nm}.parquet')

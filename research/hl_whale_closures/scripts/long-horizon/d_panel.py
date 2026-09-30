"""Daily cross-sectional panel (all coins with whale close txs): features known at day end D (bot_ms < D):
 short_dep/long_dep = sum of last-known whale position (short/long) valued at D / median HL depth (last 7d of txs),
 flow24_dep = net signed close flow (wdir*usd) in [D-24h, D) / depth, flow24_qv = same / Binance quote volume 24h,
 flow72_dep. Targets: mnA (coin - EW alt index) and raw returns over next 24h, 72h from D+1min.
Daily rank IC (Spearman) -> mean over days, t-stat; disc = decision days D <= 21.09 end, test D >= 22.09."""
import numpy as np, pandas as pd
from lh import *
from scipy.stats import spearmanr
t=pd.read_parquet(W+'txs_slim.parquet').sort_values('bot_ms')
t['sym']=t.coin.map(coin_sym); t=t[t.sym.notna()]
days=pd.date_range('2026-09-13','2026-09-29',freq='D')   # decision at 00:00 of these days
rows=[]
for D in days:
    Dm=D.value//10**6; iD=minute_of(Dm)-1   # close of last minute before D = price at D
    known=t[t.bot_ms<Dm]
    if len(known)==0: continue
    last=known.groupby(['coin','addr']).last()
    rec=known[known.bot_ms>=Dm-7*864e5]
    dep=rec.groupby('coin').depth_usd.median()
    f24=known[known.bot_ms>=Dm-864e5].assign(f=lambda x: x.wdir*x.tx_usd).groupby('coin').f.sum()
    f72=known[known.bot_ms>=Dm-3*864e5].assign(f=lambda x: x.wdir*x.tx_usd).groupby('coin').f.sum()
    for c in dep.index:
        j=SIDX[coin_sym(c)]; px=np.exp(LC[iD,j])
        L=last.loc[c]; sh=(L[L.wside=='SHORT'].pos_after).sum()*px; lo=(L[L.wside=='LONG'].pos_after).sum()*px
        qv=float(QV[max(iD-1439,0):iD+1,j].sum())
        r=dict(coin=c,D=D.strftime('%m-%d'),Dm=Dm,depth=dep[c],short_dep=sh/dep[c],long_dep=lo/dep[c],
               flow24_dep=f24.get(c,0)/dep[c],flow24_qv=f24.get(c,0)/max(qv,1),flow72_dep=f72.get(c,0)/dep[c])
        for H in (24,72):
            i1=iD+H*60
            if i1<NMIN:
                rr=100*(np.exp(LC[i1,j]-LC[iD,j])-1); aa=100*(np.exp(IDX[i1]-IDX[iD])-1)
                r[f'raw{H}']=rr; r[f'mn{H}']=rr-aa
        r['pre24']=100*(np.exp(LC[iD,j]-LC[iD-1440,j])-1)-100*(np.exp(IDX[iD]-IDX[iD-1440])-1)
        rows.append(r)
P=pd.DataFrame(rows); P['net_ovh']=P.long_dep-P.short_dep
P['split']=np.where(P.Dm<DISC_END,'disc','test')
P.to_csv(W+'d_panel.csv',index=False)
out=open(W+'out_D_panel.txt','w')
def pr(s): print(s,flush=True); out.write(s+'\n')
pr(f'panel rows={len(P)} coins/day median={P.groupby("D").size().median()}')
for feat in ('short_dep','long_dep','net_ovh','flow24_dep','flow24_qv','flow72_dep','pre24'):
    for H in (24,72):
        for split in ('disc','test'):
            v=P[(P.split==split)].dropna(subset=[f'mn{H}'])
            ics=[]
            for D,d in v.groupby('D'):
                d=d[d[feat].notna()]
                if len(d)>=8 and d[feat].nunique()>2: ics.append(spearmanr(d[feat],d[f'mn{H}']).statistic)
            ics=np.array(ics)
            if len(ics)<3: continue
            # overlapping for 72h: effective n ~ n/3
            neff=len(ics)/(3 if H==72 else 1)
            tt=ics.mean()/ (ics.std(ddof=1)/np.sqrt(neff))
            pr(f'{feat:11s} H={H:2d} [{split}] days={len(ics)} meanIC={ics.mean():+.3f} (t~{tt:+.2f}, overlap-adj) pos_days={np.mean(ics>0):.2f}')
out.close()

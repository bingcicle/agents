import sys; sys.path.insert(0,'.')
from sk import *
ep=pd.read_parquet('sim_ep_L24b.parquet')
tx=pd.read_parquet(SP+'data/whale_txs.parquet',columns=['addr','coin','ts','dir','liq','bot_ts','tx_usd','detect_src'])
tx['wdir']=np.where(tx.dir.isin(['Close Short','Short > Long','Open Long']),1,-1)
# 1) side semantics: for sim episodes after 12.09 19:00 find whale txs of same addr+coin in [open-10min, open]
s=ep[ep.dec_ms>1789240000000].copy()
agree=[];lag=[];liqf=[]
T=tx.groupby(['addr','coin'])
for i,r in s.iterrows():
    try: g=T.get_group((r.addr,r.coin))
    except KeyError: agree.append(np.nan); lag.append(np.nan); liqf.append(np.nan); continue
    w=g[(g.ts<=r.dec_ms)&(g.ts>=r.dec_ms-600000)]
    if not len(w): agree.append(np.nan); lag.append(np.nan); liqf.append(np.nan); continue
    last=w.iloc[-1]; agree.append(float(last.wdir==r.wtd)); lag.append((r.dec_ms-last.ts)/1000); liqf.append(w.liq.max())
s['agree']=agree; s['lag']=lag; s['liqf']=liqf
print('sim episodes since 12.09:',len(s),' matched to whale txs in prior 10 min:',s.agree.notna().mean().round(3),' side agreement',np.nanmean(agree).round(3),' lag median s',np.nanmedian(lag).round(1), ' any system (liq) fill in window', np.nanmean(liqf).round(3))
dS=s[s.distress&(s.day>='2026-09-22')]
print('TEST distress episodes: side agreement',dS.agree.mean().round(3),'lag median',dS.lag.median(),' liq fills share',dS.liqf.mean())
# 2) overlap U WDISTRESS TEST trades vs S DISTRESS TEST trades (same addr+coin, within 1 h)
U=pd.read_parquet('u_events.parquet'); U=U[(U.wpnl2<-3)&(U.day>='2026-09-22')]
U['side']=-U.wdir; U['y']=U.fad24_ew-COST-U.fad24_fund.fillna(0); U=dedupe(U[U.y.notna()],24)
S=dedupe(ep[ep.distress&(ep.day>='2026-09-22')&ep.y.notna()],24)
m=0
for i,r in U.iterrows():
    if ((S.addr==r.addr)&(S.coin==r.coin)&((S.dec_ms-r.dec_ms).abs()<24*3600e3)).any(): m+=1
print('U WDISTRESS TEST trades',len(U),'with an S_DISTRESS trade same wallet+coin within 24h:',m)
m2=sum(((U.coin==r.coin)&((U.dec_ms-r.dec_ms).abs()<24*3600e3)).any() for _,r in S.iterrows())
print('S_DISTRESS TEST trades',len(S),'with a U WDISTRESS trade same coin within 24h:',m2)

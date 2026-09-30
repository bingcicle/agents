"""Is the post-impulse continuation whale-specific? Compare continuation m2->m300 (last-trade prices, 1s bars) after
whale anchors vs after Binance 3-s jumps of the same size with NO whale tx >=$1k within +-300 s (same symbol-days)."""
from common_s import *
from s1 import dense
A=pd.read_parquet(OUT+'anchors.parquet'); R=pd.read_parquet(OUT+'s1_anchor.parquet'); T=pd.read_parquet(OUT+'txs1k.parquet')
X=A.merge(R,on='ep'); X['jump']=X.m2; X['cont']=X.m300-X.m2
sds=X[X.rel24>=1][['symbol','day']].drop_duplicates().values
rows=[]
for sym,day in sds:
    base,N,cf,hw,lw=dense(sym,day); D0=int(pd.Timestamp(day).timestamp())
    lc=np.log(cf)*100; s=np.arange(D0,D0+86400-400); i=s-base
    j=lc[i+2]-lc[i-1]
    ws=T[(T.symbol==sym)&(T.ts>=(D0-400)*1000)&(T.ts<(D0+86800)*1000)].ts.values//1000
    near=np.zeros(len(s),bool)
    for x in np.unique(ws):
        a=max(0,x-D0-300); b=min(len(s),x-D0+301)
        if b>a: near[a:b]=True
    ok=np.where((np.abs(j)>=0.1)&~near)[0]; busy=-1
    for k in ok:
        if s[k]<busy: continue
        sg=np.sign(j[k]); c=sg*(lc[i[k]+300]-lc[i[k]+2])
        if np.isfinite(c): rows.append(dict(symbol=sym,day=day,jump=abs(j[k]),cont=c,pre60=sg*(lc[i[k]-1]-lc[i[k]-61])))
        busy=s[k]+300
Nl=pd.DataFrame(rows); Nl['per']=np.where(Nl.day<='2026-09-21','D','T')
bins=[0.1,0.2,0.3,0.5,1,100]
E=X[(X.rel24>=1)&X.cont.notna()].copy(); E['bj']=pd.cut(E.jump,bins); Nl['bj']=pd.cut(Nl.jump,bins)
out=pd.concat([E.groupby('bj',observed=True).cont.agg(['size','mean','median']).add_prefix('whale_'),
               Nl.groupby('bj',observed=True).cont.agg(['size','mean','median']).add_prefix('null_')],axis=1)
print(out.round(3))
E4=X[(X.rel24>=4)&X.cont.notna()].copy(); E4['bj']=pd.cut(E4.jump,bins)
print('rel24>=4:'); print(pd.concat([E4.groupby('bj',observed=True).cont.agg(['size','mean']).add_prefix('whale_'),Nl.groupby('bj',observed=True).cont.agg(['size','mean']).add_prefix('null_')],axis=1).round(3))
# reweighted null to rel24>=4 jump distribution
w=E4.bj.value_counts(normalize=True); nm=Nl.groupby('bj',observed=True).cont.mean()
print('rel24>=4 whale cont mean %.3f vs jump-matched null %.3f'%(E4.cont.mean(), (w*nm).sum()))

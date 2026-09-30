import sys; sys.path.insert(0,'.')
from sk import *
ep=pd.read_parquet('sim_ep_L24.parquet')
# EW alt index as minute log-level (buy&hold approximated by mean simple hourly returns chained is complex; use hourly mean log-ret for beta only)
hr=np.arange(0,NM,60)
LC=np.log(C[:,hr])
lr=np.diff(LC,axis=1)                         # hourly log returns per symbol
ewr=np.nanmean(np.clip(lr[ALT],-.3,.3),axis=0)
def beta_at(k, m, days=7):
    h=m//60; h0=max(0,h-24*days)
    y=lr[k,h0:h]; x=ewr[h0:h]; ok=np.isfinite(y)&np.isfinite(x)
    if ok.sum()<48: return np.nan
    x=x[ok]; y=y[ok]; return np.cov(x,y)[0,1]/np.var(x)
te=entry_time(ep.dec_ms.values); me=m_of_close_at(te); mx=me+1440
k=ep.symbol.map(SI).values
bet=np.array([beta_at(int(k[i]),int(me[i])) for i in range(len(ep))])
ep['beta']=bet
ok=mx<NM
ewret=np.full(len(ep),np.nan)
for i in np.flatnonzero(ok):
    a=C[ALT,mx[i]]/C[ALT,me[i]]-1; msk=np.isfinite(a)&(ALT!=k[i]); ewret[i]=a[msk].mean()
ep['ewret']=100*ewret
ep['bh']=ep.raw-ep.side*np.clip(ep.beta,0,3)*ep.ewret      # beta-hedged our-side return
ep['yb']=ep.bh-COST-ep.fund.fillna(0)
ep.to_parquet('sim_ep_L24b.parquet')
print('beta median by distress', ep.groupby('distress').beta.median().round(2).to_dict())
for per,(a,b) in {'DISC':('2026-08-27','2026-09-21'),'TEST':('2026-09-22','2026-09-29')}.items():
    d=ep[(ep.day>=a)&(ep.day<=b)&ep.y.notna()]
    x=dedupe(d[d.distress],24)
    print(f'== {per}')
    print('  EW-hedged (beta 1)   ', bat(x))
    print('  beta-hedged (past 7d)', bat(x.assign(y=x.yb)))
    print('  median beta', round(x.beta.median(),2), ' mean ewret our-side', round((x.side*x.ewret).mean(),2))
    xs=x[x.side<0]; xl=x[x.side>0]
    print('  SHORT-only (whale short in loss) EW', bat(xs,short=True), '| beta', bat(xs.assign(y=xs.yb),short=True))
    print('  LONG-only  EW', bat(xl,short=True), '| beta', bat(xl.assign(y=xl.yb),short=True))
    print('  without NIL', bat(x[x.coin!='NIL'],short=True), ' without NIL & best day', bat(x[(x.coin!='NIL')],short=True))

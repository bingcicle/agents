# Is "buy a sharp 3-min dump in alts" a generic edge (no whale)? All Binance symbols, 1m klines 20.08-29.09.
import os, numpy as np, pandas as pd, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
K=SP+'data/k1m/'
T0=int(pd.Timestamp('2026-08-20').value//10**6); T1=int(pd.Timestamp('2026-09-30').value//10**6); N=(T1-T0)//60000
syms=[s for s in sorted(os.listdir(K)) if s not in ('BTCUSDT','ETHUSDT')]
C=np.full((len(syms),N),np.nan); H=C.copy(); QV=np.zeros((len(syms),N))
for i,s in enumerate(syms):
    for fn in os.listdir(K+s):
        try: z=np.load(K+s+"/"+fn)
        except Exception: continue
        j=(z['ot']-T0)//60000; m=(j>=0)&(j<N)
        C[i,j[m]]=z['c'][m]; H[i,j[m]]=z['h'][m]; QV[i,j[m]]=z['qv'][m]
lr=np.diff(np.log(C),axis=1,prepend=np.nan)
idx=np.nancumsum(np.nanmean(np.where(np.isfinite(lr),lr,np.nan),axis=0))   # EW alt log index
# whale activity per coin-day (from 12.09): any whale SELL close (Close Long) with tx_usd>=20k within [t-5min,t]
SYM=json.load(open(SP+'infra/symmap.json'))
w=pd.read_parquet(SP+'data/whale_txs.parquet',columns=['coin','ts','dir','tx_usd','liq'])
w=w[(w.tx_usd>=20000)&(w.dir=='Close Long')]; w['sym']=w.coin.map(SYM); w=w[w.sym.isin(syms)]
wm={s:np.zeros(N,bool) for s in syms}
for s,g in w.groupby('sym'):
    j=((g.ts.values-T0)//60000); j=j[(j>=0)&(j<N)]
    a=wm[s]
    for k in range(0,6): a[np.clip(j+k,0,N-1)]=True     # whale sold in this or previous 5 minutes
wday={}
for s,g in w.groupby('sym'):
    wday[s]=set(((g.ts.values-T0)//86400000).tolist())
rows=[]
for i,s in enumerate(syms):
    c=C[i]; h=H[i]
    r3=c/np.roll(c,3)-1; r3[:3]=np.nan
    busy=-1
    for X in (1.5,):
        pass
    cand=np.where(r3<=-0.015)[0]
    for t in cand:
        if t<=busy or t+62>=N: continue
        e_close=c[t+1]; e_worst=h[t+1]
        if not(np.isfinite(e_close) and np.isfinite(e_worst)): continue
        out={}
        for Hm in (30,60):
            x=c[t+1+Hm]
            if not np.isfinite(x): out=None; break
            out[f'g{Hm}']=100*(x/e_close-1); out[f'gw{Hm}']=100*(x/e_worst-1)
            out[f'alt{Hm}']=100*(np.exp(idx[t+1+Hm]-idx[t+1])-1)
        if out is None: continue
        busy=t+61
        day=(t*60000)//86400000
        qv60=np.nansum(QV[i,max(0,t-1440):t])/1440
        rows.append(dict(sym=s,t=t,day=pd.Timestamp(T0+t*60000,unit='ms').strftime('%m-%d'),r3=100*r3[t],whale5=wm[s][t],
                         wday=day in wday.get(s,set()),qv_min=qv60,**out))
D=pd.DataFrame(rows)
D['per']=np.where(D.day<'09-12','EARLY',np.where(D.day<='09-21','DISC','TEST'))
D.to_parquet(SP+'work/synthesis/dumpfade_events.parquet')
def cb(x,cl,n=3000,seed=0):
    x=np.asarray(x,float); u,inv=np.unique(np.asarray(cl),return_inverse=True); s=np.bincount(inv,weights=x); k=np.bincount(inv); r=np.random.default_rng(seed)
    m=[s[p].sum()/k[p].sum() for p in (r.integers(0,len(u),len(u)) for _ in range(n))]; return np.percentile(m,[5,95])
def rep(d,lab):
    for per in ('EARLY','DISC','TEST'):
        g=d[d.per==per]
        if len(g)<10: print(f'{lab:40s} {per} n={len(g)}'); continue
        for col,cost in (('g60',0.146+0.05),('gw60',0.146),):
            y=g[col]-cost; ya=y-g.alt60
            cd=cb(y,g.day); cs=cb(y,g.sym)
            print(f'{lab:40s} {per:5s} n={len(g):5d} {col:4s} net={y.mean():+.3f} med={y.median():+.3f} CId[{cd[0]:+.2f},{cd[1]:+.2f}] CIs[{cs[0]:+.2f},{cs[1]:+.2f}] altneutral={ya.mean():+.3f} /day={len(g)/g.day.nunique():.1f}')
print('events',len(D))
for X in (1.5,2.0,3.0):
    d=D[D.r3<=-X]
    rep(d,f'dump>={X} all coins')
d=D[D.r3<=-1.5]
rep(d[d.day>='09-12'][~d[d.day>='09-12'].wday],'dump>=1.5, coin-day WITHOUT whale sells')
rep(d[d.wday],'dump>=1.5, coin-day WITH whale sells')
rep(d[d.whale5],'dump>=1.5, whale sold in last 5 min')
lo=d.qv_min<d.qv_min.quantile(0.5)
rep(d[lo],'dump>=1.5, low-volume half')
rep(d[~lo],'dump>=1.5, high-volume half')

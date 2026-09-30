"""Skeptic ms-level study on raw aggTrades (own download, 281 symbol-days = all days with an anchor rel24>=2 or td>=0.3).
All anchors on those symbol-days + 3 placebo times each (same symbol/day/wdir, >=120 s from any >=$1k tx of the coin).
Signed log-ret x100 in whale dir vs base = last trade strictly before t0 (t0 = HL fill ts, ms)."""
import numpy as np, pandas as pd, os, sys
from multiprocessing import Pool
OUT='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-impulse-latency/'
PATH=[-5000,-2000,-1000,-500,-250,-100,-50,0,50,100,150,200,250,300,400,500,750,1000,2000,5000,10000,60000,300000]
LAT=[0,50,100,150,200,250,300,400,500,750,1000,1500,2000,3000,5000]
HOR=[30,60,300,600]
def work(args):
    sym,day,ev=args
    z=np.load(f'{OUT}agg/{sym}_{day}.npz'); t=z['t']; p=z['p']; bm=z['bm']; q=z['q']
    # add next day's first hour for long horizons? only needed for H<=600s near midnight: load next day if exists
    nd=(pd.Timestamp(day)+pd.Timedelta(days=1)).strftime('%Y-%m-%d'); fn=f'{OUT}agg/{sym}_{nd}.npz'
    if os.path.exists(fn):
        z2=np.load(fn); k=z2['t']<t[-1]+900000+86400000
        k=z2['t']<(int(pd.Timestamp(nd).timestamp())*1000+900000)
        t=np.concatenate([t,z2['t'][k]]); p=np.concatenate([p,z2['p'][k]]); bm=np.concatenate([bm,z2['bm'][k]]); q=np.concatenate([q,z2['q'][k]])
        end=t[-1]
    else:
        end=int(pd.Timestamp(nd).timestamp())*1000  # no data beyond midnight
    lp=np.log(p); buy=~bm   # buy=True: buyer is taker (lifts ask)
    tb,pb=t[buy],p[buy]; ts_,ps_=t[bm],p[bm]; qb=(q*p)[buy]; qs=(q*p)[bm]
    cqb=np.concatenate([[0],np.cumsum(qb)]); cqs=np.concatenate([[0],np.cumsum(qs)])
    cpb=np.concatenate([[0],np.cumsum(qb*pb)]); cps=np.concatenate([[0],np.cumsum(qs*ps_)])
    rows=[]
    for r in ev.itertuples():
        t0=int(r.t0); w=int(r.wdir)
        ib=np.searchsorted(t,t0,'left')-1
        if ib<0: continue
        base=lp[ib]; o={'key':r.key,'kind':r.kind,'t0':t0,'wdir':w,'age':t0-t[ib]}
        for off in PATH:
            j=np.searchsorted(t,t0+off,'right')-1
            o[f'o{off}']=w*(lp[j]-base)*100 if (j>=0 and t0+off<=end) else np.nan
        # our side (trade in whale dir): w>0 we buy -> buyer-taker trades
        ot,op,cq,cp=(tb,pb,cqb,cpb) if w>0 else (ts_,ps_,cqs,cps)
        xt,xp=(ts_,ps_) if w>0 else (tb,pb)
        for L in LAT:
            k=np.searchsorted(ot,t0+L,'left')
            o[f'a{L}']=w*(np.log(op[k])-base)*100 if (k<len(ot) and ot[k]-(t0+L)<10000) else np.nan
            k2=np.searchsorted(ot,t0+L+500,'left')
            if k2>k:
                vw=(cp[k2]-cp[k])/(cq[k2]-cq[k]); o[f'v{L}']=w*(np.log(vw)-base)*100
            else: o[f'v{L}']=o[f'a{L}']
            j0=np.searchsorted(t,t0+L,'left'); j1=np.searchsorted(t,t0+L+1000,'right')
            if j1>j0:
                ww=p[j0:j1].max() if w>0 else p[j0:j1].min(); o[f'w{L}']=w*(np.log(ww)-base)*100
            else: o[f'w{L}']=o[f'a{L}']
        for H in HOR:
            th=t0+H*1000
            if th+3000>end: 
                o[f'x{H}']=np.nan; o[f'xw{H}']=np.nan; continue
            k=np.searchsorted(xt,th,'left')
            o[f'x{H}']=w*(np.log(xp[k])-base)*100 if (k<len(xt) and xt[k]-th<10000) else np.nan
            j0=np.searchsorted(t,th,'left'); j1=np.searchsorted(t,th+3000,'right')
            if j1>j0:
                ww=p[j0:j1].min() if w>0 else p[j0:j1].max(); o[f'xw{H}']=w*(np.log(ww)-base)*100
            else: o[f'xw{H}']=np.nan
        j2=np.searchsorted(t,t0+2000,'right')-1; m2=w*(lp[j2]-base)*100; o['m2s']=m2
        if m2>=0.05:
            js=np.searchsorted(t,t0-2000,'left'); seg=w*(lp[js:j2+1]-base)*100
            for fr,nm in ((0.1,'t10'),(0.5,'t50'),(0.9,'t90')):
                h=np.where(seg>=fr*m2)[0]; o[nm]=t[js+h[0]]-t0 if len(h) else np.nan
        o['n_pre1s']=np.searchsorted(t,t0)-np.searchsorted(t,t0-1000); o['n_post1s']=np.searchsorted(t,t0+1000)-np.searchsorted(t,t0)
        rows.append(o)
    return pd.DataFrame(rows)
def main():
    A=pd.read_parquet(OUT+'anchors.parquet'); T=pd.read_parquet(OUT+'txs1k.parquet')
    have=set(f[:-4] for f in os.listdir(OUT+'agg'))
    A['sd']=A.symbol+'_'+A.day
    E=A[A.sd.isin(have)].copy()
    rng=np.random.default_rng(11)
    jobs=[]
    for sd,g in E.groupby('sd'):
        sym,day=sd.rsplit('_',1)
        D0=int(pd.Timestamp(day).timestamp())
        ev=pd.DataFrame({'key':g.ep.values,'t0':g.ts.values,'wdir':g.wdir.values,'kind':'ev'})
        s=T[(T.symbol==sym)&(T.ts>=(D0-200)*1000)&(T.ts<(D0+86600)*1000)].ts.values//1000
        ok=np.ones(86400-700,bool)   # leave last ~12 min so H<=600 fits
        for x in np.unique(s):
            a=max(0,x-D0-120); b=min(len(ok),x-D0+121)
            if b>a: ok[a:b]=False
        cand=np.where(ok)[0]
        pls=[]
        for rep in range(3):
            ps=(D0+rng.choice(cand,len(g)))*1000+rng.integers(0,1000,len(g))
            pls.append(pd.DataFrame({'key':g.ep.values,'t0':ps,'wdir':g.wdir.values,'kind':f'p{rep}'}))
        jobs.append((sym,day,pd.concat([ev]+pls)))
    print('jobs',len(jobs),'anchors',len(E),flush=True)
    with Pool(3) as pool:
        outs=list(pool.imap_unordered(work,jobs,chunksize=2))
    R=pd.concat(outs); R.to_parquet(OUT+'ms_s.parquet'); print(R.shape, R.kind.value_counts().to_dict())
if __name__=='__main__': main()

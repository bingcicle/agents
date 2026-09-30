import numpy as np, pandas as pd
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/p2-fastmove/'
COST=0.246; COST_LO=0.15; COST_HI=0.346; HEDGE=0.10
SPLIT=int(pd.Timestamp('2026-07-01').value//10**6)
def load():
    d=pd.read_parquet(W+'trades.parquet')
    d['net']=d.gross-COST; d['nnet']=d.neutral-COST-HEDGE
    dt=pd.to_datetime(d.ts,unit='ms')
    d['month']=dt.dt.strftime('%m'); d['week']=dt.dt.strftime('%G-%V'); d['day']=dt.dt.strftime('%m-%d')
    d['per']=np.where(d.ts<SPLIT,'TRAIN','TEST')
    return d
def cci(x,cl,B=1000,seed=0):
    """cluster bootstrap CI90 of mean; returns lo,hi"""
    x=np.asarray(x,float); cl=pd.factorize(np.asarray(cl))[0]
    k=cl.max()+1; S=np.bincount(cl,x,k); n=np.bincount(cl,minlength=k).astype(float)
    rng=np.random.default_rng(seed); w=rng.multinomial(k,np.ones(k)/k,size=B).astype(float)
    m=(w@S)/(w@n); return float(np.percentile(m,5)),float(np.percentile(m,95))
def stats(g,col='net'):
    x=g[col].values
    if len(x)==0: return {}
    lo,hi=cci(x,g.sym); wlo,whi=cci(x,g.week)
    xs=np.sort(x)[::-1]; k=max(1,int(round(len(x)*.1)))
    pos=g[g[col]>0].groupby('sym')[col].sum()
    return dict(n=len(x),mean=x.mean(),med=np.median(x),win=(x>0).mean(),ci_coin=(round(lo,3),round(hi,3)),ci_week=(round(wlo,3),round(whi,3)),
                sum_wo_top10=xs[k:].sum(),q1=np.percentile(x,1),q5=np.percentile(x,5),mn=x.min(),mx=x.max(),maxshare_coin=pos.max()/pos.sum() if pos.sum()>0 else np.nan)

import numpy as np, pandas as pd
def cb(v,cl,n=3000,seed=0):
    v=np.asarray(v,float); u,inv=np.unique(np.asarray(cl),return_inverse=True); k=len(u)
    s=np.bincount(inv,v,k); c=np.bincount(inv,minlength=k); rng=np.random.default_rng(seed); p=rng.integers(0,k,(n,k))
    b=s[p].sum(1)/c[p].sum(1); return (round(np.percentile(b,5),2),round(np.percentile(b,95),2))
def add(D):
    ts=pd.to_datetime(D.t,unit='ms'); D=D.copy()
    D['month']=ts.dt.month; D['week']=ts.dt.strftime('%G%V'); D['day']=ts.dt.strftime('%m%d')
    D['per']=np.where(ts<pd.Timestamp('2026-07-01'),'TRAIN','TEST'); return D
def sm(x,col='y'):
    v=x[col].dropna().values; x=x[x[col].notna()]
    if len(v)<3: return dict(n=len(v))
    s=np.sort(v)[::-1]; k=int(round(len(v)*.1))
    return dict(n=len(v),mean=round(v.mean(),2),med=round(np.median(v),2),ciC=cb(v,x.sym),ciW=cb(v,x.week),ciD=cb(v,x.day),
                wo10=round(s[k:].sum(),1),win=round((v>0).mean(),2))

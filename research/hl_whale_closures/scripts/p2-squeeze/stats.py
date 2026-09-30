import numpy as np, pandas as pd
def cboot(v, cl, n=2000, seed=0, stat=np.mean):
    v=np.asarray(v,float); cl=np.asarray(cl)
    u,inv=np.unique(cl,return_inverse=True); k=len(u)
    if k<3: return (np.nan,np.nan)
    order=np.argsort(inv); vs=v[order]; cnt=np.bincount(inv,minlength=k); st_=np.concatenate([[0],np.cumsum(cnt)])
    sums=np.array([vs[st_[j]:st_[j+1]].sum() for j in range(k)])
    rng=np.random.default_rng(seed); pick=rng.integers(0,k,(n,k))
    if stat is np.mean:
        b=sums[pick].sum(1)/cnt[pick].sum(1)
    else:
        groups=[vs[st_[j]:st_[j+1]] for j in range(k)]
        b=np.array([stat(np.concatenate([groups[j] for j in p])) for p in pick[:400]])
    return (round(float(np.percentile(b,5)),2), round(float(np.percentile(b,95)),2))
def prep(T):
    T=T.copy(); ts=pd.to_datetime(T.t,unit='ms')
    T['month']=ts.dt.strftime('%m'); T['week']=ts.dt.strftime('%G-%V'); T['day']=ts.dt.strftime('%m-%d')
    T['per']=np.where(ts<pd.Timestamp('2026-07-01'),'TRAIN','TEST')
    T['f0']=T.fund.fillna(0)
    T['y']=T.ew-0.246-T.f0            # primary: EW hedge, costs 0.146+0.10, funding
    T['y_nf']=T.ew-0.246
    T['y_btc']=T.btc-0.246-T.f0
    T['y_raw']=T.raw-0.146-T.f0
    T['y_sl10']=T.ew_sl10-0.246-T.f0; T['y_sl20']=T.ew_sl20-0.246-T.f0
    return T
def summ(d,col='y',full=True):
    v=d[col].values
    if len(v)<3: return dict(n=len(v))
    r=dict(n=len(v),mean=round(v.mean(),2),med=round(float(np.median(v)),2),win=round((v>0).mean(),2))
    if full:
        r['ciC']=cboot(v,d.sym); r['ciW']=cboot(v,d.week)
        xs=np.sort(v)[::-1]; k=max(1,int(round(len(v)*.1))); r['wo10']=round(xs[k:].sum(),1); r['sum']=round(v.sum(),1)
        r['ncoin']=d.sym.nunique(); r['topcoin_share']=round(d.groupby('sym')[col].sum().clip(lower=0).max()/max(1e-9,d.groupby('sym')[col].sum().clip(lower=0).sum()),2)
    return r

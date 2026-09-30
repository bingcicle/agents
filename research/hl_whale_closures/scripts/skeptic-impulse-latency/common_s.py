import numpy as np, pandas as pd, os
OUT='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/skeptic-impulse-latency/'
_alt={}
def altmat(day):
    if day not in _alt:
        z=np.load(OUT+'alt1s/'+day+'.npz'); _alt[day]=(list(z['syms']),int(z['base']),z['M'])
    return _alt[day]
def alt_ret(sym,day,s1,s2):
    """equal-weight mean log-ret x100 over other symbols from end of second s1 to end of second s2 (arrays)"""
    syms,base,M=altmat(day)
    keep=[i for i,s in enumerate(syms) if s!=sym]
    MM=M[keep]
    i1=np.clip(np.asarray(s1)-base,0,M.shape[1]-1); i2=np.clip(np.asarray(s2)-base,0,M.shape[1]-1)
    with np.errstate(all='ignore'):
        r=np.nanmean(MM[:,i2]-MM[:,i1],axis=0)*100
    return r
def add_alt(df,s1col,s2col,out='alt'):
    df[out]=np.nan
    for (sym,day),g in df.groupby(['symbol','day']):
        df.loc[g.index,out]=alt_ret(sym,day,g[s1col].values.astype(np.int64),g[s2col].values.astype(np.int64))
    return df
I1=pd.read_parquet(OUT+'idx1m.parquet')
def btc_ret(ms1,ms2):
    t=I1.index.values.astype(float); v=I1.btc.values
    return (np.interp(ms2,t,v)-np.interp(ms1,t,v))*100
def boot(v,cl,stat=np.mean,n=2000,seed=0):
    v=np.asarray(v,float); cl=np.asarray(cl)
    u,inv=np.unique(cl,return_inverse=True); k=len(u)
    rng=np.random.default_rng(seed)
    # per-cluster sums and counts for mean; for median need full resample
    if stat is np.mean:
        s=np.bincount(inv,weights=v,minlength=k); c=np.bincount(inv,minlength=k)
        W=rng.integers(0,k,(n,k))
        st=s[W].sum(1)/c[W].sum(1)
    else:
        groups=[v[inv==i] for i in range(k)]
        st=np.array([stat(np.concatenate([groups[i] for i in rng.integers(0,k,k)])) for _ in range(n)])
    return np.percentile(st,5),np.percentile(st,95),(st<=0).mean()
def battery(d,col,label,wal='addr',extra=None):
    x=d[d[col].notna()]
    v=x[col].values
    if len(v)==0: return f'{label}: n=0'
    top=np.sort(v)[::-1]; k=max(1,int(round(len(v)*0.1)))
    s=[f'{label}: n={len(v)} days={x.day.nunique()} wal={x[wal].nunique()} coins={x.coin.nunique()} mean={v.mean():+.3f} med={np.median(v):+.3f} win={(v>0).mean():.2f} sum={v.sum():+.1f} sum_wo_top10={top[k:].sum():+.1f}']
    for c in (wal,'day','coin'):
        lo,hi,p=boot(v,x[c].values); pos=x[x[col]>0].groupby(c)[col].sum()
        s.append(f'   CI90 mean by {c}: ({lo:+.3f},{hi:+.3f}) P<=0 {p:.2f}; max share of pos sum {pos.max()/pos.sum():.2f}')
    ds=sorted(x.day.unique()); h1=x[x.day.isin(ds[:len(ds)//2])][col].mean(); h2=x[x.day.isin(ds[len(ds)//2:])][col].mean()
    s.append(f'   halves by day: {h1:+.3f} / {h2:+.3f}')
    return '\n'.join(s)

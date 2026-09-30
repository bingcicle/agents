import numpy as np, pandas as pd, os, json, functools
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
SYM=json.load(open(SP+'infra/symmap.json'))
def cboot(x, cl, stat=np.mean, n=4000, seed=7):
    x=np.asarray(x,float); cl=np.asarray(cl)
    u,inv=np.unique(cl,return_inverse=True); G=[x[inv==i] for i in range(len(u))]; k=len(G)
    rng=np.random.default_rng(seed); out=np.empty(n)
    for b in range(n):
        out[b]=stat(np.concatenate([G[i] for i in rng.integers(0,k,k)]))
    return np.percentile(out,5), np.percentile(out,95), (out<=0).mean()
def signflip(g, cl, cost, n=20000, seed=3, stat=np.mean):
    """null: gross has no directional info -> flip sign of gross per cluster; returns P(stat(net_null) >= stat(net_obs))."""
    g=np.asarray(g,float); cl=np.asarray(cl); cost=np.asarray(cost,float)
    u,inv=np.unique(cl,return_inverse=True); rng=np.random.default_rng(seed)
    obs=stat(g-cost); cnt=0
    for b in range(n):
        s=rng.choice([-1,1],len(u))[inv]
        if stat(s*g-cost)>=obs-1e-12: cnt+=1
    return cnt/n
@functools.lru_cache(maxsize=None)
def k1m(sym):
    d=SP+'data/k1m/'+sym+'/'
    if not os.path.isdir(d): return None
    P=[]
    for fn in sorted(os.listdir(d)):
        z=np.load(d+fn); P.append(pd.DataFrame({'o':z['o'],'c':z['c']},index=z['ot']))
    s=pd.concat(P).sort_index(); return s[~s.index.duplicated()]
def px_open_at(sym, ms):
    """open price of the minute containing ms (approx price at ms, known at the minute start) -> use close of previous minute."""
    k=k1m(sym)
    if k is None: return np.nan
    m=(int(ms)//60000)*60000
    i=k.index.searchsorted(m,side='left')
    if i>=len(k) or k.index[i]!=m: return np.nan
    return float(k.o.iloc[i])
def ret(sym, t0, t1, sign):
    a=px_open_at(sym,t0); b=px_open_at(sym,t1)
    return sign*100*(b/a-1) if np.isfinite(a) and np.isfinite(b) else np.nan
@functools.lru_cache(maxsize=1)
def alts():
    return [s for s in sorted(os.listdir(SP+'data/k1m/')) if s!='BTCUSDT']
def altidx(t0,t1,sign):
    r=[]
    for s in alts():
        v=ret(s,t0,t1,1)
        if np.isfinite(v): r.append(v)
    return sign*np.mean(r) if len(r)>=20 else np.nan

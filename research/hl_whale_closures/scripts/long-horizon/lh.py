"""long-horizon lens helpers: minute price matrix, EW alt index, funding, returns."""
import os, json, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/long-horizon/'
T0=1787184000000           # 2026-08-20 00:00 UTC
NMIN=41*1440               # through 2026-09-29 23:59
SYM=json.load(open(SP+'infra/symmap.json'))
DISC_END=pd.Timestamp('2026-09-22').value//10**6   # discovery < this
TEST_END=pd.Timestamp('2026-09-30').value//10**6

def build_matrix():
    fn=W+'pxmat.npz'
    if os.path.exists(fn):
        z=np.load(fn,allow_pickle=True); return z['syms'].tolist(), z['C'], z['QV']
    syms=sorted(os.listdir(SP+'data/k1m'))
    C=np.full((NMIN,len(syms)),np.nan,np.float64); QV=np.zeros((NMIN,len(syms)),np.float32)
    for j,s in enumerate(syms):
        d=SP+'data/k1m/'+s+'/'
        for f in sorted(os.listdir(d)):
            z=np.load(d+f); i=((z['ot']-T0)//60000).astype(int); ok=(i>=0)&(i<NMIN)
            C[i[ok],j]=z['c'][ok]; QV[i[ok],j]=z['qv'][ok]
    C=pd.DataFrame(C).ffill().values   # ffill gaps (no trade minutes)
    np.savez(fn,syms=np.array(syms),C=C,QV=QV)
    return syms,C,QV

SYMS,C,QV=build_matrix()
SIDX={s:i for i,s in enumerate(SYMS)}
LC=np.log(C)
# equal-weight alt index (all symbols except BTC), from mean of 1m log returns over available symbols
_r=np.diff(LC,axis=0); _alt=[i for i,s in enumerate(SYMS) if s!='BTCUSDT']
_m=np.nanmean(_r[:,_alt],axis=1); _m=np.nan_to_num(_m)
IDX=np.concatenate([[0.0],np.cumsum(_m)])      # log index level
BTC=LC[:,SIDX['BTCUSDT']]

def coin_sym(coin):
    s=SYM.get(coin); return s if s in SIDX else None

def minute_of(ms):
    return ((np.asarray(ms,dtype=np.int64)-T0)//60000).astype(np.int64)

def entry_minute(bot_ms):
    """index of the 1m candle whose CLOSE is the entry price: first candle that closes at least 60 s after bot knew.
    candle i covers [T0+i*60s, T0+(i+1)*60s); its close ~ price at T0+(i+1)*60s. Need close time >= bot_ms+60s."""
    bot_ms=np.asarray(bot_ms,dtype=np.float64)
    return np.ceil((bot_ms+60000-T0)/60000).astype(np.int64)-1

def logret(j, i0, i1):
    """log returns (in %) of symbol index j between candle closes i0 and i1 (vectorized). NaN if out of range."""
    j=np.asarray(j); i0=np.asarray(i0); i1=np.asarray(i1)
    ok=(i0>=0)&(i1<NMIN)&(i0<NMIN)&(i1>=0)
    out=np.full(len(j),np.nan)
    out[ok]=100*(np.exp(LC[i1[ok],j[ok]]-LC[i0[ok],j[ok]])-1)
    return out

def idxret(i0,i1,which='alt'):
    L=IDX if which=='alt' else BTC
    i0=np.asarray(i0); i1=np.asarray(i1); ok=(i0>=0)&(i1<NMIN)
    out=np.full(len(i0),np.nan); out[ok]=100*(np.exp(L[i1[ok]]-L[i0[ok]])-1); return out

# ---------------- funding (HL hourly, proxy for Binance)
_F={}
def _fund(coin):
    if coin in _F: return _F[coin]
    fn=W+'funding/'+coin+'.json'
    if not os.path.exists(fn):
        fn=SP+'data/hl/'+coin+'_funding.json'
    if not os.path.exists(fn): _F[coin]=None; return None
    r=json.load(open(fn))
    if not r: _F[coin]=None; return None
    t=np.array([x['time'] for x in r],np.int64); f=np.array([float(x['fundingRate']) for x in r])
    o=np.argsort(t); t=t[o]; f=f[o]
    _F[coin]=(t,np.concatenate([[0],np.cumsum(f)]),np.median(f),t[0],t[-1]); return _F[coin]

def funding_pct(coin, t0_ms, t1_ms):
    """sum of hourly funding rates (in %) paid by a LONG over (t0,t1]; for hours outside the fetched range, use the coin median rate."""
    F=_fund(coin); t0=np.asarray(t0_ms,np.int64); t1=np.asarray(t1_ms,np.int64)
    if F is None: return np.full(len(t0),np.nan)
    t,cs,med,ta,tb=F
    a=np.searchsorted(t,t0,side='right'); b=np.searchsorted(t,t1,side='right')
    s=cs[b]-cs[a]
    # missing hours (beyond coverage)
    miss=np.clip((t1-np.maximum(t0,tb))/3.6e6,0,None)+np.clip((np.minimum(t1,ta)-t0)/3.6e6,0,None)
    return 100*(s+miss*med)

def cluster_ci(x, cl, stat=np.mean, n=2000, seed=0):
    x=np.asarray(x,float); ok=~np.isnan(x); x=x[ok]; cl=np.asarray(cl)[ok]
    if len(x)==0: return (np.nan,np.nan,np.nan)
    codes,inv=np.unique(cl,return_inverse=True); k=len(codes)
    rng=np.random.default_rng(seed)
    # precompute cluster sums and counts for mean; for median fall back to concat
    if stat is np.mean:
        s=np.bincount(inv,weights=x,minlength=k); c=np.bincount(inv,minlength=k)
        W_=rng.multinomial(k,np.ones(k)/k,size=n)   # n x k counts
        st=(W_@s)/(W_@c)
    else:
        groups=[x[inv==i] for i in range(k)]; st=[]
        for _ in range(n):
            p=rng.integers(0,k,k); st.append(stat(np.concatenate([groups[i] for i in p])))
        st=np.array(st)
    return float(np.percentile(st,5)),float(np.percentile(st,95)),float((st<=0).mean())

def honest(x, df, cols=('coin','day','addr'), n=2000):
    x=np.asarray(x,float); ok=~np.isnan(x); xx=x[ok]
    if len(xx)==0: return 'n=0'
    top=np.sort(xx)[::-1]; k=max(1,int(round(len(xx)*0.1)))
    s=f'n={len(xx)} med={np.median(xx):+.3f} mean={xx.mean():+.3f} win={100*(xx>0).mean():.0f}% sum={xx.sum():+.1f} sum_wo_top10={top[k:].sum():+.1f}'
    for c in cols:
        if c in df:
            lo,hi,p=cluster_ci(x,df[c].values,n=n); s+=f' | CI90mean_{c}=[{lo:+.3f},{hi:+.3f}] P<=0={p:.2f}'
            pos=pd.Series(xx[xx>0]).groupby(np.asarray(df[c].values)[ok][xx>0]).sum()
            if pos.sum()>0: s+=f' maxshare_{c}={pos.max()/pos.sum():.2f}'
    return s

def summary(x, label=''):
    x=np.asarray(x,float); x=x[~np.isnan(x)]
    if len(x)==0: return f'{label} n=0'
    top=np.sort(x)[::-1]; k=max(1,int(round(len(x)*0.1)))
    return f'{label} n={len(x)} med={np.median(x):+.3f} mean={x.mean():+.3f} win={100*(x>0).mean():.0f}% sum={x.sum():+.1f} sum_wo_top10={top[k:].sum():+.1f}'

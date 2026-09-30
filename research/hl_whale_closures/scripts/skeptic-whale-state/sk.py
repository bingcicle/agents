# Skeptic helpers: outcomes from my own minute matrix, cluster bootstrap, battery.
import os, json, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
WD=SP+'work/skeptic-whale-state/'
T0=1787184000000
Z=np.load(WD+'pm.npz'); SYMS=list(Z['syms']); C=Z['C']; H=Z['H']; L=Z['L']
# ffill small gaps (<=5 min) for close
C=pd.DataFrame(C.T).ffill(limit=5).values.T
SI={s:i for i,s in enumerate(SYMS)}; IB=SI['BTCUSDT']; IE=SI.get('ETHUSDT')
SYM=json.load(open(SP+'infra/symmap.json'))
NM=C.shape[1]
ALT=np.array([i for i,s in enumerate(SYMS) if s not in ('BTCUSDT',)])
COST=0.146
def m_of_close_at(t):   # index m whose candle CLOSES at time t (t multiple of 60000): open = t-60000
    return ((np.asarray(t)-60000-T0)//60000).astype(np.int64)
def entry_time(dec_ms, delay_min=0):
    # end of the first full minute after dec (same timing convention as lens, re-derived): ceil(dec/60s)*60s + 60s
    return (np.ceil(np.asarray(dec_ms)/60000.0)*60000).astype(np.int64)+60000+delay_min*60000
_fund={}
def fund_series(coin):
    if coin in _fund: return _fund[coin]
    fn=SP+f'work/long-horizon/funding/{coin}.json'
    if not os.path.exists(fn): _fund[coin]=None; return None
    d=json.load(open(fn)); t=np.array([x['time'] for x in d],np.int64); r=np.array([float(x['fundingRate']) for x in d])
    o=np.argsort(t); _fund[coin]=(t[o],np.cumsum(r[o])); return _fund[coin]
def outcomes(df, side, hours=24, delay_min=0):
    """df: symbol, coin, dec_ms. side: array +1 LONG/-1 SHORT (OUR side). Returns DataFrame of our-side % returns:
    raw, mn_ew (minus buy&hold EW basket of all alts ex BTC & coin), btc (minus BTC), fund (% paid by us), worst-entry adj."""
    te=entry_time(df.dec_ms.values, delay_min); tx=te+int(hours*3600e3)
    me=m_of_close_at(te); mx=m_of_close_at(tx)
    k=df.symbol.map(SI).values
    n=len(df); out={c:np.full(n,np.nan) for c in ('raw','ew','btc','fund','slip_in')}
    for i in range(n):
        if pd.isna(k[i]) or mx[i]>=NM or me[i]<0: continue
        ki=int(k[i]); pe=C[ki,me[i]]; px=C[ki,mx[i]]
        if not (pe>0 and px>0): continue
        r=px/pe-1
        a=C[ALT,mx[i]]/C[ALT,me[i]]-1; msk=np.isfinite(a)&(ALT!=ki); ew=a[msk].mean()
        b=C[IB,mx[i]]/C[IB,me[i]]-1
        s=side[i]
        out['raw'][i]=100*s*r; out['ew'][i]=100*s*(r-ew); out['btc'][i]=100*s*(r-b)
        # worst price within the entry minute vs its close (extra slippage if you had to pay the extreme)
        hi=H[ki,me[i]]; lo=L[ki,me[i]]
        out['slip_in'][i]=100*((hi/pe-1) if s>0 else (1-lo/pe))
        fs=fund_series(df.coin.values[i])
        if fs is not None:
            ta,cs=fs
            if ta[0]<=te[i] and ta[-1]>=tx[i]-3600e3:
                a0=np.searchsorted(ta,te[i],'right')-1; a1=np.searchsorted(ta,tx[i],'right')-1
                out['fund'][i]=100*s*(cs[a1]-cs[a0])   # positive = we PAY
    return pd.DataFrame(out,index=df.index)
def cboot(v, cl, stat=np.mean, n=2000, seed=0):
    v=np.asarray(v,float); cl=np.asarray(cl)
    u,inv=np.unique(cl,return_inverse=True); k=len(u)
    groups=[v[inv==j] for j in range(k)]
    rng=np.random.default_rng(seed); st=np.empty(n)
    for b in range(n):
        pick=rng.integers(0,k,k); st[b]=stat(np.concatenate([groups[j] for j in pick]))
    return (round(float(np.percentile(st,5)),2), round(float(np.percentile(st,95)),2))
def bat(x, col='y', short=False):
    v=x[col].values
    if len(v)<3: return f'n={len(v)}'
    s=f'n={len(v)} mean={v.mean():+.2f} med={np.median(v):+.2f} win={(v>0).mean():.2f}'
    if short: return s
    s+=f' ciC={cboot(v,x.coin)} ciD={cboot(v,x.day)} ciW={cboot(v,x.addr)}'
    days=sorted(x.day.unique()); h=len(days)//2
    s+=f' h1/h2={x[x.day.isin(days[:h])][col].mean():+.2f}/{x[x.day.isin(days[h:])][col].mean():+.2f}'
    xs=np.sort(v)[::-1]; kk=max(1,int(round(len(v)*.1))); s+=f' sum={v.sum():+.1f} wo10={xs[kk:].sum():+.1f}'
    if 'side' in x:
        s+=f' L {int((x.side>0).sum())}:{x[x.side>0][col].mean():+.2f} S {int((x.side<0).sum())}:{x[x.side<0][col].mean():+.2f}'
    return s
def dedupe(x, hours):
    x=x.sort_values('dec_ms'); keep=[]; last={}
    for i,(k,t) in enumerate(zip(x.coin+'|'+x.side.astype(str), x.dec_ms)):
        if k not in last or t>=last[k]+hours*3600e3: keep.append(True); last[k]=t
        else: keep.append(False)
    return x[np.array(keep)]

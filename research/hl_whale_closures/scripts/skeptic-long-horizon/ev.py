"""evaluation helpers (skeptic)."""
import numpy as np, pandas as pd
from px import *
EARLY_END=int(pd.Timestamp('2026-09-12 18:00').value//10**6)
DISC_END=int(pd.Timestamp('2026-09-22').value//10**6)
HS=(1,2,4,8,12,24,48)

def union(dfs, gap_h=6, key=('coin','side')):
    B=pd.concat(dfs,ignore_index=True).sort_values('dec_ms').reset_index(drop=True)
    keep=[]; last={}
    for i,r in B.iterrows():
        k=tuple(r[c] for c in key)
        if k in last and r.dec_ms-last[k]<gap_h*3600e3: continue
        last[k]=r.dec_ms; keep.append(i)
    return B.loc[keep].reset_index(drop=True)

def attach(E, hs=HS, delay_s=60, beta=True):
    E=E.copy(); E['sym']=E.coin.map(sym_of); nos=E.sym.isna().sum()
    E=E[E.sym.notna()].copy()
    E['j']=E.sym.map(SI).astype(int)
    E['m0']=entry_min(E.dec_ms.values,delay_s)
    E['split']=np.where(E.dec_ms<EARLY_END,'early',np.where(E.dec_ms<DISC_END,'disc','test'))
    E['day']=pd.to_datetime(E.dec_ms,unit='ms').dt.strftime('%m-%d')
    j=E.j.values; m0=E.m0.values; s=E.side.values
    for L in (60,240,1440):
        E[f'pre{L}']=s*(sret(j,m0-L,m0)-ew_basket(m0-L,m0,j))
    if beta: E['beta']=beta_alt(j,m0)
    for h in hs:
        m1=m0+h*60; r=sret(j,m0,m1); a=ew_basket(m0,m1,j); b=sret(np.full(len(j),BTCJ),m0,m1)
        E[f'raw{h}']=s*r; E[f'mnA{h}']=s*(r-a); E[f'mnB{h}']=s*(r-b)
        E[f'mnM{h}']=s*(r-ew_median(m0,m1,j))
        E[f'lgA{h}']=s*100*(np.log1p(r/100)-np.log1p(a/100))
        if beta: E[f'mnb{h}']=s*(r-np.clip(np.nan_to_num(E.beta.values,nan=1.0),0,3)*a)
    return E

def cboot(x, cl, n=2000, seed=0, stat='mean'):
    x=np.asarray(x,float); ok=~np.isnan(x); x=x[ok]; cl=np.asarray(cl)[ok]
    u,inv=np.unique(cl,return_inverse=True); k=len(u); rng=np.random.default_rng(seed)
    if stat=='mean':
        s=np.bincount(inv,x,k); c=np.bincount(inv,minlength=k)
        W=rng.multinomial(k,np.ones(k)/k,size=n); st=(W@s)/(W@c)
    else:
        g=[x[inv==i] for i in range(k)]; st=np.array([np.median(np.concatenate([g[i] for i in rng.integers(0,k,k)])) for _ in range(n)])
    return np.percentile(st,5),np.percentile(st,95)

def line(v, col, cl=('coin','day','addr')):
    x=v[col].values.astype(float); ok=~np.isnan(x); xx=x[ok]
    if len(xx)<3: return f'n={len(xx)}'
    top=np.sort(xx)[::-1]; k=max(1,int(round(len(xx)*.1)))
    s=f'n={len(xx):3d} mean={xx.mean():+.2f} med={np.median(xx):+.2f} win={100*(xx>0).mean():.0f}% sd={xx.std():.1f} sum-top10%={top[k:].sum():+.0f}'
    for c in cl:
        lo,hi=cboot(x,v[c].values); s+=f' CI{c[0]}[{lo:+.2f},{hi:+.2f}]'
    return s

def placebo(E, h, col='mnA', nd=300, seed=7, window='split', match_pre=None):
    """random entry minutes in same coin & same hour-of-day & same side, within same split window (or +-3 days).
    returns null distribution of the mean across events."""
    rng=np.random.default_rng(seed)
    j=E.j.values; s=E.side.values; m0=E.m0.values
    b=dict(early=(ms_to_min(pd.Timestamp('2026-08-30').value//10**6),ms_to_min(EARLY_END)),
           disc=(ms_to_min(EARLY_END),ms_to_min(DISC_END)),test=(ms_to_min(DISC_END),NM))
    if window=='split':
        lo=np.array([b[x][0] for x in E.split]); hi=np.array([b[x][1] for x in E.split])-h*60-1
    else:
        lo=m0-3*1440; hi=np.minimum(m0+3*1440,NM-h*60-1)
    hod=m0%1440
    res=np.full((nd,len(E)),np.nan)
    for d in range(nd):
        # draw day offset, keep hour-of-day (+-30 min jitter)
        nday=np.maximum((hi-lo)//1440,1)
        mm=lo-(lo%1440)+rng.integers(0,nday+1)*1440+hod+rng.integers(-30,31,len(E))
        mm=np.where(mm<lo,mm+1440,mm); mm=np.where(mm>hi,mm-1440,mm)
        bad=(mm<lo)|(mm>hi); mm=np.clip(mm,0,NM-h*60-1)
        m1=mm+h*60; r=sret(j,mm,m1)
        if col=='mnA': r=r-ew_basket(mm,m1,j)
        elif col=='mnB': r=r-sret(np.full(len(j),BTCJ),mm,m1)
        v=s*r; v[bad]=np.nan; res[d]=v
    ok=~np.isnan(E[f'{col}{h}'].values)
    return np.nanmean(res[:,ok],axis=1)

"""Skeptic's own price infrastructure (independent of lens lh.py).
Minute grid: minute m covers [T0+m*60s, T0+(m+1)*60s); P[m] = close of that candle (price at end of minute).
Missing minutes: NaN in raw; forward-filled only within 30 minutes (longer gaps stay NaN -> event dropped & counted)."""
import os, json, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
MY=SP+'work/skeptic-long-horizon/'
T0=int(pd.Timestamp('2026-08-20').value//10**6)
NM=41*1440
SYMMAP=json.load(open(SP+'infra/symmap.json'))

def _build():
    fn=MY+'pm.npz'
    if os.path.exists(fn):
        z=np.load(fn,allow_pickle=True); return z['syms'].tolist(),z['P'],z['Q']
    syms=sorted(os.listdir(SP+'data/k1m'))
    P=np.full((NM,len(syms)),np.nan); Q=np.zeros((NM,len(syms)),np.float32)
    for j,s in enumerate(syms):
        for f in sorted(os.listdir(SP+'data/k1m/'+s)):
            z=np.load(SP+'data/k1m/'+s+'/'+f); m=(z['ot']-T0)//60000; ok=(m>=0)&(m<NM)
            P[m[ok],j]=z['c'][ok]; Q[m[ok],j]=z['qv'][ok]
    P=pd.DataFrame(P).ffill(limit=30).values
    np.savez(fn,syms=np.array(syms),P=P,Q=Q); return syms,P,Q

SYMS,P,Q=_build()
SI={s:i for i,s in enumerate(SYMS)}
BTCJ=SI['BTCUSDT']
ALTJ=np.array([i for i,s in enumerate(SYMS) if s!='BTCUSDT'])

def sym_of(coin):
    s=SYMMAP.get(coin); return s if s in SI else None

def ms_to_min(ms):
    return ((np.asarray(ms,np.int64)-T0)//60000).astype(np.int64)

def entry_min(dec_ms, delay_s=60):
    """first minute whose close time (T0+(m+1)*60s) >= dec_ms + delay_s."""
    return (np.ceil((np.asarray(dec_ms,float)+delay_s*1000-T0)/60000)-1).astype(np.int64)

def sret(j,m0,m1):
    """simple % return of symbol j between closes m0 and m1."""
    j=np.asarray(j); m0=np.asarray(m0); m1=np.asarray(m1)
    ok=(m0>=0)&(m1<NM)&(m1>=0)&(m0<NM)
    out=np.full(len(j),np.nan)
    out[ok]=100*(P[m1[ok],j[ok]]/P[m0[ok],j[ok]]-1)
    return out

def _altmat(m0,m1,excl):
    m0=np.asarray(m0); m1=np.asarray(m1); ok=(m0>=0)&(m1<NM)&(m0<NM)&(m1>=0)
    R=np.full((len(m0),len(ALTJ)),np.nan)
    if ok.any():
        R[ok]=P[m1[ok]][:,ALTJ]/P[m0[ok]][:,ALTJ]-1
    if excl is not None:
        ex=np.asarray(excl); pos=np.searchsorted(ALTJ,ex); hit=(pos<len(ALTJ))&(ALTJ[np.minimum(pos,len(ALTJ)-1)]==ex)
        R[np.where(hit)[0],pos[hit]]=np.nan
    return R

def ew_basket(m0,m1,excl=None):
    """equal-weight BUY-AND-HOLD basket of all alts (ex BTC, ex the traded coin) formed at m0: mean simple return to m1."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore'); return 100*np.nanmean(_altmat(m0,m1,excl),axis=1)

def ew_median(m0,m1,excl=None):
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore'); return 100*np.nanmedian(_altmat(m0,m1,excl),axis=1)

# hourly log-return panel for betas
_H=None
def hourly():
    global _H
    if _H is None:
        Ph=P[59::60]            # close of each hour
        _H=np.diff(np.log(Ph),axis=0)   # row h = return of hour h+1
    return _H

def beta_alt(j, m0, lookback_h=168):
    """beta of coin j hourly log return on EW alt mean over the previous lookback hours (strictly before m0)."""
    H=hourly(); ew=np.nanmean(H[:,ALTJ],axis=1); out=np.full(len(j),np.nan)
    for k in range(len(j)):
        hh=int(m0[k]//60)-1   # last complete hour index in H ~ hh-1
        lo=max(0,hh-lookback_h); y=H[lo:hh,j[k]]; x=ew[lo:hh]; ok=~np.isnan(y)&~np.isnan(x)
        if ok.sum()<48: continue
        xv=x[ok]-x[ok].mean(); out[k]=(xv*(y[ok]-y[ok].mean())).sum()/(xv*xv).sum()
    return out

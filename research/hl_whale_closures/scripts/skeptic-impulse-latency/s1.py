"""Skeptic: per-anchor 1s-bar metrics, own code. Signed log-ret x100 in whale dir (wdir).
p0 = last trade price strictly before the tx second (close of sec-1, ffilled).
m{k}: ffilled close at end of sec+k. Official worst-in-3s: buy->max high [s,s+2], sell->min low; fallback 10 s.
Placebo: 3 random seconds per anchor, same symbol/day/wdir, >=120 s from any >=$1k tx of that coin."""
import numpy as np, pandas as pd, os
from numpy.lib.stride_tricks import sliding_window_view as sw
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
OUT=SP+'work/skeptic-impulse-latency/'
B=SP+'data/bars1s/'
A=pd.read_parquet(OUT+'anchors.parquet')
T=pd.read_parquet(OUT+'txs1k.parquet')
MOFF=[-300,-120,-60,-10,-2,-1,0,1,2,3,5,10,30,60,120,300,600,1800,3600]
LAT=[1,2,3,5,10,60,300]
HOR=[30,60,300,600,1800,3600]
PRE=3700; POST=3800
def load(sym,day):
    fn=f'{B}{sym}/{day}.npz'
    if not os.path.exists(fn): return None
    z=np.load(fn); return z['sec'],z['h'],z['l'],z['c'],z['n']
def dense(sym,day):
    D0=int(pd.Timestamp(day).timestamp()); base=D0-PRE; N=PRE+86400+POST
    h=np.full(N,np.nan); l=np.full(N,np.nan); c=np.full(N,np.nan); n=np.zeros(N)
    got={}
    for dd in (-1,0,1):
        d2=(pd.Timestamp(day)+pd.Timedelta(days=dd)).strftime('%Y-%m-%d')
        r=load(sym,d2); got[dd]=r is not None
        if r is None: continue
        s,hh,ll,cc,nn=r; i=s-base; m=(i>=0)&(i<N)
        h[i[m]]=hh[m]; l[i[m]]=ll[m]; c[i[m]]=cc[m]; n[i[m]]=nn[m]
    cf=pd.Series(c).ffill().values.copy()
    if not got[1]:
        cut=PRE+86400; cf[cut:]=np.nan; h[cut:]=np.nan; l[cut:]=np.nan
    def fmax(a,w):
        ap=np.concatenate([a,np.full(w-1,np.nan)]); return np.nanmax(sw(ap,w),axis=1)
    def fmin(a,w):
        ap=np.concatenate([a,np.full(w-1,np.nan)]); return np.nanmin(sw(ap,w),axis=1)
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        h3=fmax(h,3); h10=fmax(h,10); l3=fmin(l,3); l10=fmin(l,10)
    hw=np.where(np.isfinite(h3),h3,h10); lw=np.where(np.isfinite(l3),l3,l10)
    return base,N,cf,hw,lw
def metr(base,N,cf,hw,lw,sec,wd,tsms):
    def g(a,i):
        out=np.full(len(i),np.nan); ok=(i>=0)&(i<N); out[ok]=a[i[ok]]; return out
    i0=sec-base
    lp0=np.log(g(cf,i0-1))
    S=lambda px: wd*(np.log(px)-lp0)*100
    o={'p0':np.exp(lp0)}
    for k in MOFF: o[f'm{k}']=S(g(cf,i0+k))
    inw=lambda i: np.where(wd>0,g(hw,i),g(lw,i))   # worst when trading IN whale dir (follow entry / fade exit)
    agw=lambda i: np.where(wd>0,g(lw,i),g(hw,i))   # worst when trading AGAINST whale dir (follow exit / fade entry)
    for L in LAT:
        s=(tsms+L*1000)//1000-base
        o[f'fe{L}']=S(inw(s))     # follow entry px (signed)
        o[f'ae{L}']=S(agw(s))     # fade entry px
    for H in HOR:
        o[f'fx{H}']=S(agw(i0+H))  # follow exit
        o[f'ax{H}']=S(inw(i0+H))  # fade exit
    return o
def main():
  rng=np.random.default_rng(7)
  res=[]; pres=[]
  for (sym,day),grp in A.groupby(['symbol','day']):
      if not os.path.exists(f'{B}{sym}/{day}.npz'): continue
      base,N,cf,hw,lw=dense(sym,day)
      sec=grp.ts.values//1000; wd=grp.wdir.values; tsms=grp.ts.values
      o=metr(base,N,cf,hw,lw,sec,wd,tsms); o['ep']=grp.ep.values; res.append(pd.DataFrame(o))
      D0=int(pd.Timestamp(day).timestamp())
      coin=grp.coin.iloc[0]
      allsec=T[(T.symbol==sym)&(T.ts>=(D0-200)*1000)&(T.ts<(D0+86600)*1000)].ts.values//1000
      ok=np.ones(86400,bool)
      for s in np.unique(allsec):
          a=max(0,s-D0-120); b=min(86400,s-D0+121); ok[a:b]=False
      cand=np.where(ok)[0]
      if len(cand)<600: continue
      for rep in range(3):
          ps=D0+rng.choice(cand,len(grp)); pms=ps*1000+tsms%1000
          po=metr(base,N,cf,hw,lw,ps,wd,pms); po['ep']=grp.ep.values; po['rep']=rep; po['pts']=pms; pres.append(pd.DataFrame(po))
  R=pd.concat(res); P=pd.concat(pres)
  R.to_parquet(OUT+'s1_anchor.parquet'); P.to_parquet(OUT+'s1_placebo.parquet')
  print(R.shape,P.shape, R.p0.notna().sum())

if __name__=="__main__": main()

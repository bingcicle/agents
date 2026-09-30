# Add past market-neutral returns (whale trade direction) at decision time: coin - EW alt index over the previous 4h/24h/72h/7d.
import numpy as np, pandas as pd, sys
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state/'
P=np.load(W+'pxmat.npz'); mins=P['mins']; C=P['C']; syms=list(P['syms']); A=P['altidx']; t0=mins[0]; si={s:i for i,s in enumerate(syms)}
def add(fn):
    ev=pd.read_parquet(W+fn)
    j0=((ev.dec_ms.values-t0)//60000).astype(int)   # minute containing dec; C[:,j0-1] = close of the last complete minute
    k=ev.symbol.map(si).values
    for lab,h in (('4h',4),('24h',24),('72h',72),('7d',168)):
        jb=j0-1-60*h; ok=(jb>=0)&~pd.isna(k)&(j0<C.shape[1])
        kk=np.where(pd.isna(k),0,k).astype(int)
        cr=np.where(ok, C[kk,np.clip(j0-1,0,C.shape[1]-1)]/C[kk,np.clip(jb,0,None)]-1, np.nan)
        ar=np.where(ok, A[np.clip(j0,0,len(A)-1)]/A[np.clip(jb+1,0,None)]-1, np.nan)
        ev[f'past_mn_{lab}']=100*ev.wdir.values*(cr-ar)
    ev.to_parquet(W+fn); print(fn, ev[[c for c in ev.columns if c.startswith('past_mn')]].notna().mean().round(2).to_dict())
for fn in sys.argv[1:]: add(fn)

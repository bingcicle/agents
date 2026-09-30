# Minute close matrix for all Binance symbols (k1m) + equal-weight alt index (ex BTC) built from mean 1m log-returns.
import os, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
K=SP+'data/k1m/'
syms=sorted(os.listdir(K))
t0=pd.Timestamp('2026-08-20').value//10**6 if False else 1787184000000
t1=1790726400000  # 2026-09-30 00:00 UTC
mins=np.arange(t0,t1,60000); n=len(mins)
C=np.full((len(syms),n),np.nan,dtype=np.float64)
for i,s in enumerate(syms):
    for fn in sorted(os.listdir(K+s)):
        z=np.load(K+s+'/'+fn); ot=z['ot']; j=((ot-t0)//60000).astype(int); ok=(j>=0)&(j<n)
        C[i,j[ok]]=z['c'][ok]
# ffill up to 3 min
df=pd.DataFrame(C.T); df=df.ffill(limit=3); C=df.values.T
lr=np.diff(np.log(C),axis=1)
alt=[i for i,s in enumerate(syms) if s!='BTCUSDT']
m=np.nanmean(np.clip(lr[alt],-0.2,0.2),axis=0); m=np.nan_to_num(m)
idx=np.concatenate([[0],np.cumsum(m)])
np.savez_compressed('pxmat.npz',mins=mins,C=C,syms=np.array(syms),altidx=np.exp(idx))
print(len(syms),n,'alt index start/end',np.exp(idx[0]),np.exp(idx[-1]))

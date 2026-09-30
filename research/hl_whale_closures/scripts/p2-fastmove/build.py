# Build dense 1m matrices (minutes x alts) for 2026-03-01..2026-09-29, float32, saved as .npy
import os, numpy as np, pandas as pd, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-fastmove/'
t0=int(pd.Timestamp('2026-03-01').value//10**6); t1=int(pd.Timestamp('2026-09-30').value//10**6)
T=(t1-t0)//60000
syms=sorted(s for s in os.listdir(SP+'data/k1m') if s not in ('BTCUSDT','ETHUSDT'))
F=('o','h','l','c','qv')
M={f:np.full((T,len(syms)),np.nan,np.float32) for f in F}
btc=np.full(T,np.nan)
for j,s in enumerate(syms+['BTCUSDT']):
    d=SP+'data/k1m/'+s+'/'
    for fn in sorted(os.listdir(d)):
        z=np.load(d+fn); i=(z['ot']-t0)//60000; ok=(i>=0)&(i<T); i=i[ok]
        if s=='BTCUSDT': btc[i]=z['c'][ok]; continue
        for f in F: M[f][i,j]=z[f][ok]
for f in F: np.save(W+f'M_{f}.npy',M[f])
np.save(W+'btc_c.npy',btc)
json.dump({'syms':syms,'t0':t0,'T':T},open(W+'meta.json','w'))
print(T,len(syms),np.isnan(M['c']).mean())

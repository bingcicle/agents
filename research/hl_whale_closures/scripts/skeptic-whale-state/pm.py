# My own minute close matrix from raw k1m (independent of the lens's pxmat.npz)
import os, json, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
K=SP+'data/k1m/'
T0=1787184000000  # 2026-08-20 00:00 UTC
T1=1790726400000  # 2026-09-30 00:00 UTC
SYM=json.load(open(SP+'infra/symmap.json'))
def build():
    syms=sorted(os.listdir(K)); n=(T1-T0)//60000
    C=np.full((len(syms),n),np.nan,np.float64); H=C.copy(); L=C.copy(); O=C.copy()
    for i,s in enumerate(syms):
        for fn in sorted(os.listdir(K+s)):
            z=np.load(K+s+'/'+fn); j=((z['ot']-T0)//60000).astype(int); ok=(j>=0)&(j<n)
            C[i,j[ok]]=z['c'][ok]; H[i,j[ok]]=z['h'][ok]; L[i,j[ok]]=z['l'][ok]; O[i,j[ok]]=z['o'][ok]
    np.savez('pm.npz',syms=np.array(syms),C=C,H=H,L=L,O=O)
if __name__=='__main__':
    build(); z=np.load('pm.npz'); print(z['C'].shape, np.isnan(z['C']).mean())

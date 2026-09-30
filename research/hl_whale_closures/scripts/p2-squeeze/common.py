import numpy as np, pandas as pd, os, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-squeeze/'
Z=np.load(W+'panel.npz')
SYMS=list(Z['syms']); T0=int(Z['T0']); NS=len(SYMS)
C=Z['c']; H=Z['h']; L=Z['l']; QV=Z['qv']; TB=Z['tb']; OI=Z['oi']; TR=Z['tr']
NM=C.shape[1]
# ffill close gaps up to 5 min
Cf=pd.DataFrame(C.T).ffill(limit=5).values.T.astype(np.float32)
IB=SYMS.index('BTCUSDT')
ALT=np.array([i for i,s in enumerate(SYMS) if s!='BTCUSDT'])
OI[OI<=0]=np.nan
OIf=pd.DataFrame(OI.T).ffill(limit=3).values.T.astype(np.float32)
NH=NM//60
def mclose(t_ms):  # minute index whose candle closes at t
    return (np.asarray(t_ms)-T0)//60000-1

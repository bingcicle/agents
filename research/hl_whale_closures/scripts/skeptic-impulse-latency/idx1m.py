import numpy as np, pandas as pd, os
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
K=SP+'data/k1m/'
R={}
for sym in sorted(os.listdir(K)):
    parts=[]
    for fn in sorted(os.listdir(K+sym)):
        z=np.load(K+sym+'/'+fn); parts.append(pd.Series(z['c'],index=z['ot']))
    s=pd.concat(parts); s=s[~s.index.duplicated()].sort_index()
    if len(s)<5000: continue
    R[sym]=np.log(s).diff()
RR=pd.DataFrame(R).sort_index()
full=np.arange(RR.index.min(),RR.index.max()+60000,60000)
RR=RR.reindex(full)
alt=RR.drop(columns=['BTCUSDT','ETHUSDT']).clip(-0.1,0.1).mean(axis=1).fillna(0).cumsum()
btc=RR['BTCUSDT'].fillna(0).cumsum()
# value at candle CLOSE time = ot+60000
I=pd.DataFrame({'alt':alt.values,'btc':btc.values},index=full+60000)
I.to_parquet('idx1m.parquet'); print(I.shape, RR.shape[1], I.index.min(), I.index.max())

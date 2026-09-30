import os, numpy as np, pandas as pd, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-skeptic-fastmove/'
t0=int(pd.Timestamp('2026-03-01').value//10**6); t1=int(pd.Timestamp('2026-09-30').value//10**6); T=(t1-t0)//60000
syms=sorted(s for s in os.listdir(SP+'data/k1m') if s not in ('BTCUSDT','ETHUSDT'))
O=np.full((T,len(syms)),np.nan,np.float32);C=np.full((T,len(syms)),np.nan);Q=np.full((T,len(syms)),np.nan,np.float32);NT=np.zeros((T,len(syms)),np.int32)
info=[]
for j,s in enumerate(syms):
    fs=sorted(os.listdir(SP+'data/k1m/'+s))
    for fn in fs:
        z=np.load(SP+'data/k1m/'+s+'/'+fn); ot=z['ot']
        assert (ot%60000==0).all()
        i=(ot-t0)//60000; ok=(i>=0)&(i<T)
        O[i[ok],j]=z['o'][ok];C[i[ok],j]=z['c'][ok];Q[i[ok],j]=z['qv'][ok];NT[i[ok],j]=z['n'][ok]
    v=~np.isnan(C[:,j]); idx=np.where(v)[0]
    info.append(dict(sym=s,first=fs[0][:10],last=fs[-1][:10],nfiles=len(fs),first_min=idx[0],last_min=idx[-1],gaps=int((~v[idx[0]:idx[-1]+1]).sum()),zero_trade=int((NT[idx[0]:idx[-1]+1,j]==0).sum())))
np.save(W+'O.npy',O);np.save(W+'C.npy',C);np.save(W+'Q.npy',Q);np.save(W+'NT.npy',NT)
pd.DataFrame(info).to_csv(W+'syminfo.csv',index=False); json.dump({'syms':syms,'t0':t0,'T':T},open(W+'meta.json','w'))

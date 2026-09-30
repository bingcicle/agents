import os, numpy as np, pandas as pd
from concurrent.futures import ProcessPoolExecutor
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-squeeze/'
T0=int(pd.Timestamp('2026-03-01').value//10**6); ND=213; NM=ND*1440; N5=ND*288
days=[(pd.Timestamp('2026-03-01')+pd.Timedelta(days=i)).strftime('%Y-%m-%d') for i in range(ND)]
syms=sorted(os.listdir(SP+'data/k1m'))
def one(sym):
    A={k:np.full(NM,np.nan,np.float32) for k in ('c','h','l','qv','tb')}
    for d in days:
        fn=f'{SP}data/k1m/{sym}/{d}.npz'
        if not os.path.exists(fn): continue
        z=np.load(fn); ot=z['ot']
        if len(ot)==0: continue
        m=((ot-T0)//60000).astype(np.int64); ok=(m>=0)&(m<NM); m=m[ok]
        for k,s in (('c','c'),('h','h'),('l','l'),('qv','qv'),('tb','tbqv')): A[k][m]=z[s][ok]
    O=np.full(N5,np.nan,np.float32); OV=np.full(N5,np.nan,np.float32); TR=np.full(N5,np.nan,np.float32)
    for d in days:
        fn=f'{SP}data/metrics/{sym}/{d}.csv'
        if not os.path.exists(fn): continue
        try: x=pd.read_csv(fn,usecols=['create_time','sum_open_interest','sum_open_interest_value','sum_taker_long_short_vol_ratio'])
        except Exception: continue
        if len(x)==0: continue
        t=pd.to_datetime(x.create_time).values.astype('datetime64[ms]').astype(np.int64)
        k=((t-T0)//300000).astype(np.int64); ok=(k>=0)&(k<N5)
        O[k[ok]]=x.sum_open_interest.values[ok]; OV[k[ok]]=x.sum_open_interest_value.values[ok]; TR[k[ok]]=x.sum_taker_long_short_vol_ratio.values[ok]
    return sym,A,O,OV,TR
if __name__=='__main__':
    n=len(syms)
    out={k:np.full((n,NM),np.nan,np.float32) for k in ('c','h','l','qv','tb')}
    oi=np.full((n,N5),np.nan,np.float32); oiv=oi.copy(); tr=oi.copy()
    with ProcessPoolExecutor(3) as ex:
        for i,(s,A,O,OV,TR) in enumerate(ex.map(one,syms)):
            for k in A: out[k][i]=A[k]
            oi[i]=O; oiv[i]=OV; tr[i]=TR
            if i%20==0: print(i,s,flush=True)
    np.savez(W+'panel.npz',syms=np.array(syms),T0=T0,oi=oi,oiv=oiv,tr=tr,**out)
    print('done')

import os, numpy as np, pandas as pd
from concurrent.futures import ProcessPoolExecutor
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-skeptic-squeeze/'
T0=int(pd.Timestamp('2026-03-01').value//10**6); ND=213; NM=ND*1440; N5=ND*288
days=pd.date_range('2026-03-01',periods=ND).strftime('%Y-%m-%d')
syms=sorted(os.listdir(SP+'data/k1m'))
def one(sym):
    c=np.full(NM,np.nan,np.float32); h=c.copy(); l=c.copy(); o=c.copy(); qv=c.copy()
    for d in days:
        fn=f'{SP}data/k1m/{sym}/{d}.npz'
        if not os.path.exists(fn): continue
        z=np.load(fn); m=(z['ot']-T0)//60000; k=(m>=0)&(m<NM); m=m[k]
        # verify open times on minute grid
        assert np.all((z['ot'][k]-T0)%60000==0)
        c[m]=z['c'][k]; h[m]=z['h'][k]; l[m]=z['l'][k]; o[m]=z['o'][k]; qv[m]=z['qv'][k]
    oi=np.full(N5,np.nan,np.float64)
    for d in days:
        fn=f'{SP}data/metrics/{sym}/{d}.csv'
        if not os.path.exists(fn): continue
        try: x=pd.read_csv(fn,usecols=['create_time','sum_open_interest'])
        except Exception: continue
        t=pd.to_datetime(x.create_time).values.astype('datetime64[ms]').astype(np.int64)
        k=(t-T0)//300000; ok=(k>=0)&(k<N5)&((t-T0)%300000==0)
        oi[k[ok]]=x.sum_open_interest.values[ok]
    return sym,c,h,l,o,qv,oi
if __name__=='__main__':
    n=len(syms)
    C=np.full((n,NM),np.nan,np.float32); Hh=C.copy(); Ll=C.copy(); O=C.copy(); QV=C.copy(); OI=np.full((n,N5),np.nan)
    with ProcessPoolExecutor(3) as ex:
        for i,(s,c,h,l,o,qv,oi) in enumerate(ex.map(one,syms)):
            C[i]=c;Hh[i]=h;Ll[i]=l;O[i]=o;QV[i]=qv;OI[i]=oi
    np.save(W+'C.npy',C);np.save(W+'H.npy',Hh);np.save(W+'L.npy',Ll);np.save(W+'O.npy',O);np.save(W+'QV.npy',QV);np.save(W+'OI.npy',OI)
    open(W+'syms.txt','w').write('\n'.join(syms)); print('done',n)

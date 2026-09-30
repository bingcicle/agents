import os, numpy as np, pandas as pd, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-skeptic-fastmove/'
m=json.load(open(W+'meta.json')); syms=m['syms']; t0=m['t0']; T=m['T']
O=np.load(W+'O.npy').astype(float); C=np.load(W+'C.npy'); Q=np.load(W+'Q.npy'); NT=np.load(W+'NT.npy')
LC=np.log(C); R=np.diff(LC,axis=0,prepend=np.nan); R[np.abs(R)>0.5]=np.nan
cnt=np.sum(~np.isnan(R),1); S=np.nansum(R,1)
# leave-one-out index per coin: I_j = cumsum((S - r_j)/(cnt-1))
def oi(s):
    d=SP+f'data/metrics/{s}/'
    df=pd.concat([pd.read_csv(d+f,usecols=['create_time','sum_open_interest']) for f in sorted(os.listdir(d))])
    ms=pd.to_datetime(df.create_time).values.astype('datetime64[ms]').astype('int64')
    df=pd.DataFrame({'ms':ms,'oi':df.sum_open_interest.values}).dropna().drop_duplicates('ms').sort_values('ms')
    return df.ms.values, df.oi.values.astype(float)
H=60; out=[]
for j,s in enumerate(syms):
    rj=np.nan_to_num(R[:,j]); has=~np.isnan(R[:,j])
    I=np.cumsum(np.where(cnt-has>0,(S-rj)/np.maximum(cnt-has,1),0))
    lc=LC[:,j]; first=np.argmax(~np.isnan(lc))
    oms,ov=oi(s)
    for (X,N) in ((1.5,3),(2,5),(3,3)):
        rel=np.full(T,np.nan); rel[N:]=100*((lc[N:]-lc[:-N])-(I[N:]-I[:-N]))
        cand=np.where(rel<=-X)[0]; cand=cand[(cand>=first+1440)&(cand<T-H-3)]
        nxt=-1; keep=[]
        for t in cand:
            if t<=nxt: continue
            keep.append(t); nxt=t+H   # non-overlap by time regardless of data validity (so NaN exits are counted)
        t=np.array(keep,int)
        if len(t)==0: continue
        # OI change over 30 min, using last row with ms <= tclose - lag
        tclose=t0+(t+1)*60000; och={}
        for lag in (0,5,10,15):
            tc=tclose-lag*60000
            k1=np.searchsorted(oms,tc,'right')-1; k0=np.searchsorted(oms,tc-30*60000,'right')-1
            v=np.full(len(t),np.nan); ok=(k0>=0)&(k1>=0)
            ok&=(oms[np.maximum(k1,0)]>tc-15*60000)
            v[ok]=100*(ov[k1[ok]]/ov[k0[ok]]-1); och[f'oi{lag}']=v
        # also 'future' OI (look-ahead probe): change over [tclose, tclose+30]
        k1=np.searchsorted(oms,tclose+30*60000,'right')-1; k0=np.searchsorted(oms,tclose,'right')-1
        och['oi_fut']=100*(ov[np.clip(k1,0,len(ov)-1)]/ov[np.clip(k0,0,len(ov)-1)]-1)
        e1=O[t+1,j]; e2=C[t+1,j]; e3=O[t+2,j]; x=C[t+H,j]
        out.append(pd.DataFrame(dict(sym=s,X=X,N=N,t=t,ts=tclose,move=rel[t],c0=C[t,j],e1=e1,e_c1=e2,e_o2=e3,x=x,
            idx=100*(np.exp(I[t+H]-I[t])-1),nt1=NT[t+1,j],qv1=Q[t+1,j],
            qv24=np.array([np.nansum(Q[max(k-1440,0):k,j])/1440 for k in t]),**och)))
    print(j,s,flush=True) if j%30==0 else None
d=pd.concat(out,ignore_index=True); d.to_parquet(W+'ev.parquet'); print(len(d))

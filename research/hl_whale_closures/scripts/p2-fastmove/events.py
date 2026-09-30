# Compute trades for all 58 pre-declared cells -> trades.parquet
import os, json, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-fastmove/'
meta=json.load(open(W+'meta.json')); syms=meta['syms']; t0=meta['t0']; T=meta['T']
O=np.load(W+'M_o.npy',mmap_mode='r'); H_=np.load(W+'M_h.npy',mmap_mode='r'); L_=np.load(W+'M_l.npy',mmap_mode='r')
C=np.load(W+'M_c.npy'); QV=np.load(W+'M_qv.npy',mmap_mode='r')
LC=np.log(C.astype(np.float64))
R=np.diff(LC,axis=0,prepend=np.nan)
R[np.abs(R)>0.5]=np.nan   # data glitches
idxr=np.nanmean(R,axis=1); idxr=np.nan_to_num(idxr)
I=np.cumsum(idxr)          # equal-weight alt index (cum log)
np.save(W+'index_I.npy',I)
del R
# OI loader
def oi_series(s):
    d=SP+f'data/metrics/{s}/'
    if not os.path.isdir(d): return None
    parts=[pd.read_csv(d+f,usecols=['create_time','sum_open_interest']) for f in sorted(os.listdir(d))]
    df=pd.concat(parts); df['ms']=pd.to_datetime(df.create_time).values.astype('datetime64[ms]').astype('int64')
    df=df.dropna().drop_duplicates('ms').sort_values('ms')
    return df.ms.values, df.sum_open_interest.values.astype(float)
cells=[]
for side in ('DUMP','PUMP'):
    for X in (1,1.5,2,3,5):
        for N in (1,3,5,15):
            cells.append(dict(cell=f'A_{side}_X{X}_N{N}',side=side,X=X,N=N,H=60,tp=None,filt=None))
    for H in (15,30,60,240): cells.append(dict(cell=f'B_{side}_H{H}',side=side,X=2,N=5,H=H,tp=None,filt=None))
    cells.append(dict(cell=f'B_{side}_TP1',side=side,X=2,N=5,H=60,tp=1.0,filt=None))
    for f in ('VOLHI','VOLLO','OIDN','OIUP'): cells.append(dict(cell=f'B_{side}_{f}',side=side,X=2,N=5,H=60,tp=None,filt=f))
assert len(cells)==58
out=[]
for j,s in enumerate(syms):
    lc=LC[:,j]
    if np.isnan(lc).all(): continue
    o=np.asarray(O[:,j],np.float64); h=np.asarray(H_[:,j],np.float64); l=np.asarray(L_[:,j],np.float64); c=C[:,j].astype(np.float64)
    qv=np.nan_to_num(np.asarray(QV[:,j],np.float64))
    cq=np.concatenate([[0],np.cumsum(qv)])
    first=np.argmax(~np.isnan(lc))
    oi=oi_series(s)
    rel_cache={}
    for N in (1,3,5,15):
        rel=np.full(T,np.nan); rel[N:]=100*((lc[N:]-lc[:-N])-(I[N:]-I[:-N])); rel_cache[N]=rel
    for cd in cells:
        rel=rel_cache[cd['N']]; sg=-1 if cd['side']=='DUMP' else 1
        cand=np.where((sg*rel>=cd['X']))[0] if True else None
        cand=cand[(cand>=first+1440)&(cand<T-cd['H']-1)]
        if len(cand)==0: continue
        N=cd['N']; H=cd['H']
        # features
        vr=(cq[cand+1]-cq[cand+1-N])/N/np.maximum((cq[cand+1-N]-cq[cand+1-N-1440])/1440,1e-9)
        oich=np.full(len(cand),np.nan)
        if oi is not None:
            tc=t0+(cand+1)*60000-5*60000
            k1=np.searchsorted(oi[0],tc,side='right')-1; k0=np.searchsorted(oi[0],tc-30*60000,side='right')-1
            ok=(k0>=0)&(k1>=0)&(oi[0][np.maximum(k1,0)]>tc-15*60000)
            oich[ok]=100*(oi[1][k1[ok]]/oi[1][k0[ok]]-1)
        f=cd['filt']; m=np.ones(len(cand),bool)
        if f=='VOLHI': m=vr>=3
        elif f=='VOLLO': m=vr<3
        elif f=='OIDN': m=oich<=-0.5
        elif f=='OIUP': m=oich>=0
        cand=cand[m]; vr=vr[m]; oich=oich[m]
        # non-overlap loop
        keep=[]; exits=[]; nxt=-1
        tp=cd['tp']; my=-sg  # our side: fade
        for ii,t in enumerate(cand):
            if t<=nxt: continue
            e=o[t+1]
            if not np.isfinite(e) or not np.isfinite(c[t+H]): continue
            ex_t=t+H; ex_px=c[t+H]; hit=0
            if tp is not None:
                lvl=e*(1+my*tp/100)
                seg=h[t+1:t+H+1] if my>0 else l[t+1:t+H+1]
                w=np.where(seg>=lvl)[0] if my>0 else np.where(seg<=lvl)[0]
                if len(w): ex_t=t+1+w[0]; ex_px=lvl; hit=1
            keep.append(ii); exits.append((ex_t,ex_px,hit)); nxt=ex_t
        if not keep: continue
        keep=np.array(keep); ex=np.array(exits)
        tt=cand[keep]; ext=ex[:,0].astype(int)
        e=o[tt+1]; gross=my*100*(ex[:,1]/e-1)
        ridx=100*(np.exp(I[ext]-I[tt])-1)
        out.append(pd.DataFrame(dict(cell=cd['cell'],sym=s,t=tt,ts=t0+(tt+1)*60000,side=cd['side'],my=my,move=rel_cache[N][tt],
             gross=gross,idx=ridx,neutral=gross-my*ridx,tp_hit=ex[:,2],hold=ext-tt,vr=vr[keep],oich=oich[keep],
             tr7=100*(I[tt]-I[np.maximum(tt-10080,0)]))))
    if j%20==0: print(j,s,sum(len(x) for x in out),flush=True)
df=pd.concat(out,ignore_index=True)
df.to_parquet(W+'trades.parquet'); print(len(df)); print(df.groupby('cell').size())

import numpy as np, pandas as pd, os, json
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/p2-skeptic-squeeze/'
T0=int(pd.Timestamp('2026-03-01').value//10**6)
SY=open(W+'syms.txt').read().split('\n'); NS=len(SY); IB=SY.index('BTCUSDT')
C=np.load(W+'C.npy'); OI=np.load(W+'OI.npy'); NM=C.shape[1]; NH=NM//60
Hi=np.load(W+'H.npy',mmap_mode='r'); Lo=np.load(W+'L.npy',mmap_mode='r'); QV=np.load(W+'QV.npy',mmap_mode='r')
ALT=np.array([i for i in range(NS) if i!=IB])
def px(m):  # close of minute m (candle ending at T0+(m+1)min), strictly no ffill
    return C[:,m]
def ffpx(m,back=5):
    out=C[:,m].astype(float).copy()
    for b in range(1,back+1):
        k=np.isnan(out); 
        if not k.any(): break
        out[k]=C[k,m-b]
    return out
OI[OI<=0]=np.nan; OIl=np.log(OI)
def oi_at(k):  # stamp k known at k+1 (value describes end of stamp interval); use latest stamp <= k with ffill 3
    out=OIl[:,k].copy()
    for b in range(1,4):
        n=np.isnan(out)
        if not n.any(): break
        out[n]=OIl[n,k-b]
    return out
# funding (Binance archive via p2-squeeze download; HL proxy for Sep) - reuse their raw files, own cumsum
FC={}
SYMMAP=json.load(open(SP+'infra/symmap.json')); INV={v:k for k,v in SYMMAP.items() if v}
def fcum(i):
    if i in FC: return FC[i]
    s=SY[i]; ts=[];rs=[]
    fn=SP+f'work/p2-squeeze/funding/{s}.csv'
    if os.path.exists(fn):
        f=pd.read_csv(fn); ts.append(f.calc_time.values.astype(np.int64)); rs.append(f.last_funding_rate.values.astype(float))
    c=INV.get(s); fh=SP+f'work/long-horizon/funding/{c}.json' if c else None
    if fh and os.path.exists(fh):
        d=json.load(open(fh)); t=np.array([x['time'] for x in d],np.int64); r=np.array([float(x['fundingRate']) for x in d])
        cut=ts[0].max() if ts else 0; k=t>cut+3600000; ts.append(t[k]); rs.append(r[k])
    FC[i]=None
    if ts:
        t=np.concatenate(ts); r=np.concatenate(rs); o=np.argsort(t); FC[i]=(t[o],np.cumsum(r[o]))
    return FC[i]
def fund_long(i,t0,t1):  # cumulative funding rate paid by LONG between t0,t1 (%), nan if no data
    f=fcum(i)
    if f is None or f[0][0]>t0 or f[0][-1]<t1-9*3600000: return np.nan
    k0=np.searchsorted(f[0],t0,'right')-1; k1=np.searchsorted(f[0],t1,'right')-1
    return 100*(f[1][k1]-(f[1][k0] if k0>=0 else 0))
def run(L=24,X=.12,Y=.05,Hh=24,side=-1,delay=2,oilag=2,dirn='dump',hours=None,sl=None,idx='ew'):
    """decision at full hour t. ex over [t-L,t] from closes of minute ending at t. OI stamp index t/5min - oilag.
    entry close of minute ending t+delay min. side: -1 SHORT coin."""
    rows=[]; last=np.full(NS,-10**9)
    hs=range(L+2,NH-1) if hours is None else hours
    for h in hs:
        m=h*60-1
        if m+delay+Hh*60>=NM: break
        a=ffpx(m); b=ffpx(m-L*60); r=a/b-1
        ex=r-np.nanmean(r[ALT])
        k=h*12-oilag; doi=np.exp(oi_at(k)-oi_at(k-L*12))-1
        with np.errstate(invalid='ignore'):
            s=(ex<=-X)&(doi<=-Y) if dirn=='dump' else (ex>=X)&(doi<=-Y)
        s[IB]=False
        cand=np.flatnonzero(s)
        if len(cand)==0: continue
        me=m+delay; mx=me+Hh*60
        pe=C[:,me]; pxx=C[:,mx]
        ra=pxx/pe-1
        for i in cand:
            if h<last[i]+Hh: continue
            if not(np.isfinite(pe[i]) and np.isfinite(pxx[i])): 
                rows.append(dict(sym=SY[i],h=h,miss=1)); continue
            last[i]=h
            msk=np.isfinite(ra); msk[IB]=False; msk[i]=False
            ew=np.nanmean(ra[msk]); md=np.nanmedian(ra[msk])
            rc=ra[i]; t0=T0+(me+1)*60000; t1=T0+(mx+1)*60000
            hp=np.asarray(Hi[i,me+1:mx+1],float); lp=np.asarray(Lo[i,me+1:mx+1],float)
            mae=(np.nanmax(hp)/pe[i]-1) if side<0 else (1-np.nanmin(lp)/pe[i])
            d=dict(sym=SY[i],h=h,t=T0+h*3600000,miss=0,ex=ex[i],doi=doi[i],rc=100*rc,ew=100*ew,md=100*md,btc=100*ra[IB],
                   fl=fund_long(i,t0,t1),mae=100*mae,qv24=float(np.nansum(QV[i,m-1440+1:m+1])),
                   slipH=100*(np.asarray(Hi[i,me])/pe[i]-1), slipL=100*(1-np.asarray(Lo[i,me])/pe[i]))
            for s_ in (0.10,0.20):
                st=pe[i]*(1-side*s_)
                hit=np.flatnonzero(hp>=st) if side<0 else np.flatnonzero(lp<=st)
                if len(hit):
                    kk=me+1+hit[0]; xp=max(st,C[i,kk-1]) if side<0 else min(st,C[i,kk-1])
                    r2=C[:,kk]/pe-1; m2=np.isfinite(r2); m2[IB]=False; m2[i]=False
                    d[f'sl{int(s_*100)}']=100*side*((xp/pe[i]-1)-np.nanmean(r2[m2]))
                else: d[f'sl{int(s_*100)}']=100*side*(rc-ew)
            rows.append(d)
    D=pd.DataFrame(rows); D['side']=side; D['L']=L; D['H']=Hh
    ok=D.miss==0
    D['y']=np.where(ok, side*(D.rc-D.ew)-0.246 - side*D.fl.fillna(0), np.nan)
    D['y_nf']=side*(D.rc-D.ew)-0.246
    D['y_btc']=side*(D.rc-D.btc)-0.246-side*D.fl.fillna(0)
    D['y_raw']=side*D.rc-0.146-side*D.fl.fillna(0)
    D['y_md']=side*(D.rc-D.md)-0.246-side*D.fl.fillna(0)
    return D

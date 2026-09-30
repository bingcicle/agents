from common import *
import sys
SYMMAP=json.load(open(SP+'infra/symmap.json')); INV={v:k for k,v in SYMMAP.items() if v}
XL={1:.03,4:.05,12:.08,24:.12}; YL={1:.01,4:.02,12:.04,24:.05}; TKL={1:.577,4:.541,12:.522,24:.514}
HRS=np.arange(26,NH-1); MD=HRS*60-1; K5=HRS*12-2-1  # bucket starting t-10min
TT=T0+HRS.astype(np.int64)*3600000
cq=np.cumsum(np.nan_to_num(QV,nan=0.0).astype(np.float64),axis=1); ct=np.cumsum(np.nan_to_num(TB,nan=0.0).astype(np.float64),axis=1)
FEAT={}
for Lh in (1,4,12,24):
    r=Cf[:,MD]/Cf[:,MD-Lh*60]-1; ex=r-np.nanmean(r[ALT],axis=0)
    with np.errstate(all='ignore'):
        doi=OIf[:,K5]/OIf[:,K5-Lh*12]-1
        tk=(ct[:,MD]-ct[:,MD-Lh*60])/(cq[:,MD]-cq[:,MD-Lh*60])
    FEAT[Lh]=(ex,doi,tk)
QV24=(cq[:,MD]-cq[:,MD-1440])
del cq,ct
def signal(fam,Lh):
    ex,doi,tk=FEAT[Lh]; X,Y=XL[Lh],YL[Lh]
    with np.errstate(invalid='ignore'):
        if fam=='SQZ': s=(ex>=X)&(doi<=-Y); side=-1
        elif fam=='PLAIN': s=(ex>=X)&(np.abs(doi)<Y); side=-1
        elif fam=='NEWLONG': s=(ex>=X)&(doi>=Y); side=-1
        elif fam=='LIQDUMP': s=(ex<=-X)&(doi<=-Y); side=1
        elif fam=='SQZTK': s=(ex>=X)&(doi<=0)&(tk>=TKL[Lh]); side=-1
    return s,side
# funding
FUND={}
def fund_cum(i):
    if i in FUND: return FUND[i]
    s=SYMS[i]; ts=[]; rs=[]
    fn=W+f'funding/{s}.csv'
    if os.path.exists(fn):
        f=pd.read_csv(fn); ts.append(f.calc_time.values.astype(np.int64)); rs.append(f.last_funding_rate.values.astype(float))
    c=INV.get(s); fh=SP+f'work/long-horizon/funding/{c}.json' if c else None
    if fh and os.path.exists(fh):
        d=json.load(open(fh)); t=np.array([x['time'] for x in d],np.int64); r=np.array([float(x['fundingRate']) for x in d])
        cut=ts[0].max() if ts else 0; k=t>cut+3600000; ts.append(t[k]); rs.append(r[k])
    if ts:
        t=np.concatenate(ts); r=np.concatenate(rs); o=np.argsort(t); FUND[i]=(t[o],np.cumsum(r[o]))
    else: FUND[i]=None
    return FUND[i]
def trades(fam,Lh,Hh,SLS=(0.10,0.20)):
    s,side=signal(fam,Lh); out=[]
    for i in range(NS):
        if i==IB: continue
        idx=np.flatnonzero(s[i]); last=-10**9
        for j in idx:
            if HRS[j]<last+Hh: continue
            last=HRS[j]; out.append((i,j))
    if not out: return pd.DataFrame()
    ii=np.array([o[0] for o in out]); jj=np.array([o[1] for o in out])
    me=MD[jj]+2; mx=me+Hh*60
    ok=mx<NM; ii,jj,me,mx=ii[ok],jj[ok],me[ok],mx[ok]
    pe=Cf[ii,me]; px=Cf[ii,mx]
    rows=[]
    for n in range(len(ii)):
        i,a,b=ii[n],me[n],mx[n]
        if not (pe[n]>0 and px[n]>0): continue
        alt=Cf[ALT,b]/Cf[ALT,a]-1; msk=np.isfinite(alt)&(ALT!=i); ew=float(alt[msk].mean())
        btc=float(Cf[IB,b]/Cf[IB,a]-1); r=float(px[n]/pe[n]-1)
        hp=H[i,a+1:b+1]; lp=L[i,a+1:b+1]
        adv = (np.nanmax(hp)/pe[n]-1) if side<0 else (1-np.nanmin(lp)/pe[n])   # max adverse excursion of coin leg
        slres={}
        for sl in SLS:
            stop=pe[n]*(1+sl) if side<0 else pe[n]*(1-sl)
            hit=np.flatnonzero(hp>=stop) if side<0 else np.flatnonzero(lp<=stop)
            if len(hit):
                k=a+1+hit[0]
                # gap: if the minute opened beyond the stop, use previous close as proxy for open
                prev=Cf[i,k-1]
                xp=max(stop,prev) if side<0 else min(stop,prev)
                alt2=Cf[ALT,k]/Cf[ALT,a]-1; m2=np.isfinite(alt2)&(ALT!=i)
                slres[sl]=(side*(xp/pe[n]-1), float(alt2[m2].mean()), float(Cf[IB,k]/Cf[IB,a]-1), 1)
            else: slres[sl]=(side*r, ew, btc, 0)
        te=T0+(a+1)*60000; tx=T0+(b+1)*60000
        fc=fund_cum(i); fund=np.nan
        if fc is not None and fc[0][0]<=te and fc[0][-1]>=tx-8*3600000:
            k0=np.searchsorted(fc[0],te,'right')-1; k1=np.searchsorted(fc[0],tx,'right')-1
            fund=100*side*(fc[1][k1]-(fc[1][k0] if k0>=0 else 0))
        slip=(1-L[i,a]/pe[n]) if side<0 else (H[i,a]/pe[n]-1)  # entry at worst of entry minute instead of close
        d=dict(sym=SYMS[i],t=int(TT[jj[n]]),side=side,raw=100*side*r,ew=100*side*(r-ew),btc=100*side*(r-btc),
               fund=fund,mae=100*adv,slip=100*slip,qv24=float(QV24[i,jj[n]]),
               ex=float(FEAT[Lh][0][i,jj[n]]),doi=float(FEAT[Lh][1][i,jj[n]]))
        for sl,(cr,e2,b2,h) in slres.items():
            d[f'ew_sl{int(sl*100)}']=100*(cr-side*e2); d[f'hit{int(sl*100)}']=h
        rows.append(d)
    df=pd.DataFrame(rows); df['fam']=fam; df['L']=Lh; df['H']=Hh
    return df
CELLS=[('SQZ',L_,H_) for L_ in (1,4,12,24) for H_ in (4,12,24,48)]+\
      [(f,L_,H_) for f in ('PLAIN','NEWLONG','LIQDUMP') for L_ in (1,4,12,24) for H_ in (12,24)]+\
      [('SQZTK',L_,24) for L_ in (1,4,12,24)]
if __name__=='__main__':
    assert len(CELLS)==44
    allt=[]
    for c in CELLS:
        d=trades(*c); print(c,len(d),flush=True); allt.append(d)
    T=pd.concat(allt,ignore_index=True); T.to_parquet(W+'trades_grid.parquet'); print('saved',len(T))

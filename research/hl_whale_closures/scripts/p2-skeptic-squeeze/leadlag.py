import numpy as np
W='./'; C=np.load(W+'C.npy',mmap_mode='r'); OI=np.load(W+'OI.npy'); QV=np.load(W+'QV.npy',mmap_mode='r')
res={}
for i in range(0,153,3):
    c=np.asarray(C[i],float); oi=OI[i]; qv=np.asarray(QV[i],float)
    n5=len(oi); c5=c[4::5][:n5]  # close of minute 4 of each 5-min block => at block end = time (k+1)*5min
    # c at time k*5min: close of minute k*5-1
    ck=np.full(n5,np.nan); ck[1:]=c[np.arange(1,n5)*5-1]
    doi=np.abs(np.diff(np.log(oi)))  # between stamp k and k+1 -> index k
    q5=np.nansum(qv[:n5*5].reshape(n5,5),axis=1)  # volume in [k*5, k*5+5)
    for lag in range(-3,4):
        # doi_k (stamp k->k+1) vs volume in block k+lag  ; block k = [T_k, T_k+5)
        a=doi[max(0,-lag):len(doi)-max(0,lag)]; b=q5[max(0,lag):len(doi)-max(0,-lag)+max(0,lag)-max(0,lag)][:len(a)]
        b=q5[np.arange(len(doi))+lag][max(0,-lag):len(doi)-max(0,lag)] if False else None
        idx=np.arange(len(doi)); j=idx+lag; ok=(j>=0)&(j<n5)
        x=doi[ok]; y=np.log1p(q5[j[ok]]); m=np.isfinite(x)&np.isfinite(y)&(y>0)
        from scipy.stats import spearmanr
        res.setdefault(lag,[]).append(spearmanr(x[m],y[m])[0])
for lag,v in res.items(): print('lag',lag,'median rho',round(np.nanmedian(v),3))

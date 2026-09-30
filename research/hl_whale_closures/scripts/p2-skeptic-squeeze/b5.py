import pandas as pd,numpy as np
E=pd.read_parquet('simE.parquet'); E=E[E.dec<=pd.Timestamp('2026-09-28 23:57').value//10**6]
def dd(x,h=24):
    x=x.sort_values('dec'); keep=[]; last={}
    for c,t in zip(x.sym,x.dec):
        if c not in last or t>=last[c]+h*3600e3: keep.append(True); last[c]=t
        else: keep.append(False)
    return x[np.array(keep)]
S=E[E.side=='LONG']; L=S[S.wpnl<0].copy(); L['liq']=L.liq.fillna(1e9)
TH=(0.3,0.5,1,2,3,5,8,10,20)
obs={t:dd(L[L.liq<t]).y.mean() for t in TH}; print({k:round(v,2) for k,v in obs.items()})
rng=np.random.default_rng(1); mx=[]; m5=[]
for k in range(400):
    L['lp']=L.groupby('sym').liq.transform(lambda v: rng.permutation(v.values))
    vals=[dd(L[L.lp<t]).y.mean() for t in TH]; mx.append(np.nanmax(vals)); m5.append(vals[5])
mx=np.array(mx); m5=np.array(m5)
print('null liq<5: mean',m5.mean().round(2),'sd',m5.std().round(2),'p',(m5>=obs[5]).mean())
print('null max over thresholds: q50',np.median(mx).round(2),'q95',np.quantile(mx,.95).round(2),'p(max>=0.93)',(mx>=obs[5]).mean())
# global (not within coin) permutation
mg=[]
for k in range(400):
    L['lp']=rng.permutation(L.liq.values); mg.append(dd(L[L.lp<5]).y.mean())
mg=np.array(mg); print('global perm liq<5 p',(mg>=obs[5]).mean(),'sd',mg.std().round(2))

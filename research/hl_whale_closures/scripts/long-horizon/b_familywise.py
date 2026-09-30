"""Family-wise placebo for B-union: same random times (same coin, same split window, same hour-of-day, same side) across all
horizons; statistic = max over horizons of z = mean/sd_null. Compare the observed max-z. 300 draws."""
import numpy as np, pandas as pd
from lh import *
B=pd.read_parquet(W+'b_union_final.parquet')
HS=[1,2,4,8,12,24,48,72]
j=B.sym.map(SIDX).values.astype(int); s=B.side.values; hod=((B.i0.values%1440)//60)
b0=minute_of(pd.Timestamp('2026-08-30').value//10**6); b1=minute_of(pd.Timestamp('2026-09-12 18:00').value//10**6); b2=minute_of(DISC_END)
sp_=B.split.values
lo=np.where(sp_=='early',b0,np.where(sp_=='disc',b1,b2)); hi=np.where(sp_=='early',b1,np.where(sp_=='disc',b2,NMIN))-72*60-1
hi=np.maximum(hi,lo+1440)
rng=np.random.default_rng(7); ND=300
null=np.zeros((ND,len(HS)))
for k in range(ND):
    nd=np.maximum((hi-lo)//1440,1); dd=rng.integers(0,nd); mm=rng.integers(0,60,len(B))
    i0=lo-(lo%1440)+dd*1440+hod*60+mm; i0=np.where(i0<lo,i0+1440,i0)
    for q,h in enumerate(HS):
        i1=np.minimum(i0+h*60,NMIN-1); ok=(i0+h*60)<NMIN
        x=s*(logret(j,i0,i1)-idxret(i0,i1)); x[~ok]=np.nan
        null[k,q]=np.nanmean(x)
obs=np.array([np.nanmean(B[f'mnA{h}']) if f'mnA{h}' in B else np.nan for h in HS])
# obs for 1,2,4,48,72 need computing
i0=B.i0.values
for q,h in enumerate(HS):
    if np.isnan(obs[q]): 
        x=s*(logret(j,i0,i0+h*60)-idxret(i0,i0+h*60)); obs[q]=np.nanmean(x)
mu=null.mean(0); sd=null.std(0)
z_obs=(obs-mu)/sd; z_null=(null-mu)/sd
maxz=z_null.max(1)
out=open(W+'out_B_familywise.txt','w')
txt=('B-union pooled (early+disc+test) family-wise placebo over horizons '+str(HS)+'\n'+
     'obs mean: '+' '.join(f'{h}h:{o:+.3f}' for h,o in zip(HS,obs))+'\n'+
     'null mean/sd: '+' '.join(f'{h}h:{m:+.3f}/{d:.3f}' for h,m,d in zip(HS,mu,sd))+'\n'+
     'z_obs: '+' '.join(f'{h}h:{z:+.2f}' for h,z in zip(HS,z_obs))+'\n'+
     f'max z_obs={z_obs.max():+.2f} ; P(max z_null >= max z_obs) = {(maxz>=z_obs.max()).mean():.3f} (family-wise over {len(HS)} horizons, 1 event definition)\n')
print(txt); out.write(txt); out.close()

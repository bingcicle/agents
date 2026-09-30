"""Block placebo: all events of one coin within one split are shifted by the SAME random whole-day offset (circular within the
split window), so intra-coin clustering / overlapping windows are preserved. Compare with independent-draw placebo."""
import numpy as np, pandas as pd
from ev import *
bnd=dict(early=(ms_to_min(pd.Timestamp('2026-08-30').value//10**6),ms_to_min(EARLY_END)),
         disc=(ms_to_min(EARLY_END),ms_to_min(DISC_END)),test=(ms_to_min(DISC_END),NM))
def block_null(E,h,nd=1000,seed=3):
    rng=np.random.default_rng(seed); E=E.dropna(subset=[f'mnA{h}'])
    j=E.j.values; s=E.side.values; m0=E.m0.values; sp=E.split.values; coin=E.coin.values
    groups=[np.where((coin==c)&(sp==p))[0] for c,p in set(zip(coin,sp))]
    res=np.empty(nd); per=np.full((nd,len(E)),np.nan)
    for d in range(nd):
        mm=m0.copy()
        for g in groups:
            lo,hi=bnd[sp[g[0]]]; hi2=hi-h*60-1; L=hi2-lo
            nday=max(L//1440,1); off=int(rng.integers(1,nday))*1440
            mm[g]=lo+((m0[g]-lo+off)%(nday*1440))
        m1=mm+h*60; r=sret(j,mm,m1)-ew_basket(mm,m1,j); per[d]=s*r
    return np.nanmean(per,axis=1), per
out=open(MY+'out_b_block.txt','w')
def pr(x): print(x); out.write(x+'\n')
for nm,fn in (('union',MY+'U.parquet'),('trig',MY+'G_trig.parquet')):
    E=pd.read_parquet(fn)
    for h in (8,12,24):
        s=f'{nm:5s} h={h:2d}'
        for sp in ('early','disc','test','ALL'):
            v=E if sp=='ALL' else E[E.split==sp]; v=v.dropna(subset=[f'mnA{h}'])
            nb,_=block_null(v,h,nd=600)
            ni=placebo(v,h,'mnA',nd=300)
            obs=v[f'mnA{h}'].mean()
            s+=f' | {sp}: obs={obs:+.2f} blockP={(nb>=obs).mean():.3f} (sd {nb.std():.2f}) indepP={(ni>=obs).mean():.3f} (sd {ni.std():.2f})'
        pr(s)
out.close()

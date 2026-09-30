"""B-union robustness: entry delay (1,5,15,30,60 min after bot knew) x horizon (4..24h) mnA means per split; plus subgroups."""
import numpy as np, pandas as pd
from lh import *
B=pd.read_parquet(W+'b_union_ev.parquet')
j=B.sym.map(SIDX).values.astype(int); s=B.side.values
out=open(W+'out_B_robust.txt','w')
def pr(x): print(x); out.write(x+'\n')
pr('B-union mnA mean (median) by entry delay (min after bot_ts; base = +1 min) x horizon; per split')
for split in ('early','disc','test'):
    m=(B.split==split).values
    for dl in (1,5,15,30,60):
        i0=B.i0.values+(dl-1)
        line=f'[{split}] delay={dl:2d}m |'
        for h in (4,6,8,10,12,16,24):
            x=s*(logret(j,i0,i0+h*60)-idxret(i0,i0+h*60))
            line+=f' {h}h:{np.nanmean(x[m]):+.2f}({np.nanmedian(x[m]):+.2f})'
        pr(line)
# subgroups at 8h and 24h
B['val']=B.max_val.fillna(B.pos_usd) if 'pos_usd' in B else B.max_val
for h in (8,24):
    pr(f'\n-- subgroups h={h}: mean mnA per split (n)')
    groups={'LONG':B.side==1,'SHORT':B.side==-1,'pre240<-1 (fading a dump/pump)':B.pre240<-1,'pre240>=-1':B.pre240>=-1,
            'ratio>=3':B.get('ratio_max',B.get('ratio')).fillna(0)>=3 if 'ratio_max' in B else B.ratio>=3}
    for g,msk in groups.items():
        line=f'{g:32s}'
        for split in ('early','disc','test'):
            v=B[msk&(B.split==split)][f'mnA{h}'].dropna(); line+=f' {split}:{v.mean():+.2f}({len(v)})'
        pr(line)
out.close()

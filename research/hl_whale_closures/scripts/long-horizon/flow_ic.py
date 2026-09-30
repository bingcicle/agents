"""Cross-sectional rank IC of trailing whale close flow vs forward mnA, for every hourly decision time (not only 00:00 UTC).
Universe A: coins with nonzero flow in the window. Universe B: all coins with known depth (flow 0 included).
Report mean IC per split, share of positive decision-days, and daily-block t (IC averaged per day first, days as units)."""
import numpy as np, pandas as pd
from lh import *
from scipy.stats import spearmanr
P=pd.read_parquet(W+'flow_panel.parquet')
out=open(W+'out_flow_ic.txt','w')
def pr(x): print(x); out.write(x+'\n')
for L in (4,24,72):
    P[f'fd{L}']=P[f'f{L}']/P.dep; P[f'fq{L}']=P[f'f{L}']/P[f'qv{L}'].clip(lower=1)
for L in (4,24,72):
  for feat in (f'fd{L}',f'fq{L}'):
    for H in (8,24,48):
        line=f'{feat:6s} H={H:2d} |'
        for split in ('disc','test'):
            for uni in ('nz','all'):
                ics=[]
                for (h,day),d in P[(P.split==split)].groupby(['h','day']):
                    d=d.dropna(subset=[f'mnA{H}',feat])
                    if uni=='nz': d=d[d[f'f{L}']!=0]
                    if len(d)<8 or d[feat].nunique()<3: continue
                    ics.append((day,spearmanr(d[feat],d[f'mnA{H}']).statistic))
                if not ics: continue
                I=pd.DataFrame(ics,columns=['day','ic']); dm=I.groupby('day').ic.mean()
                tt=dm.mean()/(dm.std(ddof=1)/np.sqrt(len(dm)))
                line+=f' {split}/{uni}: IC={I.ic.mean():+.3f} days+={np.mean(dm>0):.2f} t_day={tt:+.1f} |'
        pr(line)

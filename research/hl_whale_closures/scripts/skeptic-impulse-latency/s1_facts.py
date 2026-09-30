import numpy as np, pandas as pd
OUT='./'
A=pd.read_parquet('anchors.parquet'); R=pd.read_parquet('s1_anchor.parquet'); P=pd.read_parquet('s1_placebo.parquet')
X=A.merge(R,on='ep'); PP=P.merge(A[['ep','td','rel24','per']],on='ep')
pd.set_option('display.width',250)
bins=[0,0.01,0.05,0.1,0.3,1,1e9]
X['b']=pd.cut(X.td,bins); PP['b']=pd.cut(PP.td,bins)
cols=['m-60','m-1','m0','m1','m2','m60','m300','m1800']
t=X.groupby('b',observed=True)[cols].mean(); t['n']=X.groupby('b',observed=True).size(); t['nw']=X.groupby('b',observed=True).addr.nunique()
pt=PP.groupby('b',observed=True)[['m1','m60','m300','m1800']].mean().add_prefix('P_')
print(pd.concat([t,pt],axis=1).round(3))
# share before entry: (m1-P)/(m60-P) etc ratio of means
t2=pd.DataFrame({'s_m1_m60':(t.m1-pt.P_m1)/(t.m60-pt.P_m60),'s_m2_m300':(t.m2-pt.P_m1)/(t.m300-pt.P_m300)})
print(t2.round(2))
# median-based robustness
print('medians by td bucket'); print(X.groupby('b',observed=True)[['m1','m60','m300']].median().round(3))
# rel24 buckets
X['rb']=pd.cut(X.rel24,[0,0.1,0.5,1,2,4,1e9]); PP['rb']=pd.cut(PP.rel24,[0,0.1,0.5,1,2,4,1e9])
t=X.groupby('rb',observed=True)[cols].mean(); t['n']=X.groupby('rb',observed=True).size()
print(pd.concat([t,PP.groupby('rb',observed=True)[['m1','m60','m300']].mean().add_prefix('P_')],axis=1).round(3))
# official rule latency curve by td bucket, gross follow = fx{H}-fe{L}
for H in (60,300):
    rows={}
    for L in (1,2,3,5,10):
        rows[f'L{L}']=X.assign(g=X[f'fx{H}']-X[f'fe{L}']).groupby('b',observed=True).g.mean()
    rows['P_L2']=PP.assign(g=PP[f'fx{H}']-PP['fe2']).groupby('b',observed=True).g.mean()
    print('official worst3s follow H=',H); print(pd.DataFrame(rows).round(3))
# D vs T per bucket for L2 H300
X['g']=X.fx300-X.fe2
print(X.groupby(['b','per'],observed=True).g.agg(['size','mean','median']).round(3).unstack())

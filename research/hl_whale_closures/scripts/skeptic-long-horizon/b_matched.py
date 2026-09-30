"""Matched placebo for B: for each event draw random hour-grid times in the SAME coin and SAME split window with the
same side and similar state (signed pre-4h move, signed pre-24h move and realized vol, each in the coin's own terciles).
Also a local placebo (+-3 days, same hour-of-day). Null = mean of placebo event returns."""
import numpy as np, pandas as pd
from ev import *
U=pd.read_parquet(MY+'U.parquet')
rng=np.random.default_rng(11)
coins=U.coin.unique()
# panel on 15-minute grid for event coins
rows=[]
grid=np.arange(ms_to_min(pd.Timestamp('2026-08-31').value//10**6),NM-60,15)
Lr=np.log(P)
for c in coins:
    j=SI[sym_of(c)]
    m=grid
    pre240=sret(np.full(len(m),j),m-240,m)-ew_basket(m-240,m,np.full(len(m),j))
    pre1440=sret(np.full(len(m),j),m-1440,m)-ew_basket(m-1440,m,np.full(len(m),j))
    # realized vol: std of 15-min log returns over prior 24h
    r15=np.full(len(m),np.nan)
    x=Lr[:,j]
    d15=x[m]-x[m-15]
    s=pd.Series(d15).rolling(96,min_periods=48).std().values*np.sqrt(96)*100
    df=pd.DataFrame(dict(coin=c,j=j,m=m,pre240=pre240,pre1440=pre1440,vol=s))
    for h in (8,12,24):
        m1=m+h*60; df[f'f{h}']=sret(np.full(len(m),j),m,m1)-ew_basket(m,m1,np.full(len(m),j))
    rows.append(df)
Pn=pd.concat(rows,ignore_index=True)
Pn['split']=np.where(T0+Pn.m*60000<EARLY_END,'early',np.where(T0+Pn.m*60000<DISC_END,'disc','test'))
Pn.to_parquet(MY+'panel15.parquet')
# event state
j=U.j.values; m0=U.m0.values
x=Lr[:,j[0]]
U['vol']=[pd.Series(Lr[m-96*15:m+1:15,jj]).diff().std()*np.sqrt(96)*100 for jj,m in zip(j,m0)]
U['pre240u']=U.side*U.pre240; U['pre1440u']=U.side*U.pre1440   # unsigned coin move
out=open(MY+'out_b_matched.txt','w')
def pr(s): print(s); out.write(s+'\n')
pr('event vol (24h realized, pct) median %.2f vs panel median %.2f' % (U.vol.median(), Pn.vol.median()))
pr('event pre240 (signed by our side) mean %.2f med %.2f ; pre1440 mean %.2f med %.2f' % (U.pre240.mean(),U.pre240.median(),U.pre1440.mean(),U.pre1440.median()))
for h in (8,12,24):
    pr(f'=== h={h}')
    res=[]
    for i,r in U.iterrows():
        if np.isnan(r[f'mnA{h}']): res.append(None); continue
        q=Pn[(Pn.coin==r.coin)&(Pn.split==r.split)].dropna(subset=[f'f{h}','pre240','pre1440','vol'])
        # exclude +-h hours around the event itself
        q=q[np.abs(q.m-r.m0)>h*60]
        if len(q)<50: res.append(None); continue
        def tert(col,val):
            qs=np.nanpercentile(q[col],[33.3,66.7]); b=np.digitize(val,qs); return np.digitize(q[col],qs)==b
        sel=q[tert('pre240',r.pre240u)&tert('pre1440',r.pre1440u)&tert('vol',r.vol)]
        if len(sel)<10: sel=q[tert('pre240',r.pre240u)&tert('vol',r.vol)]
        res.append(r.side*sel[f'f{h}'].values)
    ok=[k for k,v in enumerate(res) if v is not None]
    V=U.iloc[ok]
    obs=V[f'mnA{h}'].values
    exp_=np.array([res[k].mean() for k in ok])
    ND=1000; null=np.array([np.mean([res[k][rng.integers(len(res[k]))] for k in ok]) for _ in range(ND)])
    for sp in ('early','disc','test','ALL'):
        mask=np.ones(len(V),bool) if sp=='ALL' else (V.split.values==sp)
        o=obs[mask].mean(); e=exp_[mask].mean()
        nd=np.array([np.mean([res[k][rng.integers(len(res[k]))] for k,mm in zip(ok,mask) if mm]) for _ in range(400)])
        pr(f'[{sp:5s}] n={mask.sum()} obs mean={o:+.2f} matched-expected={e:+.2f} excess={o-e:+.2f} | matched-null P(null>=obs)={(nd>=o).mean():.3f} | excess median={np.median(obs[mask]-exp_[mask]):+.2f}')
out.close()

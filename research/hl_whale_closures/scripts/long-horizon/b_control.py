"""B (fade after full close) vs generic short-term reversal. For each event: side-signed mnA pre-move over 1h/4h before entry.
Baseline: all coin-minutes sampled hourly for the same coins (whale universe), same split: fwd 8h mnA against/with pre-move in the same pre-move bucket."""
import numpy as np, pandas as pd
from lh import *
Bt=pd.read_parquet(W+'b_full_ev.parquet'); Bt=Bt[Bt.max_val>=100e3]; Bt['src']='tx'
Bg=pd.read_parquet(W+'trig_B_ev.parquet'); Bg['src']='trig'
cols=['coin','sym','i0','side','split','day','addr','src']+[f'mnA{h}' for h in (1,2,4,8,24)]
B=pd.concat([Bt[cols],Bg[cols]])
# dedupe: same coin & side within 6h -> keep first (tx preferred)
B=B.sort_values(['coin','i0']); keep=[]; last={}
for i,r in B.iterrows():
    k=(r.coin,r.side)
    if k in last and r.i0-last[k]<360: continue
    last[k]=r.i0; keep.append(i)
B=B.loc[keep].copy() if B.index.is_unique else B.reset_index(drop=True)
B=B.reset_index(drop=True)
j=B.sym.map(SIDX).values.astype(int); i0=B.i0.values
for L in (60,240):
    B[f'pre{L}']=B.side*(logret(j,i0-L,i0)-idxret(i0-L,i0))
print('events after union/dedupe:',len(B), B.split.value_counts().to_dict())
print(B.groupby('split')[['pre60','pre240','mnA8']].mean().round(3))
# baseline: hourly grid for coins in B, random side = -sign(pre240) (fade the move, like B is typically fading a dump)
coins=B.sym.unique(); rows=[]
for s in coins:
    jj=SIDX[s]
    ii=np.arange(minute_of(pd.Timestamp('2026-08-30').value//10**6),NMIN-8*60-1,60)
    pre240=logret(np.full(len(ii),jj),ii-240,ii)-idxret(ii-240,ii)
    f8=logret(np.full(len(ii),jj),ii,ii+480)-idxret(ii,ii+480)
    rows.append(pd.DataFrame({'sym':s,'i0':ii,'pre240':pre240,'f8':f8}))
G=pd.concat(rows).dropna()
G['split']=np.where(T0+G.i0*60000<pd.Timestamp('2026-09-12 18:00').value//10**6,'early',np.where(T0+G.i0*60000<DISC_END,'disc','test'))
bins=[-100,-5,-2,-1,-0.5,0,0.5,1,2,5,100]
# for events: side-signed pre240 (negative = price went against our side = we're fading a move)
B['bin']=pd.cut(B.pre240,bins)
out=open(W+'out_B_control.txt','w')
def pr(s): print(s); out.write(s+'\n')
for split in ('early','disc','test'):
    g=G[G.split==split]
    # baseline expectation for side-signed: for a random coin-hour, choose side s; signed pre = s*pre_raw; signed fwd = s*f8. Use both sides -> symmetric table
    gg=pd.concat([pd.DataFrame({'pre':g.pre240,'fwd':g.f8}),pd.DataFrame({'pre':-g.pre240,'fwd':-g.f8})])
    gg['bin']=pd.cut(gg.pre,bins); base=gg.groupby('bin',observed=True).fwd.mean()
    b=B[B.split==split].dropna(subset=['mnA8'])
    exp=b.bin.map(base).astype(float)
    pr(f'[{split}] B n={len(b)} mean mnA8={b.mnA8.mean():+.3f} med={b.mnA8.median():+.3f} | pre240 mean={b.pre240.mean():+.2f} | baseline(same pre-move bucket, generic coin-hours) mean={exp.mean():+.3f} | excess={ (b.mnA8-exp).mean():+.3f} CIcoin={tuple(round(x,2) for x in cluster_ci((b.mnA8-exp).values,b.coin.values,n=1000)[:2])}')
    pr('   baseline table (signed pre240 bucket -> mean signed fwd8 mnA): '+' '.join(f'{str(k)}:{v:+.2f}' for k,v in base.items()))
    for L,nm in ((1,'mnA1'),(4,'mnA4'),(24,'mnA24')):
        pr(f'   {nm}: mean={b[nm].mean():+.3f}')
B.to_parquet(W+'b_union.parquet')

"""Final B numbers (re-attached with complete funding): B-union fade after full close, horizons 8/12/24h.
Pooled across early+disc+test and per split; placebo (same coin/split window/hour/side, 300 draws); sign-perm in coin;
leave-top-coin-out; LONG/SHORT; cost sensitivity (0.146 / 0.10 / maker 0.04)."""
import numpy as np, pandas as pd
from lh import *; from evalx import *
B=pd.read_parquet(W+'b_union_ev.parquet')
base=B[['addr','coin','dec_ms','side','day','src','pre240']].copy()
B=attach(base,hs=[8,12,24])
B.to_parquet(W+'b_union_final.parquet')
out=open(W+'out_B_final.txt','w')
def pr(x): print(x); out.write(x+'\n')
pr(f'B-union events={len(B)} splits={B.split.value_counts().to_dict()} coins={B.coin.nunique()} wallets={B.addr.nunique()}')
for h in (8,12,24):
    pr(f'\n===== h={h}h')
    for name,v in (('POOLED',B),('early',B[B.split=='early']),('disc',B[B.split=='disc']),('test',B[B.split=='test'])):
        v=v.dropna(subset=[f'mnA{h}'])
        nm,_=placebo(v,h,'mnA',300); sp_=signperm(v,h,'mnA',300); obs=v[f'mnA{h}'].mean()
        pr(f'[{name}] mnA: '+honest(v[f'mnA{h}'].values,v,('coin','day','addr'),n=1000))
        pr(f'     placebo null mean={nm.mean():+.3f} sd={nm.std():.3f} P(null>=obs)={(nm>=obs).mean():.3f} | signperm P={(sp_>=obs).mean():.3f}')
        c=v.groupby('coin')[f'mnA{h}'].sum().sort_values()
        pr(f'     w/o top1 coin ({c.index[-1]}) mean={v[v.coin!=c.index[-1]][f"mnA{h}"].mean():+.3f}; w/o top3 coins={v[~v.coin.isin(c.index[-3:])][f"mnA{h}"].mean():+.3f}')
        for sd,s_ in ((1,'LONG'),(-1,'SHORT')):
            vv=v[v.side==sd]; pr(f'     {s_}: n={len(vv)} mnA mean={vv[f"mnA{h}"].mean():+.3f} med={vv[f"mnA{h}"].median():+.3f} raw mean={vv[f"raw{h}"].mean():+.3f} mnB mean={vv[f"mnB{h}"].mean():+.3f}')
        fund=v[f'fund{h}'].fillna(0)
        pr(f'     net mnA: cost0.146+fund={np.mean(v[f"mnA{h}"]-0.146-fund):+.3f} (med {np.median(v[f"mnA{h}"]-0.146-fund):+.3f}) | cost0.10={np.mean(v[f"mnA{h}"]-0.10-fund):+.3f} | maker0.04={np.mean(v[f"mnA{h}"]-0.04-fund):+.3f} | no funding, 0.146={np.mean(v[f"mnA{h}"]-0.146):+.3f} | fund mean={fund.mean():+.3f}')
        pr(f'     raw net (no hedge) 0.146+fund: mean={np.mean(v[f"raw{h}"]-0.146-fund):+.3f} med={np.median(v[f"raw{h}"]-0.146-fund):+.3f}')
out.close()

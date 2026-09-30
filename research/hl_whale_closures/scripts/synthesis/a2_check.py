# Synthesis recheck of A2 from the skeptic's independent fill list (s_a2_exit.parquet): day-level P&L, concentration, exits.
import pandas as pd, numpy as np
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
X=pd.read_parquet(SP+'work/skeptic-hl-dislocation/s_a2_exit.parquet')
X['real']=X['R1000_0.1_px_fb_bn']          # maker exit fair-0.1 (lat 1s), fallback taker fair60-0.25
X['real_hl']=X['R1000_0.1_px_fb_hl']       # maker exit, fallback HL own close
X['hl']=X['fb_hl']                          # all exits at HL own 5m close 1-6 min later
X['tk']=X['fb_bn']                          # all taker
# conservative: maker exit only if tracked opposite volume through our ask >= $5k is not known -> use min(real, hl) where hl known
X['cons']=np.where(X.hl.notna(), np.minimum(X.real, X.hl), X.real)
def cb(x,cl,n=4000,seed=1):
    x=np.asarray(x,float); u,inv=np.unique(np.asarray(cl),return_inverse=True)
    s=np.bincount(inv,weights=x); k=np.bincount(inv); r=np.random.default_rng(seed)
    m=[s[p].sum()/k[p].sum() for p in (r.integers(0,len(u),len(u)) for _ in range(n))]
    return np.percentile(m,[5,95])
print('fills',len(X),'days',X.day.nunique(),'whales',X.whale.nunique(),'coins',X.coin.nunique(),'LONG share',(X.wd<0).mean().round(2))
for col in ('P60','real','real_hl','hl','tk','cons'):
    for nm,g in (('DISC',X[~X.test]),('TEST',X[X.test]),('ALL',X)):
        g=g[g[col].notna()]
        cw=cb(g[col],g.whale); cd=cb(g[col],g.day)
        print(f'{col:8s} {nm} n={len(g):3d} mean={g[col].mean():+.3f} med={g[col].median():+.3f} win={(g[col]>0).mean():.2f} CIw[{cw[0]:+.2f},{cw[1]:+.2f}] CId[{cd[0]:+.2f},{cd[1]:+.2f}]')
print()
# per-day P&L at $5k per fill, 'real' and 'cons'
d=X.groupby('day').agg(n=('real','size'),real=('real','sum'),cons=('cons','sum'),hl=('hl','sum'))
d['usd_real_5k']=d.real/100*5000; d['usd_cons_5k']=d.cons/100*5000
print(d.round(2).to_string())
alld=pd.date_range('2026-09-12','2026-09-29').strftime('%m-%d')
dd=d.reindex(alld).fillna(0)
for c in ('usd_real_5k','usd_cons_5k'):
    print(c,'per calendar day: mean',dd[c].mean().round(1),'median',dd[c].median().round(1),'days>0',(dd[c]>0).sum(),'of',len(dd),'-> month',(dd[c].mean()*30).round(0))
tot=X.real.sum(); print('share of real sum from 09-18 & 09-23:', (X[X.day.isin(['09-18','09-23'])].real.sum()/tot).round(2))
for nm,m in (('w/o 18&23', ~X.day.isin(['09-18','09-23'])),('w/o 23',X.day!='09-23'),('LONG',X.wd<0)):
    g=X[m]; print(nm,'n',len(g),'real mean',g.real.mean().round(3),'cons mean',g.cons.mean().round(3),'TEST real',g[g.test].real.mean().round(3))
top=X.groupby('whale').real.sum().sort_values(ascending=False)
print('top whales share of positive real sum', (top.head(3)/top[top>0].sum()).round(2).tolist(), 'n fills by top3', X.whale.isin(top.index[:3]).sum())
print('fills per whale', X.whale.value_counts().head(8).tolist())

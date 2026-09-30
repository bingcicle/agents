import pandas as pd, numpy as np, sys
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/'
sys.path.insert(0,W+'skeptic-whale-state'); from sk import dedupe, COST
s=pd.read_parquet(W+'skeptic-whale-state/sim_ep_L24.parquet'); s=s[s.y.notna()&(s.day>='2026-08-27')&(s.day<='2026-09-29')]
s['wk']=pd.to_datetime(s.day).dt.isocalendar().week
s['yb']=s.btc-COST-s.fund.fillna(0); s['yr']=s.raw-COST-s.fund.fillna(0)
def cb(x,cl,n=3000,seed=0):
    x=np.asarray(x,float); u,inv=np.unique(np.asarray(cl),return_inverse=True); S=np.bincount(inv,weights=x); k=np.bincount(inv); r=np.random.default_rng(seed)
    m=[S[p].sum()/k[p].sum() for p in (r.integers(0,len(u),len(u)) for _ in range(n))]; return np.percentile(m,[5,95])
for lab,m in (('SHORT distress (wpnl<0 & liq_dist<5)',s.distress&(s.side<0)),('SHORT wpnl<0 any liq',(s.wpnl<0)&(s.side<0)),('SHORT liq_dist<5 any pnl',(s.liq_dist<5)&(s.side<0)),('SHORT all',s.side<0),('LONG distress',s.distress&(s.side>0))):
    d=dedupe(s[m],24)
    for col in ('y','yb','yr'):
        cd=cb(d[col],d.day); cc=cb(d[col],d.coin)
        wk=d.groupby('wk')[col].agg(['size','mean']).round(2)
        print(f'{lab:38s} {col:2s} n={len(d):4d} mean={d[col].mean():+.2f} med={d[col].median():+.2f} CId[{cd[0]:+.2f},{cd[1]:+.2f}] CIc[{cc[0]:+.2f},{cc[1]:+.2f}] weeks:', ' '.join(f'{int(w)}:{r["size"]:.0f}/{r["mean"]:+.2f}' for w,r in wk.iterrows()))
d=dedupe(s[s.distress&(s.side<0)],24)
for ex in (['NIL'],['NIL','VVV'],['NIL','VVV','PONS']):
    g=d[~d.coin.isin(ex)]; print('w/o',ex,'n',len(g),'y',g.y.mean().round(2),'yb',g.yb.mean().round(2),'TEST y',g[g.day>='2026-09-22'].y.mean().round(2))
print('trades/day DISC',round(len(d[d.day<'2026-09-22'])/26,1),'TEST',round(len(d[d.day>='2026-09-22'])/7,1))

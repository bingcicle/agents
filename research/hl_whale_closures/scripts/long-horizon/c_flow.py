"""Mechanism C: net signed whale close flow per coin per period (hour / 4h / day), known by bot_ts, predicts next period mnA return?
flow_usd = sum(wdir*tx_usd) over txs whose bot_ts falls in the period; flow_rel = flow_usd / Binance quote volume of the same period.
Next-period return: from period end + 1 min, over H hours (H = period length and multiples). Cross-sectional (mnA) + raw."""
import numpy as np, pandas as pd
from lh import *
from scipy.stats import spearmanr
t=pd.read_parquet(W+'txs_slim.parquet')
t['sym']=t.coin.map(coin_sym); t=t[t.sym.notna()].copy()
t['j']=t.sym.map(SIDX).astype(int)
t['m']=minute_of(t.bot_ms.values)
t=t[(t.m>=0)&(t.m<NMIN)]
out=W+'out_C.txt'; fo=open(out,'w')
def P(*a):
    s=' '.join(str(x) for x in a); print(s,flush=True); fo.write(s+'\n')
QVc=np.vstack([np.zeros((1,QV.shape[1])),np.cumsum(QV,axis=0,dtype=np.float64)])
res={}
for per in (1,4,24):
    pm=per*60
    t['p']=t.m//pm
    g=t.groupby(['j','p']).agg(flow=('tx_usd',lambda x: 0),n=('tx_usd','size'))
    g['flow']=t.assign(f=t.wdir*t.tx_usd).groupby(['j','p']).f.sum()
    g['gross']=t.groupby(['j','p']).tx_usd.sum()
    g=g.reset_index()
    a=g.p.values*pm; b=np.minimum(a+pm,NMIN)
    g['qv']=QVc[b,g.j.values]-QVc[a,g.j.values]
    g['flow_rel']=g.flow/g.qv.clip(lower=1)
    g['end_ms']=T0+b*60000
    g['split']=np.where(g.end_ms<=DISC_END,'disc','test')
    # entry at close of the minute after period end (i0 = b), exit after H hours
    for H in (1,4,8,24,72):
        i0=b; i1=b+H*60
        j=g.j.values
        g[f'r{H}']=logret(j,i0,i1); g[f'a{H}']=idxret(i0,i1,'alt'); g[f'mn{H}']=g[f'r{H}']-g[f'a{H}']
        # pre-period return for control
    g['pre']=logret(g.j.values,a-1,b)-idxret(a-1,b,'alt')
    g['coin']=[SYMS[x] for x in g.j]; g['day']=pd.to_datetime(g.end_ms,unit='ms').dt.strftime('%m-%d')
    g=g[g.end_ms>=pd.Timestamp('2026-09-13').value//10**6]
    res[per]=g
    g.to_parquet(W+f'c_flow_{per}h.parquet')
    P(f'\n######## period={per}h: coin-periods with flow={len(g)}, coins={g.coin.nunique()}; |flow_rel| quantiles', np.round(g.flow_rel.abs().quantile([.5,.75,.9,.97]).values,4).tolist())
    for split in ('disc','test'):
        v=g[g.split==split]
        for H in (per, per*2 if per<24 else 48, 24, 72):
            if f'mn{H}' not in v: continue
            x=v[[f'mn{H}','flow_rel','flow','pre']].dropna()
            if len(x)<20: continue
            rho=spearmanr(x.flow_rel,x[f'mn{H}']).statistic
            rho_pre=spearmanr(x.pre,x[f'mn{H}']).statistic
            rho_fp=spearmanr(x.flow_rel,x.pre).statistic
            # sign-follow rule on top |flow_rel| (above discovery 75% quantile)
            thr=res[per][res[per].split=='disc'].flow_rel.abs().quantile(0.75)
            s=v[v.flow_rel.abs()>=thr].dropna(subset=[f'mn{H}'])
            sr=np.sign(s.flow_rel)*s[f'mn{H}']
            P(f'[{split}] H={H:>2}h n={len(x)} spearman(flow_rel, next mnA)={rho:+.3f} | spearman(pre-move, next)={rho_pre:+.3f} | spearman(flow,pre)={rho_fp:+.3f} || follow-flow top25%|flow_rel| n={len(s)} mean mnA={sr.mean():+.3f} med={sr.median():+.3f} '
              f'CIcoin={tuple(round(q,3) for q in cluster_ci(sr.values,s.coin.values,n=500)[:2])} CIday={tuple(round(q,3) for q in cluster_ci(sr.values,s.day.values,n=500)[:2])} | buyflow n={int((s.flow_rel>0).sum())} {sr[s.flow_rel>0].mean():+.3f} sellflow n={int((s.flow_rel<0).sum())} {sr[s.flow_rel<0].mean():+.3f}')
fo.close()

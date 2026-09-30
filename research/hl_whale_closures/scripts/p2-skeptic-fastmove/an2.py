import numpy as np, pandas as pd, sys
exec(open('an.py').read().split("pd.set_option")[0])
g=d[(d.X==2)&(d.N==5)].copy(); g['oidn']=g.oi5<=-0.5
print('March bounce by coin top:',d[d.mon==3].groupby('sym').bounce.agg(['mean','size']).sort_values('mean').tail(5).round(3).to_dict())
print('\nOIDN(lag5) vs rest, gross e1 / neutral by month:')
print(g.pivot_table(index='oidn',columns='mon',values='g_e1',aggfunc='mean').round(3).to_string())
print(g.pivot_table(index='oidn',columns='mon',values='neu',aggfunc='mean').round(3).to_string())
print(g.pivot_table(index='oidn',columns='mon',values='g_e1',aggfunc='size').to_string())
f=g[g.oidn]
for p in ('TR','TE'):
    fp=f[f.per==p]
    for col in ('g_e1','g_e_c1','neu'): rep(fp,col,f'OIDN {p} {col}')
    x=fp.g_e1-0.246; xs=np.sort(x.values)[::-1]; k=int(len(xs)*.1)
    pos=fp.assign(n=x)[x>0].groupby('day').n.sum()
    print(f'  net base {x.mean():+.3f} low(0.15) {fp.g_e1.mean()-0.15:+.3f} high(0.346) {fp.g_e1.mean()-0.346:+.3f} sum={x.sum():.1f} sum w/o top10%={xs[k:].sum():.1f} q1={np.percentile(x,1):.2f} q5={np.percentile(x,5):.2f} min={x.min():.2f}; top day share of +sum {pos.max()/pos.sum():.2f}')
    daysum=fp.groupby('day').g_e1.sum().sort_values(ascending=False); print('  top days',daysum.head(5).round(1).to_dict(), 'total',round(fp.g_e1.sum(),1))
    print('  mean w/o top3 days', fp[~fp.day.isin(daysum.index[:3])].g_e1.mean().round(3))
# incremental: within month-coin FE regression of gross on oidn
import statsmodels.formula.api as smf
for p in ('TR','TE'):
    gp=g[g.per==p].dropna(subset=['oi5'])
    r=smf.ols('g_e1 ~ oidn + C(mon)',gp).fit(cov_type='cluster',cov_kwds={'groups':pd.factorize(gp.day)[0]})
    print(p,'incremental OIDN effect (month FE, day-clustered):',round(r.params['oidn[T.True]'],3),'se',round(r.bse['oidn[T.True]'],3))
# threshold sensitivity
for th in (-0.25,-0.5,-1,-2):
    for p in ('TR','TE'):
        fp=g[(g.per==p)&(g.oi5<=th)]; print(f'th {th} {p} n={len(fp)} g={fp.g_e1.mean():+.3f}',end=' | ')
    print()

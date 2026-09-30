from common_s import *
A=pd.read_parquet(OUT+'anchors.parquet'); R=pd.read_parquet(OUT+'s1_anchor.parquet')
X=A.merge(R,on='ep')
# reversal claim
for K,c,thr,H in ((300,'m120',1.0,3600),(300,'m120',0.5,3600),(60,'m30',0.5,3600)):
    d=X[X[c]>=thr]; g=d[f'ae{K}']-d[f'ax{H}']
    print(f'fade K{K} {c}>={thr} H{H}: n={g.notna().sum()} D {g[d.per=="D"].mean():+.3f} T {g[d.per=="T"].mean():+.3f}')
# bot detection lag
X['lag']=X.bot_ts-X.ts/1000
print('bot lag by src (median, q90):', X.groupby('detect_src').lag.quantile([.5,.9]).unstack().round(2).to_dict())
# candidate extras
d=pd.read_parquet(OUT+'cand_L300.parquet')
print('per-day net (L300):'); print(d.groupby(['per','day']).net.agg(['size','mean']).round(3).T.to_string())
x=d[d.per=='T']
lodo=[x[x.day!=dy].net.mean() for dy in sorted(x.day.unique())]; print('T leave-one-day-out net range', round(min(lodo),3), round(max(lodo),3))
loco=[x[x.coin!=c].net.mean() for c in x.coin.unique()]; print('T leave-one-coin-out net range', round(min(loco),3), round(max(loco),3))
# excluding top 3 coins by net sum
top3=x.groupby('coin').net.sum().sort_values().index[-3:]; print('T without top3 coins', list(top3), round(x[~x.coin.isin(top3)].net.mean(),3), 'n', (~x.coin.isin(top3)).sum())
# time overlap across coins: trades starting within 60 s of another coin's trade
t=d.sort_values('t0').t0.values; close=np.r_[False,np.diff(t)<60000]|np.r_[np.diff(t)<60000,False]; print('share trades within 60 s of another coin trade', close.mean().round(2))
# neighbours rel24 thresholds with touch L500 H300 dedupe, D/T net
from cand_ms import EV, mk
for thr in (1,2,3,4,6,8):
    dd=mk(EV[EV.rel24>=thr],500,300)
    print(f'rel24>={thr}: '+' | '.join(f'{p}: n={len(dd[dd.per==p])} gross {dd[dd.per==p].g.mean():+.3f} net146 {dd[dd.per==p].g.mean()-0.146:+.3f} net10 {dd[dd.per==p].g.mean()-0.10:+.3f}' for p in ('D','T')))

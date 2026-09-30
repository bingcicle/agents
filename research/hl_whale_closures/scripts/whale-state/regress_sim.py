# Sim universe: does whale PnL / liq distance at the trigger predict the market-neutral move (whale trade dir), beyond the coin's own
# past market-neutral returns (generic reversal)? OLS, winsorized y, cluster SE by coin x day. Usage: python regress_sim.py disc|test
import sys, numpy as np, pandas as pd, statsmodels.api as sm
W='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state/'
which=sys.argv[1]
d=pd.read_parquet(W+'sim_events_out.parquet')
d=d[d.day<='2026-09-21'] if which=='disc' else d[(d.day>='2026-09-22')&(d.day<='2026-09-29')]
d=d.copy(); d['cd']=d.coin+'|'+d.day
d['mn24']=d.L24-d.alt24; d['mn8']=d.L8-d.alt8; d['mid_1800mn']=d.mid_1800-d.alt_s1800
def X(cols):
    x=pd.DataFrame({'wpnl':np.clip(d.wpnl,-30,30)/10,
        'liq_lt5':(d.liq_dist<5).astype(int),'liq_nan':d.liq_dist.isna().astype(int),
        'past24':np.clip(d.past_mn_24h,-30,30)/10,'past72':np.clip(d.past_mn_72h,-50,50)/10,'past7d':np.clip(d.past_mn_7d,-80,80)/10,
        'whale_buys':(d.wdir==1).astype(int),'log_ratio':np.log(np.clip(d.whale_ratio,0.1,None))})
    return sm.add_constant(x[cols])
print(f'== sim {which}: n={len(d)} coin-days={d.cd.nunique()} wallets={d.addr.nunique()}')
for y in ('mid_300','mid_1800mn','mn8','mn24'):
    for cols in (['wpnl','liq_lt5','liq_nan','whale_buys','log_ratio'], ['wpnl','liq_lt5','liq_nan','past24','past72','past7d','whale_buys','log_ratio'], ['past24','past72','past7d','whale_buys','log_ratio']):
        xx=X(cols); m=d[y].notna()&xx.notna().all(axis=1)
        yy=np.clip(d.loc[m,y],-15,15) if y.startswith('mn') else np.clip(d.loc[m,y],-5,5)
        r=sm.OLS(yy,xx[m]).fit(cov_type='cluster',cov_kwds={'groups':pd.factorize(d.loc[m,'cd'])[0]})
        print(f'-- y={y} n={int(m.sum())} R2={r.rsquared:.3f} :: '+'  '.join(f'{k}={r.params[k]:+.3f}(t{r.tvalues[k]:+.1f})' for k in cols))

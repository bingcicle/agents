# Fact check: whale PnL effect on 24h mn return in S DISC is generic reversal of the coin's past mn return
import sys; sys.path.insert(0,'.')
from sk import *
import statsmodels.api as sm
ep=pd.read_parquet('sim_ep_L24b.parquet')
# whale-direction 24h mn outcome (gross): y_w = -our fade ew  (since side=-wtd)
ep['y_w']=-ep.ew
te=entry_time(ep.dec_ms.values); me=m_of_close_at(te)
# past mn returns in whale-direction over 24h, 72h, 7d, measured up to the last closed minute before dec (me-1 is safe: te-60s <= dec+60s... use minute closing at floor(dec/60s)*60s)
mp=m_of_close_at((np.floor(ep.dec_ms.values/60000)*60000).astype(np.int64))
k=ep.symbol.map(SI).values
for lab,h in (('p24',24),('p72',72),('p7d',168)):
    v=np.full(len(ep),np.nan)
    for i in range(len(ep)):
        a=mp[i]-60*h
        if a<0 or pd.isna(k[i]) or mp[i]>=NM: continue
        r=C[int(k[i]),mp[i]]/C[int(k[i]),a]-1; al=C[ALT,mp[i]]/C[ALT,a]-1; al=np.nanmean(al[np.isfinite(al)&(ALT!=k[i])])
        v[i]=100*ep.wtd.values[i]*(r-al)
    ep[lab]=v
d=ep[(ep.day<='2026-09-21')&ep.y_w.notna()].copy(); d['cd']=d.coin+'|'+d.day
d['whale_buys']=(d.wtd==1).astype(int)
def fit(cols):
    z=d.dropna(subset=cols+['y_w'])
    X=sm.add_constant(z[cols].assign(wpnl=np.clip(z.wpnl,-50,50)/10) if 'wpnl' in cols else z[cols])
    m=sm.OLS(np.clip(z.y_w,-15,15),X).fit(cov_type='cluster',cov_kwds={'groups':pd.factorize(z.cd)[0]})
    return ' '.join(f'{c}={m.params[c]:+.3f}(t{m.tvalues[c]:+.1f})' for c in cols)+f' n={len(z)}'
print('DISC y=24h mn in whale dir (clip 15), cluster coin-day')
print(' wpnl only        :',fit(['wpnl']))
print(' wpnl + past      :',fit(['wpnl','p24','p72','p7d']))
print(' wpnl + past + buy:',fit(['wpnl','p24','p72','p7d','whale_buys']))
print(' corr(wpnl,p24/p72/p7d)', d[['wpnl','p24','p72','p7d']].corr().iloc[0].round(2).to_dict())
d=ep[(ep.day>='2026-09-22')&ep.y_w.notna()].copy(); d['cd']=d.coin+'|'+d.day; d['whale_buys']=(d.wtd==1).astype(int)
print('TEST same')
print(' wpnl only        :',fit(['wpnl']))
print(' wpnl + past      :',fit(['wpnl','p24','p72','p7d']))
ep.to_parquet('sim_ep_L24c.parquet')

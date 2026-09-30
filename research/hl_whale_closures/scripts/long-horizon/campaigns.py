"""Unwind campaigns of big positions (no 6h-gap requirement). Per (addr,coin): reference position = sp of the first observed tx
(if the first tx is within 6h of data start, flagged). Track cumulative closed size / ref; times t10,t25,t50,t90 (bot_ms when crossed).
Descriptive: mnA drift in WHALE direction from t10 to t90 (during the unwind) and after t90 (overhang removal), per campaign.
Tradable variant A25: enter in whale direction at bot_ts when 25% of ref is closed (unwind in progress), hold H.
Tradable variant B90: enter AGAINST whale direction when 90% closed, hold H."""
import numpy as np, pandas as pd
from lh import *; from evalx import *
t=pd.read_parquet(W+'txs_slim.parquet').sort_values('ts')
t['sym']=t.coin.map(coin_sym); t=t[t.sym.notna()]
rows=[]
for (a,c),d in t.groupby(['addr','coin']):
    ref=d.sp.iloc[0]; px0=d.px.iloc[0]; dep=d.depth_usd.iloc[:5].median()
    cum=np.cumsum(d.sz.values)/ref; bot=d.bot_ms.values
    r=dict(addr=a,coin=c,wdir=d.wdir.iloc[0],ref_val=ref*px0,ratio_ref=ref*px0/dep if dep>0 else np.nan,ts_first=d.ts.iloc[0],
           n=len(d),closed_total=cum[-1])
    for q in (10,25,50,90):
        k=np.where(cum>=q/100)[0]; r[f't{q}']=bot[k[0]] if len(k) else np.nan
    rows.append(r)
Cp=pd.DataFrame(rows)
Cp=Cp[(Cp.ref_val>=200e3)&(Cp.ts_first>=t.ts.min()+6*3.6e6)]   # drop pairs already open at data start? keep flag
print('pairs ref>=200k (excluding first 6h):',len(Cp))
Cp['dur_10_90_h']=(Cp.t90-Cp.t10)/3.6e6
big=Cp[(Cp.ratio_ref>=3)&Cp.t90.notna()].copy()
j=big.coin.map(lambda c: SIDX[coin_sym(c)]).values
i10=entry_minute(big.t10.values); i90=entry_minute(big.t90.values)
big['drift_during']=big.wdir*(logret(j,i10,i90)-idxret(i10,i90))
for H in (24,72):
    big[f'after{H}']=-big.wdir*(logret(j,i90,i90+H*60)-idxret(i90,i90+H*60))
out=open(W+'out_campaigns.txt','w')
def pr(s): print(s); out.write(s+'\n')
pr(f'big campaigns ratio_ref>=3 and reached 90% closed: n={len(big)} coins={big.coin.nunique()}; duration t10->t90 h: median={big.dur_10_90_h.median():.2f} q75={big.dur_10_90_h.quantile(.75):.1f} q90={big.dur_10_90_h.quantile(.9):.1f}')
pr('  drift DURING unwind (whale dir, minus alt idx, t10->t90): '+summary(big.drift_during.values))
pr('  drift during, only campaigns lasting >=2h: '+summary(big[big.dur_10_90_h>=2].drift_during.values))
for H in (24,72): pr(f'  AFTER 90% closed, against whale dir, {H}h: '+summary(big[f'after{H}'].values))
# by ratio of ref
Cp['rb']=pd.cut(Cp.ratio_ref,[0,1,3,10,1000])
pr(Cp.groupby('rb',observed=True).agg(n=('n','size'),reach50=('t50',lambda x: x.notna().mean()),reach90=('t90',lambda x: x.notna().mean()),dur_med=('dur_10_90_h','median')).round(2).to_string())
# tradable A25 / B90 events
evA=Cp[Cp.t25.notna()&(Cp.ratio_ref>=2)].copy(); evA['dec_ms']=evA.t25; evA['side']=evA.wdir; evA['day']=pd.to_datetime(evA.dec_ms,unit='ms').dt.strftime('%m-%d')
evB=Cp[Cp.t90.notna()&(Cp.ratio_ref>=2)].copy(); evB['dec_ms']=evB.t90; evB['side']=-evB.wdir; evB['day']=pd.to_datetime(evB.dec_ms,unit='ms').dt.strftime('%m-%d')
evA=attach(evA); evB=attach(evB)
out.close()
compact(evA,'A25: follow whale dir when 25% of its (first-seen) position is closed, ratio_ref>=2, ref>=200k',out=W+'out_campaigns.txt',splits=('disc','test'))
compact(evB,'B90: fade whale dir when 90% of its position is closed, ratio_ref>=2, ref>=200k',out=W+'out_campaigns.txt',splits=('disc','test'))

import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from myload import *; from mystats import *
pd.set_option('display.width',250)
f=follow()
f['net']=f.s_net_official_pct; f['gross']=f.s_gross_tape_pct; f['cost']=f.s_costs_pct
fl=f.s_flags.fillna('').str.split(';')
has=lambda t: fl.apply(lambda L: t in L)
f['vf']=(f.s_status=='verified')&f.net.notna()
prof=['F4_розумний','F5_перший','F7_без_ратіо','F9_без_ратіо_90','F10_розумний_60']
f['hd']=f.vf&~has('no_whale_fill')&~has('trigger_unmatched')&~has('partial')&~has('no_tape')&~f.s_trig_match.isin(['none','legacy_amb','legacy_none'])\
    &(f.s_lag_s<=20)&f.s_exit_reason_tape.notna()&~f.s_exit_reason_tape.fillna('').str.endswith('_late')&~(f.strategy.isin(prof)&(f.prof_status=='uncertain'))
f['day']=f['loc'].dt.strftime('%m-%d')
print(f.strategy.value_counts().to_dict())
f.to_pickle('f.pkl')
END=pd.Timestamp('2026-09-30 21:07:06')
def rep(d,name,since):
    days=(END-pd.Timestamp(since)).total_seconds()/86400
    d=d.sort_values('open_ts_ms'); x=d.net.values; n=len(x)
    if n==0: print(name,0); return
    k=max(1,int(round(n*.1)))
    s=f'{name}: n={n} ev={d.whale_addr.str.cat(d.coin).nunique()} w={d.whale_addr.nunique()} days={d.day.nunique()} med={np.median(x):+.3f} mean={x.mean():+.3f} win={np.mean(x>0):.2f} sum={x.sum():+.2f} $/mo={x.sum()*10/days*30:+.0f} woTop10={np.sort(x)[::-1][k:].sum():+.2f}'
    if n>=4:
        h=n//2; s+=f' halves {x[:h].mean():+.3f}/{x[h:].mean():+.3f}'
        s+=f' CIday mean {np.round(cboot(x,d.day.values)[:2],3)} med {np.round(cboot(x,d.day.values,np.median)[:2],3)}'
    print(s)
# VVV pair
W='0x0871deb3'
for card in ['F1_швидкий','F2_2хв','F3_3хв']:
    pass
cards=[c for c in f.strategy.unique()]
print(cards)

import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from myload import *; from mystats import *
t=twap()
t['net']=t.s_net_official_pct; t['gross']=t.s_gross_tape_pct; t['cost']=t.s_costs_pct
t['hd']=(t.s_status=='verified')&t.net.notna()&~t.s_flags.fillna('').str.split(';').apply(lambda L:'partial' in L)&(t.s_exit_reason_tape!='cancelled')
t['day']=t['loc'].dt.strftime('%m-%d'); t['sign']=np.where(t.our_side=='LONG',1,-1)
t['t0']=t.s_entry_ts_ms.fillna(t.entry_ts_ms); t['t1']=t.s_exit_ts_ms.fillna(t.t0+3600000)
d=t[(t.strategy=='T2_твап_скорочення')&(t['loc']>='2026-09-13')&t.hd].copy()
d['btc']=[ret('BTCUSDT',a,b,s) for a,b,s in zip(d.t0,d.t1,d.sign)]
d['alt']=[altidx(a,b,s) for a,b,s in zip(d.t0,d.t1,d.sign)]
d['coin_r']=[ret(SYM.get(c) or 'X',a,b,s) if SYM.get(c) else np.nan for c,a,b,s in zip(d.coin,d.t0,d.t1,d.sign)]
for nm,s in [('EVAL 13.09+',d),('IN 13-17.09 10:32',d[d['loc']<'2026-09-17 10:32']),('OOS',d[d['loc']>='2026-09-17 10:32'])]:
    e=s.dropna(subset=['btc'])
    print(f'{nm}: n={len(s)} net mean {s.net.mean():+.3f} | with bn n={len(e)} net {e.net.mean():+.3f} gross {e.gross.mean():+.3f} coin {e.coin_r.mean():+.3f} btc {e.btc.mean():+.3f} alt {e.alt.mean():+.3f} net-btc {(e.net-e.btc).mean():+.3f} net-alt {(e.net-e.alt).mean():+.3f} L/S {int((s.sign>0).sum())}/{int((s.sign<0).sum())}')
    if len(e)>3:
        # beta-adjusted: regress coin_r on alt across trades? simple: net - 1.0*alt and net - beta*btc with beta from k1m
        print('    CI day net-alt', np.round(cboot((e.net-e.alt).values,e.day.values)[:2],3), ' CI day net', np.round(cboot(e.net.values,e.day.values)[:2],3))
o=d[d['loc']>='2026-09-17 10:32']; o['frac']=o.usd/o.pos_usd
print('T2 OOS >=0.9:', o[o.frac>=0.9].net.agg(['size','median','mean','sum']).round(3).to_dict(), ' <0.9:', o[o.frac<0.9].net.agg(['size','median','mean','sum']).round(3).to_dict())
print(o[['date_entry','coin','our_side','frac','move_pct','net','btc','alt']].round(3).to_string())

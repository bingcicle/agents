import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from myload import *; from mystats import *
pd.set_option('display.width',250)
r=rev(); r=r[r.entered==1].copy()
r['net']=r.s_net_official_pct; r['gross']=r.s_gross_tape_pct; r['cost']=r.s_costs_pct
fl=r.s_flags.fillna('').str.split(';')
r['ok']=(r.s_status=='verified')&r.net.notna()&~fl.apply(lambda L:'partial' in L or 'no_whale_fill' in L)
r['bn']=r.s_dump_move_pct
r['day']=r['loc'].dt.strftime('%m-%d')
END=pd.Timestamp('2026-09-30 21:07:06')
for since in ['2026-09-13','2026-09-19']:
    days=(END-pd.Timestamp(since)).total_seconds()/86400
    print('==== since',since)
    for card in ['R1_загальний','R4_великий','R8_тп80','R2_breakout']:
        d=r[(r.strategy==card)&(r['loc']>=since)&r.ok]
        d['b']=pd.cut(d.bn,[-100,0,0.5,1,1.5,2,3,100])
        print(card, 'paper-verified n=',len(d), 'mean',round(d.net.mean(),3))
        print(d.groupby('b').net.agg(['size','mean','median','sum']).round(3).T.to_string())
    # R1 events with BN move >=2 (proposed R4-BN), dedupe by sig_id
    d=r[(r.strategy=='R1_загальний')&(r['loc']>=since)&r.ok&(r.bn>=2)].sort_values('entry_ts_ms')
    x=d.net.values; n=len(x)
    print('R1 & BN>=2: n',n,'wallets',d.whale_addr.nunique(),'days',d.day.nunique(),'med',np.median(x).round(3),'mean',x.mean().round(3),'sum',x.sum().round(2),'$/mo',round(x.sum()*10/days*30))
    if n>=3:
        print('  CI90 mean wallet',np.round(cboot(x,d.whale_addr.values)[:2],3),'day',np.round(cboot(x,d.day.values)[:2],3), 'signflip wallet',signflip(d.gross.values,d.whale_addr.values,d.cost.values))
    print(d[['date','coin','our_side','bn','move_3m_pct','dump_move_pct','net']].assign(w=d.whale_addr.str[:10]).round(3).to_string())
    d=r[(r.strategy=='R4_великий')&(r['loc']>=since)&r.ok&(r.bn>=2)]
    print('R4 card & BN>=2: n',len(d),'sum',d.net.sum().round(2))
    # R4 card all ok (what bot really does)
    d=r[(r.strategy=='R4_великий')&(r['loc']>=since)&r.ok]
    print('R4 paper ok: n',len(d),'mean',d.net.mean().round(3),'med',d.net.median().round(3),'sum',d.net.sum().round(2))
    # spearman on R1
    d=r[(r.strategy=='R1_загальний')&(r['loc']>=since)&r.ok]
    from scipy.stats import spearmanr
    print('R1 spearman(bn, net):',spearmanr(d.bn,d.net))
    # LONG only vs short for bn>=2

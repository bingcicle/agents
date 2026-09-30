import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
from mystats import *
r=pd.read_pickle('r1_bn.pkl'); r=r[r.bn_dec.notna()].copy()
rows=[]
for i,x in r.iterrows():
    sym=SYM.get(x.coin); k=k1m(sym) if sym else None
    sg=1 if x.our_side=='LONG' else -1
    hold=int(round((x.s_exit_ts_ms-x.s_entry_ts_ms)/60000)); hold=max(hold,1)
    L=max(1,int(np.ceil(x.span_s/60)))
    m1=(int(x.decision_ms)//60000)*60000
    if k is None: rows.append((i,np.nan,0)); continue
    w=k.o.loc[m1-3*86400000:m1+3*86400000+hold*60000]; v=w.values; idx=w.index.values
    pre=-sg*100*(v[L:-hold]/v[:-L-hold]-1)          # dump-direction move over L minutes ending at j
    fwd=sg*100*(v[L+hold:]/v[L:-hold]-1)           # our side, hold minutes
    tj=idx[L:-hold]
    sel=(pre>=x.bn_dec)&np.isfinite(fwd)&(np.abs(tj-m1)>7200000)
    rows.append((i,float(np.nanmean(fwd[sel])) if sel.sum() else np.nan,int(sel.sum())))
D=pd.DataFrame(rows,columns=['i','null_g','null_n']).set_index('i'); r=r.join(D)
for nm,s in [('bn>=1.5',r[r.bn_dec>=1.5]),('bn>=2',r[r.bn_dec>=2]),('bn<1.5',r[r.bn_dec<1.5])]:
    e=s.dropna(subset=['null_g'])
    ex=(e.gross-e.null_g)
    print(f'{nm}: n={len(e)} gross {e.gross.mean():+.3f} generic-null gross {e.null_g.mean():+.3f} (median null_n {e.null_n.median():.0f}) excess {ex.mean():+.3f} CIday {np.round(cboot(ex.values,e.day.values)[:2],3)} CIwallet {np.round(cboot(ex.values,e.whale_addr.values)[:2],3)}')
    for a,b in [('2026-09-13','2026-09-22'),('2026-09-22','2026-10-01')]:
        q=e[(e['loc']>=a)&(e['loc']<b)]; print(f'    {a[5:]}..: n={len(q)} net {q.net.mean():+.3f} excess {(q.gross-q.null_g).mean():+.3f}')

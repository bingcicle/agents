"""T1_15: (a) check settlement prices vs own worst-in-3s on 1s bars; (b) generic-pump null: same coin +-3 days, every minute where the
Binance move over the same TWAP duration >= the trade's own Binance move, SHORT 60 min (k1m opens), gross; compare with trade gross."""
import pandas as pd, numpy as np, os, warnings; warnings.filterwarnings('ignore')
from mystats import *
t=pd.read_pickle('t.pkl')
d=t[(t.strategy=='T1_твап_відкриття')&(t['loc']>='2026-09-17 10:32')&t.hd].copy()
def bars(sym, s0, s1):
    out=[]
    for dd in sorted({pd.Timestamp(s,unit='s').strftime('%Y-%m-%d') for s in (s0,s1)}):
        fn=SP+f'data/bars1s/{sym}/{dd}.npz'
        if os.path.exists(fn):
            z=np.load(fn); out.append(pd.DataFrame({'h':z['h'],'l':z['l'],'c':z['c']},index=z['sec']))
    return pd.concat(out).sort_index() if out else None
rows=[]
for i,r in d.iterrows():
    sym=SYM.get(r.coin); k=k1m(sym) if sym else None
    # (a) prices
    e_s=int(r.s_entry_ts_ms//1000); x_s=int(r.s_exit_ts_ms//1000)
    b=bars(sym,e_s-5,x_s+5) if sym else None
    my_e=my_x=np.nan
    if b is not None:
        be=b.loc[e_s:e_s+2]; bx=b.loc[x_s:x_s+2]
        if len(be): my_e=be.l.min()
        if len(bx): my_x=bx.h.max()
    my_gross=-100*(my_x/my_e-1) if np.isfinite(my_e) and np.isfinite(my_x) else np.nan
    # (b) generic null
    st=int(r.twap_id.split('-')[1])*1000; dec=r.decision_ms
    bnmove=np.nan; nullg=np.nan; nn=0
    if k is not None:
        m0=(st//60000)*60000; m1=(int(dec)//60000)*60000
        if m0 in k.index and m1 in k.index:
            bnmove=100*(k.o[m1]/k.o[m0]-1)
            L=max(1,(m1-m0)//60000)
            w=k.o.loc[m0-3*86400000:m0+3*86400000+3600000]
            v=w.values; idx=w.index.values
            if len(v)>L+61:
                pre=100*(v[L:-60]/v[:-L-60]-1)              # move over L minutes ending at j
                fwd=-100*(v[L+60:]/v[L:-60]-1)             # SHORT 60 min from j
                sel=(pre>=bnmove)&np.isfinite(fwd)
                # exclude the trade's own window +-2h
                tj=idx[L:-60]; sel&=np.abs(tj-m1)>7200000
                nn=int(sel.sum()); nullg=float(np.nanmean(fwd[sel])) if nn else np.nan
    rows.append((i,my_gross,bnmove,nullg,nn))
D=pd.DataFrame(rows,columns=['i','my_gross','bn_move','null_gross','null_n']).set_index('i'); d=d.join(D)
d['is15']=d.move_pct>=1.5
print(d[['date_entry','coin','move_pct','bn_move','gross','my_gross','null_gross','null_n','net']].round(3).to_string())
for nm,s in [('T1 all',d),('T1_15',d[d.is15]),('T1 <1.5',d[~d.is15])]:
    e=s.dropna(subset=['null_gross'])
    print(f'{nm}: n={len(e)} gross {e.gross.mean():+.3f} null_gross(generic pump fade same coin +-3d) {e.null_gross.mean():+.3f} excess {(e.gross-e.null_gross).mean():+.3f} CIday {np.round(cboot((e.gross-e.null_gross).values,e.day.values)[:2],3)} my_gross-settle gross {(e.my_gross-e.gross).mean():+.3f}')

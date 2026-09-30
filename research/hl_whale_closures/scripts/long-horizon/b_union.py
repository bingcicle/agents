"""B-union: fade whale trade direction after (near) full close. Sources: whale_txs full closes (12.09-29.09, max_val>=100k over prior 72h)
+ bot follow trades with exit_reason full_close (30.08-29.09). Dedupe: same coin & same side within 6h -> first only."""
import numpy as np, pandas as pd
from lh import *; from evalx import *
Bt=pd.read_parquet(W+'b_full_ev.parquet'); Bt=Bt[Bt.max_val>=100e3].copy(); Bt['src']='tx'
Bg=pd.read_parquet(W+'trig_B_ev.parquet').copy(); Bg['src']='trig'
common=[c for c in Bt.columns if c in Bg.columns]
B=pd.concat([Bt[common],Bg[common]]).sort_values(['coin','i0'])
keep=[]; last={}
for i,r in enumerate(B.itertuples()):
    k=(r.coin,r.side)
    if k in last and r.i0-last[k]<360: continue
    last[k]=r.i0; keep.append(i)
B=B.iloc[keep].reset_index(drop=True)
j=B.sym.map(SIDX).values.astype(int); i0=B.i0.values
for L in (60,240): B[f'pre{L}']=B.side*(logret(j,i0-L,i0)-idxret(i0-L,i0))
B['lag_s']=(B.entry_ms-B.dec_ms)/1000
B.to_parquet(W+'b_union_ev.parquet')
out=W+'out_B_union.txt'; open(out,'w').write(f'B-union events={len(B)} by split {B.split.value_counts().to_dict()} by src {B.src.value_counts().to_dict()}\n')
#compact(B,'B-union: fade after full close',out=out,ndraw=200)
fo=open(out,'a')
for h in (8,24):
    for split in ('early','disc','test'):
        v=B[(B.split==split)].dropna(subset=[f'mnA{h}'])
        fo.write(f'\n-- h={h} [{split}] honesty on mnA: '+honest(v[f'mnA{h}'].values,v,('coin','day','addr'),n=1000))
        fo.write(f'\n   net (raw-0.146-fund): '+summary(v[f'net{h}'].values)+f' | netmn: '+summary(v[f'netmn{h}'].values)+f' | raw: '+summary(v[f'raw{h}'].values)+f' | mnB: '+summary(v[f'mnB{h}'].values))
        days=sorted(v.day.unique()); hh=len(days)//2
        fo.write(f'\n   halves mnA: {v[v.day.isin(days[:hh])][f"mnA{h}"].mean():+.3f} / {v[v.day.isin(days[hh:])][f"mnA{h}"].mean():+.3f}')
        no=nonoverlap(v,h)
        fo.write(f'\n   non-overlapping per coin (n={len(no)}): mnA mean={no[f"mnA{h}"].mean():+.3f} med={no[f"mnA{h}"].median():+.3f} win={100*(no[f"mnA{h}"]>0).mean():.0f}%')
        for sd,nm in ((1,'LONG'),(-1,'SHORT')):
            vv=v[v.side==sd]; fo.write(f'\n   {nm}: n={len(vv)} mnA mean={vv[f"mnA{h}"].mean():+.3f} med={vv[f"mnA{h}"].median():+.3f} raw mean={vv[f"raw{h}"].mean():+.3f}')
        for s_ in ('tx','trig'):
            vv=v[v.src==s_]; fo.write(f'\n   src={s_}: n={len(vv)} mnA mean={vv[f"mnA{h}"].mean():+.3f}')
fo.write('\n'); fo.close()
print(open(out).read()[-6000:])

import numpy as np, pandas as pd, sys
from ev import *
BT=pd.read_parquet(MY+'ev_tx.parquet'); BG=pd.read_parquet(MY+'ev_trig.parquet'); BB=pd.read_parquet(MY+'ev_bat.parquet')
U=union([BT[['addr','coin','dec_ms','side','maxval','src']],BG[['addr','coin','dec_ms','side','maxval','src']]])
print('union events',len(U),U.src.value_counts().to_dict())
U=attach(U)
U.to_parquet(MY+'U.parquet')
print('splits',U.split.value_counts().to_dict(),'coins',U.coin.nunique(),'wallets',U.addr.nunique())
out=open(MY+'out_b_main.txt','w')
def pr(s): print(s); out.write(s+'\n')
for h in (4,8,12,24,48):
    pr(f'\n===== h={h}')
    for sp in ('early','disc','test','ALL'):
        v=U if sp=='ALL' else U[U.split==sp]
        v=v[~v[f'mnA{h}'].isna()]
        nm=placebo(v,h,'mnA',nd=200)
        obs=v[f'mnA{h}'].mean()
        pr(f'[{sp:5s}] mnA {line(v,f"mnA{h}")} | placeboP={(nm>=obs).mean():.3f} null={nm.mean():+.2f}')
        for c in ('raw','mnB','mnM','lgA','mnb'):
            pr(f'         {c}: mean={v[f"{c}{h}"].mean():+.2f} med={v[f"{c}{h}"].median():+.2f}')
        for sd,nmn in ((1,'LONG'),(-1,'SHORT')):
            w=v[v.side==sd]; pr(f'         {nmn}: n={len(w)} mnA mean={w[f"mnA{h}"].mean():+.2f} med={w[f"mnA{h}"].median():+.2f} raw={w[f"raw{h}"].mean():+.2f}')
out.close()

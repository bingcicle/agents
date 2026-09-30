import sys; sys.path.insert(0,'.')
import numpy as np, pandas as pd
from st import bat, fmt, boot
T=pd.read_parquet('twap_with_drift.parquet')
REG=pd.Timestamp('2026-09-17 10:32')
out=[]
def P(*s):
    l=' '.join(str(x) for x in s); print(l); out.append(l)
for c in ['T1_твап_відкриття','T1_твап_відкриття_15','T1_твап_відкриття_20']:
    for start,w in [(REG,'OOS17'),(pd.Timestamp('2026-09-13'),'EVAL13'),(pd.Timestamp('2026-09-20'),'since20')]:
        d=T[(T.card==c)&(T.dt>=start)&T.hd]
        P(c,w,'n',len(d))
        for col in ['net','gross_tape','coin_ret','btc_ret','alt_ret','net_x_btc','net_x_alt','net_x_plc']:
            x=d[col].dropna()
            if len(x)>2:
                lo,hi,p=boot(x.values,d.loc[x.index,'wallet'].values,np.mean); lo2,hi2,p2=boot(x.values,d.loc[x.index,'day'].values,np.mean)
                P(f'    {col}: n={len(x)} med={x.median():+.3f} mean={x.mean():+.3f} CI90mean wallet=[{lo:+.3f},{hi:+.3f}] day=[{lo2:+.3f},{hi2:+.3f}]')
        P('    placebo pct mean', round(d.plc_pct.mean(),3))
d=T[(T.card.str.startswith('T1'))&(T.dt>=REG)]
P(d[['card','dloc','coin','side','twap_side','twap_kind','twap_dur_s','move_pct','hd','net','coin_ret','btc_ret','alt_ret','plc_pct']].assign(w=d.wallet.str[:10]).round(3).sort_values('dloc').to_string())
# overlap of T1 cards events
e=T[T.card.str.startswith('T1')&(T.dt>=REG)].groupby('event').card.apply(lambda s:','.join(sorted(x[-3:] for x in s)))
P(e.value_counts().to_dict())
e=T[T.card.str.startswith('T2')&(T.dt>=REG)].groupby('event').card.apply(lambda s:','.join(sorted(x[-3:] for x in s)))
P(e.value_counts().to_dict())
open('out_h1b_t1.txt','w').write('\n'.join(out))

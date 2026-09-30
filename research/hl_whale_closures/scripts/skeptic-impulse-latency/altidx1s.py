"""Per-day 1s matrix of log close (ffilled) for all symbols having bars1s that day; used for equal-weight alt index
excluding own symbol. Saves alt1s/{day}.npz with syms, base, L (nsym x N float32)."""
import numpy as np, pandas as pd, os
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
B=SP+'data/bars1s/'; OUT=SP+'work/skeptic-impulse-latency/alt1s/'
os.makedirs(OUT,exist_ok=True)
PRE=3700; POST=3800
days=pd.date_range('2026-09-12','2026-09-29').strftime('%Y-%m-%d')
for day in days:
    syms=[s for s in os.listdir(B) if os.path.exists(f'{B}{s}/{day}.npz') and s not in ('BTCUSDT','ETHUSDT')]
    D0=int(pd.Timestamp(day).timestamp()); base=D0-PRE; N=PRE+86400+POST
    M=np.full((len(syms),N),np.nan,np.float32)
    for k,s in enumerate(syms):
        c=np.full(N,np.nan)
        for dd in (-1,0,1):
            d2=(pd.Timestamp(day)+pd.Timedelta(days=dd)).strftime('%Y-%m-%d'); fn=f'{B}{s}/{d2}.npz'
            if not os.path.exists(fn): continue
            z=np.load(fn); i=z['sec']-base; m=(i>=0)&(i<N); c[i[m]]=z['c'][m]
        M[k]=np.log(pd.Series(c).ffill().values)
    np.savez(OUT+day+'.npz',syms=np.array(syms),base=base,M=M)
    print(day,len(syms),flush=True)

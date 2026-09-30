"""Trigger-based episodes from bot follow trades (legacy 30.08-12.09 + current 12.09-30.09): consistent over a month.
A-events: first trigger per (whale,coin) after >=6h without triggers of that pair. side = our_side (= whale trade dir).
B-events: whale full close seen by the bot (exit_reason full_close*), decision = close time; side = -our_side (fade)."""
import numpy as np, pandas as pd
from lh import *
BOTD=SP+'hl_whale_export_20260930/02_bot_data/'
L=pd.read_csv(BOTD+'follow_trades.csv.legacy-1789241806.csv'); L=L[L.eol=='^'].copy()
L['open_ms']=(pd.to_datetime(L.date_open)-pd.Timedelta(hours=2)).values.astype('datetime64[ms]').astype('int64')
L['close_ms']=(pd.to_datetime(L.date_close)-pd.Timedelta(hours=2)).values.astype('datetime64[ms]').astype('int64')
Cc=pd.read_csv(BOTD+'follow_trades.csv',low_memory=False); Cc=Cc[Cc.eol=='^'].copy()
# check TZ on current file
chk=((pd.to_datetime(Cc.date_open)-pd.Timedelta(hours=2)).values.astype('datetime64[ms]').astype('int64')-Cc.open_ts_ms).abs().median(); print('tz check median diff ms',chk)
Cc['open_ms']=Cc.open_ts_ms.astype('int64'); Cc['close_ms']=Cc.close_ts_ms.astype('int64')
cols=['whale_addr','coin','our_side','open_ms','close_ms','exit_reason','ratio','pos_usd','tx_pct_of_pos','tx_usd','strategy']
F=pd.concat([L[cols].assign(src='legacy'),Cc[cols].assign(src='current')])
F['side']=np.where(F.our_side=='LONG',1,-1)
U=F.sort_values('open_ms').drop_duplicates(['whale_addr','coin','open_ms'])
U=U.sort_values(['whale_addr','coin','open_ms'])
U['prev']=U.groupby(['whale_addr','coin']).open_ms.shift(1)
first_ms=U.open_ms.min()
U['start']=(U.open_ms-U.prev>=6*3600e3)|(U.prev.isna()&(U.open_ms>=first_ms+6*3600e3))
A=U[U.start].copy(); A['dec_ms']=A.open_ms; A['addr']=A.whale_addr
A['day']=pd.to_datetime(A.dec_ms,unit='ms').dt.strftime('%m-%d')
A.to_parquet(W+'trig_A.parquet')
# B: full close events (unique per whale,coin,close_ms); dedupe within 6h per pair
B=F[F.exit_reason.str.startswith('full_close')].sort_values('close_ms').drop_duplicates(['whale_addr','coin','close_ms']).copy()
B=B.sort_values(['whale_addr','coin','close_ms']); B['prev']=B.groupby(['whale_addr','coin']).close_ms.shift(1)
B=B[(B.prev.isna())|(B.close_ms-B.prev>=6*3600e3)].copy()
B['dec_ms']=B.close_ms; B['side']=-B.side; B['addr']=B.whale_addr
B['day']=pd.to_datetime(B.dec_ms,unit='ms').dt.strftime('%m-%d')
B.to_parquet(W+'trig_B.parquet')
print('A',len(A),A.src.value_counts().to_dict(),'B',len(B),B.src.value_counts().to_dict())
print(A.groupby('src').side.value_counts())

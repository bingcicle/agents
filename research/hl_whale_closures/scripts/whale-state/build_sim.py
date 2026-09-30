# Universe S: the bot simulator's own trigger events (sim_trades.csv, 27.08-29.09) which carry the whale's state AT THE TRIGGER:
# whale_pnl_pct (price return from whale entry, fraction; verified vs HL closedPnl in probe_pnl.py) and liq_dist (% to liquidation).
# Decision time = sim open time (date_open is CEST -> UTC-2h). Episode dedupe: per wallet x coin x side, new event after >30 min.
# Outcomes computed with the same code as build_outcomes.py (Binance 1s bars where available, 1m klines for long horizon).
import sys, numpy as np, pandas as pd
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W=SP+'work/whale-state/'
sys.path.insert(0,SP+'lib'); from hl import SYM
s=pd.read_csv(SP+'hl_whale_export_20260930/02_bot_data/sim_trades.csv')
s['dec_ms']=((pd.to_datetime(s.date_open)-pd.Timedelta(hours=2))-pd.Timestamp('1970-01-01'))//pd.Timedelta('1ms')
s['symbol']=s.coin.map(SYM); s=s[s.symbol.notna()].copy()
s['wdir']=np.where(s.our_side=='LONG',1,-1)   # sim follows the whale trade direction
s['addr']=s.whale_addr
s=s.sort_values(['addr','coin','wdir','dec_ms'])
gap=s.groupby(['addr','coin','wdir']).dec_ms.diff()
s=s[gap.isna()|(gap>30*60000)].copy()
s['ts']=s.dec_ms; s['dec_sec']=np.ceil(s.dec_ms/1000).astype(np.int64)
s['day']=pd.to_datetime(s.dec_ms,unit='ms').dt.strftime('%Y-%m-%d')
s['wpnl']=100*s.whale_pnl_pct
s=s.reset_index(drop=True)
s.to_parquet(W+'sim_events_state.parquet')
print('sim episodes',len(s),'days',s.day.nunique(), s.day.min(), s.day.max())

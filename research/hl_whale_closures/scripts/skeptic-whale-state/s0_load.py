import pandas as pd, numpy as np
B='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/hl_whale_export_20260930/02_bot_data/'
def load_sim():
    s=pd.read_csv(B+'sim_trades.csv')
    s['open_ms']=(pd.to_datetime(s.date_open)-pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
    s['close_ms']=(pd.to_datetime(s.date_close)-pd.Timedelta(hours=2)).astype('datetime64[ms]').astype('int64')
    s['wtd']=np.where(s.our_side=='LONG',1,-1)   # whale trade dir if sim follows
    s['sw']=-s.wtd                                # whale POSITION side
    s['wpnl']=100*s.whale_pnl_pct
    return s

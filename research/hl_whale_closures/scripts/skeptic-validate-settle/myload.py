"""Independent loader (skeptic). Settlements: last record per key (file order == settled_at order, verified)."""
import pandas as pd, numpy as np
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
B=SP+'hl_whale_export_20260930/02_bot_data/'
def settle():
    s=pd.read_csv(B+'settlements.csv',low_memory=False)
    s=s[s.eol=='^'].groupby('key').tail(1).set_index('key')
    return s
def twap():
    t=pd.read_csv(B+'twap_trades.csv',low_memory=False); t=t[t.eol=='^']
    t['key']=t.twap_id+'|'+t.strategy
    s=settle()
    t=t.join(s.add_prefix('s_'),on='key')
    t['loc']=pd.to_datetime(t.date_entry)
    t['utc_from_loc']=(t['loc']-pd.Timedelta(hours=2))
    return t
def rev():
    r=pd.read_csv(B+'rev_trades.csv',low_memory=False); r=r[r.eol=='^']
    r['key']=r.sig_id.astype(str)+'|'+r.strategy
    s=settle(); r=r.join(s.add_prefix('s_'),on='key'); r['loc']=pd.to_datetime(r.date)
    return r
def follow():
    f=pd.read_csv(B+'follow_trades.csv',low_memory=False); f=f[f.eol=='^']
    s=settle(); f=f.join(s.add_prefix('s_'),on='trade_id'); f['loc']=pd.to_datetime(f.date_open)
    return f

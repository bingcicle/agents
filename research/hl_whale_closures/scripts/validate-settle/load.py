"""Unified settled-trade table for validate-settle lens.
Output: trades.parquet (one row per bot trade row = card x event), official net from settlements.csv
(last record per key wins, as the server does).
Headline reconstruction (best effort; server.py is not in the export):
  follow: sig_ok = no 'no_whale_fill'/'trigger_unmatched' flag and trig_match not in (none, legacy_amb, legacy_none)
          stale  = settlement lag_s missing or > 20 s
          px_ok  = status=='verified' & entry_src!='none' & exit_src!='none' & no 'no_tape' flag
          partial= 'partial' token in flags; exit_ok = exit_reason_tape present; late = exit_reason_tape endswith '_late'
          unc    = profile cards (F4,F5,F7,F9,F10) with prof_status=='uncertain' (server excludes them)
  twap:   px_ok & not partial & exit_reason_tape!='cancelled'  (TWAP fills are system fills -> 'no_whale_fill' ignored)
  rev:    sig_ok = |tape dump_move_pct| >= card threshold (R1/R2/R7/R8 1%, R4 2%, R5 3%) & no 'no_whale_fill';
          px_ok & not partial
All *_ms are UTC; 'dloc' = local CEST string as in bot CSVs; 'day' = local calendar day.
"""
import pandas as pd, numpy as np, sys
BOT = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/hl_whale_export_20260930/02_bot_data/'
OUT = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle/'

def tok(flags, t):
    return flags.fillna('').str.split(';').apply(lambda L: t in L)

s = pd.read_csv(BOT + 'settlements.csv', low_memory=False)
s = s[s['eol'] == '^'].drop_duplicates('key', keep='last')
scols = ['key', 'status', 'flags', 'net_official_pct', 'net_live_pct', 'net_tape_pct', 'gross_tape_pct', 'costs_pct',
         'funding_pct', 'entry_src', 'exit_src', 'exit_reason_tape', 'trig_match', 'lag_s', 'dump_move_pct',
         'entry_ts_ms', 'exit_ts_ms', 'entry_px_tape', 'exit_px_tape', 'net60_tape_pct', 'curve_final', 'settled_at']
s = s[scols].rename(columns={c: 's_' + c for c in scols if c != 'key'})

# ---------------- follow
f = pd.read_csv(BOT + 'follow_trades.csv', low_memory=False)
f = f[f['eol'] == '^']
f = f.merge(s, left_on='trade_id', right_on='key', how='left')
fl = f['s_flags']
f['sig_ok'] = ~(tok(fl, 'no_whale_fill') | tok(fl, 'trigger_unmatched')) & ~f['s_trig_match'].isin(['none', 'legacy_amb', 'legacy_none'])
f['stale'] = f['s_lag_s'].isna() | (f['s_lag_s'] > 20)
f['px_ok'] = (f['s_status'] == 'verified') & (f['s_entry_src'] != 'none') & (f['s_exit_src'] != 'none') & ~tok(fl, 'no_tape') & f['s_net_official_pct'].notna()
f['partial'] = tok(fl, 'partial')
f['exit_ok'] = f['s_exit_reason_tape'].notna()
f['late'] = f['s_exit_reason_tape'].fillna('').str.endswith('_late')
prof_cards = ['F4_розумний', 'F5_перший', 'F7_без_ратіо', 'F9_без_ратіо_90', 'F10_розумний_60']
f['unc'] = f['strategy'].isin(prof_cards) & (f['prof_status'] == 'uncertain')
f['head'] = f['sig_ok'] & ~f['stale'] & f['px_ok'] & ~f['partial'] & f['exit_ok'] & ~f['late'] & ~f['unc']
fo = pd.DataFrame({
    'family': 'F', 'card': f['strategy'], 'key': f['trade_id'], 'event': f['whale_addr'] + ':' + f['coin'] + ':' + f['open_ts_ms'].astype('Int64').astype(str),
    'dloc': f['date_open'], 'ts_ms': f['open_ts_ms'], 'wallet': f['whale_addr'], 'coin': f['coin'], 'side': f['our_side'],
    'net': f['s_net_official_pct'], 'net_live_row': f['net_pct'], 'gross_tape': f['s_gross_tape_pct'], 'costs': f['s_costs_pct'],
    'hold_s': f['hold_s'], 'exit_reason': f['exit_reason'], 'exit_reason_tape': f['s_exit_reason_tape'],
    'lag_s': f['lag_s'], 'tx_pct': f['tx_pct_of_pos'], 'prof_status': f['prof_status'], 'prof_cont_pct': f['prof_cont_pct'],
    'detect_src': f['detect_src'], 'status': f['s_status'], 'flags': f['s_flags'], 'head': f['head'], 'settled': f['s_net_official_pct'].notna(),
    'sig_ok': f['sig_ok'], 'stale': f['stale'], 'px_ok': f['px_ok'], 'partial': f['partial'], 'late': f['late'], 'unc': f['unc'],
    'exit_ts_ms': f['close_ts_ms'], 'pos_usd': f['pos_usd'], 'twap_usd': np.nan, 'move_pct': np.nan, 'dump_tape': np.nan,
})

# ---------------- twap (current + legacy for pre-13.09 context)
parts = []
for fn in ['twap_trades.csv', 'twap_trades.csv.legacy-1789309812.csv']:
    t = pd.read_csv(BOT + fn, low_memory=False)
    if 'eol' in t:
        t = t[t['eol'] == '^']
    t['src_file'] = fn
    parts.append(t)
t = pd.concat(parts, ignore_index=True)
t['key'] = t['twap_id'].astype(str) + '|' + t['strategy']
t = t.merge(s, on='key', how='left')
fl = t['s_flags']
t['px_ok'] = (t['s_status'] == 'verified') & (t['s_entry_src'] != 'none') & (t['s_exit_src'] != 'none') & ~tok(fl, 'no_tape') & t['s_net_official_pct'].notna()
t['partial'] = tok(fl, 'partial')
t['head'] = t['px_ok'] & ~t['partial'] & (t['s_exit_reason_tape'] != 'cancelled')
if 'entry_ts_ms' not in t or t['entry_ts_ms'].isna().any():
    est = (pd.to_datetime(t['date_entry']) - pd.Timedelta(hours=2)).astype('int64') // 10**6
    t['entry_ts_ms'] = t.get('entry_ts_ms', pd.Series(np.nan, index=t.index)).fillna(est)
to = pd.DataFrame({
    'family': 'T', 'card': t['strategy'], 'key': t['key'], 'event': t['twap_id'].astype(str),
    'dloc': t['date_entry'], 'ts_ms': t['entry_ts_ms'], 'wallet': t['whale_addr'], 'coin': t['coin'], 'side': t['our_side'],
    'net': t['s_net_official_pct'], 'net_live_row': t['net60_pct'], 'gross_tape': t['s_gross_tape_pct'], 'costs': t['s_costs_pct'],
    'hold_s': np.nan, 'exit_reason': t['exit_reason'], 'exit_reason_tape': t['s_exit_reason_tape'],
    'lag_s': np.nan, 'tx_pct': np.nan, 'prof_status': None, 'prof_cont_pct': np.nan, 'detect_src': t['src'],
    'status': t['s_status'], 'flags': t['s_flags'], 'head': t['head'], 'settled': t['s_net_official_pct'].notna(),
    'sig_ok': True, 'stale': False, 'px_ok': t['px_ok'], 'partial': t['partial'], 'late': False, 'unc': False,
    'exit_ts_ms': t['s_exit_ts_ms'], 'pos_usd': t['pos_usd'], 'twap_usd': t['usd'], 'move_pct': t['move_pct'], 'dump_tape': np.nan,
})
to['twap_kind'] = t['kind'].values; to['twap_dur_s'] = t['dur_s'].values; to['src_file'] = t['src_file'].values
to['cancel_after_entry'] = t['cancel_after_entry'].values; to['twap_side'] = t['twap_side'].values

# ---------------- rev
r = pd.read_csv(BOT + 'rev_trades.csv', low_memory=False)
r = r[r['eol'] == '^'].copy()
r = r[r['entered'] == 1]
r['key'] = r['sig_id'].astype(str) + '|' + r['strategy']
r = r.merge(s, on='key', how='left')
thr = {'R1_загальний': 1, 'R2_breakout': 1, 'R7_одним': 1, 'R8_тп80': 1, 'R4_великий': 2, 'R5_дуже': 3, 'R6_волт': 1}
fl = r['s_flags']
r['sig_ok'] = (r['s_dump_move_pct'].abs() >= r['strategy'].map(thr) - 1e-9) & ~tok(fl, 'no_whale_fill')
r['px_ok'] = (r['s_status'] == 'verified') & (r['s_entry_src'] != 'none') & (r['s_exit_src'] != 'none') & ~tok(fl, 'no_tape') & r['s_net_official_pct'].notna()
r['partial'] = tok(fl, 'partial')
r['head'] = r['sig_ok'] & r['px_ok'] & ~r['partial']
ro = pd.DataFrame({
    'family': 'R', 'card': r['strategy'], 'key': r['key'], 'event': r['sig_id'].astype(str),
    'dloc': r['date'], 'ts_ms': r['entry_ts_ms'], 'wallet': r['whale_addr'], 'coin': r['coin'], 'side': r['our_side'],
    'net': r['s_net_official_pct'], 'net_live_row': np.nan, 'gross_tape': r['s_gross_tape_pct'], 'costs': r['s_costs_pct'],
    'hold_s': np.nan, 'exit_reason': r['exit_reason'], 'exit_reason_tape': r['s_exit_reason_tape'],
    'lag_s': r['lag_s'], 'tx_pct': np.nan, 'prof_status': None, 'prof_cont_pct': np.nan, 'detect_src': r['detect_src'],
    'status': r['s_status'], 'flags': r['s_flags'], 'head': r['head'], 'settled': r['s_net_official_pct'].notna(),
    'sig_ok': r['sig_ok'], 'stale': False, 'px_ok': r['px_ok'], 'partial': r['partial'], 'late': False, 'unc': False,
    'exit_ts_ms': r['s_exit_ts_ms'], 'pos_usd': np.nan, 'twap_usd': np.nan, 'move_pct': r['dump_move_pct'], 'dump_tape': r['s_dump_move_pct'],
})

allt = pd.concat([fo, to, ro], ignore_index=True)
allt['dt'] = pd.to_datetime(allt['dloc'])
allt['day'] = allt['dt'].dt.strftime('%Y-%m-%d')
allt['ts_ms'] = allt['ts_ms'].astype(float)
allt['vf'] = allt['status'] == 'verified'
allt = allt.rename(columns={'head': 'hd'})
allt.to_parquet(OUT + 'trades.parquet')
print(allt.groupby(['family']).agg(n=('key', 'size'), settled=('settled', 'sum'), hd=('hd', 'sum')))
print(allt[allt.dt >= '2026-09-13'].groupby('card').agg(n=('key', 'size'), settled=('settled', 'sum'), hd=('hd', 'sum'), vf=('vf', 'sum')))

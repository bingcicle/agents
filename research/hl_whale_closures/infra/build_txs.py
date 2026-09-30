# Build a deduplicated table of every whale close transaction seen in close_batch events (12.09-30.09).
# One row per (addr, coin, tx hash). Also a batch table.
import json, glob, pandas as pd, numpy as np
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
D=SP+'hl_whale_export_20260930/02_bot_data/'
rows=[]; batches=[]
for f in sorted(glob.glob(D+'events-*.jsonl')):
    for line in open(f):
        e=json.loads(line)
        if e['kind']!='close_batch': continue
        depth = e['old_val']/e['ratio'] if e['ratio'] else np.nan
        tmax=max((t['ts'] for t in e['txs']), default=np.nan)
        batches.append(dict(bot_ts=e['ts'], addr=e['addr'], coin=e['coin'], wside=e['side'], full_close=e['full_close'],
            detect_src=e['detect_src'], snap_ms=e['snap_ms'], old_size=e['old_size'], old_val=e['old_val'], ratio=e['ratio'],
            new_size=e['new_size'], n_txs=e['n_txs'], n_dup=e['n_dup'], n_passive=e['n_passive'], truncated=e['truncated'],
            depth_usd=depth, last_tx_ms=tmax))
        for t in e['txs']:
            rows.append(dict(addr=e['addr'], coin=e['coin'], wside=e['side'], h=t['h'], ts=t['ts'], px=t['px'], pf=t['pf'], pl=t['pl'],
                sz=t['sz'], sp=t['sp'], dir=t['dir'], liq=t['liq'], nf=t['n'], bot_ts=e['ts'], detect_src=e['detect_src'],
                batch_ratio=e['ratio'], batch_old_val=e['old_val'], depth_usd=depth, batch_full=e['full_close']))
tx=pd.DataFrame(rows); b=pd.DataFrame(batches)
print('raw tx rows',len(tx))
tx=tx.sort_values('bot_ts').drop_duplicates(['addr','coin','h'],keep='first')   # first time the bot saw it
tx['tx_usd']=tx.sz*tx.px
tx['tx_pct']=100*tx.sz/tx.sp.abs().replace(0,np.nan)
tx['sweep_pct']=100*(tx.pl/tx.pf-1)          # signed price range of the tx on HL
tx['detect_lag_s']=tx.bot_ts-tx.ts/1000
tx=tx.sort_values(['ts']).reset_index(drop=True)
tx.to_parquet(SP+'data/whale_txs.parquet'); b.to_parquet(SP+'data/close_batches.parquet')
print('unique txs',len(tx),'batches',len(b))
print(tx[['tx_usd','tx_pct','detect_lag_s','depth_usd','batch_ratio']].describe(percentiles=[.1,.5,.9,.99]).T)
big=tx[(tx.tx_usd>=5000)&(tx.tx_pct>=5)]
print('txs >=5k & >=5% pos:',len(big),'coins',big.coin.nunique(),'addrs',big.addr.nunique())
print(big.detect_src.value_counts())

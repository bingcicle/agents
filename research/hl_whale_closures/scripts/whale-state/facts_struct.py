# Structural facts about whale state at material closes (no outcomes used, all periods 12.09-29.09).
import sys, numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/whale-state')
from ws import *
ev = load()
print('EVENTS (universe U: rolling-60s close >=5% of position & >=$5k, ratio>=1, liq=0, Binance symbol; 30-min episodes)')
print(f'  n={len(ev)} wallets={ev.addr.nunique()} coins={ev.coin.nunique()} days={ev.day.nunique()} ; disc={int((ev.day<=DISC_END).sum())} test={int((ev.day>=TEST_START).sum())}')
print(f'  whale buys (close short) share={ (ev.wdir==1).mean():.2f} ; detect ws={ (ev.detect_src=="ws").mean():.2f} ; median latency tx->decision {ev.lat_s.median():.2f}s q90 {ev.lat_s.quantile(.9):.1f}s')
print('\nHERDING (other tracked wallets closing same coin, same direction, known before decision)')
for Wm in (5, 30, 120):
    n = ev[f'nw_same_{Wm}']; u = ev[f'usd_same_{Wm}'] / ev.depth_usd; o = ev[f'usd_opp_{Wm}'] / ev.depth_usd
    print(f'  {Wm:4d} min: share with >=1 other wallet {(n>=1).mean():.3f}, >=2 {(n>=2).mean():.3f}, >=3 {(n>=3).mean():.3f}; '
          f'other same-dir USD/depth median(if>0) {u[u>0].median():.3f} ; share with opposite-dir other closes {(o>0).mean():.3f}')
# is herding the SAME entity? correlation of wallets co-closing
print('  top coins among HERD30_2P events:', ev[ev.nw_same_30 >= 2].coin.value_counts().head(8).to_dict())
print('\nSTAGE')
print(f'  gap to previous close of same wallet/coin: <30m {(ev.gap_prev_h<0.5).mean():.2f}, 0.5-6h {((ev.gap_prev_h>=0.5)&(ev.gap_prev_h<6)).mean():.2f}, 6-24h {((ev.gap_prev_h>=6)&(ev.gap_prev_h<24)).mean():.2f}, >=24h {(ev.gap_prev_h>=24).mean():.2f}, none-before {ev.gap_prev_h.isna().mean():.2f}')
print(f'  FRESH6 known share {ev.fresh6_known.mean():.2f}, fresh6 among known {ev[ev.fresh6_known==1].fresh6.mean():.2f}; FRESH24 among known {ev[ev.fresh24_known==1].fresh24.mean():.2f}')
print(f'  NEARFULL (roll60 close >=50% of pos) share {(ev.r_pct>=50).mean():.3f}; single tx >=80% {(ev.tx_pct>=80).mean():.3f}; flips (Long>Short/Short>Long) in events {ev.flip.sum()}')
print('\nTYPE')
print(f'  vault events {ev.vault.sum()} ({ev[ev.vault==1].addr.nunique()} vaults); ncoins_past: <=1 {(ev.ncoins_past<=1).mean():.2f}, 2 {(ev.ncoins_past==2).mean():.2f}, >=3 {(ev.ncoins_past>=3).mean():.2f}')
print(f'  activity (txs>=1k per tracked day): median {ev.tx_per_day_past.median():.1f}; top-10 wallets share of events {ev.addr.value_counts().head(10).sum()/len(ev):.2f}')
print('\nWHALE PnL AT THE CLOSE (HL API closedPnl -> entry; sample)')
k = ev.wpnl.notna()
print(f'  API rows {ev.api_n.notna().sum()} ; with PnL {k.sum()} ({k.mean():.2f} of events) wallets {ev[k].addr.nunique()} coins {ev[k].coin.nunique()}')
if k.sum():
    p = ev[k].wpnl
    print(f'  PnL% quantiles p5 {p.quantile(.05):.1f} p25 {p.quantile(.25):.1f} p50 {p.median():.1f} p75 {p.quantile(.75):.1f} p95 {p.quantile(.95):.1f} ; share in loss {(p<0).mean():.2f}; loss<-3% {(p<-3).mean():.2f}; profit>3% {(p>3).mean():.2f}')
    for lab, m in (('whale closes SHORT (buys)', ev.wdir == 1), ('whale closes LONG (sells)', ev.wdir == -1)):
        q = ev[k & m].wpnl
        print(f'    {lab}: n={len(q)} median pnl {q.median():.1f}% share loss {(q<0).mean():.2f}')
    # coverage bias: is PnL availability related to activity?
    print(f'  coverage by activity: tx/day<5 {ev[ev.tx_per_day_past<5].wpnl.notna().mean():.2f}, 5-50 {ev[(ev.tx_per_day_past>=5)&(ev.tx_per_day_past<50)].wpnl.notna().mean():.2f}, >=50 {ev[ev.tx_per_day_past>=50].wpnl.notna().mean():.2f}')
# system fills
tx = pd.read_parquet(SP + 'data/whale_txs.parquet', columns=['addr', 'coin', 'ts', 'liq', 'tx_usd', 'tx_pct', 'dir', 'nf'])
L = tx[tx.liq == 1]
print('\nSYSTEM FILLS (liq==1: TWAP slices / liquidations / ADL) in whale_txs 12.09-30.09')
print(f'  n={len(L)} wallets={L.addr.nunique()} coins={L.coin.nunique()} usd total={L.tx_usd.sum()/1e6:.2f}M; >=5k: {(L.tx_usd>=5000).sum()} ; tx_pct>=5: {(L.tx_pct>=5).sum()}')
print('  dir', L.dir.value_counts().to_dict())

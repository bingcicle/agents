import sys, json, numpy as np, pandas as pd
SP = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/'
W = SP + 'work/hl-dislocation/'
sys.path.insert(0, SP + 'lib')
from hl import boot_ci, honesty, summary, kl, SYM
TEST0 = pd.Timestamp('2026-09-22').value // 10**6
END = pd.Timestamp('2026-09-30').value // 10**6

def load_tx(all_rows=False):
    """Whale txs with Binance 1s context. exc_* = HL price vs Binance fair (Binance last px in the second BEFORE the tx x
    (1+baseline basis)), in %, signed in the WHALE trade direction (+ = HL printed beyond Binance fair in whale direction)."""
    w = pd.read_parquet(W + 'q1_tx.parquet')
    if not all_rows:
        w = w[(w.liq == 0) & w.dir.isin(['Close Long', 'Close Short']) & w['bn-1'].notna() & w.basis.notna()].copy()
    F = w['bn-1'] * (1 + w.basis)
    w['F'] = F
    w['exc_pl'] = 100 * w.wdir * (w.pl / F - 1)
    w['exc_pf'] = 100 * w.wdir * (w.pf / F - 1)
    w['exc_px'] = 100 * w.wdir * (w.px / F - 1)
    w['sweep'] = 100 * w.wdir * (w.pl / w.pf - 1)
    w['td'] = w.tx_usd / w.depth_usd
    for k in [-300, -60, -10, -5, -2, 0, 1, 2, 3, 5, 10, 30, 60, 120, 300, 600, 1800]:
        w[f'mv{k}'] = 100 * w.wdir * (w[f'bn{k}'] / w['bn-1'] - 1)
    for lat in (0, 1, 2):
        w[f'hmv{lat}'] = 100 * w.wdir * (w[f'hedge_w{lat}'] / w['bn-1'] - 1)   # adverse hedge price move (worst in 3 s)
    w['dt'] = pd.to_datetime(w.ts, unit='ms')
    w['day'] = w.dt.dt.strftime('%m-%d')
    w['test'] = w.ts >= TEST0
    return w

def episodes(df, gap_s=60, keys=('addr', 'coin')):
    """Burst id: consecutive txs of the same keys with gaps < gap_s seconds form one episode."""
    df = df.sort_values(list(keys) + ['ts'])
    new = (df[list(keys)] != df[list(keys)].shift()).any(axis=1) | (df.ts.diff() > gap_s * 1000)
    return new.cumsum().reindex(df.index)

def fmt_h(h):
    keys = ['n', 'median', 'mean', 'win', 'mean_ci90_whale', 'mean_ci90_day', 'mean_ci90_coin', 'med_ci90_whale', 'med_ci90_day',
            'med_ci90_coin', 'half1_mean', 'half2_mean', 'sum', 'sum_wo_top10', 'max_share_whale', 'max_share_day', 'max_share_coin']
    out = []
    for k in keys:
        if k in h:
            v = h[k]
            out.append(f'{k}={v:.3f}' if isinstance(v, float) else f'{k}={v}')
    return ' '.join(out)

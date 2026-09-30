"""(7) state table of every F/R/T card 20.09-30.09; (8) R1 and R4 since 19.09; VVV pair bootstrap."""
import sys, json
import numpy as np, pandas as pd
W = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle/'
sys.path.insert(0, W)
from st import bat, fmt, boot, END_LOCAL

a = pd.read_parquet(W + 'trades.parquet')
out = []
def P(*s):
    line = ' '.join(str(x) for x in s); print(line); out.append(line)

D20 = pd.Timestamp('2026-09-20')
order = ['F1_1хв', 'F2_2хв', 'F3_3хв', 'F6_1хв_перший', 'F8_ratio35', 'F4_розумний', 'F5_перший', 'F7_без_ратіо', 'F9_без_ратіо_90',
         'F10_розумний_60', 'R1_загальний', 'R2_breakout', 'R4_великий', 'R5_дуже', 'R7_одним', 'R8_тп80',
         'T1_твап_відкриття', 'T1_твап_відкриття_15', 'T1_твап_відкриття_20', 'T2_твап_скорочення', 'T2_твап_скорочення_15', 'T2_твап_скорочення_20']
rows = []
for c in order:
    for kind in ('HEAD', 'ALL'):
        d = a[(a.card == c) & (a.dt >= D20) & a.settled]
        if kind == 'HEAD':
            d = d[d.hd]
        r = bat(d, D20, label=c, nboot=1500)
        rows.append({'card': c, 'set': kind, 'n': r['n'], 'wallets': r['wallets'], 'days': r['days'], 'per_day': r['per_day'],
                     'median': round(r.get('median', np.nan), 3), 'mean': round(r.get('mean', np.nan), 3),
                     'win%': round(100 * r.get('win', np.nan), 1), 'sum_pp': round(r.get('sum_pp', np.nan), 2),
                     'usd@1000': round(r.get('usd_1000', np.nan), 0), 'usd/month': round(r.get('usd_month', np.nan), 0),
                     'sum_wo_top10': round(r.get('sum_wo_top10', np.nan), 2),
                     'mean_CI90_day': r.get('mean_ci90_day'), 'mean_CI90_wallet': r.get('mean_ci90_wallet'),
                     'half1_med/mean': r.get('half1'), 'half2_med/mean': r.get('half2'),
                     'LONG(n,med,mean,sum)': r.get('LONG'), 'SHORT(n,med,mean,sum)': r.get('SHORT')})
tab = pd.DataFrame(rows)
tab.to_csv(W + 'card_state_20_30.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
P('======== (7) card state 20.09 00:00 - 30.09 21:07 CEST (10.88 days); official net; HEAD = reconstructed headline, ALL = all settled paper rows')
P(tab[['card', 'set', 'n', 'wallets', 'days', 'per_day', 'median', 'mean', 'win%', 'sum_pp', 'usd@1000', 'usd/month', 'sum_wo_top10', 'mean_CI90_day']].to_string(index=False))
P(tab[['card', 'set', 'half1_med/mean', 'half2_med/mean', 'LONG(n,med,mean,sum)', 'SHORT(n,med,mean,sum)', 'mean_CI90_wallet']].to_string(index=False))
# unique-event view of whole F family (dedupe: one row per event, F1 net)
f = a[(a.family == 'F') & (a.dt >= D20) & a.hd]
P(f'F family HEAD rows={len(f)} unique events={f.event.nunique()} duplication x{len(f)/max(1,f.event.nunique()):.2f}')

# ---------------- (8) R1 and R4 since 19.09
P('\n======== (8) R1 and R4 since 19.09')
for start, wn in [(pd.Timestamp('2026-09-19'), 'since 19.09 00:00'), (pd.Timestamp('2026-09-19 10:39'), 'since 19.09 10:39 (R4 review)')]:
    for c in ['R1_загальний', 'R4_великий', 'R8_тп80', 'R2_breakout', 'R5_дуже']:
        for kind in ('HEAD', 'ALL'):
            d = a[(a.card == c) & (a.dt >= start) & a.settled]
            if kind == 'HEAD':
                d = d[d.hd]
            r = bat(d, start, label=f'{c} {kind} {wn}')
            P(fmt(r))
            if c in ('R1_загальний', 'R4_великий') and kind == 'HEAD' and len(d) >= 3:
                lo, hi, p = boot(d.net.values, d.wallet.values, np.median)
                P(f'    P(median>0) by wallet = {1-p:.3f}; gross_tape median={d.gross_tape.median():+.3f}; tape |dump| median={d.dump_tape.abs().median():.2f}')
                P('    per wallet sum:', d.groupby(d.wallet.str[:10]).net.agg(['size', 'sum']).round(2).sort_values('sum').to_dict('index'))
                P('    trades:', d[['dloc', 'coin', 'side', 'net', 'dump_tape', 'move_pct']].round(3).values.tolist())
# drift control for R1 head since 19.09 (30 min hold) with BTC
import drift
d = a[(a.card.isin(['R1_загальний', 'R4_великий'])) & (a.dt >= pd.Timestamp('2026-09-19')) & a.hd].copy()
res = []
for i, r in d.iterrows():
    sg = 1 if r.side == 'LONG' else -1
    t0 = r.ts_ms; t1 = r.exit_ts_ms if np.isfinite(r.exit_ts_ms) else t0 + 1800_000
    cr, src = drift.ret(r.coin, t0, t1, sg)
    b = drift.btc_ret(t0, t1, sg, src) if src else np.nan
    res.append((i, cr, b))
dd = pd.DataFrame(res, columns=['i', 'coin_ret', 'btc_ret']).set_index('i')
d = d.join(dd)
for c in ['R1_загальний', 'R4_великий']:
    x = d[d.card == c]
    P(f'  drift {c}: n={len(x)} net mean={x.net.mean():+.3f} btc_ret(our dir) mean={x.btc_ret.mean():+.3f} net-btc mean={(x.net-x.btc_ret).mean():+.3f} median={(x.net-x.btc_ret).median():+.3f}; LONG n={(x.side=="LONG").sum()} SHORT n={(x.side=="SHORT").sum()}')

# ---------------- VVV pair: day bootstrap on F1 events
v = a[(a.wallet.str.startswith('0x0871deb3')) & (a.coin == 'VVV') & (a.card == 'F1_1хв') & (a.dt >= D20) & a.hd]
lo, hi, p = boot(v.net.values, v.day.values, np.mean)
lo2, hi2, p2 = boot(v.net.values, v.day.values, np.median)
P(f'\nVVV pair F1 since 20.09: n={len(v)} days={v.day.nunique()} mean={v.net.mean():+.3f} CI90 by day mean [{lo:+.3f},{hi:+.3f}] P(mean<=0)={p:.3f}; median {v.net.median():+.3f} CI90 [{lo2:+.3f},{hi2:+.3f}]; win={(v.net>0).mean():.2f}; sum={v.net.sum():+.2f} ($ {v.net.sum()*10:+.0f}; /month {v.net.sum()*10/10.88*30:+.0f})')
xs = np.sort(v.net.values)[::-1]; P(f'   sum w/o top10% = {xs[max(1,int(round(len(xs)*.1))):].sum():+.2f}; halves (chron) mean {v.net.values[:len(v)//2].mean():+.3f} / {v.net.values[len(v)//2:].mean():+.3f}')
v3 = a[(a.wallet.str.startswith('0x0871deb3')) & (a.coin == 'VVV') & (a.card == 'F3_3хв') & (a.dt >= D20) & a.hd]
P(f'   F3 same pair: n={len(v3)} mean={v3.net.mean():+.3f} median={v3.net.median():+.3f} sum={v3.net.sum():+.2f}')
# pair in-sample history for context (all F1 head rows of that pair)
h = a[(a.wallet.str.startswith('0x0871deb3')) & (a.coin == 'VVV') & (a.card == 'F1_1хв') & a.hd]
P('   F1 pair by period:', h.groupby(np.where(h.dt < D20, 'before 20.09', 'since 20.09')).net.agg(['size', 'median', 'mean', 'sum']).round(3).to_dict('index'))
c = a[(a.wallet.str.startswith('0x523852be')) & (a.coin == 'CHIP') & (a.card == 'F1_1хв') & a.hd]
P('   CHIP pair F1 by period:', c.groupby(np.where(c.dt < D20, 'before 20.09', 'since 20.09')).net.agg(['size', 'median', 'mean', 'sum']).round(3).to_dict('index'))
open(W + 'out_h3_cards.txt', 'w').write('\n'.join(out))

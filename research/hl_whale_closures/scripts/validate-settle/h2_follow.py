"""(2) F4-F1 paired since 18.09; (3) F1-PROF & V9 since 18.09; (4) ok & lag<=1.8 since 17.09 10:32;
(5) pairs 0x523852be/CHIP and 0x0871deb3/VVV since 20.09; (6) A_any brake on F1 since 20.09."""
import sys, json
import numpy as np, pandas as pd
W = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle/'
sys.path.insert(0, W)
from st import bat, fmt, boot, END_LOCAL

a = pd.read_parquet(W + 'trades.parquet')
F = a[a.family == 'F'].copy()
F['fc'] = F.exit_reason_tape.fillna('').str.startswith('full_close') | F.exit_reason.fillna('').str.startswith('full_close')
out = []
def P(*s):
    line = ' '.join(str(x) for x in s); print(line); out.append(line)

D18 = pd.Timestamp('2026-09-18'); D20 = pd.Timestamp('2026-09-20'); REG17 = pd.Timestamp('2026-09-17 10:32')
D13 = pd.Timestamp('2026-09-13')

# ---------------- (2) F4 - F1 paired
P('======== (2) F4 - F1 paired on identical events')
for start, wn in [(D18, 'since 18.09'), (pd.Timestamp('2026-09-17 19:49'), 'since 17.09 19:49 (registration)'), (D13, 'since 13.09 (incl. in-sample)')]:
    f4 = F[(F.card == 'F4_розумний') & (F.dt >= start) & F.hd]
    f1 = F[(F.card == 'F1_1хв') & F.hd][['event', 'net', 'side']].rename(columns={'net': 'net_f1', 'side': 'side_f1'})
    pr = f4.merge(f1, on='event', how='left')
    P(f'-- {wn}: F4 head n={len(f4)}, with F1 pair={pr.net_f1.notna().sum()}')
    pr = pr[pr.net_f1.notna()].copy()
    pr['diff'] = pr.net - pr.net_f1
    inf = pr[pr['diff'].abs() > 1e-6]
    P(f'   pairs={len(pr)} informative={len(inf)} days_inf={inf.day.nunique()} wallets_inf={inf.wallet.nunique()} '
      f'diff all: mean={pr["diff"].mean():+.3f} sum={pr["diff"].sum():+.2f} | informative: median={inf["diff"].median():+.3f} mean={inf["diff"].mean():+.3f} pos={int((inf["diff"]>0).sum())}/{len(inf)}')
    if len(inf) >= 3:
        for c in ('day', 'wallet'):
            lo, hi, p = boot(inf['diff'].values, inf[c].values, np.median)
            lo2, hi2, p2 = boot(inf['diff'].values, inf[c].values, np.mean)
            P(f'   CI90 by {c}: median [{lo:+.3f},{hi:+.3f}]  mean [{lo2:+.3f},{hi2:+.3f}] P(mean<=0)={p2:.3f}')
        xs = np.sort(inf['diff'].values)[::-1]; k = max(1, int(round(len(xs) * .1)))
        P(f'   informative diff sum={xs.sum():+.2f} w/o top10%={xs[k:].sum():+.2f}; top diffs:', inf.nlargest(3, 'diff')[['dloc', 'coin', 'side', 'diff']].round(3).values.tolist())
        P('   informative by side:', inf.groupby('side')['diff'].agg(['size', 'median', 'mean', 'sum']).round(3).to_dict('index'))
    r = bat(f4, start, label=f'F4 head {wn}')
    P(fmt(r))
    sh = f4[f4.side == 'SHORT']
    P(f'   F4 SHORT subsample n={len(sh)} median={sh.net.median():+.3f} mean={sh.net.mean():+.3f} sum={sh.net.sum():+.2f}; LONG n={(f4.side=="LONG").sum()} median={f4[f4.side=="LONG"].net.median():+.3f} sum={f4[f4.side=="LONG"].net.sum():+.2f}')
    r1 = bat(F[(F.card == 'F1_1хв') & (F.dt >= start) & F.hd & F.event.isin(f4.event)], start, label=f'F1 on same events {wn}', full=False)
    P(fmt(r1))

# ---------------- (3) F1-PROF and V9
P('\n======== (3) F1-PROF (F1 head & prof ok & tx<20 & lag<2) since 18.09 and V9 (+prof_cont_pct>=30)')
f1 = F[(F.card == 'F1_1хв') & F.hd]
base18 = f1[f1.dt >= D18]
mech = (base18.tx_pct < 20) & (base18.lag_s < 2)
prof = base18[(base18.prof_status == 'ok') & mech]
v9 = prof[prof.prof_cont_pct >= 30]
ctrl = base18[(base18.prof_status != 'ok') & mech]
for d, nm in [(prof, 'F1-PROF'), (v9, 'V9'), (ctrl, 'control NOT-ok & tx<20 & lag<2'), (base18, 'F1 base since 18.09')]:
    r = bat(d, D18, label=nm)
    P(fmt(r))
    P(f'   gross_tape median={d.gross_tape.median():+.3f} mean={d.gross_tape.mean():+.3f}; full_close share={d.fc.mean():.3f}; '
      f'max share n: wallet={d.wallet.value_counts(normalize=True).max() if len(d) else np.nan:.3f} day={d.day.value_counts(normalize=True).max() if len(d) else np.nan:.3f}')
if len(prof):
    P('   F1-PROF trades per day:', prof.groupby('day').net.agg(['size', 'sum']).round(2).to_dict('index'))
    P('   F1-PROF per wallet:', prof.groupby(prof.wallet.str[:10]).net.agg(['size', 'sum', 'median']).round(3).sort_values('sum').to_dict('index'))
    # criteria
    r = bat(prof, D18)
    crit = {
        'n>=40': r['n'] >= 40,
        'median>0': r['median'] > 0,
        'CI90_wallet_lo>0': r['med_ci90_wallet'][0] > 0,
        'CI90_day_lo>0': r['med_ci90_day'][0] > 0,
        'no_wallet>30%n': r['max_share_n_wallet'] <= 0.30,
        'no_day>30%n': r['max_share_n_day'] <= 0.30,
        '>=10_wallets': r['wallets'] >= 10,
        '>=15_days': r['days'] >= 15,
        'sum_wo_top10>0': r['sum_wo_top10'] > 0,
        'gross_median>0.30': prof.gross_tape.median() > 0.30,
        'full_close>=40%': prof.fc.mean() >= 0.40,
        'FAIL_rule: n>=40 & median<=0': (r['n'] >= 40) and (r['median'] <= 0),
        'FAIL_rule: control not worse (ctrl median>=prof median)': ctrl.net.median() >= prof.net.median(),
    }
    P('   CRITERIA F1-PROF:', json.dumps({k: bool(v) for k, v in crit.items()}, ensure_ascii=False))
# same rule on F1 since 13.09 excluding the 12 build trades? (context only)
P('   context: F1-PROF from 13.09 incl. build trades:', fmt(bat(f1[(f1.dt >= D13) & (f1.prof_status == 'ok') & (f1.tx_pct < 20) & (f1.lag_s < 2)], D13, full=False)))

# ---------------- (4) ok & lag <= 1.8
P('\n======== (4) follow ok & lag<=1.8 s (F1 head) since 17.09 10:32')
base17 = f1[f1.dt >= REG17]
ok18 = base17[(base17.prof_status == 'ok') & (base17.lag_s <= 1.8)]
for d, nm in [(ok18, 'ok&lag<=1.8'), (base17[(base17.prof_status != 'ok') & (base17.lag_s <= 1.8)], 'control not-ok & lag<=1.8'),
              (base17[(base17.prof_status == 'ok') & (base17.lag_s > 1.8)], 'ok & lag>1.8'), (base17, 'F1 base since 17.09 10:32')]:
    P(fmt(bat(d, REG17, label=nm)))
    P(f'   gross_tape median={d.gross_tape.median():+.3f}; full_close share={d.fc.mean():.3f}')
r = bat(ok18, REG17)
P('   CRITERIA ok&lag<=1.8:', json.dumps({'n>=50': r['n'] >= 50, '>=25_wallets': r['wallets'] >= 25, 'median>0': r['median'] > 0,
                                           'CI90_med_wallet_lo>0': r['med_ci90_wallet'][0] > 0, 'sum_wo_top10>0': r['sum_wo_top10'] > 0}))
# also on F2/F3 as a descriptive robustness (not registered)
for c in ['F2_2хв', 'F3_3хв']:
    d = F[(F.card == c) & F.hd & (F.dt >= REG17) & (F.prof_status == 'ok') & (F.lag_s <= 1.8)]
    P('   descriptive', fmt(bat(d, REG17, label=f'{c} ok&lag<=1.8', full=False)))

# ---------------- (5) pairs
P('\n======== (5) pairs 0x523852be/CHIP and 0x0871deb3/VVV since 20.09 (any card, official net)')
for w, coin in [('0x523852be', 'CHIP'), ('0x0871deb3', 'VVV')]:
    d = a[(a.wallet.str.lower().str.startswith(w)) & (a.dt >= D20)]
    P(f'-- {w} all coins since 20.09: rows={len(d)} events={d.event.nunique()} coins={d.coin.value_counts().to_dict()}')
    dp = d[d.coin == coin]
    P(f'   pair rows={len(dp)} events={dp.event.nunique()} settled={int(dp.settled.sum())} head={int(dp.hd.sum())}')
    if len(dp):
        P('   per card:', dp.groupby('card').net.agg(['size', 'median', 'mean', 'sum']).round(3).to_dict('index'))
        ev = dp[dp.hd].groupby('event').agg(dloc=('dloc', 'first'), side=('side', 'first'), n_cards=('card', 'size'),
                                            net_f1=('net', lambda s: np.nan), net_mean=('net', 'mean'), net_min=('net', 'min'), net_max=('net', 'max'))
        f1p = dp[(dp.card == 'F1_1хв') & dp.hd].set_index('event').net
        ev['net_f1'] = f1p.reindex(ev.index)
        P('   per event (head):\n' + ev.round(3).to_string())
        P(f'   events: n={len(ev)} median(F1)={ev.net_f1.median():+.3f} mean(F1)={ev.net_f1.mean():+.3f} sum(F1)={ev.net_f1.sum():+.2f} | median(event-mean over cards)={ev.net_mean.median():+.3f} sum={ev.net_mean.sum():+.2f}')
    # also last activity date of the wallet in whale txs
    last = a[a.wallet.str.lower().str.startswith(w)].dt.max()
    P(f'   last bot trade on this wallet (any coin): {last}')
try:
    wt = pd.read_parquet('/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/data/whale_txs.parquet',
                         columns=['addr', 'coin', 'ts', 'tx_usd', 'tx_pct'])
    for w, coin in [('0x523852be', 'CHIP'), ('0x0871deb3', 'VVV')]:
        x = wt[wt.addr.str.lower().str.startswith(w)]
        x = x.assign(d=pd.to_datetime(x.ts, unit='ms').dt.strftime('%m-%d'))
        P(f'   whale_txs {w}: total={len(x)}; by coin={x.coin.value_counts().head(5).to_dict()}; since 20.09 UTC on {coin}: '
          f'{x[(x.coin == coin) & (x.ts >= 1789862400000)].groupby("d").size().to_dict()} (txs>=$5k&>=5%: {int(((x.coin == coin) & (x.ts >= 1789862400000) & (x.tx_usd >= 5000) & (x.tx_pct >= 5)).sum())})')
except Exception as e:
    P('whale_txs read error', e)

# ---------------- (6) A_any on F1 since 20.09
P('\n======== (6) A_any (one trade per wallet x coin x local day) on F1 head since 20.09')
b = f1[f1.dt >= D20].sort_values('ts_ms').copy()
b['k'] = b.wallet + '|' + b.coin + '|' + b.day
b['first'] = ~b.duplicated('k')
kept = b[b['first']]
days_w = (END_LOCAL - D20).total_seconds() / 86400
P(f'base n={len(b)} mean={b.net.mean():+.4f} median={b.net.median():+.4f} sum={b.net.sum():+.2f} ($1000: {b.net.sum()*10:+.0f}, /month {b.net.sum()*10/days_w*30:+.0f})')
P(f'kept n={len(kept)} mean={kept.net.mean():+.4f} median={kept.net.median():+.4f} sum={kept.net.sum():+.2f} ($1000: {kept.net.sum()*10:+.0f}, /month {kept.net.sum()*10/days_w*30:+.0f})')
rem = b[~b['first']]
P(f'removed n={len(rem)} mean={rem.net.mean():+.4f} median={rem.net.median():+.4f} sum={rem.net.sum():+.2f}')
dmean = kept.net.mean() - b.net.mean(); dmed = kept.net.median() - b.net.median()
P(f'delta mean={dmean:+.4f} delta median={dmed:+.4f}')
# cluster bootstrap by day and by wallet of delta mean/median
rng = np.random.default_rng(3)
for c in ('day', 'wallet'):
    groups = {g: (d.net.values, d['first'].values) for g, d in b.groupby(c)}
    keys = list(groups)
    dm, dmd = [], []
    for _ in range(3000):
        pick = rng.integers(0, len(keys), len(keys))
        nets = np.concatenate([groups[keys[i]][0] for i in pick]); fs = np.concatenate([groups[keys[i]][1] for i in pick])
        if fs.sum() == 0:
            continue
        dm.append(nets[fs].mean() - nets.mean()); dmd.append(np.median(nets[fs]) - np.median(nets))
    P(f'   CI90 by {c}: dmean [{np.percentile(dm,5):+.4f},{np.percentile(dm,95):+.4f}] P<=0={np.mean(np.array(dm)<=0):.3f}; dmedian [{np.percentile(dmd,5):+.4f},{np.percentile(dmd,95):+.4f}]')
# placebo: remove the same number of trades at random within each day
pl = []
nrem_day = rem.groupby('day').size()
for _ in range(3000):
    keep_mask = np.ones(len(b), bool)
    for dday, k in nrem_day.items():
        idx = np.where(b.day.values == dday)[0]
        keep_mask[rng.choice(idx, k, replace=False)] = False
    pl.append(b.net.values[keep_mask].mean() - b.net.mean())
pl = np.array(pl)
P(f'   placebo (random removal of same count per day): dmean mean={pl.mean():+.4f} p95={np.percentile(pl,95):+.4f}; P(placebo>=obs)={np.mean(pl>=dmean):.3f}')
# halves
h = b.dt.sort_values().iloc[len(b) // 2]
for nm, sub in [('half1', b[b.dt < h]), ('half2', b[b.dt >= h])]:
    k2 = sub[sub['first']]
    P(f'   {nm}: base mean={sub.net.mean():+.4f} kept mean={k2.net.mean():+.4f} delta={k2.net.mean()-sub.net.mean():+.4f} (n {len(sub)}->{len(k2)})')
P(fmt(bat(kept, D20, label='F1 A_any kept since 20.09')))
P(fmt(bat(b, D20, label='F1 base since 20.09')))
# A_any with UTC day (sensitivity)
b['dutc'] = pd.to_datetime(b.ts_ms, unit='ms').dt.strftime('%Y-%m-%d')
k3 = b[~(b.wallet + '|' + b.coin + '|' + b.dutc).duplicated()]
P(f'   UTC-day variant: kept n={len(k3)} mean={k3.net.mean():+.4f} delta={k3.net.mean()-b.net.mean():+.4f}')
open(W + 'out_h2_follow.txt', 'w').write('\n'.join(out))

"""(1) T2 family since 17.09 (post-registration) and since 13.09 (EVAL_SINCE) with 17.09 criteria, market-drift control,
T2 >=90%-of-position pre-registration, T1 family."""
import sys, json
import numpy as np, pandas as pd
sys.path.insert(0, '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle')
from st import bat, fmt, boot, END_LOCAL
import drift

W = '/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/validate-settle/'
a = pd.read_parquet(W + 'trades.parquet')
T = a[a.family == 'T'].copy()
T['sign'] = np.where(T.side == 'LONG', 1, -1)
T['frac'] = T.twap_usd / T.pos_usd

# ---- drift columns
rows = []
for i, r in T.iterrows():
    t0 = r.ts_ms
    t1 = r.exit_ts_ms if np.isfinite(r.exit_ts_ms) else t0 + 3600_000
    cr, src = drift.ret(r.coin, t0, t1, r.sign)
    if src is None:
        rows.append((i, np.nan, np.nan, np.nan, np.nan, np.nan, None)); continue
    b = drift.btc_ret(t0, t1, r.sign, src)
    al = drift.alt_index_ret(t0, t1, r.sign, src)
    pm, rr = drift.same_day_placebo(r.coin, int(t0), int(round((t1 - t0) / 60000)) * 60000, r.sign, src)
    pct = float((rr < cr).mean()) if isinstance(rr, np.ndarray) and len(rr) else np.nan
    rows.append((i, cr, b, al, pm, pct, src))
dd = pd.DataFrame(rows, columns=['i', 'coin_ret', 'btc_ret', 'alt_ret', 'plc_mean', 'plc_pct', 'px_src']).set_index('i')
T = T.join(dd)
T['net_x_btc'] = T.net - T.btc_ret
T['net_x_alt'] = T.net - T.alt_ret
T['net_x_plc'] = T.net - T.plc_mean
T.to_parquet(W + 'twap_with_drift.parquet')

REG = pd.Timestamp('2026-09-17 10:32')   # registration moment (raw log wf_32eebcdf mtime, CEST)
out = []
def P(*s):
    line = ' '.join(str(x) for x in s); print(line); out.append(line)

P('T2 trades between 17.09 00:00 and REG:', len(T[(T.card == 'T2_твап_скорочення') & (T.dt >= '2026-09-17') & (T.dt < REG)]))
cards = ['T2_твап_скорочення', 'T2_твап_скорочення_15', 'T2_твап_скорочення_20', 'T1_твап_відкриття', 'T1_твап_відкриття_15', 'T1_твап_відкриття_20']
for start, wname in [(REG, 'OOS since 17.09 10:32'), (pd.Timestamp('2026-09-13'), 'EVAL since 13.09')]:
    P('\n========', wname)
    for c in cards:
        for hname, d in [('HEAD', T[(T.card == c) & (T.dt >= start) & T.hd]), ('ALL', T[(T.card == c) & (T.dt >= start) & T.settled])]:
            r = bat(d, start, label=f'{c} {hname}')
            P(fmt(r))
            if c.startswith('T2') and hname == 'HEAD' and len(d):
                for col in ['gross_tape', 'coin_ret', 'btc_ret', 'alt_ret', 'net_x_btc', 'net_x_alt', 'net_x_plc']:
                    x = d[col].dropna()
                    lo, hi, p = boot(x.values, d.loc[x.index, 'wallet'].values, np.mean) if len(x) > 2 else (np.nan,) * 3
                    lo2, hi2, p2 = boot(x.values, d.loc[x.index, 'day'].values, np.mean) if len(x) > 2 else (np.nan,) * 3
                    P(f'    {col}: n={len(x)} med={x.median():+.3f} mean={x.mean():+.3f} CI90mean wallet=[{lo:+.3f},{hi:+.3f}] day=[{lo2:+.3f},{hi2:+.3f}]')
                x = d.plc_pct.dropna()
                P(f'    placebo percentile (same coin, same UTC day, all windows of same length): n={len(x)} mean={x.mean():.3f} median={x.median():.3f}')
                P('    per-day:', d.groupby('day').net.agg(['size', 'sum', 'median']).round(3).to_dict('index'))
                P('    per-wallet sum:', d.groupby(d.wallet.str[:10]).net.agg(['size', 'sum']).round(3).sort_values('sum').to_dict('index'))
                P('    per-coin sum:', d.groupby('coin').net.agg(['size', 'sum']).round(3).sort_values('sum').to_dict('index'))

# ---- 17.09 criteria for T2 (HEAD)
def crit(d, start, name):
    r = bat(d, start, label=name)
    c = {}
    c['a_CI90_median_by_wallet>0'] = r.get('med_ci90_wallet', (np.nan,))[0] > 0
    c['b_both_halves>=0 (median)'] = (r['half1'][0] >= 0) and (r['half2'][0] >= 0)
    c['b2_both_halves>=0 (mean)'] = (r['half1'][1] >= 0) and (r['half2'][1] >= 0)
    c['c_sum_wo_top10>0'] = r['sum_wo_top10'] > 0
    c['d_no_day/wallet/coin>25%sum'] = all((r[f'max_share_sum_{k}'] <= 0.25) for k in ('day', 'wallet', 'coin')) if r['sum_pp'] > 0 else False
    c['e_SHORT_not_negative'] = (r['SHORT'][0] >= 1) and (r['SHORT'][1] >= 0)
    c['skeptic_>=5_SHORT'] = r['SHORT'][0] >= 5
    c['skeptic_>=10_days'] = r['days'] >= 10
    P(f'CRITERIA {name}: ' + json.dumps({k: bool(v) for k, v in c.items()}, ensure_ascii=False))
    return r, c

P('\n======== 17.09 criteria')
for start, wname in [(REG, 'OOS'), (pd.Timestamp('2026-09-13'), 'EVAL13')]:
    for c in ['T2_твап_скорочення', 'T2_твап_скорочення_15', 'T2_твап_скорочення_20']:
        crit(T[(T.card == c) & (T.dt >= start) & T.hd], start, f'{c} {wname}')

# ---- negative alt-drift days in window (Binance equal-weight alt index, UTC day)
P('\n======== daily alt-index drift (equal-weight, Binance 1m, UTC day)')
days = pd.date_range('2026-09-13', '2026-09-29', freq='D')
dr = {}
for d0 in days:
    t0 = int(d0.value // 10**6); t1 = t0 + 86400000
    dr[d0.strftime('%m-%d')] = round(drift.alt_index_ret(t0 + 60000, t1, 1, 'bn'), 2)
P(dr)

# ---- T2 >= 90% of position (pre-registered 17.09; old 11 do not count)
P('\n======== T2 >=90% of position (frac = usd/pos_usd), post-registration')
d = T[(T.card == 'T2_твап_скорочення') & (T.dt >= REG) & T.hd].copy()
P('frac missing:', d.frac.isna().sum(), ' frac values:', sorted(d.frac.round(3).tolist()))
for th in [0.85, 0.89, 0.90, 0.95, 0.99]:
    s1 = d[d.frac >= th]; s0 = d[d.frac < th]
    P(f'th={th}: >=th n={len(s1)} med={s1.net.median():+.3f} mean={s1.net.mean():+.3f} sum={s1.net.sum():+.2f} wallets={s1.wallet.nunique()} | <th n={len(s0)} med={s0.net.median():+.3f} mean={s0.net.mean():+.3f} sum={s0.net.sum():+.2f}')
s1 = d[d.frac >= 0.90]
r = bat(s1, REG, label='T2>=90% OOS')
P(fmt(r))
P('    per-trade:', s1[['dloc', 'coin', 'side', 'wallet', 'frac', 'net', 'net_x_btc', 'net_x_alt']].assign(wallet=s1.wallet.str[:10]).round(3).to_string())
# also all T2-family cards with >=90%
for c in ['T2_твап_скорочення_15', 'T2_твап_скорочення_20']:
    dd2 = T[(T.card == c) & (T.dt >= REG) & T.hd]
    s1 = dd2[dd2.frac >= 0.9]; s0 = dd2[dd2.frac < 0.9]
    P(f'{c}: >=0.9 n={len(s1)} med={s1.net.median():+.3f} sum={s1.net.sum():+.2f} | <0.9 n={len(s0)} med={s0.net.median():+.3f} sum={s0.net.sum():+.2f}')

# ---- T2 all trades list OOS
P('\n======== T2 OOS trade list')
d = T[(T.card == 'T2_твап_скорочення') & (T.dt >= REG)]
P(d[['dloc', 'coin', 'side', 'twap_side', 'twap_kind', 'twap_dur_s', 'move_pct', 'frac', 'hd', 'net', 'gross_tape', 'coin_ret', 'btc_ret', 'alt_ret', 'plc_pct', 'px_src']].assign(twap_dur_s=d.twap_dur_s).round(3).to_string())
open(W + 'out_h1_twap.txt', 'w').write('\n'.join(out))

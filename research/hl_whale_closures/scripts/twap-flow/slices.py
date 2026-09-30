"""Slice-phase micro-profile: HL TWAP sends a slice every 30 s from start. For slices AFTER the bot saw the TWAP
(t_k = start + 30k >= seen + 5 s), measure Binance last-price return in TWAP direction over [t_k-3s, t_k+j] for j in -3..27,
and a tradable version: enter worst-in-3s at t_k-5s, exit worst-in-3s at t_k+5s. High-intensity TWAPs only (int >= 0.12)."""
import sys; sys.path.insert(0, '.')
from rules import *
D = base()
D = D[(D.intensity >= 0.1203) & (D.eligible == 0) | (D.intensity >= 0.1203) & (D.dur_min <= 15)]
rows = []
J = [-3, -2, -1, 0, 1, 2, 3, 5, 10, 15, 20, 27]
for r in D.itertuples():
    s0 = int(r.start_ms // 1000); seen = int(r.seen_ms // 1000); end = int(r.end_ms // 1000)
    ks = [k for k in range(1, 200) if s0 + 30 * k >= seen + 5 and s0 + 30 * k < end][:20]
    if not ks: continue
    b = bars(r.sym, (s0 + 30 * ks[0] - 20) * 1000, (s0 + 30 * ks[-1] + 40) * 1000)
    if b is None: continue
    for k in ks:
        t = s0 + 30 * k
        base_ = last_px(b, t - 4)
        if not base_ or np.isnan(base_): continue
        rec = {'twap_id': r.twap_id, 'per': r.per, 'cd': r.cd, 'k': k}
        for j in J:
            rec[f'j{j}'] = 100 * r.tdir * (last_px(b, t + j) / base_ - 1)
        en = worst_px(b, t - 5, r.tdir); ex = worst_px(b, t + 5, -r.tdir)
        rec['trade'] = 100 * r.tdir * (ex / en - 1)
        rows.append(rec)
s = pd.DataFrame(rows)
s.to_parquet('slices.parquet')
print('slices', len(s), 'twaps', s.twap_id.nunique())
for per in ('disc', 'test'):
    x = s[s.per == per]
    print(per, 'mean profile (base = t-4s):', ' '.join(f'j{j}={x[f"j{j}"].mean():+.4f}' for j in J))
    print(per, 'median profile:', ' '.join(f'j{j}={x[f"j{j}"].median():+.4f}' for j in J))
    print(per, line(x.trade, x.cd, 'slice trade gross (worst in/out)'), '| net@0.146 mean', round(x.trade.mean() - 0.146, 3))

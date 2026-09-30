"""1s-bar profile around TWAP start and at first-seen (ineligible: date = real first-seen)."""
import sys; sys.path.insert(0, '.')
from common import *
import time
t0 = time.time()
d = get(); d = d[d.per != 'none'].copy()
d['hb'] = [has_bars(s, ms) for s, ms in zip(d.sym, d.start_ms)]
print('with bars at start day', d.hb.mean(), d.hb.sum())
OFF = [0, 3, 5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300, 600, 900]
rows = []
btc_cache = {}
for i, r in d[d.hb].sort_values(['sym', 'start_ms']).iterrows():
    s0 = int(r.start_ms // 1000)
    seen = int(r.date_ms // 1000)
    t1 = max(s0 + 900, seen + 960)
    b = bars(r.sym, (s0 - 120) * 1000, t1 * 1000)
    if b is None:
        continue
    base = last_px(b, s0 - 1)
    if not base or np.isnan(base):
        continue
    rec = {'idx': i}
    for o in OFF:
        rec[f's{o}'] = 100 * r.tdir * (last_px(b, s0 + o) / base - 1)
    # worst entry in TWAP direction at start+3 s (hypothetical native detection) and at seen+2 s (actual)
    rec['w_start3'] = 100 * r.tdir * (worst_px(b, s0 + 3, r.tdir) / base - 1)
    rec['w_seen2'] = 100 * r.tdir * (worst_px(b, seen + 2, r.tdir) / base - 1)
    rec['l_seen'] = 100 * r.tdir * (last_px(b, seen) / base - 1)
    for h in (60, 120, 300, 600, 900):
        rec[f'seen{h}'] = 100 * r.tdir * (last_px(b, seen + 2 + h) / base - 1)
        # exit worst (selling if we are long in TWAP dir)
        rec[f'wx_seen{h}'] = 100 * r.tdir * (worst_px(b, seen + 2 + h, -r.tdir) / base - 1)
    for h in (60, 120, 180, 300):
        rec[f'wx_start{h}'] = 100 * r.tdir * (worst_px(b, s0 + 3 + h, -r.tdir) / base - 1)
    rec['q1m'] = b.loc[s0 - 60:s0 - 1, 'quote'].sum()
    rows.append(rec)
o = pd.DataFrame(rows).set_index('idx')
d = d.join(o, how='inner')
d.to_parquet('b1s.parquet')
print('built', len(d), time.time() - t0)

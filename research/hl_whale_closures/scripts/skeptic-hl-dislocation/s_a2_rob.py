from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
base = pd.read_parquet(OUT + 's_a2_hl.parquet') if os.path.exists(OUT + 's_a2_hl.parquet') else None
b = pd.read_parquet(OUT + 's_a2_base_hl.parquet')
print('=== HL-own-price exit (5m candle close at first bar end >= fill+60s; fee 0.06) with cluster CIs')
rep(b, 'HLx_5m', 'HL close exit'); rep(b, 'BNx_5m', 'BN fair same instant'); rep(b, 'res_5m', 'HL residual below fair')
print('\n=== Sensitivity (P60 and P5)')
V = [('lat0.3', dict(lat_ms=300)), ('lat1.5', {}), ('lat3', dict(lat_ms=3000)), ('lat5', dict(lat_ms=5000)), ('lat10', dict(lat_ms=10000)),
     ('bot_ts', dict(use_bot=True)), ('fill incl liq/opens', dict(fillset='all')), ('margin0.1', dict(margin=0.1)), ('margin0.25', dict(margin=0.25)),
     ('margin0.5', dict(margin=0.5)), ('ref b-2', dict(ref='b-2')), ('ref b-5', dict(ref='b-5')), ('bas5', dict(bas_col='bas5')),
     ('rearm 300', dict(rearm_s=300))]
for name, kw in V:
    f = add_epi(run(A.td >= 0.3, **kw))
    rep(f, 'P60', name)
# zero basis
A['bas0'] = 0.0
rep(add_epi(run(A.td >= 0.3, bas_col='bas0')), 'P60', 'basis=0')

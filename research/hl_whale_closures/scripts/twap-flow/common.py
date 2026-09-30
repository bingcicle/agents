from load import *
M = 60000
def R(d, k1, k2, ctrl=None):
    """% return in TWAP direction between price points k1->k2; ctrl=None raw, 'b' minus BTC, 'a' minus alt index"""
    r = 100 * d.tdir * (d['p_' + k2] / d['p_' + k1] - 1)
    if ctrl == 'b':
        r = r - 100 * d.tdir * (d['b_' + k2] / d['b_' + k1] - 1)
    elif ctrl == 'a':
        r = r - 100 * d.tdir * (d['a_' + k2] / d['a_' + k1] - 1)
    return r
def get():
    d = pd.read_parquet('/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/twap-flow/twaps_px2.parquet')
    d['dday'] = pd.to_datetime(d.date_ms, unit='ms').dt.strftime('%Y-%m-%d')
    d['per'] = np.where(d.dday <= '2026-09-21', 'disc', np.where(d.dday <= '2026-09-29', 'test', 'none'))
    d['cd'] = d.coin + '|' + d.dday
    d['durb'] = pd.cut(d.dur_s / 60, [0, 5, 15, 30, 60, 180, 720, 1e6], labels=['<=5', '5-15', '15-30', '30-60', '60-180', '180-720', '>720'])
    d['kindx'] = d.kind.fillna('na')
    return d
def line(x, cl=None, label='', stat_ci=True, n_boot=1000):
    x = np.asarray(x, float); m = np.isfinite(x)
    x = x[m]
    if len(x) < 3:
        return f'{label:28s} n={len(x)}'
    s = f'{label:28s} n={len(x):5d} med={np.median(x):+.3f} mean={x.mean():+.3f} win={100*(x>0).mean():.0f}%'
    if cl is not None and stat_ci:
        c = np.asarray(cl)[m]
        lo, hi, p = boot_ci(x, c, np.mean, n=n_boot)
        s += f' CI90mean[{lo:+.3f},{hi:+.3f}] ncl={len(set(c))}'
    return s

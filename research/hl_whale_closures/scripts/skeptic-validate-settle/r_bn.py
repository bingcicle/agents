"""R4-BN adversarial: measure Binance move uniformly from last price BEFORE first whale fill to last price at decision second,
for every verified R1 paper trade (HL>=1%), incl. one-shot closes (dump_dur=0) that the settlement headline drops by construction."""
import pandas as pd, numpy as np, os, warnings; warnings.filterwarnings('ignore')
from myload import *; from mystats import *
r=rev(); r=r[(r.entered==1)&(r.strategy=='R1_загальний')].copy()
r['net']=r.s_net_official_pct; r['gross']=r.s_gross_tape_pct; r['cost']=r.s_costs_pct
fl=r.s_flags.fillna('').str.split(';')
r['ok']=(r.s_status=='verified')&r.net.notna()&~fl.apply(lambda L:'partial' in L or 'no_whale_fill' in L)
r=r[r.ok&(r['loc']>='2026-09-13')]
r['day']=r['loc'].dt.strftime('%m-%d')
def load(sym, sec0, sec1):
    out=[]
    for d in sorted({pd.Timestamp(s,unit='s').strftime('%Y-%m-%d') for s in (sec0-1,sec1)}):
        fn=SP+f'data/bars1s/{sym}/{d}.npz'
        if os.path.exists(fn):
            z=np.load(fn); out.append(pd.DataFrame({'c':z['c'],'n':z['n']},index=z['sec']))
    if not out: return None
    return pd.concat(out).sort_index()
def last_before(b, sec):
    s=b[b.index<sec]
    return float(s.c.iloc[-1]) if len(s) and sec-s.index[-1]<=120 else np.nan
rows=[]
for i,x in r.iterrows():
    sym=SYM.get(x.coin)
    if not sym: rows.append((i,np.nan,np.nan)); continue
    f0=int(x.s_dump_first_ts_ms//1000); dec=int(x.decision_ms//1000) if np.isfinite(x.decision_ms) else int(x.entry_ts_ms//1000)
    b=load(sym,f0-200,dec+5)
    if b is None: rows.append((i,np.nan,np.nan)); continue
    p0=last_before(b,f0)          # last trade in seconds strictly before the first fill second
    p1=last_before(b,dec)         # last trade before decision second (known at decision)
    sgn=1 if x.our_side=='LONG' else -1   # dump direction = opposite of our side
    rows.append((i, -sgn*100*(p1/p0-1), (dec-f0)))
D=pd.DataFrame(rows,columns=['i','bn_dec','span_s']).set_index('i'); r=r.join(D)
print('coverage', r.bn_dec.notna().sum(), '/', len(r), ' (bars1s end 29.09)')
print('corr settlement bn vs my bn_dec:', r[['s_dump_move_pct','bn_dec']].corr().iloc[0,1].round(3))
for since in ['2026-09-13','2026-09-19']:
    d=r[(r['loc']>=since)&r.bn_dec.notna()]
    print('=== since',since,'n',len(d))
    d['b']=pd.cut(d.bn_dec,[-100,0,1,1.5,2,3,100])
    print(d.groupby('b').net.agg(['size','mean','median','sum']).round(3).T.to_string())
    for th in [1.5,2,2.5]:
        s=d[d.bn_dec>=th]; x=s.net.values
        if len(x)>=3:
            print(f' bn_dec>={th}: n={len(x)} w={s.whale_addr.nunique()} days={s.day.nunique()} med={np.median(x):+.3f} mean={x.mean():+.3f} sum={x.sum():+.2f} oneshots={int((s.s_dump_dur_s==0).sum())} CI90 mean wallet {np.round(cboot(x,s.whale_addr.values)[:2],3)} signflip {signflip(s.gross.values,s.whale_addr.values,s.cost.values):.3f}')
    s=d[(d.bn_dec>=2)]
    print(s[['date','coin','our_side','s_dump_dur_s','s_dump_move_pct','bn_dec','span_s','net']].round(3).to_string())

print('\n==== extra checks')
r['dec_minus_entry_s']=(r.decision_ms-r.entry_ts_ms)/1000
print('decision-entry s:', r.dec_minus_entry_s.describe().round(3).to_dict())
print('big one-shot losers bn_dec:'); print(r[r.s_dump_dur_s==0][['date','coin','our_side','bn_dec','net']].round(3).to_string())
# drift over hold
rows=[]
for i,x in r[r.bn_dec.notna()].iterrows():
    sg=1 if x.our_side=='LONG' else -1
    t0=x.s_entry_ts_ms; t1=x.s_exit_ts_ms
    rows.append((i, ret('BTCUSDT',t0,t1,sg), altidx(t0,t1,sg)))
E=pd.DataFrame(rows,columns=['i','btc','alt']).set_index('i'); r=r.join(E)
for th in [1.5,2.0]:
    s=r[r.bn_dec>=th]
    print(f'bn_dec>={th}: n={len(s)} LONG={int((s.our_side=="LONG").sum())} net mean {s.net.mean():+.3f} btc {s.btc.mean():+.3f} alt {s.alt.mean():+.3f} net-alt {(s.net-s.alt).mean():+.3f} CI day(net-alt) {np.round(cboot((s.net-s.alt).dropna().values,s.loc[(s.net-s.alt).dropna().index,"day"].values)[:2],3)}')
    s=s.sort_values('entry_ts_ms'); x=s.net.values; h=len(x)//2
    print('   halves mean', x[:h].mean().round(3), x[h:].mean().round(3), ' per day', s.groupby('day').net.agg(['size','sum']).round(2).to_dict('index'))
    g=s.groupby('whale_addr').net.sum(); print('   max wallet share', (g.max()/x.sum()).round(2), ' coins', s.coin.value_counts().to_dict())
s=r[r.bn_dec<1.5]; print(f'bn_dec<1.5: n={len(s)} net {s.net.mean():+.3f} alt {s.alt.mean():+.3f} net-alt {(s.net-s.alt).mean():+.3f}')
# sensitivity: p1 one second earlier (extra latency)
r.to_pickle('r1_bn.pkl')

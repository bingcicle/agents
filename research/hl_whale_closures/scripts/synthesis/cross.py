# Cross-candidate check: are the "positive" candidates the same market episodes (days/coins)? Day concentration + daily correlation.
import pandas as pd, numpy as np, sys
SP='/tmp/claude-0/-home-user-agents/7474b01a-2fc6-5c7e-b048-be34b1c68dfb/scratchpad/work/'
sys.path.insert(0,SP+'skeptic-whale-state')
C={}
def utcday(ms): return pd.to_datetime(ms,unit='ms').dt.strftime('%m-%d')
# A2 (maker exit, fallback taker)
a=pd.read_parquet(SP+'skeptic-hl-dislocation/s_a2_exit.parquet'); C['A2 maker-after-sweep']=pd.DataFrame({'day':a.day,'coin':a.coin,'y':a['R1000_0.1_px_fb_bn'],'test':a.test})
# B12 union events, mnA12 - 0.146
u=pd.read_parquet(SP+'skeptic-long-horizon/U.parquet'); u=u[u.mnA12.notna()]
C['B12 fade 12h']=pd.DataFrame({'day':pd.to_datetime(u.dec_ms,unit='ms').dt.strftime('%m-%d'),'coin':u.coin,'y':u.mnA12-0.146,'test':u.split=='test'})
# S_DISTRESS (skeptic file), dedupe coin|side 24h
from sk import dedupe
s=pd.read_parquet(SP+'skeptic-whale-state/sim_ep_L24.parquet'); s=s[s.y.notna()&s.distress&(s.day>='2026-08-27')&(s.day<='2026-09-29')]
s=dedupe(s,24)
C['S_DISTRESS 24h']=pd.DataFrame({'day':s.day.str[5:],'coin':s.coin,'y':s.y,'test':s.day>='2026-09-22'})
ss=s[s.side<0]; C['S_DISTRESS SHORT-only']=pd.DataFrame({'day':ss.day.str[5:],'coin':ss.coin,'y':ss.y,'test':ss.day>='2026-09-22'})
# validate-settle cards (HEAD rows)
t=pd.read_parquet(SP+'validate-settle/trades.parquet'); t=t[t.hd]
def card(name, since_ms, extra=None):
    d=t[(t.card==name)&(t.ts_ms>=since_ms)]
    if extra is not None: d=d[extra(d)]
    return pd.DataFrame({'day':utcday(d.ts_ms),'coin':d.coin,'y':d.net,'test':d.ts_ms>=1789948800000})
ms=lambda s: int((pd.Timestamp(s)-pd.Timedelta(hours=2)).value//10**6)   # CEST string -> UTC ms
C['T1_15 (HEAD)']=card('T1_твап_відкриття_15',ms('2026-09-17 10:32'))
C['R4 HEAD']=card('R4_великий',ms('2026-09-19 00:00'))
C['F9 HEAD']=card('F9_без_ратіо_90',ms('2026-09-20 00:00'))
C['VVV pair F1']=card('F1_1хв',ms('2026-09-20 00:00'),lambda d:(d.coin=='VVV')&d.wallet.str.startswith('0x0871deb3'))
# R1 with Binance move to decision >=1.5 (skeptic)
r=pd.read_pickle(SP+'skeptic-validate-settle/r1_bn.pkl'); r=r[r.bn_dec>=1.5]
C['R1 bn>=1.5']=pd.DataFrame({'day':r.day.astype(str).str[5:10] if r.day.dtype==object else utcday(r.decision_ms),'coin':r.coin,'y':r.net,'test':r.decision_ms>=1789948800000})
# REV P1 dedup
v=pd.read_csv(SP+'validate-bars/rev_p1_dedup.csv'); C['REV P1']=pd.DataFrame({'day':utcday(v.sig_ms),'coin':v.coin,'y':v.net,'test':v.sig_ms>=1789948800000})
# FOLLOW-BIG-TOUCH L300 H300 rel24>=4
f=pd.read_parquet(SP+'skeptic-impulse-latency/cand_L300.parquet'); C['FOLLOW-BIG-TOUCH']=pd.DataFrame({'day':utcday(f.t0),'coin':f.coin,'y':f.net,'test':f.per.astype(str).str.upper().str.startswith('T')})
rows=[]; daily={}
for k,d in C.items():
    d=d[d.y.notna()]
    tot=d.y.sum(); bd=d.groupby('day').y.sum(); bc=d.groupby('coin').y.sum()
    pos=d.y[d.y>0].sum()
    rows.append(dict(cand=k,n=len(d),days=d.day.nunique(),mean=d.y.mean(),med=d.y.median(),sum=tot,
        sh23=d[d.day=='09-23'].y.sum()/tot if tot else np.nan, n23=(d.day=='09-23').sum(),
        mean_wo23=d[d.day!='09-23'].y.mean(), topday=bd.idxmax(), topday_sh=bd.max()/tot,
        mean_wo_topday=d[d.day!=bd.idxmax()].y.mean(), topcoin=bc.idxmax(), topcoin_sh=bc.max()/tot,
        mean_wo_topcoin=d[d.coin!=bc.idxmax()].y.mean(), nil_sh=d[d.coin=='NIL'].y.sum()/tot,
        test_mean=d[d.test].y.mean(), test_n=int(d.test.sum())))
    daily[k]=bd
R=pd.DataFrame(rows); pd.set_option('display.width',250)
print(R.round(3).to_string(index=False))
D=pd.DataFrame(daily).loc[lambda x: x.index>='09-12'].fillna(0)
print('\ndaily sums (units: % per trade summed), 12.09-29.09'); print(D.round(2).to_string())
print('\nSpearman corr of daily sums'); print(D.corr(method='spearman').round(2).to_string())

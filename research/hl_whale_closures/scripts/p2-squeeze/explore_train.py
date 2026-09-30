from common import *
# hourly grid decision points
hrs=np.arange(24, NH)  # decision at hour h (t=T0+h*3600e3)
m=hrs*60-1               # minute closing at t
k5=hrs*12-1-1            # 5-min OI bucket with create_time <= t-5min  (create_time = bucket start? use conservative)
rows=[]
for Lh in (1,4,12,24):
    r=Cf[:,m]/Cf[:,m-Lh*60]-1
    alt=np.nanmean(r[ALT],axis=0)
    ex=r-alt
    oi=OIf[:,k5]/OIf[:,k5-Lh*12]-1
    tt=pd.Timestamp(T0,unit='ms')+pd.to_timedelta(hrs,unit='h')
    tr=np.asarray(tt<pd.Timestamp("2026-07-01"))
    e=ex[:,tr].ravel(); o=oi[:,tr].ravel(); ok=np.isfinite(e)&np.isfinite(o)
    e=e[ok]; o=o[ok]
    print(f'L={Lh}h n={ok.sum()} ex q: '+' '.join(f'{q}:{np.quantile(e,q)*100:+.1f}' for q in (0.001,0.005,0.01,0.05,0.5,0.95,0.99,0.995,0.999)))
    print('   oi q: '+' '.join(f'{q}:{np.quantile(o,q)*100:+.1f}' for q in (0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99)))
    for X in (0.03,0.05,0.08,0.12,0.15,0.2):
        p=e>=X; d=e<=-X
        print(f'   X={X}: pump n={p.sum()} (oi<=-5%: {(p&(o<=-0.05)).sum()}, oi<=-10%: {(p&(o<=-0.10)).sum()}, oi>=+10%: {(p&(o>=0.10)).sum()})  dump n={d.sum()} (oi<=-5%: {(d&(o<=-0.05)).sum()}, <=-10% {(d&(o<=-0.10)).sum()})')
print('oi coverage', np.isfinite(OI).mean(), 'c coverage', np.isfinite(C).mean())

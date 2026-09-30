"""Evaluate event sets at long horizons. events df needs: coin, dec_ms (when bot knew), side (+1 LONG/-1 SHORT), addr, day.
Entry = close of first 1m candle closing >= dec_ms + 60 s. Exit = close h hours later."""
import numpy as np, pandas as pd
from lh import *
HS=[1,2,4,8,24,48,72]
COST=0.146

def attach(ev, hs=HS):
    ev=ev.copy()
    ev['sym']=ev.coin.map(coin_sym); ev=ev[ev.sym.notna()].copy()
    j=ev.sym.map(SIDX).values.astype(int); i0=entry_minute(ev.dec_ms.values)
    ev['i0']=i0; ev['entry_ms']=T0+(i0+1)*60000
    EARLY_END=pd.Timestamp('2026-09-12 18:00').value//10**6
    ev['split']=np.where(ev.dec_ms<EARLY_END,'early',np.where(ev.dec_ms<DISC_END,'disc','test'))
    s=ev.side.values
    for h in hs:
        i1=i0+h*60
        r=logret(j,i0,i1); a=idxret(i0,i1,'alt'); b=idxret(i0,i1,'btc')
        f=funding_pct(ev.coin.values[0],0,0) if False else None
        ev[f'raw{h}']=s*r; ev[f'mnA{h}']=s*(r-a); ev[f'mnB{h}']=s*(r-b)
        # funding paid by our side (HL hourly proxy)
        fp=np.array([funding_pct(c,np.array([t0]),np.array([t0+h*3600000]))[0] for c,t0 in zip(ev.coin.values,ev.entry_ms.values)])
        ev[f'fund{h}']=s*fp
        ev[f'net{h}']=ev[f'raw{h}']-COST-np.nan_to_num(ev[f'fund{h}'])
        ev[f'netmn{h}']=ev[f'mnA{h}']-COST-np.nan_to_num(ev[f'fund{h}'])
    return ev

def placebo(ev, h, col='mnA', ndraw=200, seed=1):
    """random times in same coin, same split window, same hour-of-day, same side. Returns (null means array, per-event placebo mean)."""
    rng=np.random.default_rng(seed)
    j=ev.sym.map(SIDX).values.astype(int); s=ev.side.values
    hod=((ev.i0.values%1440)//60)
    b0=minute_of(pd.Timestamp('2026-08-30 00:00').value//10**6); b1=minute_of(pd.Timestamp('2026-09-12 18:00').value//10**6); b2=minute_of(DISC_END)
    sp_=ev.split.values
    lo=np.where(sp_=='early',b0,np.where(sp_=='disc',b1,b2))
    hi=np.where(sp_=='early',b1,np.where(sp_=='disc',b2,NMIN))-h*60-1
    nd=(hi-lo)//1440
    R=np.full((ndraw,len(ev)),np.nan)
    for k in range(ndraw):
        dd=rng.integers(0,np.maximum(nd,1)); mm=rng.integers(0,60,len(ev))
        i0=lo-(lo%1440)+dd*1440+hod*60+mm
        i0=np.where(i0<lo,i0+1440,i0); i0=np.minimum(i0,hi)
        i1=i0+h*60; r=logret(j,i0,i1)
        if col=='mnA': r=r-idxret(i0,i1,'alt')
        elif col=='mnB': r=r-idxret(i0,i1,'btc')
        R[k]=s*r
    ok=~np.isnan(ev[f'{col}{h}'].values)
    nullm=np.nanmean(R[:,ok],axis=1)
    return nullm, np.nanmean(R,axis=0)

def signperm(ev,h,col='mnA',ndraw=500,seed=2):
    """permute sides within coin (keeps times and LONG/SHORT mix per coin); null of mean."""
    rng=np.random.default_rng(seed); x=ev[f'{col}{h}'].values/ev.side.values  # unsigned coin move
    out=[]
    groups=[np.where(ev.coin.values==c)[0] for c in ev.coin.unique()]
    for _ in range(ndraw):
        s=ev.side.values.copy()
        for g in groups: s[g]=rng.permutation(s[g])
        out.append(np.nanmean(s*x))
    return np.array(out)

def nonoverlap(ev, h, key='coin'):
    """keep first event per coin, skip later events whose entry is within h hours of the last kept one (same coin)."""
    keep=[]; last={}
    for idx,r in ev.sort_values('entry_ms').iterrows():
        k=r[key]
        if k in last and r.entry_ms<last[k]+h*3600000: continue
        last[k]=r.entry_ms; keep.append(idx)
    return ev.loc[keep]

def report(ev, name, hs=HS, cols=('raw','mnA','mnB','net','netmn'), do_placebo=True, out=None):
    lines=[f'==== {name}  (events={len(ev)})']
    for split in ['early','disc','test']:
        e=ev[ev.split==split]
        for h in hs:
            v=e[~e[f'raw{h}'].isna()]
            if len(v)==0: continue
            ln=f'[{split}] h={h:>2}h n={len(v)} coins={v.coin.nunique()} days={v.day.nunique()} | '
            ln+=' '.join(f'{c}:med={np.nanmedian(v[f"{c}{h}"]):+.2f}/mean={np.nanmean(v[f"{c}{h}"]):+.2f}' for c in cols)
            ln+=f' | fund_mean={np.nanmean(v[f"fund{h}"]):+.3f}'
            for sd,nm in ((1,'L'),(-1,'S')):
                vv=v[v.side==sd]
                if len(vv): ln+=f' | {nm} n={len(vv)} mnA={np.nanmean(vv[f"mnA{h}"]):+.2f} raw={np.nanmean(vv[f"raw{h}"]):+.2f}'
            lines.append(ln)
            lines.append('      mnA honesty: '+honest(v[f'mnA{h}'].values,v,('coin','day','addr'),n=1000))
            if do_placebo:
                nm,_=placebo(v,h,'mnA',100); sp_=signperm(v,h,'mnA',200)
                obs=np.nanmean(v[f'mnA{h}'])
                lines.append(f'      placebo(same coin/hour/side, mnA): null mean={nm.mean():+.3f} sd={nm.std():.3f} P(null>=obs)={(nm>=obs).mean():.2f} | signperm-in-coin: sd={sp_.std():.3f} P(>=obs)={(sp_>=obs).mean():.2f}')
    txt='\n'.join(lines); print(txt, flush=True)
    if out: open(out,'a').write(txt+'\n')
    return txt

def compact(ev, name, hs=HS, out=None, ndraw=100, splits=('early','disc','test')):
    lines=[f'==== {name} (events={len(ev)})',
           'split h | n coins days | raw mean/med | mnA mean/med win | mnB mean | netmn mean | fund | L n:mnA  S n:mnA | CI90 mnA-mean by coin ; by day | placebo null mean, P(null>=obs) | signperm P']
    for split in splits:
        e=ev[ev.split==split]
        for h in hs:
            v=e[~e[f'raw{h}'].isna()]
            if len(v)<5: continue
            x=v[f'mnA{h}'].values
            lc=cluster_ci(x,v.coin.values,n=1000); ld=cluster_ci(x,v.day.values,n=1000)
            nm,_=placebo(v,h,'mnA',ndraw); sp_=signperm(v,h,'mnA',200); obs=np.nanmean(x)
            L=v[v.side==1]; S=v[v.side==-1]
            lines.append(f'{split:5s} {h:>2} | {len(v):4d} {v.coin.nunique():3d} {v.day.nunique():2d} | {np.nanmean(v[f"raw{h}"]):+.2f}/{np.nanmedian(v[f"raw{h}"]):+.2f} | '
                f'{obs:+.2f}/{np.nanmedian(x):+.2f} {100*np.nanmean(x>0):.0f}% | {np.nanmean(v[f"mnB{h}"]):+.2f} | {np.nanmean(v[f"netmn{h}"]):+.2f} | {np.nanmean(v[f"fund{h}"]):+.3f} | '
                f'L{len(L)}:{np.nanmean(L[f"mnA{h}"]) if len(L) else np.nan:+.2f} S{len(S)}:{np.nanmean(S[f"mnA{h}"]) if len(S) else np.nan:+.2f} | '
                f'[{lc[0]:+.2f},{lc[1]:+.2f}] ; [{ld[0]:+.2f},{ld[1]:+.2f}] | {nm.mean():+.2f}, {(nm>=obs).mean():.2f} | {(sp_>=obs).mean():.2f}')
    txt='\n'.join(lines); print(txt,flush=True)
    if out: open(out,'a').write(txt+'\n')
    return txt

import numpy as np, pandas as pd
from ev import *
from scipy import stats
BG=pd.read_parquet(MY+'ev_trig.parquet')
cols=['addr','coin','dec_ms','side','maxval','src','fsrc','ratio_max']
G=attach(union([BG[cols]]),hs=(4,8,12,24,48))
G.to_parquet(MY+'G_trig.parquet')
out=open(MY+'out_b_trig.txt','w')
def pr(s): print(s); out.write(s+'\n')
pr(f'trig-only events {len(G)} coins {G.coin.nunique()} wallets {G.addr.nunique()} {G.split.value_counts().to_dict()}')
for h in (8,12,24):
    pr(f'== h={h}')
    for sp in ('early','disc','test','ALL'):
        v=G if sp=='ALL' else G[G.split==sp]; v=v.dropna(subset=[f'mnA{h}']); x=v[f'mnA{h}'].values
        nm=placebo(v,h,'mnA',nd=300)
        top1=np.sort(x)[-1]; hl=np.median((x[:,None]+x[None,:])/2)
        pr(f'[{sp:5s}] {line(v,f"mnA{h}")} P={(nm>=x.mean()).mean():.3f} | wo_top1={(x.sum()-top1)/(len(x)-1):+.2f} HL={hl:+.2f} wilcoxP={stats.wilcoxon(x).pvalue:.3f} | lgA={v[f"lgA{h}"].mean():+.2f} mnb={v[f"mnb{h}"].mean():+.2f} mnB={v[f"mnB{h}"].mean():+.2f} raw={v[f"raw{h}"].mean():+.2f}')
        for sd,nn in ((1,'L'),(-1,'S')):
            w=v[v.side==sd]; pr(f'        {nn}: n={len(w)} mean={w[f"mnA{h}"].mean():+.2f} med={w[f"mnA{h}"].median():+.2f}')
    # ratio split
    v=G.dropna(subset=[f'mnA{h}'])
    for lo,hi in ((0,3),(3,6),(6,1e9)):
        w=v[(v.ratio_max>=lo)&(v.ratio_max<hi)]; pr(f'   ratio[{lo},{hi}) n={len(w)} '+' '.join(f'{sp}:{w[w.split==sp][f"mnA{h}"].mean():+.2f}' for sp in ('early','disc','test')))
out.close()

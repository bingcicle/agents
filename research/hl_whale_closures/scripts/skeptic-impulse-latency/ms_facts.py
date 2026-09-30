from common_s import *
from cand_ms import EV, PL, mk, COST
pd.set_option('display.width',250)
bins=[0,0.01,0.05,0.1,0.3,1,1e9]
EV['b']=pd.cut(EV.td,bins)
cols=['o-2000','o-1000','o-500','o-250','o-100','o-50','o0','o100','o150','o200','o250','o300','o500','o1000','o2000','o60000','o300000']
t=EV.groupby('b',observed=True)[cols].mean(); t['n']=EV.groupby('b',observed=True).size(); print(t.round(3).to_string())
PL['b']=PL.key.map(EV.set_index('key').b)
print('placebo'); print(PL.groupby('b',observed=True)[['o-1000','o-250','o200','o1000','o60000']].mean().round(3))
x=EV[EV.td>=0.3]; print('t50 td>=0.3 (m2s>=0.05):', x.t50.notna().sum(),'of',len(x), x.t50.quantile([.1,.25,.5,.75,.9]).to_dict(), 'share t50<=0', (x.t50<=0).mean().round(2))
x=EV[EV.rel24>=4]; print('t50 rel24>=4:', x.t50.quantile([.1,.25,.5,.75,.9]).to_dict(), 't10', x.t10.quantile([.25,.5,.75]).to_dict())
print('pre-move rel24>=4 (mean/median):', {c:(round(x[c].mean(),3),round(x[c].median(),3)) for c in ('o-5000','o-1000','o-250','o-100','o0','o150','o200','o300','o1000')})
# latency curve rel24>=4, all anchors (no dedupe), by period, several price models
for H in (60,300):
  for per in ('D','T'):
    y=EV[(EV.rel24>=4)&(EV.per==per)]
    s=' '.join(f'L{L}:{(y[f"x{H}"]-y[f"a{L}"]).mean():+.3f}' for L in (0,100,150,200,250,300,500,1000,2000,3000,5000))
    print(f'touch H{H} {per} n={len(y)}: {s}')
  y=EV[EV.rel24>=4]
  for ent in ('v','w'):
    s=' '.join(f'L{L}:{(y[f"x{H}"]-y[f"{ent}{L}"]).mean():+.3f}' for L in (0,100,200,300,500,1000,2000,3000))
    print(f'{ent}-entry H{H} ALL: {s}')
# td 0.3-1 own-node vs WS
y=EV[(EV.td>=0.3)&(EV.td<1)]
print('td0.3-1 touch H60: ', ' '.join(f'L{L}:{(y.x60-y[f"a{L}"]).mean():+.3f}' for L in (0,150,300,500,1000,1500,2000)))
y=EV[(EV.td>=1)]
print('td>=1 touch H60: ', ' '.join(f'L{L}:{(y.x60-y[f"a{L}"]).mean():+.3f}' for L in (0,150,300,500,1000,1500,2000)))
# candidate: price model sensitivity (dedupe per model)
for ent,ex,lab in (('a','x','touch/touch'),('v','x','vwap500/touch'),('w','x','worst1s/touch'),('a','xw','touch/worst3s'),('w','xw','worst1s/worst3s')):
    for L in (300,1000,2000):
        d=mk(EV[EV.rel24>=4],L,300,ent,ex)
        r=[f'{per}: n={len(d[d.per==per])} gross {d[d.per==per].g.mean():+.3f} net146 {d[d.per==per].g.mean()-COST:+.3f} med {d[d.per==per].g.median()-COST:+.3f}' for per in ('D','T')]
        print(f'{lab:18s} L{L}: '+' | '.join(r))

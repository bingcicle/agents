from common_s import *
from cand_ms import EV, PL, mk
COST=0.146
for thr in (6,8):
  for L in (300,2000):
    for ent,ex,lab in (('a','x','touch'),('v','x','vwap500'),('w','x','worst1s')):
        d=mk(EV[EV.rel24>=thr],L,300,ent,ex)
        d['s1']=(d.t0+L)//1000; d['s2']=(d.t0+300000)//1000; d=add_alt(d,'s1','s2','alt')
        d['net']=d.g-COST; d['net_alt']=d.g-d.wdir*d.alt-COST
        if lab!='touch':
            print(f'rel24>={thr} L{L} {lab}: '+' | '.join(f'{p}: n={len(d[d.per==p])} net {d[d.per==p].net.mean():+.3f} med {d[d.per==p].net.median():+.3f}' for p in ('D','T'))); continue
        for p in ('D','T'):
            x=d[d.per==p]; print(battery(x,'net',f'rel24>={thr} L{L} touch {p}'))
            pl=PL[PL.key.isin(x.key)]; pg=(pl.x300-pl[f'a{L}'])
            print(f'   gross {x.g.mean():+.3f} placebo {pg.mean():+.3f} | net_alt {x.net_alt.mean():+.3f} med {x.net_alt.median():+.3f} | SHORT n={(x.wdir<0).sum()} net {x[x.wdir<0].net.mean():+.3f} LONG n={(x.wdir>0).sum()} net {x[x.wdir>0].net.mean():+.3f}')
            print('   top coins', x.groupby('coin').net.agg(['size','sum']).sort_values('sum').tail(4).round(2).to_dict('index'))

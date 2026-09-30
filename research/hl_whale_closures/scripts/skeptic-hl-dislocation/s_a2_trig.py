from s_a2 import *
import warnings; warnings.filterwarnings('ignore')
A['exc_self'] = 100 * A.wd * (A.pl / (A['b-1'] * (1 + A.bas)) - 1)
key = A.set_index('h')[['exc_self', 'td']]
for nm, m in [('td>=0.3', A.td >= 0.3), ('td>=1', A.td >= 1), ('exc>=1', A.exc_self >= 1), ('td>=0.3 & exc<1', (A.td >= 0.3) & (A.exc_self < 1)),
              ('td>=0.3 & exc>=1', (A.td >= 0.3) & (A.exc_self >= 1)), ('td 0.3-1', (A.td >= 0.3) & (A.td < 1)), ('exc>=1 & td<0.3', (A.exc_self >= 1) & (A.td < 0.3))]:
    f = add_epi(run(m))
    rep(f, 'P60', nm)
f = run(A.td >= 1)
pd.set_option('display.width', 250)
f['dt'] = pd.to_datetime(f.ts, unit='ms').dt.strftime('%m-%d %H:%M')
print(f[f.test][['dt', 'coin', 'whale', 'exc', 'td', 'P5', 'P60', 'P300']].round(2).to_string())

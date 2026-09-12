
let sSortK='v', sSortD=-1;
const trades=[{v:5},{v:null},{v:1},{v:null},{v:9}];
const run=()=>{const nul=sSortD===-1?-Infinity:Infinity;
  const t=trades.slice();
  t.sort((a,b)=>{let x=a[sSortK],y=b[sSortK];
    if(typeof x==='string'||typeof y==='string')
      return sSortD*String(x??'').localeCompare(String(y??''));
    x=(x==null||isNaN(x))?nul:x; y=(y==null||isNaN(y))?nul:y;
    return sSortD*(x-y);});
  return t.map(o=>o.v);};
const d=run(); sSortD=1; const a=run();
if(JSON.stringify(d)!==JSON.stringify([9,5,1,null,null])) {console.error('FAIL desc',d);process.exit(1);}
if(JSON.stringify(a)!==JSON.stringify([1,5,9,null,null])) {console.error('FAIL asc',a);process.exit(1);}
console.log('sort: ↓=спадання, null-и в кінці — OK');

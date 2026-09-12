const pctTxt=(v,d)=>((v>=0?'+':'')+v.toFixed(d===1?1:2)+'%');
function curveSvg(curve,curveN,w=680,h=180){
  const vals=curve.map((v,i)=>[i+1,v,(curveN||[])[i]??0]).filter(p=>p[1]!=null);
  if(!vals.length) return '<div class="empty-note">Ще немає завершених угод</div>';
  const ys=vals.map(p=>p[1]);
  const mn=Math.min(0,...ys), mx=Math.max(0,...ys), rng=(mx-mn)||1;
  const L=34,R=8,T=10,B=18;
  const X=m=>L+(m-1)/59*(w-L-R);
  const Y=v=>T+(mx-v)/rng*(h-T-B);
  // "найкраща хвилина" — лише серед хвилин із достатнім покриттям:
  // одна щаслива угода на m2 не має перемагати 30 угод на m30
  const maxN=Math.max(1,...vals.map(p=>p[2]));
  const minN=Math.max(3,Math.ceil(0.3*maxN));
  let best=null;
  for(const p of vals) if(p[2]>=minN&&(best==null||p[1]>best[1])) best=p;
  const pts=vals.map(p=>X(p[0]).toFixed(1)+','+Y(p[1]).toFixed(1)).join(' ');
  let grid='';
  for(const m of [10,20,30,40,50,60])
    grid+=`<line x1="${X(m)}" y1="${T}" x2="${X(m)}" y2="${h-B}" stroke="var(--line)" stroke-width="1"/>
           <text x="${X(m)}" y="${h-5}" fill="var(--ink3)" font-size="9" text-anchor="middle">${m}хв</text>`;
  return `<svg id="curveSvg" width="${w}" height="${h}" style="max-width:100%"
      onmousemove="curveTip(event,${w},${L},${R})" onmouseleave="hideTip()">
    ${grid}
    <line x1="${L}" y1="${Y(0)}" x2="${w-R}" y2="${Y(0)}" stroke="var(--line2)" stroke-width="1" stroke-dasharray="4,3"/>
    <text x="${L-4}" y="${Y(0)+3}" fill="var(--ink3)" font-size="9" text-anchor="end">0%</text>
    <text x="${L-4}" y="${Y(mx)+3}" fill="var(--ink3)" font-size="9" text-anchor="end">${pctTxt(mx,1)}</text>
    ${mn<0?`<text x="${L-4}" y="${Y(mn)+3}" fill="var(--ink3)" font-size="9" text-anchor="end">${pctTxt(mn,1)}</text>`:''}
    <polyline points="${pts}" fill="none" stroke="var(--signal)" stroke-width="2" stroke-linejoin="round"/>
    ${best?`<circle cx="${X(best[0])}" cy="${Y(best[1])}" r="3.5" fill="var(--signal)" stroke="var(--bg)" stroke-width="1.5"/>
    <text x="${best[0]>52?X(best[0])-6:X(best[0])}" y="${Math.max(Y(best[1])-8,11)}" fill="var(--ink)" font-size="10" font-weight="600" text-anchor="${best[0]>52?'end':(best[0]<5?'start':'middle')}">m${best[0]} ${pctTxt(best[1])} · n=${best[2]}</text>`:''}
  </svg>`;
}

// m2 = +10% але n=1; m1/m3..m12 = 0.5% з n=10 -> best мусить ігнорувати m2
const curve = [0.5, 10, 0.6, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5];
const n     = [10,  1,  10,  10,  10,  10,  10,  10,  10,  10,  10,  10];
const svg = curveSvg(curve, n);
const lab = (svg.match(/>m(\d+) ([^<]*)</)||[]);
if(!lab[1]) { console.error("FAIL: немає best-мітки"); process.exit(1); }
if(lab[1] === "2") { console.error("FAIL: best = m2 з n=1"); process.exit(1); }
if(!lab[2].includes("n=")) { console.error("FAIL: у мітці немає n"); process.exit(1); }
console.log("best label: m"+lab[1], lab[2]);
// всі хвилини з n<minN: best-мітки немає, але крива рендериться
const svg2 = curveSvg([1,2,3], [1,1,1]);
if(svg2.includes(">m")) { console.error("FAIL: мітка при малих n"); process.exit(1); }
if(!svg2.includes("polyline")) { console.error("FAIL: крива зникла"); process.exit(1); }
console.log("малі n: мітки немає, крива є — OK");

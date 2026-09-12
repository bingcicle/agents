#!/usr/bin/env bash
# Батарея регресій whale_terminal: усі сюїти паралельно, вивід у _out/, зведення PASS/FAIL.
# Запуск: hl_terminal/tests/run_battery.sh [додаткові сюїти]
# Сюїти самодостатні: беруть server.py / settle.py / html / CLAUDE.md з ../ (відносно файлу),
# тимчасові дані пишуть у tests/_tmp/. Секретів не потребують і не містять.
# test_settle: реальна проба стрічки (tests/fixtures/tape_probe/{HYPEUSDT,PUMPUSDT}.zip, не в git)
# використовується, якщо є; інакше сюїта генерує синтетичну і каже про це у виводі.
cd "$(dirname "$0")"
mkdir -p _out
suites="test_audit_fixes.py test_audit2.py test_audit3.py test_audit4.py test_audit5.py test_audit6.py test_audit7.py test_f4_tiebreak.py test_fills_logic.py test_strat2.py test_v28.py test_v29.py test_v210.py test_v211.py test_v212.py test_v213.py test_v214.py test_v215.py test_ws_frames.py test_ws_frames2.py test_settle.py test_v216.py test_v218.py test_v219.py test_v220.py $*"
for s in $suites; do
  ( timeout 900 python3 "$s" > "_out/$s.txt" 2>&1; echo "$? $s" > "_out/$s.rc" ) &
done
wait
fail=0
for s in $suites; do
  rc=$(cut -d' ' -f1 "_out/$s.rc")
  if [ "$rc" = "0" ]; then echo "PASS $s"; else fail=1; echo "FAIL($rc) $s :: $(grep -m1 -E 'Error|assert|Traceback' "_out/$s.txt" | head -c 160)"; fi
done
if command -v node >/dev/null 2>&1; then
  for j in test_curve.js test_sort.js; do
    if node "$j" > "_out/$j.txt" 2>&1; then echo "PASS $j"; else fail=1; echo "FAIL $j :: $(head -c 160 "_out/$j.txt")"; fi
  done
fi
exit $fail

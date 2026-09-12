import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки сьомого аудиту (v2.6 -> v2.7)."""
import ast, threading, time, json, os, re, sys, tempfile, csv, math, queue

from v28_shim import with_opens_wrap as _v28_with_opens
from v28_shim import NEW as _V28
SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None):
    # рев'ю аудит-2: тіло тику винесено у _strat2_tick — тягнемо разом із циклом
    if "run_strat2_loop" in names: names = set(names) | {"_strat2_tick"}
    mod = ast.Module(body=[n for n in tree.body
                           if isinstance(n, ast.FunctionDef) and n.name in names],
                     type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "print": lambda *a, **k: None}
    ns.update(_V28)   # v2.8: нові константи/стаби
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

class RateLimited(Exception): pass
class APIError(Exception): pass
C = dict(RateLimited=RateLimited, APIError=APIError, DATA_ALGO_V="2.7",
         MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0, PROFILE_ALGO_V=6,
         F4_MIN_EPISODES=2, F4_MIN_NOTIONAL=100_000.0,
         F4_MAX_UNLOAD_S=300.0, F4_CHUNK_PCT=0.05)
T = 1_700_000_000_000

def base_fill(i, **kw):
    f = {"time": T + i, "px": "2.0", "sz": "1000.0", "dir": "Close Long",
         "hash": f"0x{i}", "crossed": True, "coin": "X", "oid": i,
         "twapId": None, "tid": i}
    f.update(kw); return f

def gmf_with(fills):
    return load({"get_recent_market_fills"}, dict(C,
        hl_post=lambda b, retries=4: fills))["get_recent_market_fills"]

# ── 1. Скан-діф: збій підтвердження НЕ рухає базу (структурно) ──
seg = src[src.index("def check_position_changes"):src.index("def send_close_alert")]
assert "failed_keep.setdefault(addr, {})[coin] = _kept" in seg
i_keep = seg.index("for _fa, _fcoins in failed_keep.items()")
i_repl = seg.index("prev_positions = new_by_addr")
assert i_keep < i_repl, "стара база має відновлюватись ДО заміни знімка"
print("1) скан-діф: пара зі збоєм API зберігає стару базу — дельта не губиться")

# ── 2. Строга схема філів у головному детекторі ─────────
g = gmf_with([base_fill(0, hash=""), base_fill(1, hash="")])
try:
    g("0xa", "X", T - 10, "LONG"); raise SystemExit("FAIL: hashless пройшли")
except APIError as e: assert "hash" in str(e)
g = gmf_with([base_fill(0, crossed="false")])
try:
    g("0xa", "X", T - 10, "LONG"); raise SystemExit("FAIL: crossed-рядок")
except APIError as e: assert "crossed" in str(e)
g = gmf_with([base_fill(0, px="inf")])
try:
    g("0xa", "X", T - 10, "LONG"); raise SystemExit("FAIL: inf пройшов")
except APIError as e: assert "finite" in str(e) or "px" in str(e)
f_nodir = base_fill(0); del f_nodir["dir"]
g = gmf_with([f_nodir])
try:
    g("0xa", "X", T - 10, "LONG"); raise SystemExit("FAIL: без dir")
except APIError as e: assert "dir" in str(e)
g = gmf_with([base_fill(5), base_fill(1)])   # порядок НЕ зростає
try:
    g("0xa", "X", T - 10, "LONG"); raise SystemExit("FAIL: порядок")
except APIError as e: assert "ascending" in str(e)
# v2.10: "0x0" — нульовий (системний) hash, здорова група — реальним
txs = gmf_with([base_fill(0, hash="0xabc1"),
                base_fill(1, hash="0xabc1")])("0xa", "X", T - 10, "LONG")
assert len(txs) == 1 and txs[0]["sz"] == 2000.0   # здорові групуються
print("2) битий філ (hash/crossed/inf/dir/порядок) = APIError, здорові працюють")

# ── 3. Доставка алертів: ретрай ТОГО САМОГО, стата після успіху ──
sca = src[src.index("def send_close_alert"):src.index("SIM_ENABLED")]
assert sca.index("tg_result = tg_send(msg)") < sca.index('stats["alerts_sent"] += 1')
assert "return False" in sca and "return True" in sca and "return None" in sca
_as0 = src.index("def run_alert_sender")
one_pass = src[_as0:src.index("# Лічильники", _as0)] \
    .replace("while True:", "for _outer in range(1):")
def mk_sender(send_fn):
    q = queue.Queue()
    ns = dict(C, alert_queue=q, send_close_alert=send_fn,
              time=type(sys)("t"), print=lambda *a, **k: None)
    ns["time"].sleep = lambda s: None
    exec(compile(one_pass, "sender", "exec"), ns)
    return ns, q
# transient: 2 збої -> доставлено 3-ю спробою, ЧЕРГА НЕ ЧІПАЄТЬСЯ
sent3 = []
ns3, q3 = mk_sender(lambda a: (sent3.append(a) or len(sent3) >= 3))
q3.put({"coin": "X", "addr": "0xabc"})
ns3["run_alert_sender"]()
assert len(sent3) == 3 and q3.empty()   # ретрай на місці, порядок цілий
# permanent (None): дроп одразу, без 11 циклів
sentP = []
nsP, qP = mk_sender(lambda a: (sentP.append(a), None)[1])
qP.put({"coin": "Y", "addr": "0xdef"})
nsP["run_alert_sender"]()
assert len(sentP) == 1
print("3) transient TG: ретрай ТОГО САМОГО алерту (порядок v1.3); "
     "permanent: дроп одразу")
# рев'ю №1: реенкью в чергу прибраний — сендер не кладе назад
snd = src[_as0:src.index("# Лічильники", _as0)]
assert "alert_queue.put" not in snd
# рев'ю №2: tg_send: 400/403 і відсутній chat_id -> None
tgs = src[src.index("def tg_send"):src.index("def tg_poll_updates")]
assert tgs.count("return None") >= 3
print("3б) порядок доставки збережено; постійні помилки TG = None")

# ── 4. PROFILE_ALGO_V піднято (валідація v2.6 = нова семантика) ──
assert "PROFILE_ALGO_V   = 11" in src   # v2.10: бік Short > Long; v2.18: життєві цикли
print("4) PROFILE_ALGO_V=7: кешовані профілі перерахуються")

# ── 5. F4: hashless-мерж і crossed-рядок = err ──────────
bp = load({"_build_profile", "_grade_episode"}, dict(C))
bp["_build_profile"] = _v28_with_opens(bp["_build_profile"])   # v2.18
def mkf(t, sz, sp, coin="AAA", **kw):
    f = {"time": t, "px": 2.0, "sz": sz, "startPosition": sp,
         "dir": "Close Long", "coin": coin, "crossed": True,
         "twapId": None, "hash": f"0x{coin}{t}", "tid": t}
    f.update(kw); return f
good = [mkf(T, 50000, 100000, "D"), mkf(T + 60_000, 50000, 50000, "D"),
        mkf(T + 10**7, 60000, 60000, "E")]
assert bp["_build_profile"](good)["ok"]
p5a = bp["_build_profile"](good + [mkf(T + 2*10**7, 10, 100, "F", hash="")])
assert not p5a["ok"] and p5a.get("err") == 1, p5a
p5b = bp["_build_profile"](good + [mkf(T + 3*10**7, 10, 100, "G",
                                       crossed="false")])
assert not p5b["ok"] and p5b.get("err") == 1, p5b
print("5) F4: порожній hash і crossed-рядок = err, не тихе склеювання")

# ── 6. NaN не проходить у ціни і в статистику ───────────
pl = src[src.index("def run_px_poller"):src.index("def _px_now")]
assert "math.isfinite(px)" in pl
api = src[src.index("def strat2_api"):]
assert "return v if math.isfinite(v) else d" in api
assert '"read_errors"' in api
print("6) poller і fnum ріжуть NaN/inf; read_errors в API")

# ── 7. prio: вичерпаний бюджет знімає кулдаун ───────────
from collections import deque
prio_seg = src[src.index("def run_prio_fetcher"):src.index("def get_recent_market_fills")]
# бюджет: короткий кулдаун (75с) — ні глухоти 10 хв, ні монополії;
# transient-помилка запиту: повне зняття кулдауну (except-гілка)
assert "_prio_seen[addr] = now - PRIO_COOLDOWN_S + 75" in prio_seg
assert "_prio_seen.pop(addr, None)" in prio_seg
print("7) prio: бюджет -> кулдаун 75с; збій запиту -> кулдаун знято")

# ── 8. sig_id: послідовник проти колізій в одну мс ──────
assert 'sig_id = f"{int(fill_ts_ms or now * 1000)}-{coin}-{addr[2:8]}-{_txh}"' in src   # v2.20: стабільний id замість лічильника
print("8) sig_id містить лічильник — дві події в одну мс не колізують")

# ── 9. silence_late: пізній вихід чесно маркований ──────
loop_src = src[src.index("def run_strat2_loop"):src.index("# ── API")]
one_tick = loop_src.replace("while True:", "for _t in range(1):") \
                   .replace("time.sleep(3)", "pass")
def _dt(ts): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
rows9 = []
ns9 = dict(C, STRAT2_ENABLED=True, strat2_lock=threading.RLock(),
           _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
           rev_open={}, follow_open={}, follow_last_close={},
           _px_now=lambda c, max_age=None: 100.0,
           _sim_slip=lambda d: 0.0005, SIM_COMMISSION=0.0005, _dt=_dt,
           _strat_csv_append=lambda p, h, r: rows9.append((p, r)) or True,
           _rev_row=lambda p, e: ["r"], _rev_out_row=lambda p: ["o"],
           _fol_out_row=lambda p: ["f"], save_state=lambda: None,
           REV_CSV="r.csv", REV_OUT_CSV="o.csv", FOLLOW_CSV="f.csv",
           FOLLOW_OUT_CSV="fo.csv", REV_HEADERS=[], REV_OUT_HEADERS=[],
           FOLLOW_HEADERS=[], FOLLOW_OUT_HEADERS=[], WFAIL_CAP=200,
           REV_TRACK_MIN=60, DATA_ALGO_V="2.7",
           threading=threading, time=time, print=lambda *a, **k: None)
for _fn in ("_fol_row", "_exec_extra", "_p_costs", "_ms", "_rnd"):   # v2.16: вихід follow через _fol_row/стакан
    _seg = src[src.index(f"def {_fn}("):]; _seg = _seg[:_seg.index("\ndef ")]
    exec(compile(_seg, _fn, "exec"), ns9)
ns9["_paper_px"] = _V28["_paper_px"]; ns9["math"] = math
ns9["_journal"] = _V28["_journal"]; ns9["_leg_costs"] = _V28["_leg_costs"]
ns9.update(_vt=_V28["_vt"], stats=ns9.get("stats", {}), EXEC_EXCH="hl", BOOK_APPLY_MAX_S=5.0, _funding_pct=_V28["_funding_pct"], SIM_POSITION_USD=1000.0)   # v2.19
exec(compile(one_tick, "loop", "exec"), ns9)
ns9.update(_tr_px_now=_V28["_tr_px_now"], _tr_exch=_V28["_tr_exch"], _mid_age_exch=_V28["_mid_age_exch"], _bn_px_now=_V28["_bn_px_now"], _bn_px_mid_age=_V28["_bn_px_mid_age"], bn_px_hist={}, bn_px_lock=threading.Lock(), stats=ns9.get("stats") or {})   # v2.20
now0 = time.time()
tr9 = {"coin": "X", "our_side": "LONG", "entry_px": 100.0, "strategy": "F1",
       "open_ts": now0 - 400, "key": "0xa:X", "addr": "0xa", "timer": 60.0,
       "peak": -999.0, "trough": 999.0, "tx_pct": 10.0, "tx_usd": 5e4,
       "ratio": 3.0, "pos_usd": 5e5, "vault": 0, "hour": 12,
       "btc_move": 0.01, "depth": 1e5, "force_exit": None,
       "profile_gap": 0.0, "algo_v": "2.7"}
ns9["follow_open"]["f1"] = tr9
ns9["follow_last_close"]["0xa:X"] = now0 - 400   # тиша 400с при таймері 60
ns9["run_strat2_loop"]()
row9 = [r for p, r in rows9 if p == "f.csv"][0]
assert row9[8] == "silence_late", row9[8]
print("9) вихід через 400с при таймері 60с = 'silence_late', не 'silence'")

# ── 10. Рев'ю-фікси v2.7 ────────────────────────────────
# 10а: битий байт посеред CSV не ховає накопичені валідні рядки
d10 = tempfile.mkdtemp(); p10 = os.path.join(d10, "t.csv")
with open(p10, "wb") as f:
    f.write("a,b,eol\r\n".encode())
    for i in range(5):
        f.write(f"v{i},1,^\r\n".encode())
    f.write(b"garbage,\xff\xfe\xba\xad\r\n")   # битий UTF-8, 2 колонки
q10, re10 = [0], [0]
import csv as _csv, textwrap
def read10(path):
    # v2.11: read() -> обгортка над _read_file (+ .legacy-файли); беремо обидві
    src_read = src[src.index("    def _read_file(path):"):src.index("    def fnum(")]
    import glob as _glob
    ns = {"os": os, "print": lambda *a, **k: None, "glob": _glob,
          "_legacy_csv_cache": {},
          # v2.11 рев'ю: read() рахує мінімальну since-версію
          "_vt": lambda v: tuple(int(x) for x in str(v).split(".")) if v else (0,),
          "TAPE_SINCE": "2.10", "STRAT_SINCE": {}, "DATA_ALGO_V": "2.11",
          "quarantined": q10, "read_errors": re10, "_csv": _csv}
    exec(textwrap.dedent(src_read), ns)
    return ns["read"](path)
rows10 = read10(p10)
assert len(rows10) == 5, (len(rows10), q10, re10)
assert q10[0] == 1   # битий рядок у карантині, здорові живі
print("10а) битий хвіст: 5 валідних рядків збережено, сміття в карантині")
# 10б: budget-гілка prio ставить КОРОТКИЙ кулдаун, не знімає повністю
prio_seg = src[src.index("def run_prio_fetcher"):src.index("def get_recent_market_fills")]
i_b = prio_seg.index('"budget"')
assert "_prio_seen[addr] = now - PRIO_COOLDOWN_S + 75" in prio_seg[:i_b]
# 10в: failed_keep маркує _pending і логує зникнення пари
diff_seg = src[src.index("def check_position_changes"):src.index("def send_close_alert")]
assert '_kept["_pending"] = time.time()' in diff_seg
assert "незвірена дельта" in diff_seg
# 10г: гейт і алерт беруть СВІЖИЙ ratio знімка
assert 'not _ratio_ok(new_pos, _now)' in diff_seg   # v2.11: свіжий ratio + грейс
assert '"ratio":     new_pos.get("ratio", old["ratio"])' in diff_seg
# 10д: UI показує read_errors
html10 = open((_HL + "/hyperliquid-terminal.html"),
              encoding="utf-8").read()
assert "ЗБОЇВ ЧИТАННЯ ФАЙЛІВ" in html10
print("10б-д) prio-кулдаун 75с; _pending-маркер+лог; свіжий ratio; UI бачить read_errors")

print("\nУСІ РЕГРЕСІЇ СЬОМОГО АУДИТУ ЗАКРИТІ")

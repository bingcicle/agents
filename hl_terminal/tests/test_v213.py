import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.13 — відповіді на зовнішню критику v2.12 (34 сценарії):
1 вхід у відомо скасований твап; 2 ідентичність (reply-пост остаточний,
twapId по хвилинах/неоднозначність, дедуп каналів по dur/usd); 3 монета
вільна після годинного холду; 4 вихід при скасуванні + розділення вибірок;
5 час/ціна входу (late 15с, свіжий час, жива ціна, свічки не кешують
порожнє); 6 стан (битий state -> .corrupt+.bak; стара схема final_row);
7 ratio старту епізоду в алерті/rev, швидкість з відкритими входами й
активацією; 8 перший слайс, вага повторів скану, float 1%."""
import ast, threading, time, json, os, re, sys, math, tempfile
import calendar, datetime as _dtmod, html as _htmlmod, glob, hashlib, shutil
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from v28_shim import NEW, _median

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)
HERE = os.path.dirname(os.path.abspath(__file__))

def load(names, extra=None, assigns=()):
    # рев'ю аудит-2: тіло тику винесено у _strat2_tick — тягнемо разом із циклом
    if "run_strat2_loop" in names: names = set(names) | {"_strat2_tick"}
    body = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in assigns
                        for t in n.targets))]
    mod = ast.Module(body=body, type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "hashlib": hashlib, "shutil": shutil,
          "print": lambda *a, **k: None}
    ns.update(NEW)
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

def const(name):
    m = re.search(rf"^{name}\s*=\s*([^#\n]+)", src, re.M)
    return eval(m.group(1).strip())

def _dt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else ""

C = dict(PROFILE_ALGO_V=9, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.15",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True,
         F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0, F5_NAME="F5_перший",
         F5_FIRST_SHOT_S=3600.0, F7_NAME="F7_без_ратіо", RATIO_GRACE_S=1800,
         FC_ENABLED=True, FC_MAX_EPISODE_S=300)
assert const("DATA_ALGO_V") == "2.20" and const("TWAP_LATE_S") == 15.0
assert const("TWAP_FIRST_SLICE_S") == 25.0 and const("TWAP_VERIFY_S") == 60

# ── фейкова біржа (формат = api.hyperliquid.xyz, перевірено живими запитами) ──
EX = {"hist": {}, "slices": {}, "candles": {}, "calls": []}
def _fake_prio(body):
    t = body.get("type"); EX["calls"].append(t)
    if t == "twapHistory":
        return list(EX["hist"].get(body["user"], []))
    if t == "userTwapSliceFills":
        return list(EX["slices"].get(body["user"], []))
    if t == "candleSnapshot":
        req = body["req"]; px = EX["candles"].get(req["coin"], {}).get(req["startTime"])
        return ([{"t": req["startTime"], "T": req["startTime"] + 59_999, "c": str(px)}]
                if px else [])
    return {}
def ex_twap(addr, coin, side, start, minutes, sz, twap_id, status="activated",
            executed=0.0, sp=0.0, px=100.0, slice_at=1.5, with_slice=True, t_rev=None):
    # time — час ревізії статусу у СЕКУНДАХ (як у живій відповіді біржі),
    # state.timestamp — старт у мс
    EX["hist"].setdefault(addr, []).append(
        {"time": int(t_rev if t_rev is not None else start), "twapId": twap_id,
         "state": {"coin": coin, "user": addr, "side": "B" if side == "buy" else "A",
                   "sz": str(sz), "executedSz": str(executed), "minutes": minutes,
                   "reduceOnly": False, "timestamp": int(start * 1000)},
         "status": {"status": status}})
    if with_slice:
        f = {"coin": coin, "side": "B" if side == "buy" else "A",
             "time": int((start + slice_at) * 1000), "px": str(px), "sz": "1", "tid": 1, "dir": "x"}
        if sp is not None:
            f["startPosition"] = str(sp)
        EX["slices"].setdefault(addr, []).append({"twapId": twap_id, "fill": f})
def mk_tw():
    ns = load({"twap_parse", "_twap_usd", "_twap_hl_dt", "_iso_ts", "_tme_parse", "_tme_text",
               "_coin_canon", "_twap_find", "_twap_register", "_twap_sig_write", "_twap_drop",
               "_twap_enter", "_twap_tick", "_twap_ingest", "_twap_row", "_px_at",
               "_twap_resolve_kind", "_twap_verify", "_twap_api", "_twap_candle_close",
               "_twap_by_post", "_twap_apply_exchange", "_twap_dedup_by_id",
               "_twap_cohort_name", "_twap_trackers", "_twap_sync_trackers",
               "_twap_cancel_after_entry", "_twap_find_all", "_twap_close_min",
               "_twap_trade_closed", "_twap_fnum", "_twap_exit", "_twap_curve_row",
               "_twap_chain_head"},
              dict(C, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
                   twap_stats={"posts": 0, "starts": 0, "eligible": 0, "cancelled": 0,
                               "entered": 0, "dropped": 0, "fetch_err": 0, "parse_err": 0},
                   TWAP_MAX_DUR_S=900, TWAP_MIN_MOVE=1.0, TWAP_ENTRY_LEAD=60.0,
                   TWAP_LATE_S=15.0, TWAP_BACKLOG_S=3600, TWAP_HOLD_MIN=60,
                   TWAP_TRACK_MIN=120, TWAP_SIG_CSV="tws.csv", TWAP_CSV="tw.csv",
                   _strat_csv_append=lambda p, h, r: True, _dt=_dt,
                   strat2_lock=threading.RLock(), rev_open={},
                   hl_post_prio=lambda b, retries=2, direct=False, **kw: _fake_prio(b),
                   _hl_post_prio_direct=lambda b, retries=2, **kw: _fake_prio(b),
                   _prio_opener=object(), _twap_api_cache={},
                   _twap_candle_miss={}, TWAP_CANDLE_TRIES=3,
                   _px_now=lambda c, max_age=20: 100.0,
                   _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
                   _sim_slip=lambda d: 0.0005, save_state=lambda: None,
                   px_lock=threading.Lock(), px_hist={}, px_min={}),
              assigns=("_HL_START", "_HL_USER", "_HL_USER_PFX", "_HL_PERIOD", "_HL_ETA",
                       "_HL_PRICE", "_HL_CLOSED", "_X_START", "_X_ADDR", "_X_PRICE",
                       "_X_CREATED", "_X_SIZE", "_TWAP_MONTHS", "TWAP_SIG_HEADERS",
                       "TWAP_HEADERS", "_X_TWAPID", "_X_EXEC", "_X_STATUS",
                       "_HL_FILLED", "_HL_TIME", "TWAP_CURVE_HEADERS"))
    ns["_px_at"] = lambda c, ts, tol=90.0: 100.0
    return ns
def ex_reset(TW):
    EX["hist"].clear(); EX["slices"].clear(); EX["candles"].clear(); EX["calls"].clear()
    TW["_twap_api_cache"].clear(); TW["_twap_candle_miss"].clear()
A = "0x" + "a" * 40
def reg(TW, addr, start, dur=600.0, side="sell", coin="HYPE", ch="TWAPx", pid=1, usd=165_570.0):
    p = {"kind": "start", "addr": addr, "coin": coin, "side": side, "usd": usd,
         "start": start, "end": start + dur, "dur": dur, "px_msg": 82.78, "exact": True}
    return TW["_twap_register"](ch, pid, p, start - 30)[0]

# ═══ 1. Відомо скасований твап — входу НЕМАЄ ═══════════════════════
TW = mk_tw(); now0 = time.time()
r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 701, status="terminated", sp=0.0)
TW["_twap_resolve_kind"](r)                       # як при читанні поста
assert r["exch_cancelled"] == 1 and r["state"] == "dropped" and r["reason"] == "cancelled"
# і навіть якщо статус лише у допоміжних полях (без apply) — тик застосує
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 701, sp=0.0)
TW["_twap_verify"](r, now0 + 40)                  # вид відомий, стан activated
r["exch_cancelled"] = 1                           # «дізнались» про скасування
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "dropped" and r["reason"] == "cancelled" and not TW["rev_open"]
# скасування, що прийшло між звіркою і входом: остання перевірка перед входом
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 702, sp=0.0)
TW["_twap_tick"](now0 + 100)                       # вид відомий
assert r["kind"] == "open" and r["state"] == "watch"
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 702, status="terminated", sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0        # рух є — без останньої перевірки увійшли б
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "dropped" and r["reason"] == "cancelled" and not TW["rev_open"]
print("1) відомо скасований твап: вхід неможливий (ingest, тик, перед входом)")

# ═══ 2. Ідентичність заявок і скасувань ════════════════════════════
# (а) повідомлення-відповідь на старт A гасить A; повторне читання того
# самого поста НЕ гасить B (раніше — евристичний фолбек по монеті/сумі)
TW = mk_tw()
_hl = lambda usd, t0: ("$%s selling HYPE 🟥 Frequency: $1 every 60 seconds (10 cycles) ETA: 9m "
                       "Price: $85 User: " + A + " Period: "
                       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0)) + " - "
                       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0 + 600)) + " UTC+0") % usd
TW["_twap_ingest"]("HL_TWAP", 101, now0, _hl("5.10M", now0 + 100), now0)
TW["_twap_ingest"]("HL_TWAP", 102, now0, _hl("5.11M", now0 + 400), now0)
regs = TW["twap_reg"]
ra = [x for x in regs.values() if abs(x["start"] - (now0 + 100)) < 1][0]
rb = [x for x in regs.values() if abs(x["start"] - (now0 + 400)) < 1][0]
assert ra is not rb and ra["state"] == rb["state"] == "watch"
cancel = "⛔️ $5.10m TWAP with HYPE closed by user 0.00%\nFilled: 0/1 HYPE\nTime: x"
for _ in range(3):   # перше читання + два перечитування (кожні 30с)
    TW["_twap_ingest"]("HL_TWAP", 103, now0 + 10, cancel, now0 + 10, "", seen=(_ > 0), reply_pid=101)
assert ra["state"] == "dropped" and rb["state"] == "watch", (ra["state"], rb["state"])
assert TW["twap_stats"]["cancelled"] == 1
# HL-edit старту (той самий pid) — теж остаточно: дропнутий A не «переходить» на B
for _ in range(2):
    TW["_twap_ingest"]("HL_TWAP", 101, now0, "⛔️ TWAP is cancelled [0/1] " + _hl("5.10M", now0 + 100),
                       now0 + 20, seen=True)
assert rb["state"] == "watch" and TW["twap_stats"]["cancelled"] == 1
# (б) біржа: A (10 хв, 1000) і B (15 хв, 10000, terminated) за 8с — A НЕ дістає id B
TW = mk_tw(); rA = reg(TW, A, now0 + 30, dur=600.0)
ex_reset(TW)
ex_twap(A, "HYPE", "sell", rA["start"], 10, 1000, 801, sp=0.0)
ex_twap(A, "HYPE", "sell", rA["start"] + 8, 15, 10000, 802, status="terminated", sp=0.0)
assert TW["_twap_verify"](rA, now0 + 40) and rA["twap_id"] == 801 and not rA["exch_cancelled"]
# два twapId з однаковою тривалістю однаково близько (2с) — не вгадуємо
TW = mk_tw(); rC = reg(TW, A, now0 + 30, dur=600.0)
ex_reset(TW)
ex_twap(A, "HYPE", "sell", rC["start"] - 1, 10, 1000, 803, sp=0.0)
ex_twap(A, "HYPE", "sell", rC["start"] + 1, 10, 1000, 804, sp=0.0)
assert not TW["_twap_verify"](rC, now0 + 40) and rC["twap_id"] is None and rC["exch"] == "ambiguous"
# (в) різні канали: $100k/10 хв і $1M/15 хв за 60с — ДВА записи; той самий
# твап (та ж сума/тривалість) з іншого каналу — один
TW = mk_tw()
r1 = reg(TW, A, now0 + 100, dur=600.0, ch="TWAPx", pid=1, usd=100_000.0)
r2 = reg(TW, A, now0 + 160, dur=900.0, ch="HL_TWAP", pid=2, usd=1_000_000.0)
assert r1 is not r2 and len(TW["twap_reg"]) == 2
r3 = reg(TW, A, now0 + 103, dur=600.0, ch="HL_TWAP", pid=3, usd=104_000.0)
assert r3 is r1 and r1["src"] == "TWAPx+HL_TWAP" and len(TW["twap_reg"]) == 2
# 30-хв непридатний не «поглинає» придатний 10-хв
TW = mk_tw()
rl = reg(TW, A, now0 + 100, dur=1800.0, ch="TWAPx", pid=1, usd=100_000.0)
rs = reg(TW, A, now0 + 130, dur=600.0, ch="HL_TWAP", pid=2, usd=100_000.0)
assert rl["state"] == "ineligible" and rs is not rl and rs["state"] == "watch"
print("2) ідентичність: reply-пост остаточний, twapId по хвилинах, неоднозначність, дедуп каналів по dur/usd")

# ═══ 3. Монета вільна після годинного холду ═════════════════════════
_tmod = time
class FakeTime:
    """Керований годинник: entry_ts береться з time.time() (№5b), тому
    симульований час має жити і в параметрі тику, і в time.time()."""
    def __init__(self): self.now = _tmod.time()
    def time(self): return self.now
    sleep = staticmethod(_tmod.sleep); localtime = staticmethod(_tmod.localtime)
    strftime = staticmethod(_tmod.strftime); gmtime = staticmethod(_tmod.gmtime)
    mktime = staticmethod(_tmod.mktime); strptime = staticmethod(_tmod.strptime)
TW = mk_tw(); T = FakeTime(); TW["time"] = T
r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 901, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
T.now = r["end"] - 50; TW["_twap_tick"](T.now)
assert r["state"] == "entered" and abs(r["entry_ts"] - T.now) < 1e-6
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
tr["samples"] = [0.1] * 61                        # минула 61 хвилина, трекер іде до 120
B = "0x" + "b" * 40
r3 = reg(TW, B, tr["entry_ts"] + 61 * 60 + 30, pid=6)
ex_reset(TW); ex_twap(B, "HYPE", "sell", r3["start"], 10, 2000, 903, sp=0.0)
T.now = r3["end"] - 50; TW["_twap_tick"](T.now)
assert r3["state"] == "entered", r3["reason"]      # монета вільна після 60 хв холду
assert not tr.get("done")                          # старий трекер далі спостерігає до 120
# а всередині холду — зайнята
r4 = reg(TW, B, r3["entry_ts"] + 600, pid=7)
ex_reset(TW); ex_twap(B, "HYPE", "sell", r4["start"], 10, 2000, 904, sp=0.0)
T.now = r4["end"] - 50; TW["_twap_tick"](T.now)
assert r4["state"] == "dropped" and r4["reason"] == "busy"
print("3) монета звільняється після 60-хв холду, 120-хв спостереження її не тримає")

# ═══ 4. Вихід при скасуванні; розділення вибірок ═══════════════════
TW = mk_tw(); r = reg(TW, A, now0 + 30)
T = FakeTime(); TW["time"] = T; T.now = r["end"] - 50   # entry_ts узгоджений з тиком (час ревізії біржі = старт, ДО входу)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1001, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](T.now)
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
tr["samples"] = [0.3, 0.4, 0.5]                    # 3 хвилини після входу
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1001, status="terminated", sp=0.0)
TW["_twap_tick"](r["end"] + 90)                    # підтвердження після кінця
assert r["cancel_after_entry"] == 1 and tr["twap"]["exit_min"] == 4 and tr["twap"]["exit_reason"] == "cancelled"
tr["samples"] += [0.6, 0.7] + [1.0] * 115
row = TW["_twap_row"](tr); h = TW["TWAP_HEADERS"]
assert row[h.index("exit_min")] == 4 and row[h.index("exit_reason")] == "cancelled"
assert abs(row[h.index("net60_pct")] - (0.6 - row[h.index("costs_pct")])) < 1e-9   # net = m4, не m60
assert row[h.index("exch_status")] == "terminated"
# finished, але виконано 70% -> completed=0, exch_status finished, exec_pct 70
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1002, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "entered", (r["state"], r["reason"], r["kind"], r["kind_src"], r["move"], list(TW["rev_open"]))
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1002, status="finished", executed=1400, sp=0.0)
TW["_twap_tick"](r["end"] + 90)
assert r["completed"] == 0 and tr["twap"]["exch_status"] == "finished" and tr["twap"]["exec_pct"] == 70.0
# без відповіді біржі 5 хв -> unconfirmed
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1003, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] - 50)
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
ex_reset(TW)
TW["_twap_tick"](r["end"] + 400)
assert r["confirm_done"] == 1 and tr["twap"]["exch_status"] == "unconfirmed" and tr["twap"]["completed"] == 0
# API: головна вибірка — всі 3 входи; підтверджені — лише 1; лічильники окремо
d4 = tempfile.mkdtemp()
def tw_row(strat, net, completed, cancel, exch, p0s="candle", p1s="candle", tid="t"):
    r_ = {k: "" for k in h}
    r_.update({"twap_id": tid, "strategy": strat, "date_entry": "2026-09-01 10:00:00",
               "coin": "HYPE", "our_side": "LONG", "net60_pct": str(net), "costs_pct": "0.15",
               "completed": str(completed), "cancel_after_entry": str(cancel),
               "exch_status": exch, "p0_src": p0s, "p1_src": p1s, "algo_v": "2.15", "eol": "^"})
    return ",".join(r_[k] for k in h)
open(os.path.join(d4, "twap_trades.csv"), "w").write(",".join(h) + "\n"
    + tw_row("T1_твап_відкриття", 1.0, 1, 0, "finished", tid="a") + "\n"
    + tw_row("T1_твап_відкриття", -2.0, 0, 1, "terminated", tid="b") + "\n"
    + tw_row("T1_твап_відкриття", 3.0, 0, 0, "finished", p0s="hist", p1s="live", tid="c") + "\n"
    + tw_row("T1_твап_відкриття_20", 5.0, 1, 0, "finished", tid="a") + "\n")
_ss = load(set(), {}, assigns=("STRAT_SINCE", "TAPE_SINCE"))
api = load({"strat2_api", "_median", "_vt", "_v_ok", "_twap_cohort_name"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
    STRAT_SINCE=_ss["STRAT_SINCE"], TAPE_SINCE=_ss["TAPE_SINCE"],
    _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
    FOLLOW_OUT_CSV=os.path.join(d4, "fo.csv"), REV_CSV=os.path.join(d4, "rev.csv"),
    REV_SIG_CSV=os.path.join(d4, "sig.csv"), FOLLOW_CSV=os.path.join(d4, "follow.csv"),
    REV_OUT_CSV=os.path.join(d4, "out.csv"), TWAP_CSV=os.path.join(d4, "twap_trades.csv"),
    TWAP_SIG_CSV=os.path.join(d4, "tws.csv"), TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
    TWAP_COHORTS=(1.0, 1.5, 2.0), F8_NAME="F8_ratio35", F9_NAME="F9_без_ратіо_90",
    T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення",
    R7_NAME="R7_одним", strat_activated={}))
o = api["strat2_api"]()["strategies"]["T1_твап_відкриття"]
assert o["n"] == 3 and o["entered"] == 3                       # усі фактичні входи
assert o["n_confirmed"] == 1 and o["median_confirmed"] == 1.0   # лише підтверджені
assert o["n_cancelled"] == 1 and o["n_partial"] == 1 and o["n_unconfirmed"] == 0
assert o["n_candle"] == 2
assert o["cohorts"]["2.0"]["n"] == 1 and o["cohorts"]["2.0"]["median"] == 5.0   # окрема стратегія
assert o["cohorts"]["1.5"]["n"] == 0 and o["cohort_names"]["2.0"] == "T1_твап_відкриття_20"
print("4) скасування = вихід хвилиною скасування; частково/непідтверджено; вибірки розділені")

# ═══ 5. Час і ціна входу ═══════════════════════════════════════════
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1101, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] + 20)                    # 20с ПІСЛЯ кінця — вже пізно (було 90с)
assert r["state"] == "dropped" and r["reason"] == "late"
# немає живої ціни — входу немає (раніше брали стару свічку P1)
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1102, sp=0.0)
bar1 = (int(r["end"] - 60) // 60 - 1) * 60 * 1000
bar0 = (int(r["start"]) // 60 - 1) * 60 * 1000
EX["candles"]["HYPE"] = {bar0: 100.0, bar1: 97.0}
TW["_px_now"] = lambda c, max_age=20: None
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "dropped" and r["reason"] == "no_price" and r["p1"] == 97.0
# свіжий час перед входом: entry_ts — фактичний виклик, а не now тику
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1103, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
_real_time = time.time
TW["time"] = type("T", (), {"time": staticmethod(lambda: r["end"] + 100), "sleep": time.sleep,
                            "localtime": time.localtime, "strftime": time.strftime,
                            "gmtime": time.gmtime, "mktime": time.mktime, "strptime": time.strptime})()
TW["_twap_tick"](r["end"] - 50)                    # тик думає, що зараз end-50, а насправді end+100
assert r["state"] == "dropped" and r["reason"] == "late"
# порожня відповідь свічок НЕ кешується: наступний тик бачить свічку
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1104, sp=0.0)
TW["_twap_tick"](now0 + 100)                       # p0: свічки ще нема -> None
assert r["p0"] is None
bar0 = (int(r["start"]) // 60 - 1) * 60 * 1000
EX["candles"]["HYPE"] = {bar0: 101.0}
TW["_twap_tick"](now0 + 130)
assert r["p0"] == 101.0 and r["p0_src"] == "candle"
# ціна з поста більше не джерело P0
assert '"msg" if rec.get("px_msg")' not in src[src.index("def _twap_tick"):src.index("def _twap_ingest")]
print("5) вхід лише в останню хвилину (+15с), свіжий час, жива ціна обов'язкова, свічки без кешу порожнього")

# ═══ 6. Стан ══════════════════════════════════════════════════════
d6 = tempfile.mkdtemp(); sf = os.path.join(d6, "state.json")
def ld():
    ns = load({"load_state"}, {"STATE_FILE": sf, "STATE_MAX_AGE_S": 3600,
        "watchlist_lock": threading.Lock(), "watchlist": {}, "sim_lock": threading.Lock(),
        "sim_positions": {}, "sim_trackers": {}, "sim_closed": [], "alerts_lock": threading.Lock(),
        "recent_alerts": [], "sent_alerts": set(), "fill_cursor": {}, "close_episodes": {},
        "fc_lock": threading.Lock(), "fc_positions": {}, "fc_episodes": {},
        "strat2_lock": threading.RLock(), "rev_open": {}, "follow_open": {},
        "follow_last_close": {}, "vault_cache": {}, "twap_lock": threading.Lock(),
        "twap_reg": {}, "twap_last_ids": {}, "strat_activated": {}})
    ns["load_state"](); return ns
good = {"saved_at": time.time() - 60, "watchlist": {"0xw": {"AAA": {"size": 1.0, "val": 1e5, "side": "LONG", "ratio": 2.5}}},
        "rev_open": {"k": {"strategy": "R1_загальний"}}, "strat_activated": {"R1_загальний": {"since": "2.10", "ts": 5}}}
json.dump(good, open(sf + ".bak", "w"))
open(sf, "w").write("{not json")
ns = ld()
assert ns["watchlist"] and ns["rev_open"] and ns["strat_activated"]["R1_загальний"]["ts"] == 5   # з .bak
assert not os.path.exists(sf) and glob.glob(sf + ".corrupt-*")                                  # битий відкладено
# save: .bak оновлюється раз на 10 хв, snap несе strat_activated
_sv = src[src.index("def _save_state_locked"):src.index("def load_state")]
assert 'shutil.copy2(STATE_FILE, STATE_FILE + ".bak")' in _sv and 'snap["strat_activated"]' in _sv
# стара схема final_row (144 поля) при новій: перебудова, не відмова. v2.15:
# done-трекер без записаної угоди — спершу рядок угоди (twap_trades), потім
# крива (twap_curves), і лише тоді трекер видаляється (два тики)
hc = load(set(), {}, assigns=("TWAP_CURVE_HEADERS",))["TWAP_CURVE_HEADERS"]
writes = []
L = load({"run_strat2_loop"}, dict(C, strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    follow_last_close={}, _px_now=lambda c, max_age=30: None, _sim_slip=lambda d: 0.0005,
    _rev_out_row=lambda p: [], _fol_out_row=lambda p: [], _rev_row=lambda p, e: [],
    _twap_row=lambda p, now=None: ["new"] * (len(h) - 1),
    _twap_curve_row=lambda p: ["c"] * (len(hc) - 1),
    _strat_csv_append=lambda p, hh, r: (writes.append(p) or True) if len(r) == len(hh) - 1 else False,
    TWAP_HEADERS=h, TWAP_CURVE_HEADERS=hc, TWAP_CURVE_CSV="tc",
    REV_HEADERS=[], REV_OUT_HEADERS=[], FOLLOW_OUT_HEADERS=[], FOLLOW_HEADERS=[],
    REV_CSV="r", REV_OUT_CSV="o", FOLLOW_OUT_CSV="fo", FOLLOW_CSV="f", TWAP_CSV="t",
    _sig_retry_lock=threading.Lock(), _sig_retry_q=[], REV_SIG_CSV="s", REV_SIG_HEADERS=[], stats={},
    save_state=lambda: None, _dt=_dt, time=type("T", (), {"time": staticmethod(time.time),
        # цикл спить ПЕРЕД обробкою: два тики, третій sleep зупиняє
        "sleep": staticmethod(lambda s, _c=[0]: (_c.__setitem__(0, _c[0] + 1),
                                                 (_ for _ in ()).throw(SystemExit()) if _c[0] > 2 else None)),
        "localtime": time.localtime})()))
L["rev_open"]["tw|T1"] = {"strategy": "T1_твап_відкриття", "done": 1, "row_kind": "twap",
                          "final_row": ["old"] * 144, "samples": [], "entry_ts": 1}
try: L["run_strat2_loop"]()
except SystemExit: pass
assert "tw|T1" not in L["rev_open"] and writes == ["t", "tc"], writes   # угода, потім крива; не дроп
print("6) битий state -> .corrupt + відновлення з .bak; стара схема final_row перебудовується")

# ═══ 7. Ratio старту епізоду; швидкість ════════════════════════════
_ws = src[src.index("def check_wallet_worker"):src.index("def check_position_changes")]
assert '"ratio":     (_ep_ratio if (_fc and _ep_ratio) else old["ratio"]),' in _ws
assert '"live_ratio": old["ratio"],' in _ws and 'ep_ratio = (_ep_a or {}).get("ratio") or 0' in src   # v2.19: у приймачі _ingest_txs
assert "Ratio{' на старті епізоду' if a.get('ratio_src') == 'episode' else ''}" in src
# rev: 50%+50% — рядок повного закриття несе ratio старту епізоду (2.5), не 1.25
def mk_rev(eps):
    return load({"rev_on_close", "_mark_ratio", "_ratio_ok", "_grace_val"}, dict(C,
        strat2_lock=threading.RLock(), rev_open={}, fc_lock=threading.Lock(), fc_episodes=eps,
        is_vault=lambda a: False, _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0), _sim_depth=lambda c, s: 1e5, _dt=_dt,
        _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
        REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[], _strat_csv_append=lambda p, hh, r: True))
rv = mk_rev({("0xa", "AAA"): {"ratio": 2.5, "val": 2e5, "side": "LONG", "first_ts": 1, "last_ts": 1,
                              "sum_usd": 0, "seen": set(), "start_size": 2000.0, "max_sz": 1000.0,
                              "max_usd": 1e5, "max_liq": 0}})
rv["rev_on_close"]("0xa", "AAA", {"size": 1000.0, "side": "LONG", "val": 1e5, "ratio": 1.25,
                                  "ratio_hi_ts": time.time() - 100, "big_val": 2e5},
                   [{"sz": 1000.0, "px": 100.0, "hash": "0x2", "dir": "Close Long", "ts": 2}], True)
assert all(p["ratio"] == 2.5 for p in rv["rev_open"].values()) and rv["rev_open"]
# швидкість: 1 закритий + 1 відкритий вхід за добу = 2/день; активація раніше
# за перший рядок — знаменник від активації
d7 = tempfile.mkdtemp()
fh = ["date_open", "date_close", "strategy", "coin", "our_side", "whale_addr", "entry_px", "exit_px",
      "exit_reason", "hold_s", "gross_pct", "costs_pct", "net_pct", "peak_pct", "trough_pct",
      "tx_pct_of_pos", "tx_usd", "ratio", "pos_usd", "vault", "hour", "btc_move_pct", "profile_gap_s",
      "algo_v", "trade_id", "first_shot", "pair_gap_s", "prof_n_fast", "prof_n_slow", "prof_fast_pct",
      "prof_unload_med_s", "prof_unload_mean_s", "prof_window_d", "eol"]
def frow(strat, tid, date):
    r_ = [""] * len(fh); r_[0] = date; r_[2] = strat; r_[3] = "AAA"; r_[4] = "LONG"; r_[5] = "0xw"
    r_[11] = "0.15"; r_[12] = "1.0"; r_[23] = "2.13"; r_[24] = tid; r_[-1] = "^"; return ",".join(r_)
open(os.path.join(d7, "follow.csv"), "w").write(",".join(fh) + "\n" + frow("F1_1хв", "t1", _dt(time.time() - 3600)) + "\n")
api7 = load({"strat2_api", "_median", "_vt", "_v_ok", "_twap_cohort_name"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={},
    follow_open={"f2": {"strategy": "F1_1хв", "coin": "AAA", "open_ts": time.time() - 100,
                        "algo_v": "2.14"}},   # v2.14: відкритий трекер рахується лише з версією ≥ since
    wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
    STRAT_SINCE=_ss["STRAT_SINCE"], TAPE_SINCE=_ss["TAPE_SINCE"],
    _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
    FOLLOW_OUT_CSV=os.path.join(d7, "fo.csv"), REV_CSV=os.path.join(d7, "rev.csv"),
    REV_SIG_CSV=os.path.join(d7, "sig.csv"), FOLLOW_CSV=os.path.join(d7, "follow.csv"),
    REV_OUT_CSV=os.path.join(d7, "out.csv"), TWAP_CSV=os.path.join(d7, "tw.csv"),
    TWAP_SIG_CSV=os.path.join(d7, "tws.csv"), TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
    TWAP_COHORTS=(1.0, 1.5, 2.0), F8_NAME="F8_ratio35", F9_NAME="F9_без_ратіо_90",
    T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення", R7_NAME="R7_одним",
    strat_activated={"F1_1хв": {"since": "2.10", "ts": time.time() - 3 * 86400}}))
f1 = api7["strat2_api"]()["strategies"]["F1_1хв"]
assert f1["open_now"] == 1 and abs(f1["days"] - 3.0) < 0.1 and abs(f1["per_day"] - round(2 / 3.0, 2)) < 0.02, f1
# без активації — від першого рядка (мін. 1 день): 2 входи / 1 день
api7["strat_activated"].clear(); api7["_strat2_cache"]["ts"] = 0
f1 = api7["strat2_api"]()["strategies"]["F1_1хв"]
assert f1["days"] == 1.0 and f1["per_day"] == 2.0
IA = load({"_init_strat_activation"}, {"strat_activated": {}, "STRAT_SINCE": {"X": "2.10", "Y": "2.13"}})
IA["_init_strat_activation"](1000.0)
assert IA["strat_activated"] == {"X": {"since": "2.10", "ts": 1000.0}, "Y": {"since": "2.13", "ts": 1000.0}}
IA["STRAT_SINCE"]["Y"] = "2.14"; IA["_init_strat_activation"](2000.0)
assert IA["strat_activated"]["X"]["ts"] == 1000.0 and IA["strat_activated"]["Y"] == {"since": "2.14", "ts": 2000.0}
assert "_init_strat_activation()" in src[src.index("def main():"):]
print("7) ratio старту епізоду в алерті та rev; швидкість: усі входи (з відкритими), від активації")

# ═══ 8. Перший слайс, вага повторів скану, float ═══════════════════
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1201, sp=0.0, slice_at=300.0)
TW["_twap_verify"](r, now0 + 400)
assert r["kind"] is None and r["kind_src"] == "unproven" and r["twap_id"] == 1201
TW["_twap_tick"](now0 + 400)
assert r["state"] == "dropped" and r["reason"] == "kind_unproven"
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1202, sp=None)   # без startPosition
TW["_twap_verify"](r, now0 + 40)
assert r["kind"] is None and r["kind_src"] == "unproven"
# запитів за твап: кілька, не десятки (кеш 10с; звірка лише доки вид невідомий,
# перед входом і після кінця раз на 60с)
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1203, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
t = now0 + 25
while t <= r["end"] + 400:
    TW["_twap_api_cache"].clear()                  # найгірший випадок: кеш не рятує
    TW["_twap_tick"](t); t += 30
n_hist = EX["calls"].count("twapHistory")
assert r["state"] == "entered" and n_hist <= 8, (n_hist, EX["calls"])
# скан: повторна спроба резервує вагу
_sc = src[src.index("def hl_post_scan"):src.index("def _scan_probe")]
assert "if attempt:\n            _scan_budget_wait(2)" in _sc
# рух рівно 1% у float
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1204, sp=0.0)
TW["_px_at"] = lambda c, ts, tol=90.0: 0.07
TW["_px_now"] = lambda c, max_age=20: 0.0693     # sell: 0.07*0.99 -> рівно -1% у float
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "entered" and abs(r["move"] - 1.0) < 1e-6, (r["state"], r["reason"], r["move"])
print("8) перший слайс доводиться (35с, startPosition), ~≤8 історій за твап, вага повторів, 1% у float")

# ═══ 9. Рев'ю дифу v2.13 ═══════════════════════════════════════════
# (а) перечитуваний пост скасування БЕЗ прив'язки (старт відредаговано у
# cancelled ще до першого читання) НЕ гасить наступний твап того ж кита
TW = mk_tw()
cancel = "⛔️ $5.10m TWAP with HYPE closed by user 0.00%\nFilled: 0/1 HYPE\nTime: x"
TW["_twap_ingest"]("HL_TWAP", 201, now0, cancel, now0)            # перше читання: нема кого гасити
assert TW["twap_stats"]["cancelled"] == 0 and not TW["twap_reg"]
TW["_twap_ingest"]("HL_TWAP", 202, now0, _hl("5.10M", now0 + 100), now0 + 5)   # новий твап B тієї ж суми
rb = list(TW["twap_reg"].values())[0]; assert rb["state"] == "watch"
for _ in range(3):                                                  # перечитування кожні 30с
    TW["_twap_ingest"]("HL_TWAP", 201, now0, cancel, now0 + 40, seen=True)
assert rb["state"] == "watch" and TW["twap_stats"]["cancelled"] == 0, rb["state"]
# евристика при ПЕРШОМУ читанні працює і робить пост точною прив'язкою
TW = mk_tw()
TW["_twap_ingest"]("HL_TWAP", 301, now0, _hl("5.10M", now0 + 100), now0)
ra = list(TW["twap_reg"].values())[0]
TW["_twap_ingest"]("HL_TWAP", 302, now0, cancel, now0 + 10)       # без reply — евристика, перше читання
assert ra["state"] == "dropped" and "HL_TWAP/302" in ra["posts"], ra
TW["_twap_ingest"]("HL_TWAP", 303, now0, _hl("5.10M", now0 + 130), now0 + 15)   # B: та ж сума, +30с
rb = [x for x in TW["twap_reg"].values() if x is not ra][0]
for _ in range(3):
    TW["_twap_ingest"]("HL_TWAP", 302, now0, cancel, now0 + 50, seen=True)
assert rb["state"] == "watch" and TW["twap_stats"]["cancelled"] == 1
assert "if rec is None and not seen:" in src
# (б) v2.15 (аудит v2.14 №2): paper-вихід — хвилина, коли бот ДІЗНАВСЯ про
# скасування (після рестарту з 30 порожніми семплами — 31-ша), а час
# самої події з біржі (8-ма хвилина) — окремо, cancel_event_min
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1301, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] - 50)
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
tr["samples"] = [""] * 30
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1301, status="terminated", sp=0.0,
                      t_rev=tr["entry_ts"] + 7 * 60 + 10)
TW["_twap_tick"](r["end"] + 90)
assert r["exch_ts"] == int(tr["entry_ts"] + 430), r.get("exch_ts")
assert tr["twap"]["exit_min"] == 31 and tr["twap"]["cancel_event_min"] == 8, tr["twap"]
# TG-скасування прийшло першим (хвилина 31), біржа потім дає час події —
# вихід НЕ рухається назад, лише cancel_event_min
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1302, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r["end"] - 50)
tr = TW["rev_open"][f"{r['id']}|T1_твап_відкриття"]
tr["samples"] = [""] * 30
r["cancelled"] = 1; TW["_twap_cancel_after_entry"](r)
assert tr["twap"]["exit_min"] == 31
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1302, status="terminated", sp=0.0,
                      t_rev=tr["entry_ts"] + 430)
TW["_twap_tick"](r["end"] + 90)
assert tr["twap"]["exit_min"] == 31 and tr["twap"]["exit_reason"] == "cancelled"
assert tr["twap"]["cancel_event_min"] == 8
# час ревізії ДО входу (фікстура сценарію 4: time=start) — фолбек на наступну хвилину (там exit_min 4)
# (в) монета без свічок: не більше TWAP_CANDLE_TRIES запитів за бар, не щотику
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1401, sp=0.0)
t = r["start"] + 5
while t < r["end"] - 70:
    TW["_twap_api_cache"].clear(); TW["_twap_tick"](t); t += 30
assert EX["calls"].count("candleSnapshot") == 3, EX["calls"].count("candleSnapshot")
# свічка, що з'явилась пізніше (у межах спроб), береться
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1402, sp=0.0)
TW["_twap_tick"](r["start"] + 5); TW["_twap_api_cache"].clear()
TW["_twap_tick"](r["start"] + 35); TW["_twap_api_cache"].clear()
assert r.get("p0") is None
bar0 = (int(r["start"]) // 60 - 1) * 60
EX["candles"]["HYPE"] = {bar0 * 1000: 101.5}
TW["_twap_tick"](r["start"] + 65)
assert r["p0"] == 101.5 and r["p0_src"] == "candle"
# (г) підпис «Ratio на старті епізоду» — лише коли ratio справді епізодний
assert '"ratio_src": ("episode" if (_fc and _ep_ratio) else "live")' in src
assert '"ratio_src": "live",' in src and "if a.get('ratio_src') == 'episode' else ''" in src
print("9) рев'ю v2.13: перечитаний пост без прив'язки не гасить новий твап; вихід за часом біржі; свічки ≤3 спроб; підпис ratio")

print("\nALL v2.13 TESTS PASSED")

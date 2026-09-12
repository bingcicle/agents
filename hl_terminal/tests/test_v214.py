import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.14 — відповіді на зовнішню критику v2.13 (38 сценаріїв):
1 незмінність завершених угод після аварії (звірка трекерів із CSV,
перший запис виграє, синхронний save); 2 ідентичність TWAP (TwapId з
поста, кандидати по id, неоднозначність без reply, дедуп каналів ±20с/
±30с, dropped-дублі по id); 3 completed лише за повним виконанням;
4 власні списки угод когорт; 5 угода ≠ спостереження (busy/open_now/
pending); 6 пропуск ціни на межі хвилини, timer_late, late_exit у rev;
7 вхід лише зі свіжою успішною звіркою; 8 стан (.bak без state, не-dict,
міграція, версія відкритих трекерів); 9 дрібне (неокруглений рух, доказ
першого слайсу, вага запитів, стеля очікування бюджету, догортання)."""
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
          "math": math, "hashlib": hashlib, "shutil": shutil, "glob": glob,
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
assert const("DATA_ALGO_V") == "2.20" and const("TWAP_FIRST_SLICE_S") == 25.0
assert const("TWAP_VERIFY_MAX_AGE_S") == 90.0 and const("TWAP_XCH_START_S") == 20.0
assert const("TWAP_XCH_DUR_S") == 30.0

# ── фейкова біржа (формат = api.hyperliquid.xyz) ──
EX = {"hist": {}, "slices": {}, "candles": {}, "calls": [], "fail": set()}
class _APIErr(Exception): pass
def _fake_prio(body):
    t = body.get("type"); EX["calls"].append(t)
    if t in EX["fail"]:
        raise _APIErr("boom")
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
TWFN = {"twap_parse", "_twap_usd", "_twap_hl_dt", "_iso_ts", "_tme_parse", "_tme_text",
        "_coin_canon", "_twap_find", "_twap_find_all", "_twap_register", "_twap_sig_write",
        "_twap_drop", "_twap_enter", "_twap_tick", "_twap_ingest", "_twap_row", "_px_at",
        "_twap_resolve_kind", "_twap_verify", "_twap_api", "_twap_candle_close",
        "_twap_by_post", "_twap_apply_exchange", "_twap_dedup_by_id", "_twap_cohort_name",
        "_twap_trackers", "_twap_sync_trackers", "_twap_cancel_after_entry",
        "_twap_close_min", "_twap_trade_closed", "_twap_fnum", "_tme_fetch",
        "_twap_exit", "_twap_curve_row", "_twap_chain_head"}
TWASG = ("_HL_START", "_HL_USER", "_HL_USER_PFX", "_HL_PERIOD", "_HL_ETA", "_HL_PRICE",
         "_HL_CLOSED", "_X_START", "_X_ADDR", "_X_PRICE", "_X_CREATED", "_X_SIZE",
         "_TWAP_MONTHS", "TWAP_SIG_HEADERS", "TWAP_HEADERS", "_X_TWAPID", "_X_EXEC",
         "_X_STATUS", "_HL_FILLED", "_HL_TIME", "TWAP_CURVE_HEADERS")
def mk_tw():
    ns = load(TWFN, dict(C, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
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
                   px_lock=threading.Lock(), px_hist={}, px_min={},
                   _twap_ch_state={"HL_TWAP": {"ok_ts": 0.0, "err": 0}, "TWAPx": {"ok_ts": 0.0, "err": 0}}),
              assigns=TWASG)
    ns["_px_at"] = lambda c, ts, tol=90.0: 100.0
    return ns
def ex_reset(TW):
    EX["hist"].clear(); EX["slices"].clear(); EX["candles"].clear(); EX["calls"].clear()
    EX["fail"].clear()
    TW["_twap_api_cache"].clear(); TW["_twap_candle_miss"].clear()
A = "0x" + "a" * 40
B = "0x" + "b" * 40
def reg(TW, addr, start, dur=600.0, side="sell", coin="HYPE", ch="TWAPx", pid=1, usd=165_570.0):
    p = {"kind": "start", "addr": addr, "coin": coin, "side": side, "usd": usd,
         "start": start, "end": start + dur, "dur": dur, "px_msg": 82.78, "exact": True}
    return TW["_twap_register"](ch, pid, p, start - 30)[0]
_tmod = time
class FakeTime:
    def __init__(self): self.now = _tmod.time()
    def time(self): return self.now
    sleep = staticmethod(_tmod.sleep); localtime = staticmethod(_tmod.localtime)
    strftime = staticmethod(_tmod.strftime); gmtime = staticmethod(_tmod.gmtime)
    mktime = staticmethod(_tmod.mktime); strptime = staticmethod(_tmod.strptime)
now0 = time.time()
T1 = "T1_твап_відкриття"

# ═══ 1. Незмінність завершених угод після аварії ═══════════════════
d1 = tempfile.mkdtemp()
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120),
          assigns=("FOLLOW_HEADERS", "REV_HEADERS", "TWAP_HEADERS", "REV_OUT_HEADERS",
                   "FOLLOW_OUT_HEADERS", "TWAP_CURVE_HEADERS"))
fh, rh, th = FH["FOLLOW_HEADERS"], FH["REV_HEADERS"], FH["TWAP_HEADERS"]
def csvrow(h, **kv):
    r_ = {k: "" for k in h}; r_.update(kv); r_["eol"] = "^"
    return ",".join(r_[k] for k in h)
open(os.path.join(d1, "follow.csv"), "w").write(",".join(fh) + "\n"
    + csvrow(fh, strategy="F1_1хв", net_pct="-1.15", algo_v="2.14", trade_id="f1", date_open="2026-09-08 10:00:00") + "\n"
    + csvrow(fh, strategy="F1_1хв", net_pct="0.5", algo_v="2.14", trade_id="fX", date_open="2026-09-08 10:00:00")[:-8] + "\n")  # огризок без вартового
open(os.path.join(d1, "rev.csv"), "w").write(",".join(rh) + "\n"
    + csvrow(rh, sig_id="s1", strategy="R1_загальний", algo_v="2.14", entered="1", m30="1.0") + "\n")
open(os.path.join(d1, "tw.csv"), "w").write(",".join(th) + "\n"
    + csvrow(th, twap_id="tw1", strategy=T1, algo_v="2.14", net60_pct="2.0") + "\n")
# legacy-ротація теж рахується
open(os.path.join(d1, "follow.csv.legacy-1700000000.csv"), "w").write(",".join(fh) + "\n"
    + csvrow(fh, strategy="F1_1хв", net_pct="3.0", algo_v="2.13", trade_id="fL") + "\n")
ST = dict(C, STATE_FILE=os.path.join(d1, "state.json"), STATE_MAX_AGE_S=3600,
          REV_CSV=os.path.join(d1, "rev.csv"), FOLLOW_CSV=os.path.join(d1, "follow.csv"),
          TWAP_CSV=os.path.join(d1, "tw.csv"), REV_OUT_CSV=os.path.join(d1, "out.csv"),
          FOLLOW_OUT_CSV=os.path.join(d1, "fo.csv"))
keys = load({"_csv_written_keys", "_tracker_csv_key"}, ST)
K = keys["_csv_written_keys"]()
assert ("fol", "f1") in K and ("fol", "fL") in K and ("fol", "fX") not in K   # огризок — не доказ
assert ("rev", "s1", "R1_загальний") in K and ("twap", "tw1", T1) in K
tk = keys["_tracker_csv_key"]
assert tk("fol", "f1", {}) == ("fol", "f1")
assert tk("rev", "k", {"sig_id": "s1", "strategy": "R1_загальний"}) == ("rev", "s1", "R1_загальний")
assert tk("rev", "k", {"sig_id": "tw1", "strategy": T1, "row_kind": "twap"}) == ("twap", "tw1", T1)
assert tk("rev", "k", {"sig_id": "o1", "strategy": "_OUTCOME"}) == ("out", "o1")
assert tk("rev", "k", {"sig_id": "o2", "strategy": "_FOLLOW_OUT"}) == ("fo", "o2")
# load_state: відновлений трекер, чий рядок уже у CSV, — НЕ відновлюється
def mk_state_ns(extra=None):
    return load({"load_state", "_csv_written_keys", "_tracker_csv_key"}, dict(ST,
        watchlist_lock=threading.Lock(), watchlist={}, sim_lock=threading.Lock(),
        sim_positions={}, sim_trackers={}, sim_closed=[], alerts_lock=threading.Lock(),
        recent_alerts=[], sent_alerts=set(), fill_cursor={}, close_episodes={},
        fc_lock=threading.Lock(), fc_positions={}, fc_episodes={},
        strat2_lock=threading.RLock(), rev_open={}, follow_open={}, follow_last_close={},
        vault_cache={}, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
        strat_activated={}, **(extra or {})))
snap = {"saved_at": time.time() - 30,
        "follow_open": {"f1": {"strategy": "F1_1хв", "coin": "AAA", "entry_px": 100.0, "our_side": "SHORT"},
                        "f2": {"strategy": "F1_1хв", "coin": "BBB", "entry_px": 100.0, "our_side": "SHORT"},
                        "fX": {"strategy": "F1_1хв", "coin": "CCC"}},
        "rev_open": {"s1": {"sig_id": "s1", "strategy": "R1_загальний", "coin": "AAA", "state": "open"},
                     "s2": {"sig_id": "s2", "strategy": "R1_загальний", "coin": "AAA", "state": "open"},
                     "tw1|" + T1: {"sig_id": "tw1", "strategy": T1, "row_kind": "twap", "coin": "HYPE"}}}
json.dump(snap, open(ST["STATE_FILE"], "w"))
L = mk_state_ns(); L["load_state"]()
assert set(L["follow_open"]) == {"f2", "fX"}, set(L["follow_open"])   # f1 уже у CSV; fX — огризок, не доказ
# v2.15: TWAP-трекер із записаною угодою відновлюється ЛИШЕ для кривої (рядок угоди не переписується)
assert set(L["rev_open"]) == {"s2", "tw1|" + T1}, set(L["rev_open"])
assert L["rev_open"]["tw1|" + T1]["trade_written"] == 1 and L["rev_open"]["tw1|" + T1]["trade_row"] is None
# …а з записаною кривою — не відновлюється взагалі
open(os.path.join(d1, "tc.csv"), "w").write(",".join(FH["TWAP_CURVE_HEADERS"]) + "\n"
    + csvrow(FH["TWAP_CURVE_HEADERS"], twap_id="tw1", strategy=T1, algo_v="2.15") + "\n")
L = mk_state_ns({"TWAP_CURVE_CSV": os.path.join(d1, "tc.csv")}); L["load_state"]()
assert "tw1|" + T1 not in L["rev_open"]
# dedup: перший валідний запис виграє (джерело) і синхронний save після запису
assert "if k in best:\n                dups += 1\n                continue" in src
assert "        save_state()\n\n\n# ── API для вкладки" in src   # тіло тику у _strat2_tick (рев'ю аудит-2)
assert "threading.Thread(target=save_state, daemon=True).start()\n\n# ── API" not in src
print("1) аварія між CSV і state: трекер уже у CSV не відновлюється; огризок — не доказ; перший запис виграє; save синхронний")

# ═══ 2. Ідентичність TWAP ══════════════════════════════════════════
# (а) TWAPx: скасування з TwapId 111 при двох активних заявках — гасить САМЕ 111
TW = mk_tw()
rA = reg(TW, A, now0 + 30, usd=100_000.0, pid=1)
rB = reg(TW, A, now0 + 60, usd=101_000.0, pid=2)
ex_reset(TW)
ex_twap(A, "HYPE", "sell", rA["start"], 10, 1000, 111, sp=0.0)
ex_twap(A, "HYPE", "sell", rB["start"], 10, 1010, 222, sp=0.0)
TW["_twap_verify"](rA, now0 + 40); TW["_twap_verify"](rB, now0 + 70)
assert rA["twap_id"] == 111 and rB["twap_id"] == 222
xc = ("❌ TWAP отменён (частично) Статус: terminated Исполнено: 3.28%% Размер: 32.8 / 1000 HYPE "
      "TwapId: %d Субъект: " + A + " Цена в начале: $83.83 Цена в конце: $83.69 🔴 (-0.16%%)")
TW["_twap_ingest"]("TWAPx", 501, now0 + 100, xc % 111, now0 + 100)
assert rA["state"] == "dropped" and rA["reason"] == "cancelled" and rB["state"] == "watch"
assert rA["target_sz"] == 1000.0 and rA["exch"] in ("terminated", "activated")   # виконання — від біржі (звірка була), пост не перезаписує
for _ in range(2):   # перечитування — ідемпотентне, 222 живе
    TW["_twap_ingest"]("TWAPx", 501, now0 + 100, xc % 111, now0 + 130, seen=True)
assert rB["state"] == "watch" and TW["twap_stats"]["cancelled"] == 1
# id ще невідомий у реєстрі (звірка не встигла): кандидати звіряються і збіг — ПО ID
TW = mk_tw()
rA = reg(TW, A, now0 + 30, usd=100_000.0, pid=1)
rB = reg(TW, A, now0 + 60, usd=101_000.0, pid=2)
ex_reset(TW)
ex_twap(A, "HYPE", "sell", rA["start"], 10, 1000, 111, sp=0.0)
ex_twap(A, "HYPE", "sell", rB["start"], 10, 1010, 222, sp=0.0)
assert rA["twap_id"] is None and rB["twap_id"] is None
TW["_twap_ingest"]("TWAPx", 502, now0 + 100, xc % 222, now0 + 100)
assert rB["state"] == "dropped" and rB["reason"] == "cancelled" and rA["state"] == "watch"
assert rA["twap_id"] == 111 and rB["twap_id"] == 222 and "TWAPx/502" in rB["posts"]
# id з поста нікому не належить — нічого не гасимо (біржа розсудить), лічильник
TW["_twap_ingest"]("TWAPx", 503, now0 + 110, xc % 999, now0 + 110)
assert rA["state"] == "watch" and TW["twap_stats"].get("fin_unmatched") == 1
# (б) HL «closed by user» без reply і ДВІ активні заявки з тією ж сумою — неоднозначно
TW = mk_tw()
_hl = lambda usd, t0: ("$%s selling HYPE 🟥 Frequency: $1 every 60 seconds (10 cycles) ETA: 9m "
                       "Price: $85 User: " + A + " Period: "
                       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0)) + " - "
                       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0 + 600)) + " UTC+0") % usd
TW["_twap_ingest"]("HL_TWAP", 601, now0, _hl("5.10M", now0 + 100), now0)
TW["_twap_ingest"]("HL_TWAP", 602, now0, _hl("5.10M", now0 + 400), now0)
cancel = "⛔️ $5.10m TWAP with HYPE closed by user 0.00%\nFilled: 0.00/60000.0 HYPE\nTime: x"
TW["_twap_ingest"]("HL_TWAP", 603, now0 + 10, cancel, now0 + 10)
assert all(r["state"] == "watch" for r in TW["twap_reg"].values())
assert TW["twap_stats"].get("fin_ambiguous") == 1 and TW["twap_stats"]["cancelled"] == 0
# …а з різними сумами (5.10 / 5.11) — гасить лише збіг суми
TW["_twap_ingest"]("HL_TWAP", 604, now0, _hl("5.11M", now0 + 200), now0)
TW["_twap_ingest"]("HL_TWAP", 605, now0 + 12, cancel.replace("5.10m", "5.11m"), now0 + 12)
r511 = [r for r in TW["twap_reg"].values() if r["usd"] == 5.11e6][0]
assert r511["state"] == "dropped" and sum(1 for r in TW["twap_reg"].values() if r["state"] == "watch") == 2
# (в) перестворена заявка тієї ж суми за 30с з ІНШОГО каналу — НЕ той самий запис
TW = mk_tw()
r1 = reg(TW, A, now0 + 100, dur=600.0, ch="TWAPx", pid=1, usd=100_000.0)
r1["state"] = "dropped"; r1["reason"] = "cancelled"; r1["cancelled"] = 1
r2 = reg(TW, A, now0 + 130, dur=600.0, ch="HL_TWAP", pid=2, usd=101_000.0)
assert r2 is not r1 and r2["state"] == "watch"
# той самий твап з іншого каналу (старт +5с, та ж тривалість/сума) — один запис
TW = mk_tw()
r1 = reg(TW, A, now0 + 100, dur=600.0, ch="TWAPx", pid=1, usd=100_000.0)
r2 = reg(TW, A, now0 + 105, dur=600.0, ch="HL_TWAP", pid=2, usd=100_500.0)
assert r2 is r1 and r1["src"] == "TWAPx+HL_TWAP"
# старт +25с — уже інша заявка; 15 і 16 хв — різні
TW = mk_tw()
r1 = reg(TW, A, now0 + 100, dur=600.0, ch="TWAPx", pid=1, usd=100_000.0)
r2 = reg(TW, A, now0 + 125, dur=600.0, ch="HL_TWAP", pid=2, usd=100_000.0)
assert r2 is not r1
r3 = reg(TW, A, now0 + 300, dur=900.0, ch="TWAPx", pid=3, usd=100_000.0)
r4 = reg(TW, A, now0 + 303, dur=960.0, ch="HL_TWAP", pid=4, usd=100_000.0)
assert r4 is not r3 and r3["dur"] == 900.0 and r4["state"] == "ineligible"
# (г) дубль по twapId зі СКАСОВАНИМ старшим записом — dup_twapid, скасування не рахується вдруге
TW = mk_tw()
r1 = reg(TW, A, now0 + 30, ch="TWAPx", pid=1)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r1["start"], 10, 2000, 777, status="terminated", sp=0.0)
TW["_twap_resolve_kind"](r1)
assert r1["state"] == "dropped" and r1["reason"] == "cancelled" and TW["twap_stats"]["cancelled"] == 1
r2 = reg(TW, A, now0 + 35, ch="HL_TWAP", pid=2)     # той самий твап з 2-го каналу (+5с): у dropped НЕ зливаємо (v2.15)
assert r2 is not r1
TW["_twap_tick"](now0 + 80)                          # звірка дає той самий twapId 777 -> дубль, без другого сигналу/скасування
assert r2["state"] == "dropped" and r2["reason"] == "dup_twapid" and TW["twap_stats"]["cancelled"] == 1
assert r2["sig_written"] == 1 and TW["twap_stats"].get("dup_posts") == 1
assert "HL_TWAP/2" in r1["posts"]
# нова заявка тієї ж суми через 10с від іншого каналу — окремий запис із власною звіркою (аудит v2.14 №4a)
r1["twap_id"] = 777
r3 = reg(TW, A, now0 + 40, ch="HL_TWAP", pid=3, usd=167_000.0)
assert r3 is not r1 and r3["state"] == "watch" and r3["twap_id"] is None
print("2) ідентичність: TwapId з поста, кандидати по id, без reply — лише єдиний збіг суми, канали ±20с/±30с, dropped-дублі по id")

# ═══ 3. completed — лише повне виконання ══════════════════════════
def enter(TW, addr=A, start=None, tid=1301, usd=165_570.0, pid=1):
    r = reg(TW, addr, start if start is not None else now0 + 30, usd=usd, pid=pid)
    ex_reset(TW); ex_twap(addr, "HYPE", "sell", r["start"], 10, 1000, tid, sp=0.0)
    TW["_px_now"] = lambda c, max_age=20: 98.0
    TW["_twap_tick"](r["end"] - 50)
    assert r["state"] == "entered", (r["state"], r["reason"])
    return r, TW["rev_open"][f"{r['id']}|{T1}"]
xd = ("✅ TWAP завершён Статус: finished Исполнено: %s Размер: %s / 1000 HYPE TwapId: %d "
      "Субъект: " + A + " Цена в начале: $0.17 Цена в конце: $0.16 🔴 (-2.85%%)")
# (а) канал: завершено, але 700/1000 — НЕ підтверджений
TW = mk_tw(); r, tr = enter(TW, tid=1301)
TW["_twap_ingest"]("TWAPx", 701, r["end"] + 5, xd % ("70.00%", "700", 1301), r["end"] + 5)
assert r["completed"] == 0 and r["tg_done"] == 1 and r["tg_partial"] == 1
assert r["exch"] == "finished" and tr["twap"]["completed"] == 0 and tr["twap"]["exch_status"] == "finished"
# (б) 1000/1000 — підтверджений
TW = mk_tw(); r, tr = enter(TW, tid=1302)
TW["_twap_ingest"]("TWAPx", 702, r["end"] + 5, xd % ("100.00%", "1000", 1302), r["end"] + 5)
assert r["completed"] == 1 and tr["twap"]["completed"] == 1
# (в) канал сказав «повністю», біржа потім — 700/1000: прапорець знімається
TW = mk_tw(); r, tr = enter(TW, tid=1303)
TW["_twap_ingest"]("TWAPx", 703, r["end"] + 5, xd % ("100.00%", "1000", 1303), r["end"] + 5)
assert r["completed"] == 1
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 1000, 1303, status="finished", executed=700, sp=0.0)
TW["_twap_tick"](r["end"] + 90)
assert r["completed"] == 0 and r["tg_partial"] == 1 and tr["twap"]["completed"] == 0 and tr["twap"]["exec_pct"] == 70.0
# (г) HL: відредагований старт «successful completed [exec/total]»
TW = mk_tw(); r, tr = enter(TW, tid=1304)
hl_done = "✅ TWAP is successful completed [%s/1000.0] " + _hl("165.57K", r["start"])
r["posts"].append("HL_TWAP/801")
TW["_twap_ingest"]("HL_TWAP", 801, r["start"], hl_done % "300.0", r["end"] + 5, seen=True)
assert r["completed"] == 0 and r["tg_partial"] == 1
TW = mk_tw(); r, tr = enter(TW, tid=1305)
r["posts"].append("HL_TWAP/802")
TW["_twap_ingest"]("HL_TWAP", 802, r["start"], hl_done % "1000.0", r["end"] + 5, seen=True)
assert r["completed"] == 1
# (д) канал «завершено» без відповіді біржі 5 хв — статус tg_done, не unconfirmed і не confirmed
TW = mk_tw(); r, tr = enter(TW, tid=1306)
TW["_twap_ingest"]("TWAPx", 704, r["end"] + 5, xd.replace("Размер: %s / 1000 HYPE ", "") % ("100.00%", 1306), r["end"] + 5)
assert r["completed"] == 0 and r["tg_done"] == 1
ex_reset(TW)
TW["_twap_tick"](r["end"] + 400)
assert r["confirm_done"] == 1 and r["exch"] == "tg_done" and tr["twap"]["exch_status"] == "tg_done"
# (е) HL «closed by user … Time: … GMT» — час скасування = хвилина виходу
TW = mk_tw(); r, tr = enter(TW, tid=1307)
tr["samples"] = [""] * 30
_ct = tr["entry_ts"] + 5 * 60 + 10
hl_closed = ("⛔️ $165.57k TWAP with HYPE closed by user 8.52%\nFilled: 85.2/1000.0 HYPE\nTime: "
             + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(_ct)) + " GMT")
TW["_twap_ingest"]("HL_TWAP", 803, _ct + 20, hl_closed, _ct + 20, "", reply_pid=0)
assert r["cancelled"] == 1 and abs(r["tg_fin_ts"] - int(_ct)) < 1, r.get("tg_fin_ts")
# v2.15: вихід — коли дізнались (31-ша), подія з поста — cancel_event_min 6
assert tr["twap"]["exit_min"] == 31 and tr["twap"]["cancel_event_min"] == 6, tr["twap"]
print("3) completed лише за повним виконанням: 70% з каналу/біржі — partial; tg_done окремо; час скасування з поста")

# ═══ 4/5/8. API: списки угод когорт, pending-рядки, open_now, версія відкритих ═══
d4 = tempfile.mkdtemp()
h = th
def tw_row(strat, net, move, tid, algo="2.15", **kv):
    r_ = {k: "" for k in h}
    r_.update({"twap_id": tid, "strategy": strat, "date_entry": "2026-09-01 10:00:00",
               "coin": "HYPE", "our_side": "LONG", "net60_pct": str(net), "costs_pct": "0.15",
               "move_pct": str(move), "completed": "1", "cancel_after_entry": "0",
               "exch_status": "finished", "algo_v": algo, "eol": "^"})
    r_.update(kv)
    return ",".join(r_[k] for k in h)
open(os.path.join(d4, "twap_trades.csv"), "w").write(",".join(h) + "\n"
    + tw_row(T1, 1.0, 1.2, "a") + "\n"
    + tw_row(T1 + "_15", 1.7, 2.1, "b") + "\n"
    + tw_row(T1 + "_20", 1.7, 2.1, "b") + "\n"
    + tw_row(T1, 9.0, 1.3, "old", algo="2.13") + "\n")
_ss = load(set(), {}, assigns=("STRAT_SINCE", "TAPE_SINCE"))
def mk_api(rev_open=None, follow_open=None, d=d4, fol="follow.csv"):
    return load({"strat2_api", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
                 "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key",
                 "_twap_exit", "_twap_curve_row"}, dict(C,
        strat2_lock=threading.RLock(), rev_open=rev_open or {}, follow_open=follow_open or {},
        wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
        STRAT_SINCE=_ss["STRAT_SINCE"], TAPE_SINCE=_ss["TAPE_SINCE"],
        _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
        FOLLOW_OUT_CSV=os.path.join(d, "fo.csv"), REV_CSV=os.path.join(d, "rev.csv"),
        REV_SIG_CSV=os.path.join(d, "sig.csv"), FOLLOW_CSV=os.path.join(d, fol),
        REV_OUT_CSV=os.path.join(d, "out.csv"), TWAP_CSV=os.path.join(d, "twap_trades.csv"),
        TWAP_SIG_CSV=os.path.join(d, "tws.csv"), TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
        TWAP_COHORTS=(1.0, 1.5, 2.0), F8_NAME="F8_ratio35", F9_NAME="F9_без_ратіо_90",
        T1_NAME=T1, T2_NAME="T2_твап_скорочення", R7_NAME="R7_одним",
        strat_activated={}, _dt=_dt, _sim_slip=lambda d_: 0.0005), assigns=("TWAP_HEADERS",))
o = mk_api()["strat2_api"]()["strategies"][T1]
assert o["n"] == 1 and [t["move"] for t in o["trades"]] == [1.2]              # базова: лише свої входи (2.13 — за бортом)
assert o["cohorts"]["1.5"]["n"] == 1 and [t["move"] for t in o["cohorts"]["1.5"]["trades"]] == [2.1]
assert o["cohorts"]["2.0"]["trades"][0]["net30"] == 1.7 and o["cohorts"]["1.0"]["trades"] == o["trades"]
assert "cs0.trades" in open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()   # v2.16: cs0 = блок когорти без зрізу
# pending: закрита (61 хв), але ще спостережувана угода — у вибірці, не «відкрита»
def fake_tr(sig, samples, entry_ago_s, strat=T1, algo="2.15", exit_min=0):
    return {"sig_id": sig, "strategy": strat, "state": "open", "row_kind": "twap", "coin": "HYPE",
            "side": "LONG", "addr": A, "detect_ts": now0, "detect_px": 100.0,
            "entry_ts": time.time() - entry_ago_s, "entry_px": 100.0, "track_min": 120,
            "depth": 1e5, "btc_move": 0.1, "hour": 10, "algo_v": algo,
            "twap": {"src": "TWAPx", "twap_side": "sell", "usd": 1e5, "dur": 600, "kind": "open",
                     "kind_src": "slice", "twap_id": 5, "sp": 0.0, "pos_usd": 0, "move": 1.5,
                     "p0": 100.0, "p0_src": "candle", "p1": 98.5, "p1_src": "candle",
                     "cohort": 1.0, "cancel_after_entry": 1 if exit_min else 0, "completed": 0,
                     "exch_status": "terminated" if exit_min else "activated", "exec_pct": None,
                     "exit_min": exit_min, "exit_reason": "cancelled" if exit_min else ""},
            "samples": samples, "peak": 2.0, "trough": -0.5}
ro = {"p1|" + T1: fake_tr("p1", [0.5] * 61, 61 * 60 + 5),          # закрита (m60), крива триває
      "p2|" + T1: fake_tr("p2", [0.5] * 30, 30 * 60 + 5),          # ще відкрита
      "p3|" + T1: fake_tr("p3", [0.5] * 4, 4 * 60 + 5, exit_min=1),   # скасована на 1-й хв — закрита
      "p4|" + T1: fake_tr("p4", [0.5] * 30, 30 * 60 + 5, algo="2.13"),   # стара версія — не рахується
      "a|" + T1: fake_tr("a", [0.5] * 10, 10 * 60 + 5)}             # ключ уже в CSV — не «другий вхід»
full = mk_api(rev_open=ro)["strat2_api"]()
o = full["strategies"][T1]
# v2.15: вибірка — лише записані рядки (закриті p1/p3 запише цикл трекерів у
# момент виходу, API їх не синтезує); відкрита — лише p2; закриті, що ще
# ведуть криву, не «у ринку» (аудит v2.14 №5c)
assert o["n"] == 1 and o["open_now"] == 1, (o["n"], o["open_now"])
_ages = sorted(x["age_s"] for x in full["open"] if x["strategy"] == T1)   # p2, p4 (стара версія, але в ринку), a
assert len(_ages) == 3 and max(_ages) < 3600 and all(abs(a_ - 245) > 5 for a_ in _ages), _ages   # p1 (61 хв) і p3 (скасована) — ні
print("4/5/8) власні списки угод когорт; закриті угоди поза open/«у ринку»; open_now без закритих/старих/дублів")

# ═══ 5. Угода ≠ спостереження: busy ══════════════════════════════
TW = mk_tw(); T = FakeTime(); TW["time"] = T
r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1501, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
T.now = r["end"] - 50; TW["_twap_tick"](T.now)
tr = TW["rev_open"][f"{r['id']}|{T1}"]
assert r["state"] == "entered"
# скасовано за 30с після входу; бот ДІЗНАВСЯ за 140с (end+90) -> вихід
# на m3 (найближча хвилина після того, як дізнались; v2.15 — не m1 за
# порожніми семплами) -> монета вільна через 3 хв
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1501, status="terminated", sp=0.0,
                      t_rev=tr["entry_ts"] + 30)
T.now = r["end"] + 90; TW["_twap_tick"](T.now)
# v2.15: вихід вирішений лише з ціною хвилини виходу — доти угода НЕ закрита
assert tr["twap"]["exit_min"] == 3 and tr["twap"]["cancel_event_min"] == 1, tr["twap"]
assert not TW["_twap_trade_closed"](tr, T.now)
tr["samples"] = [0.3]                              # лише m1 — m3 ще нема
assert not TW["_twap_trade_closed"](tr, T.now)
tr["samples"] = [0.3, 0.3, 0.3]                    # m3 записано циклом трекерів
assert TW["_twap_trade_closed"](tr, T.now)
r3 = reg(TW, B, tr["entry_ts"] + 180 - 550, pid=6)   # закінчується за 3 хв після входу
ex_reset(TW); ex_twap(B, "HYPE", "sell", r3["start"], 10, 2000, 1503, sp=0.0)
T.now = r3["end"] - 50; TW["_twap_tick"](T.now)
assert r3["state"] == "entered", (r3["state"], r3["reason"])
# а без скасування — зайнята до m60
TW = mk_tw(); T = FakeTime(); TW["time"] = T
r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1504, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
T.now = r["end"] - 50; TW["_twap_tick"](T.now)
tr = TW["rev_open"][f"{r['id']}|{T1}"]
r4 = reg(TW, B, tr["entry_ts"] + 180 - 550, pid=7)
ex_reset(TW); ex_twap(B, "HYPE", "sell", r4["start"], 10, 2000, 1505, sp=0.0)
T.now = r4["end"] - 50; TW["_twap_tick"](T.now)
assert r4["state"] == "dropped" and r4["reason"] == "busy"
print("5) скасування на 1-й хвилині звільняє монету одразу; без скасування — зайнята до m60")

# ═══ 6. Пропуск ціни на межі хвилини; timer_late; late_exit ═══════
class _Stop(Exception): pass
def mk_loop(px_seq, entry_ago):
    """run_strat2_loop на ОДНОМУ трекері: time.sleep(3) — крок годинника,
    px_seq — ціна на кожному тику (None = нема)."""
    T_ = FakeTime(); T_.now = now0
    ticks = {"i": 0}
    def _sleep(x):
        ticks["i"] += 1
        if ticks["i"] > len(px_seq): raise _Stop()
        T_.now += 3
    T_.sleep = _sleep
    tr = {"sig_id": "L1", "strategy": "R1_загальний", "state": "open", "row_kind": "rev",
          "coin": "HYPE", "side": "LONG", "addr": A, "src": "w", "detect_ts": now0 - entry_ago,
          "detect_px": 100.0, "entry_ts": now0 + 3 - entry_ago, "entry_px": 100.0,
          "move": 1.0, "dur": 5.0, "sum_usd": 1e5, "usd_s": 1e4, "ratio": 2.0, "shtanga": 0,
          "vault": 0, "hour": 1, "depth": 1e5, "btc_move": 0.0, "algo_v": "2.14",
          "samples": [], "peak": -999.0, "trough": 999.0}
    ns = load({"run_strat2_loop", "_rev_row", "_rev_samples", "_rev_out_row", "_fol_out_row",
               "_twap_row", "_twap_exit", "_twap_curve_row", "_twap_close_min",
               "_twap_trade_closed"}, dict(C,
        time=T_, strat2_lock=threading.RLock(), rev_open={"L1": tr}, follow_open={},
        follow_last_close={}, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _px_now=lambda c, max_age=30.0: px_seq[min(ticks["i"], len(px_seq) - 1)],
        _strat_csv_append=lambda p, h_, r_: True, save_state=lambda: None,
        _sim_slip=lambda d: 0.0005, _dt=_dt, REV_CSV="r", REV_HEADERS=rh, REV_SIG_CSV="s",
        REV_SIG_HEADERS=[], REV_OUT_CSV="o", REV_OUT_HEADERS=[], FOLLOW_CSV="f",
        FOLLOW_HEADERS=fh, FOLLOW_OUT_CSV="fo", FOLLOW_OUT_HEADERS=[], TWAP_CSV="t",
        TWAP_HEADERS=th, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
        TWAP_CURVE_CSV="tc", TWAP_CURVE_HEADERS=FH["TWAP_CURVE_HEADERS"],
        stats={"delta_events": 0}))
    try:
        ns["run_strat2_loop"]()
    except _Stop:
        pass
    return tr
# перший тик 60-ї хвилини без ціни, через 3с ціна є -> m60 заповнений
tr = mk_loop([None, 101.0, 101.0], 60 * 60)
assert len(tr["samples"]) >= 60 and tr["samples"][59] == 1.0, tr["samples"][55:]
# ціни нема довше за 30с допуск -> чесний пропуск ""
tr = mk_loop([None] * 12 + [101.0], 60 * 60)
assert tr["samples"][59] == "", tr["samples"][55:]
# _twap_row: m60 порожній -> вихід першою наступною хвилиною з ціною, timer_late
TWr = load({"_twap_row", "_twap_exit", "_twap_close_min"},
           dict(C, _sim_slip=lambda d: 0.0005, _dt=_dt, TWAP_HOLD_MIN=60, TWAP_TRACK_MIN=120))
p = fake_tr("x", [0.5] * 59 + ["", "", 0.7] + [0.9] * 58, 0)
row = TWr["_twap_row"](p)
assert row[h.index("exit_min")] == 62 and row[h.index("exit_reason")] == "timer_late"
assert abs(row[h.index("net60_pct")] - (0.7 - row[h.index("costs_pct")])) < 1e-9
# rev API: m30 порожній -> net30 з m31 із позначкою late_exit
d6 = tempfile.mkdtemp()
def rev_row(sig, m30, m31, m32=""):
    r_ = {k: "" for k in rh}
    r_.update({"sig_id": sig, "strategy": "R1_загальний", "date": "2026-09-01 10:00:00", "coin": "X",
               "our_side": "LONG", "entered": "1", "costs_pct": "0.15", "algo_v": "2.17",
               "m30": m30, "m31": m31, "m32": m32, "eol": "^"})
    return ",".join(r_[k] for k in rh)
open(os.path.join(d6, "rev.csv"), "w").write(",".join(rh) + "\n" + rev_row("r1", "1.0", "5.0") + "\n"
    + rev_row("r2", "", "2.0") + "\n" + rev_row("r3", "", "", "") + "\n")
o = mk_api(d=d6)["strat2_api"]()["strategies"]["R1_загальний"]
assert o["n"] == 1 and o["n_late"] == 1 and sorted(t["late_exit"] for t in o["trades"]) == [0, 0, 1]   # v2.16: пізній вихід поза заголовком
assert abs(sorted(t["net30"] for t in o["trades"] if t["net30"] is not None)[1] - (2.0 - 0.15)) < 1e-9
print("6) ціна на межі хвилини чекає до 30с; m60 без ціни -> timer_late; rev: net30 з m31 (late_exit)")

# ═══ 7. Вхід лише зі свіжою успішною звіркою ══════════════════════
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1701, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](now0 + 100)                       # вид відомий
assert r["kind"] == "open" and r["verify_ok_ts"] > 0
EX["fail"].add("twapHistory")                      # перед входом біржа впала
TW["_twap_api_cache"].clear()                      # (кеш 10с; тики — раз на 30с)
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "watch" and r.get("verify_fail") == 1, (r["state"], r["reason"])
EX["fail"].clear()                                 # наступний тик — біржа ожила: вхід
TW["_twap_tick"](r["end"] - 20)
assert r["state"] == "entered"
# падає на всіх тиках вікна — пропуск no_verify, входу немає
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1702, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](now0 + 100)
EX["fail"].add("twapHistory")
T = FakeTime(); TW["time"] = T                     # now2 = time.time() має жити в часі тику
for t_ in (r["end"] - 50, r["end"] - 20, r["end"] + 10):
    T.now = t_; TW["_twap_api_cache"].clear(); TW["_twap_tick"](t_)
assert r["state"] == "dropped" and r["reason"] == "no_verify" and r["verify_fail"] == 3, (r["state"], r["reason"])
# verified_ts (спроба) != verify_ok_ts (успіх)
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1703, sp=0.0)
TW["_twap_verify"](r, now0 + 40)
ok1 = r["verify_ok_ts"]
EX["fail"].add("twapHistory"); TW["_twap_api_cache"].clear()
assert not TW["_twap_verify"](r, now0 + 100) and r["verified_ts"] == now0 + 100 and r["verify_ok_ts"] == ok1
print("7) збій звірки перед входом = без входу (повтор наступного тику, далі no_verify); verified_ts ≠ verify_ok_ts")

# ═══ 8. Стан: .bak без state.json; не-dict; міграція ═══════════════
d8 = tempfile.mkdtemp()
ST8 = dict(ST, STATE_FILE=os.path.join(d8, "state.json"), REV_CSV=os.path.join(d8, "rev.csv"),
           FOLLOW_CSV=os.path.join(d8, "follow.csv"), TWAP_CSV=os.path.join(d8, "tw.csv"),
           REV_OUT_CSV=os.path.join(d8, "out.csv"), FOLLOW_OUT_CSV=os.path.join(d8, "fo.csv"))
def mk_state8():
    return load({"load_state", "_csv_written_keys", "_tracker_csv_key"}, dict(ST8,
        watchlist_lock=threading.Lock(), watchlist={}, sim_lock=threading.Lock(),
        sim_positions={}, sim_trackers={}, sim_closed=[], alerts_lock=threading.Lock(),
        recent_alerts=[], sent_alerts=set(), fill_cursor={}, close_episodes={},
        fc_lock=threading.Lock(), fc_positions={}, fc_episodes={},
        strat2_lock=threading.RLock(), rev_open={}, follow_open={}, follow_last_close={},
        vault_cache={}, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
        strat_activated={}))
bak = {"saved_at": time.time() - 30, "follow_open": {"fb": {"strategy": "F1_1хв", "coin": "AAA"}}}
json.dump(bak, open(ST8["STATE_FILE"] + ".bak", "w"))
L = mk_state8(); L["load_state"]()                                   # state.json відсутній
assert set(L["follow_open"]) == {"fb"}
open(ST8["STATE_FILE"], "w").write("[]")                             # валідний JSON, не dict
L = mk_state8(); L["load_state"]()
assert set(L["follow_open"]) == {"fb"} and not os.path.exists(ST8["STATE_FILE"])
assert glob.glob(ST8["STATE_FILE"] + ".corrupt-*")
open(ST8["STATE_FILE"], "w").write('{"saved_at": 1, "rev_open": []}')   # dict, але поле не dict — не падає
L = mk_state8(); L["load_state"]()
# міграція: усі CSV (з legacy) і state.json.bak
_mig = src[src.index("if DATA_DIR != DIR:"):src.index("REFRESH_S = ")]
assert '"*.csv"' in _mig and '"state.json.bak"' in _mig and "_glob.glob" in _mig
print("8) .bak без state.json; state=[] -> .corrupt + .bak; поле не dict не валить; міграція — усі CSV/legacy/bak")

# ═══ 9. Дрібне: неокруглений рух, доказ першого слайсу, вага, стеля бюджету, догортання ═══
# рух 1.49996% — базова стратегія, НЕ когорта ≥1.5%
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1901, sp=0.0)
TW["_px_at"] = lambda c, ts, tol=90.0: 100.0
TW["_px_now"] = lambda c, max_age=20: 100.0 * (1 - 0.0149996)
TW["_twap_tick"](r["end"] - 50)
assert r["state"] == "entered" and r["strategy"] == T1 and abs(r["move"] - 1.49996) < 1e-6, (r["strategy"], r["move"])
# перший слайс: 2000 філів у відповіді, найстаріший ПІЗНІШЕ старту -> unproven
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1902, sp=0.0, slice_at=3.0)
EX["slices"][A] = ([{"twapId": 1, "fill": {"time": int((r["start"] + 2) * 1000), "px": "1", "sz": "1", "tid": 0}}] * 1999
                   + EX["slices"][A])
TW["_twap_verify"](r, now0 + 40)
assert r["kind_src"] == "unproven"
# …а якщо історія сягає старту — доведено
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1903, sp=0.0, slice_at=3.0)
EX["slices"][A] = ([{"twapId": 1, "fill": {"time": int((r["start"] - 100) * 1000), "px": "1", "sz": "1", "tid": 0}}] * 1999
                   + EX["slices"][A])
TW["_twap_verify"](r, now0 + 40)
assert r["kind_src"] == "slice" and r["kind"] == "open"
# слайс на 30-й секунді — не перший (інтервал 30с)
TW = mk_tw(); r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 1904, sp=0.0, slice_at=30.0)
TW["_twap_verify"](r, now0 + 70)
assert r["kind_src"] == "unproven"
# вага запитів HL і спільний бюджет каналу
W = load({"_hl_weight", "_profile_budget_wait", "_profile_budget_add"},
         dict(_profile_w={"proxy": NEW["deque"](), "direct": NEW["deque"]()},
              _profile_w_lock=threading.Lock(), PROFILE_W_PER_MIN={"proxy": 800, "direct": 150}),
         assigns=("_HL_LIGHT_TYPES",))
assert W["_hl_weight"]({"type": "clearinghouseState"}) == 2 and W["_hl_weight"]({"type": "twapHistory"}) == 20
assert W["_hl_weight"]({"type": "userTwapSliceFills"}, [{}] * 45) == 22
assert W["_hl_weight"]({"type": "candleSnapshot"}, []) == 20
# стеля очікування: бюджет вичерпано -> чекає не довше max_wait
class _T:
    def __init__(s): s.t = 5000.0; s.slept = 0.0
    def time(s): return s.t
    def sleep(s, x): s.slept += x; s.t += x
tt = _T(); W["time"] = tt
for _ in range(40): W["_profile_budget_add"]("proxy", [], {"type": "twapHistory"})   # 800
W["_profile_budget_wait"]("proxy", 20, max_wait=5.0)
assert 4.0 <= tt.slept <= 6.0, tt.slept
assert "hl_post_prio(body, direct=not _prio_opener, max_wait=5.0)" in src
assert "if not _profile_budget_wait(via, w_, max_wait=max_wait):" in src   # v2.15: резерв + явна відмова
assert "_profile_budget_add(via, out, body, reserved=w_)" in src
_pp = src[src.index("def _profile_post"):src.index("def _fetch_profile(")]
assert "_profile_budget_add(\"proxy\"" not in _pp and "return hl_post_prio(body, retries=1, w_next=120)" in _pp
# догортання t.me: ?before=<pid>, ≤3 сторінок, до last або зрізу
assert '?before={int(before)}' in src
_w = src[src.index("def run_twap_watcher"):src.index("def _tme_fetch") if src.index("def _tme_fetch") > src.index("def run_twap_watcher") else len(src)]
_w = src[src.index("def run_twap_watcher"):]
assert "older = _tme_fetch(ch, before=hi_)" in _w and "pages < 3" in _w and "hi_ <= lo_ + 1" in _w
assert 'st.setdefault("gaps", [])' in _w and "pid not in fresh_ids" in _w   # v2.15: черга недогорнутих пропусків
# опис допуску (CLAUDE.md): вхід в останню хвилину, до 15 с після кінця
_md = open((_HL + "/CLAUDE.md"), encoding="utf-8").read()
assert "до 15 с після кінця" in _md
print("9) неокруглений рух; доказ першого слайсу (≤25с, охоплення історії); вага 2/20/+1 за 20; стеля бюджету; догортання")

print("\nALL v2.14 TESTS PASSED")

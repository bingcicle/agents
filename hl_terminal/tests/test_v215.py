import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.15 — відповіді на зовнішній аудит v2.14 (53 сценарії):
1 незмінний запис TWAP-угоди у момент виходу (крива окремо), аварія після
показаного результату; 2 вихід не раніше отримання сигналу скасування;
3 перший філ за ланцюгом позицій, не за tid; 4 ідентичність (чуже
скасування, нова заявка після dropped); 5 єдиний факт виходу (без
накладання угод, cancelled_late, кап без ціни, «у ринку»); 6 бюджет:
атомарний резерв, відмова, спроба з 429; 7 курсор недогорнутого пропуску;
8 стан (типи полів), міграція однойменних CSV, outbox сигнальних рядків;
методика: пізній вихід R лише для рядків ≥2.14."""
import ast, threading, time, json, os, re, sys, math, tempfile, csv
import calendar, datetime as _dtmod, html as _htmlmod, glob, hashlib, shutil
import urllib.error, urllib.request
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
          "urllib": urllib, "print": lambda *a, **k: None}
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
assert const("DATA_ALGO_V") == "2.20" and const("TWAP_EXIT_CAP_MIN") == 5
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
          assigns=("FOLLOW_HEADERS", "REV_HEADERS", "TWAP_HEADERS", "REV_OUT_HEADERS",
                   "FOLLOW_OUT_HEADERS", "TWAP_CURVE_HEADERS"))
fh, rh, th, hc = FH["FOLLOW_HEADERS"], FH["REV_HEADERS"], FH["TWAP_HEADERS"], FH["TWAP_CURVE_HEADERS"]
assert len(th) == 53 + 60 + 1 and len(hc) == 7 + 120 + 1   # v2.19: +5 виконання; v2.20: +5
T1 = "T1_твап_відкриття"
A = "0x" + "a" * 40
B = "0x" + "b" * 40
now0 = time.time()

# ── фейкова біржа ──
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
                   _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
                   _twap_ch_state={"HL_TWAP": {"ok_ts": 0.0, "err": 0}, "TWAPx": {"ok_ts": 0.0, "err": 0}}),
              assigns=TWASG)
    ns["_px_at"] = lambda c, ts, tol=90.0: 100.0
    return ns
def ex_reset(TW):
    EX["hist"].clear(); EX["slices"].clear(); EX["candles"].clear(); EX["calls"].clear()
    EX["fail"].clear()
    TW["_twap_api_cache"].clear(); TW["_twap_candle_miss"].clear()
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
def fake_tr(sig, samples, entry_ts, strat=T1, algo="2.15", exit_min=0, side="SHORT"):
    return {"sig_id": sig, "strategy": strat, "state": "open", "row_kind": "twap", "coin": "HYPE",
            "side": side, "addr": A, "detect_ts": entry_ts, "detect_px": 100.0,
            "entry_ts": entry_ts, "entry_px": 100.0, "track_min": 120,
            "depth": 1e5, "btc_move": 0.1, "hour": 10, "algo_v": algo,
            "twap": {"src": "TWAPx", "twap_side": "sell", "usd": 1e5, "dur": 600, "kind": "open",
                     "kind_src": "slice", "twap_id": 5, "sp": 0.0, "pos_usd": 0, "move": 1.5,
                     "p0": 100.0, "p0_src": "candle", "p1": 98.5, "p1_src": "candle",
                     "cohort": 1.0, "cancel_after_entry": 1 if exit_min else 0, "completed": 0,
                     "exch_status": "terminated" if exit_min else "activated", "exec_pct": None,
                     "exit_min": exit_min, "exit_reason": "cancelled" if exit_min else ""},
            "samples": list(samples), "peak": 2.0, "trough": -0.5}
class _Stop(Exception): pass
def run_loop(rev_open, px_seq, t_start, writes, extra=None):
    """run_strat2_loop: один трекер-набір, ціни по тиках (3с), writes —
    (шлях, рядок) кожного успішного запису."""
    T_ = FakeTime(); T_.now = t_start
    ticks = {"i": 0}
    def _sleep(x):
        ticks["i"] += 1
        if ticks["i"] > len(px_seq): raise _Stop()
        T_.now += 3
    T_.sleep = _sleep
    extra = dict(extra or {})
    fail_paths = extra.pop("_fail_paths", set())   # шляхи, запис у які «падає»
    def _append(p, hh, r):
        if len(r) != len(hh) - 1 or p in fail_paths: return False
        writes.append((p, list(r))); return True
    ns = load({"run_strat2_loop", "_rev_row", "_rev_samples", "_rev_out_row", "_fol_out_row",
               "_twap_row", "_twap_exit", "_twap_curve_row", "_twap_close_min",
               "_twap_trade_closed", "_twap_freeze_exit"}, dict(C,
        time=T_, strat2_lock=threading.RLock(), rev_open=rev_open, follow_open={},
        follow_last_close={}, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _px_now=lambda c, max_age=30.0: px_seq[min(ticks["i"], len(px_seq) - 1)],
        _strat_csv_append=_append, save_state=lambda: None,
        _sim_slip=lambda d: 0.0005, _dt=_dt, REV_CSV="r", REV_HEADERS=rh, REV_SIG_CSV="s",
        REV_SIG_HEADERS=[], REV_OUT_CSV="o", REV_OUT_HEADERS=[], FOLLOW_CSV="f",
        FOLLOW_HEADERS=fh, FOLLOW_OUT_CSV="fo", FOLLOW_OUT_HEADERS=[], TWAP_CSV="t",
        TWAP_HEADERS=th, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
        TWAP_CURVE_CSV="tc", TWAP_CURVE_HEADERS=hc,
        stats={"delta_events": 0}, **(extra or {})))
    try:
        ns["run_strat2_loop"]()
    except _Stop:
        pass
    return ns

# ═══ 1. Незмінний запис угоди у момент виходу; аварія після показу ═══
# SHORT від 100; 59 хвилин у трекері; на 60-й хвилині ціна 99 -> +1% -> рядок
# угоди пишеться ОДРАЗУ (twap_trades), трекер лишається для кривої
entry = now0 - 60 * 60 - 3
tr = fake_tr("cr1", [0.1] * 59, entry)
ro = {"cr1|" + T1: tr}
writes = []
run_loop(ro, [99.0, 99.0], now0, writes)
tw_writes = [w for w in writes if w[0] == "t"]
assert len(tw_writes) == 1 and tr["trade_written"] == 1 and tr["trade_row"] is not None
row = tw_writes[0][1]
assert row[th.index("exit_min")] == 60 and row[th.index("exit_reason")] == "timer_60m"
_ce = 0.1 + (row[th.index("costs_pct")] - 0.1) / 2   # v2.16 аудит-2 №12: вихід по стакану без модельного сліпажу
assert abs(row[th.index("net60_pct")] - (1.0 - _ce)) < 1e-9
assert row[th.index("exit_ts")] != "" and "cr1|" + T1 in ro           # трекер живий (крива)
assert tr["twap"].get("exit_final") == [60, "timer_60m"], tr["twap"].get("exit_final")   # вихід збережено окремо
_cr = load({"_twap_curve_row", "_twap_close_min"}, dict(C, TWAP_HEADERS=th, TWAP_TRACK_MIN=120,
                                                        TWAP_HOLD_MIN=60, _dt=_dt))
tr_x = dict(tr, trade_row=None)                                          # рядка нема — exit_final достатньо
assert _cr["_twap_curve_row"](tr_x)[hc.index("exit_min")] == 60
# аварія до save_state: старий знімок (59 семплів) + рядок угоди вже у CSV
d1 = tempfile.mkdtemp()
with open(os.path.join(d1, "tw.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(th); w.writerow(row + ["^"])
ST = dict(C, STATE_FILE=os.path.join(d1, "state.json"), STATE_MAX_AGE_S=3600,
          REV_CSV=os.path.join(d1, "rev.csv"), FOLLOW_CSV=os.path.join(d1, "follow.csv"),
          TWAP_CSV=os.path.join(d1, "tw.csv"), TWAP_CURVE_CSV=os.path.join(d1, "tc.csv"),
          REV_OUT_CSV=os.path.join(d1, "out.csv"), FOLLOW_OUT_CSV=os.path.join(d1, "fo.csv"))
def mk_state_ns(st=ST):
    return load({"load_state", "_csv_written_keys", "_tracker_csv_key"}, dict(st,
        watchlist_lock=threading.Lock(), watchlist={}, sim_lock=threading.Lock(),
        sim_positions={}, sim_trackers={}, sim_closed=[], alerts_lock=threading.Lock(),
        recent_alerts=[], sent_alerts=set(), fill_cursor={}, close_episodes={},
        fc_lock=threading.Lock(), fc_positions={}, fc_episodes={},
        strat2_lock=threading.RLock(), rev_open={}, follow_open={}, follow_last_close={},
        vault_cache={}, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
        strat_activated={}))
old = fake_tr("cr1", [0.1] * 59, entry)                   # знімок ДО 60-ї хвилини
json.dump({"saved_at": time.time() - 30, "rev_open": {"cr1|" + T1: old}}, open(ST["STATE_FILE"], "w"))
L = mk_state_ns(); L["load_state"]()
res = L["rev_open"]["cr1|" + T1]
assert res["trade_written"] == 1 and res["trade_row"] is None    # угода вже записана — не переписувати
# після рестарту на 62-й хвилині ціна 120: рядка угоди БІЛЬШЕ НЕ пишеться (−20% не з'являється)
writes2 = []
run_loop({"cr1|" + T1: res}, [120.0, 120.0], now0 + 120, writes2)
assert not [w for w in writes2 if w[0] == "t"], writes2
assert res["samples"][59] == "" or res["samples"][59] < 0      # крива чесна, угода незмінна
# завершення 120 хв -> крива у twap_curves, трекер видалено; угода не переписана
res["samples"] = res["samples"][:60] + [0.2] * 60
writes3 = []
run_loop({"cr1|" + T1: res}, [100.0, 100.0], now0 + 121 * 60, writes3)
assert [w[0] for w in writes3] == ["tc"] and len(writes3[0][1]) == len(hc) - 1
# ланцюжок «done без записаної угоди» (старий state): спершу угода, потім крива
tr2 = fake_tr("cr2", [0.1] * 120, now0 - 121 * 60); tr2["done"] = 1; tr2["final_row"] = ["old"] * 144
writes4 = []
run_loop({"cr2|" + T1: tr2}, [100.0, 100.0, 100.0], now0, writes4)
assert [w[0] for w in writes4] == ["t", "tc"], [w[0] for w in writes4]
# API: результат з CSV (не з трекера); крива — з twap_curves по ключу
_ss = load(set(), {}, assigns=("STRAT_SINCE", "TAPE_SINCE"))
def mk_api(rev_open=None, d=d1):
    return load({"strat2_api", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
                 "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key",
                 "_twap_exit", "_twap_curve_row"}, dict(C,
        strat2_lock=threading.RLock(), rev_open=rev_open or {}, follow_open={},
        wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
        STRAT_SINCE=_ss["STRAT_SINCE"], TAPE_SINCE=_ss["TAPE_SINCE"],
        _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
        FOLLOW_OUT_CSV=os.path.join(d, "fo.csv"), REV_CSV=os.path.join(d, "rev.csv"),
        REV_SIG_CSV=os.path.join(d, "sig.csv"), FOLLOW_CSV=os.path.join(d, "follow.csv"),
        REV_OUT_CSV=os.path.join(d, "out.csv"), TWAP_CSV=os.path.join(d, "tw.csv"),
        TWAP_CURVE_CSV=os.path.join(d, "tc.csv"),
        TWAP_SIG_CSV=os.path.join(d, "tws.csv"), TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
        TWAP_COHORTS=(1.0, 1.5, 2.0), F8_NAME="F8_ratio35", F9_NAME="F9_без_ратіо_90",
        T1_NAME=T1, T2_NAME="T2_твап_скорочення", R7_NAME="R7_одним",
        strat_activated={}, _dt=_dt, _sim_slip=lambda d_: 0.0005), assigns=("TWAP_HEADERS",))
with open(os.path.join(d1, "tc.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(hc); w.writerow(writes3[0][1] + ["^"])
o = mk_api()["strat2_api"]()["strategies"][T1]
assert o["n"] == 1 and abs(o["median"] - (1.0 - 0.15)) < 1e-9   # v2.16 аудит-2 №12: вихід по стакану — без модельного сліпажу на цій нозі
assert o["curve"][0] is not None and o["curve"][119] is not None      # крива 120 — з twap_curves
print("1) угода пишеться у момент виходу і не переписується після аварії; крива окремо; done-ланцюжок; API з CSV")

# ═══ 2. Вихід не раніше отримання сигналу скасування ══════════════
TW = mk_tw(); T = FakeTime(); TW["time"] = T
r = reg(TW, A, now0 + 30)
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 2001, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.0
T.now = r["end"] - 50; TW["_twap_tick"](T.now)
tr = TW["rev_open"][f"{r['id']}|{T1}"]
tr["samples"] = [0.85] * 4                         # 4 хвилини минуло, ціни є
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2000, 2001, status="terminated", sp=0.0,
                      t_rev=tr["entry_ts"] + 30)   # скасовано через 30с після входу…
T.now = tr["entry_ts"] + 4 * 60 + 10               # …дізнались на 5-й хвилині (звірка після кінця)
TW["_twap_tick"](T.now)
assert tr["twap"]["exit_min"] == 5 and tr["twap"]["cancel_event_min"] == 1 and tr["twap"]["cancel_seen_min"] == 5, tr["twap"]
# вихід за ціною m5, а не m1
tr["samples"].append(-20.15)
ex = TW["_twap_exit"](tr, T.now + 60)
assert ex == (5, "cancelled", -20.15), ex
# пізнє скасування ПІСЛЯ закритої угоди не міняє вихід
tr3 = fake_tr("l1", [0.5] * 61, now0 - 62 * 60)
tr3["trade_row"] = ["frozen"]
rec3 = {"id": "l1", "trackers": ["l1|" + T1], "strategy": T1, "coin": "HYPE",
        "cancel_after_entry": 1, "completed": 0, "exch": "terminated", "exch_cancelled": 1,
        "exch_ts": now0 - 61 * 60, "target_sz": None, "exec_sz": None}
TW["rev_open"]["l1|" + T1] = tr3
TW["_twap_sync_trackers"](rec3)
assert not tr3["twap"].get("exit_min") and tr3["twap"]["cancel_event_min"] == 1
print("2) exit_min = хвилина отримання сигналу (m8, не m1); подія — cancel_event_min; закриту угоду не рухає")

# ═══ 3. Перший філ — за ланцюгом позицій, не за tid ═══════════════
ch = mk_tw()["_twap_chain_head"]
def fill(sp, sz, side="B", tid=1, t=1000):
    return {"startPosition": str(sp), "sz": str(sz), "side": side, "tid": tid, "time": t}
# купівля з 0: перший (sp 0, tid 900) і другий (sp 50, tid 100) з однаковим часом
f1, f2 = fill(0, 50, "B", 900), fill(50, 50, "B", 100)
assert ch([f2, f1]) is f1 and ch([f1, f2]) is f1
# продаж: LONG 2100 -> 2050; голова — sp 2100
g1, g2 = fill(2100, 50, "A", 900), fill(2050, 50, "A", 100)
assert ch([g2, g1]) is g1
# діра в ланцюгу (дві голови) або філ без startPosition — не доводимо
assert ch([fill(0, 50, "B"), fill(70, 50, "B")]) is None
assert ch([{"sz": "1", "side": "B", "tid": 1, "time": 1}]) is None
# через _twap_verify: open і reduce класифікуються правильно попри tid
TW = mk_tw(); r = reg(TW, A, now0 + 30, side="buy")
ex_reset(TW); ex_twap(A, "HYPE", "buy", r["start"], 10, 100, 3001, sp=None, with_slice=False)
t0 = int((r["start"] + 1) * 1000)
EX["slices"][A] = [{"twapId": 3001, "fill": dict(fill(50, 50, "B", 100, t0), coin="HYPE", px="100")},
                   {"twapId": 3001, "fill": dict(fill(0, 50, "B", 900, t0), coin="HYPE", px="100")}]
TW["_twap_verify"](r, now0 + 40)
assert r["kind"] == "open" and r["kind_src"] == "slice", (r["kind"], r["kind_src"])
TW = mk_tw(); r = reg(TW, A, now0 + 30, side="sell")
ex_reset(TW); ex_twap(A, "HYPE", "sell", r["start"], 10, 2100, 3002, sp=None, with_slice=False)
EX["slices"][A] = [{"twapId": 3002, "fill": dict(fill(2050, 50, "A", 100, t0), coin="HYPE", px="100")},
                   {"twapId": 3002, "fill": dict(fill(2100, 50, "A", 900, t0), coin="HYPE", px="100")}]
TW["_twap_verify"](r, now0 + 40)
assert r["kind"] == "reduce", r["kind"]
TW = mk_tw(); r = reg(TW, A, now0 + 30, side="buy")
ex_reset(TW); ex_twap(A, "HYPE", "buy", r["start"], 10, 100, 3003, sp=None, with_slice=False)
EX["slices"][A] = [{"twapId": 3003, "fill": dict(fill(50, 50, "B", 100, t0), coin="HYPE", px="100")},
                   {"twapId": 3003, "fill": dict(fill(120, 50, "B", 900, t0), coin="HYPE", px="100")}]
TW["_twap_verify"](r, now0 + 40)
assert r["kind_src"] == "unproven"
print("3) перший філ — голова ланцюга startPosition/sz; однаковий час і хеш-tid не плутають open/reduce; діра -> unproven")

# ═══ 4. Ідентичність ══════════════════════════════════════════════
# чуже скасування ($2.84m, повна адреса, без id/reply) НЕ гасить єдиний відомий $100k-твап
TW = mk_tw(); r = reg(TW, A, now0 + 30, usd=100_000.0)
cancel = ("⛔️ $2.84m TWAP with HYPE closed by user 0.00%\nFilled: 0.00/60000.0 HYPE\nTime: x")
TW["_twap_ingest"]("HL_TWAP", 401, now0 + 10, cancel, now0 + 10, "$2.84M selling HYPE 🟥 User: " + A)
assert r["state"] == "watch" and TW["twap_stats"]["cancelled"] == 0
# …а з тією ж сумою — гасить
TW["_twap_ingest"]("HL_TWAP", 402, now0 + 12, cancel.replace("2.84m", "100.0k"), now0 + 12,
                   "$100.0K selling HYPE 🟥 User: " + A)
assert r["state"] == "dropped" and r["reason"] == "cancelled"
# нова заявка ($101k, +10с, інший канал) після dropped-з-id — окремий запис
TW = mk_tw(); r1 = reg(TW, A, now0 + 100, dur=600.0, ch="TWAPx", pid=1, usd=100_000.0)
r1["state"] = "dropped"; r1["reason"] = "cancelled"; r1["cancelled"] = 1; r1["twap_id"] = 111
r2 = reg(TW, A, now0 + 110, dur=600.0, ch="HL_TWAP", pid=2, usd=101_000.0)
assert r2 is not r1 and r2["state"] == "watch" and r2["twap_id"] is None
print("4) чуже скасування за сумою не прив'язується; нова заявка після dropped отримує власну звірку")

# ═══ 5. Єдиний факт виходу ════════════════════════════════════════
TW = mk_tw()
xe = TW["_twap_exit"]
# m60 без ціни, допуск минув (3640с) — вихід вирішений ЗА ЧАСОМ (аудит-3 №10: ціна зі стакану чекає),
# але без ціни угода ще не закрита — монета зайнята
t60 = fake_tr("e1", [0.5] * 59 + [""], now0 - 3640)
assert xe(t60, now0) == (60, "timer_60m", None) and not TW["_twap_trade_closed"](t60, now0)
TW["rev_open"]["e1|" + T1] = t60
r5 = reg(TW, B, now0 - 550, pid=51)
ex_reset(TW); ex_twap(B, "HYPE", "sell", r5["start"], 10, 2000, 5001, sp=0.0)
TW["_px_now"] = lambda c, max_age=20: 98.8                           # рух 1.2%: лише базова когорта
T = FakeTime(); TW["time"] = T; T.now = now0; TW["_twap_tick"](T.now)
assert r5["state"] == "dropped" and r5["reason"] == "busy", (r5["state"], r5["reason"])   # без накладання угод
t60["samples"].append(0.4)                                           # m61 з ціною
assert xe(t60, now0 + 30) == (61, "timer_late", 0.4)
# скасування на m60 без ціни, m61 є -> результат є (cancelled_late)
tc = fake_tr("e2", [0.5] * 59 + ["", 0.7], now0 - 3700, exit_min=60)
assert xe(tc, now0) == (61, "cancelled_late", 0.7)
rowc = TW["_twap_row"](tc, now0)
assert rowc[th.index("exit_reason")] == "cancelled_late" and abs(rowc[th.index("net60_pct")] - (0.7 - rowc[th.index("costs_pct")])) < 1e-9
# кап: 5 хвилин без ціни після виходу -> вихід без ціни, результат порожній
tn = fake_tr("e3", [0.5] * 59 + [""] * 6, now0 - (65 * 60 + 40))
assert xe(tn, now0) == (60, "no_price", None)
assert TW["_twap_row"](tn, now0)[th.index("net60_pct")] == ""
print("5) без ціни виходу угода не закрита (нова не входить); cancelled_late бере m61; кап -> no_price")

# ═══ 6. Бюджет: атомарний резерв, відмова, спроба з 429 ═══════════
NEWB = dict(_profile_w={"proxy": NEW["deque"](), "direct": NEW["deque"]()},
            _profile_w_lock=threading.Lock(), PROFILE_W_PER_MIN={"proxy": 800, "direct": 150})
W = load({"_hl_weight", "_profile_budget_wait", "_profile_budget_add"}, NEWB,
         assigns=("_HL_LIGHT_TYPES",))
for _ in range(40): W["_profile_budget_add"]("proxy", [], {"type": "twapHistory"})   # 800/800
assert W["_profile_budget_wait"]("proxy", 20, max_wait=0.6) is False                 # відмова, не надсилання
assert sum(w for _, w in W["_profile_w"]["proxy"]) == 800
# два паралельні при 798/800 — рівно один резервує
W["_profile_w"]["proxy"].clear()
for _ in range(399): W["_profile_w"]["proxy"].append((time.time(), 2))              # 798
res6 = []
ths = [threading.Thread(target=lambda: res6.append(W["_profile_budget_wait"]("proxy", 2, max_wait=0.6)))
       for _ in range(2)]
[t_.start() for t_ in ths]; [t_.join() for t_ in ths]
assert sorted(res6) == [False, True] and sum(w for _, w in W["_profile_w"]["proxy"]) == 800
# hl_post_prio: спроба з 429 теж резервує; після max_wait — RateLimited, запиту немає
calls = {"n": 0}
class _Resp:
    def __init__(s, b): s.b = b
    def __enter__(s): return s
    def __exit__(s, *a): return False
    def read(s): return s.b
class _Opener:
    def open(s, req, timeout=10):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.HTTPError(req.full_url, 429, "rl", {}, None)
        return _Resp(b"[]")
HP = load({"hl_post_prio", "_hl_weight", "_profile_budget_wait", "_profile_budget_add"},
          dict(NEWB, _prio_opener=_Opener(), time=type("T", (), {"time": staticmethod(time.time),
                                                             "sleep": staticmethod(lambda s: None)})()),
          assigns=("_HL_LIGHT_TYPES",))
HP["_profile_w"]["proxy"].clear()
assert HP["hl_post_prio"]({"type": "twapHistory", "user": A}, retries=2) == []
assert calls["n"] == 2 and sum(w for _, w in HP["_profile_w"]["proxy"]) == 40 + HP["PROFILE_W_PER_MIN"]["proxy"] // 2   # 2 спроби × 20 + штраф за 429 (рев'ю v2.15)
for _ in range(38): HP["_profile_budget_add"]("proxy", [], {"type": "twapHistory"})   # 800
try:
    HP["hl_post_prio"]({"type": "twapHistory", "user": A}, retries=1, max_wait=0.6)
    raise AssertionError("мав відмовити через бюджет")
except NEW["RateLimited"]:
    pass
assert calls["n"] == 2                                                                # запиту не було
# prio-воркер: бюджетна відмова каналу — це не збій проксі і не 10-хв глухота
_ps = src[src.index("def run_prio_fetcher"):src.index("def get_recent_market_fills")]
_one = _ps.replace("while True:", "for _pass in range(1):", 1)
def _rl(a, post=None): raise NEW["RateLimited"]("prio budget (proxy) exhausted")
_q = NEW["deque"]([("0xbudget", "PUMP", "LONG", 5e4, 1e5, 1.5e4)])
_st = {"streak": 0, "dead_since": 0.0}; _logs = []; _seen = {}
_pst = {"triggers": 0, "added": 0, "dropped": 0, "errors": 0}
PN = dict(C, RateLimited=NEW["RateLimited"], REST_PROXY="u:p@1.2.3.4:5", _prio_q=_q,
          _prio_event=threading.Event(), _prio_lock=threading.Lock(), _prio_direct=NEW["deque"](),
          _prio_seen=_seen, _prio_proxy_state=_st, prio_stats=_pst, check_one_wallet=_rl,
          hl_post_prio=lambda *a, **k: None, _hl_post_prio_direct=lambda *a, **k: None,
          _prio_log=lambda *a: _logs.append(a), watchlist={}, watchlist_lock=threading.Lock(),
          fill_cursor={}, sent_alerts=set(), close_episodes={}, delta_seen={},
          strat2_lock=threading.RLock(), follow_last_close={}, COIN_BLACKLIST=set(),
          PRIO_COOLDOWN_S=600, PRIO_DIRECT_PER_MIN=4, _sim_depth=lambda c, s: 1e5,
          time=time, print=lambda *a, **k: None)
PN["_prio_event"].set()
exec(compile(_one, "prio", "exec"), PN)
PN["run_prio_fetcher"]()
assert _pst["dropped"] == 1 and _pst["errors"] == 0 and _st["streak"] == 0, (_pst, _st)
assert _logs and _logs[-1][6] == "budget", _logs
assert 70 <= time.time() - _seen["0xbudget"] - 0 <= 80 or abs((time.time() - _seen["0xbudget"]) - (600 - 75)) < 5, _seen
print("6) бюджет: резерв атомарний і на кожну спробу; повне вікно -> RateLimited без запиту; prio-воркер: budget-відмова = короткий кулдаун")

# ═══ 7. Курсор недогорнутого пропуску t.me ════════════════════════
pages = {None: (171, 190), 171: (151, 170), 151: (131, 150), 131: (111, 130), 111: (101, 110)}
def fake_fetch(ch, before=None):
    if before not in pages:
        raise NEW["APIError"]("no messages in page")
    lo, hi = pages[before]
    return [(pid, now0 - 100, "txt", "", 0) for pid in range(lo, hi + 1)]
ing = []
cyc = {"n": 0}
def _sleep7(x):
    cyc["n"] += 1
    if cyc["n"] > 3: raise _Stop()
WT = load({"run_twap_watcher"}, dict(C, TWAP_ENABLED=True, TWAP_CHANNELS=("X",), TWAP_POLL_S=30,
    TWAP_MAX_DUR_S=900, TWAP_ENTRY_LEAD=60.0, TWAP_MIN_MOVE=1.0, TWAP_BACKLOG_S=3600,
    _twap_ch_state={"X": {"ok_ts": 0.0, "err": 0}}, twap_lock=threading.Lock(),
    twap_last_ids={"X": 100}, twap_stats={"fetch_err": 0, "parse_err": 0},
    _tme_fetch=fake_fetch, _twap_tick=lambda now: None,
    _twap_ingest=lambda ch, pid, ts, text, now, reply, seen=False, reply_pid=0: ing.append((cyc["n"], pid, seen)),
    time=type("T", (), {"time": staticmethod(time.time), "sleep": staticmethod(_sleep7)})()))
try: WT["run_twap_watcher"]()
except _Stop: pass
c1 = {p for c, p, s in ing if c == 1 and not s}
c2 = {p for c, p, s in ing if c == 2 and not s}
assert c1 == set(range(111, 191)), sorted(c1)[:5]                    # цикл 1: 3 сторінки + остання
assert c2 == set(range(101, 111)), sorted(c2)                         # цикл 2: догорнуто решту
assert WT["_twap_ch_state"]["X"]["gaps"] == [] and WT["twap_last_ids"]["X"] == 190
assert not [1 for c, p, s in ing if c == 2 and p > 110 and not s]     # старі — як бачені
# 7б: ДРУГИЙ пропуск виникає, поки перший ще догортається (сплеск постів):
# він стає окремим записом черги і читається першим (новіші пости —
# живі заявки), старий — після нього; жоден не губиться
pages_b = {171: (151, 170), 151: (131, 150), 131: (111, 130), 111: (101, 110),
           221: (201, 220), 201: (191, 200)}
def fake_fetch_b(ch, before=None):
    if before is None:
        lo, hi = (171, 190) if cyc["n"] <= 1 else (221, 240)   # sleep(20) на старті = цикл 1
    elif before in pages_b:
        lo, hi = pages_b[before]
    else:
        raise NEW["APIError"]("no messages in page")
    return [(pid, now0 - 100, "txt", "", 0) for pid in range(lo, hi + 1)]
ing = []; cyc["n"] = 0
WT = load({"run_twap_watcher"}, dict(C, TWAP_ENABLED=True, TWAP_CHANNELS=("X",), TWAP_POLL_S=30,
    TWAP_MAX_DUR_S=900, TWAP_ENTRY_LEAD=60.0, TWAP_MIN_MOVE=1.0, TWAP_BACKLOG_S=3600,
    _twap_ch_state={"X": {"ok_ts": 0.0, "err": 0}}, twap_lock=threading.Lock(),
    twap_last_ids={"X": 100}, twap_stats={"fetch_err": 0, "parse_err": 0},
    _tme_fetch=fake_fetch_b, _twap_tick=lambda now: None,
    _twap_ingest=lambda ch, pid, ts, text, now, reply, seen=False, reply_pid=0: ing.append((cyc["n"], pid, seen)),
    time=type("T", (), {"time": staticmethod(time.time), "sleep": staticmethod(_sleep7)})()))
try: WT["run_twap_watcher"]()
except _Stop: pass
c1 = {p for c, p, s in ing if c == 1 and not s}
c2 = {p for c, p, s in ing if c == 2 and not s}
assert c1 == set(range(111, 191)), sorted(c1)[:5]
assert c2 == set(range(101, 111)) | set(range(191, 241)), (sorted(c2)[:3], sorted(c2)[-3:])
assert WT["_twap_ch_state"]["X"]["gaps"] == [] and WT["twap_last_ids"]["X"] == 240
# пропуск старший за годину — історія: знімається без читання
WT["_twap_ch_state"]["X"]["gaps"] = [[100, 111, time.time() - 3601]]
assert not [1 for g in WT["_twap_ch_state"]["X"]["gaps"] if g[2] > time.time() - 3600]
print("7) пропуск ширший за 3 сторінки догортається наступним циклом; другий пропуск — окремий запис черги; пости з пропуску — нові")

# ═══ 8. Стан: типи полів; міграція однойменних CSV; outbox сигналів ═══
d8 = tempfile.mkdtemp()
ST8 = dict(ST, STATE_FILE=os.path.join(d8, "state.json"), REV_CSV=os.path.join(d8, "rev.csv"),
           FOLLOW_CSV=os.path.join(d8, "follow.csv"), TWAP_CSV=os.path.join(d8, "tw.csv"),
           TWAP_CURVE_CSV=os.path.join(d8, "tc.csv"), REV_OUT_CSV=os.path.join(d8, "out.csv"),
           FOLLOW_OUT_CSV=os.path.join(d8, "fo.csv"))
bak = {"saved_at": time.time() - 30, "follow_open": {"fb": {"strategy": "F1_1хв", "coin": "AAA"}}}
json.dump(bak, open(ST8["STATE_FILE"] + ".bak", "w"))
for bad in ('{"saved_at": "x", "follow_open": {}}', '{"saved_at": 1, "rev_open": []}',
            '{"saved_at": 1, "sim_positions": {}}'):
    open(ST8["STATE_FILE"], "w").write(bad)
    L = mk_state_ns(ST8); L["load_state"]()
    assert set(L["follow_open"]) == {"fb"} and not os.path.exists(ST8["STATE_FILE"]), bad
# міграція: однойменний CSV у папці коду і в DATA_DIR
dc, dd = tempfile.mkdtemp(), tempfile.mkdtemp()
open(os.path.join(dc, "follow_trades.csv"), "w").write("a,b,eol\n1,2,^\n3,4,^\n")
open(os.path.join(dd, "follow_trades.csv"), "w").write("a,b,eol\n9,9,^")          # без \n у кінці
open(os.path.join(dc, "rev_trades.csv"), "w").write("x,y,eol\n1,1,^\n")
open(os.path.join(dd, "rev_trades.csv"), "w").write("x,y,z,eol\n1,1,1,^\n")     # інший заголовок
open(os.path.join(dc, "twap_signals.csv"), "w").write("q,eol\n5,^\n")            # у DATA_DIR нема
seg = src[src.index("if DATA_DIR != DIR:"):src.index("REFRESH_S = ")]
exec(seg, {"DIR": dc, "DATA_DIR": dd, "os": os, "time": time, "shutil": shutil,
           "print": lambda *a, **k: None})
assert open(os.path.join(dd, "follow_trades.csv")).read() == "a,b,eol\n9,9,^\n1,2,^\n3,4,^\n"
assert glob.glob(os.path.join(dd, "rev_trades.csv.legacy-migrated-*.csv"))
assert open(os.path.join(dd, "twap_signals.csv")).read() == "q,eol\n5,^\n"
assert not os.path.exists(os.path.join(dc, "follow_trades.csv")) and glob.glob(os.path.join(dc, "follow_trades.csv.migrated-*"))
# outbox: відмова запису сигнального рядка TWAP -> у чергу ретраю
TW = mk_tw(); r = reg(TW, A, now0 + 30)
TW["_strat_csv_append"] = lambda p, h, row: False
TW["_twap_drop"](r, "late")
assert r["sig_written"] == 1 and len(TW["_sig_retry_q"]) == 1 and TW["_sig_retry_q"][0][0] == "tws.csv"
# методика: пізній вихід R — лише для рядків ≥2.14; старі 2.13 не змінюються
d9 = tempfile.mkdtemp()
def rev_row(sig, m30, m31, algo):
    r_ = {k: "" for k in rh}
    r_.update({"sig_id": sig, "strategy": "R1_загальний", "date": "2026-09-01 10:00:00", "coin": "X",
               "our_side": "LONG", "entered": "1", "costs_pct": "0.15", "algo_v": algo,
               "m30": m30, "m31": m31, "eol": "^"})
    return ",".join(r_[k] for k in rh)
open(os.path.join(d9, "rev.csv"), "w").write(",".join(rh) + "\n" + rev_row("r1", "", "2.0", "2.13") + "\n"
    + rev_row("r2", "", "2.0", "2.17") + "\n")
o = mk_api(d=d9)["strat2_api"]()["strategies"]["R1_загальний"]
# v2.16: R since 2.16 (2.13 — legacy), пізній вихід поза заголовком (n=0), але у списку угод з net
# v2.20 (аудит v2.19 №10): старий рядок 2.13 (≥2.10) — теж у ЖУРНАЛІ paper (adm=1, поза заголовком без settlement)
_cur = [t for t in o["trades"] if not t.get("adm")]
assert o["n"] == 0 and o["n_late"] == 1 and len(o["trades"]) == 2 and len(_cur) == 1 and abs(_cur[0]["net30"] - 1.85) < 1e-9
print("8) поле не того типу -> .bak; однойменні CSV зливаються/лягають у legacy; outbox сигналів; late-exit R лише ≥2.14")


# ═══ 9. Рев'ю v2.15 (6 агентів): регресії на знахідки ═══════════════
# 9а) HIGH: дубль СКАСОВАНОГО твапу X з другого каналу не зливається у
# перестворену живу заявку Y (start +8с), і reply-скасування X не гасить Y
def _hl_start(usd, t0, dur=600, addr=A):
    return ("$%s selling HYPE 🟥 Frequency: $1 every 60 seconds (10 cycles) ETA: 9m "
            "Price: $85 User: " + addr + " Period: "
            + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0)) + " - "
            + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(t0 + dur)) + " UTC+0") % usd
def _x_start(usd, t0, addr=A):
    hh = time.gmtime(t0)
    return ("$%s продажа HYPE в течение 10 минут Цена: $85 Субъект: " + addr
            + " Создан в: %02d:%02d:%02d (UTC)") % (usd, hh.tm_hour, hh.tm_min, hh.tm_sec)
_X_CANCEL = ("❌ TWAP отменён (частично) Статус: terminated Исполнено: 0.00%% Размер: 0 / 60000 HYPE "
             "TwapId: %d Субъект: " + A + " Цена в начале: $83.83 Цена в конце: $83.69 🔴 (-0.16%%)")
def _mk9():
    TW = mk_tw(); rows = []
    TW["_sig_rows"] = rows
    TW["_strat_csv_append"] = lambda p, h, r: (rows.append((p, list(r))) or True)
    T = FakeTime(); TW["time"] = T
    return TW, T
TW, T = _mk9(); t0 = now0 - 40; T.now = now0; ex_reset(TW)
ex_twap(A, "HYPE", "sell", t0, 10, 60000, 1001, status="terminated", sp=0.0, t_rev=t0 + 8)
ex_twap(A, "HYPE", "sell", t0 + 8, 10, 60100, 1002, status="activated", sp=0.0)
TW["_twap_ingest"]("HL_TWAP", 11, t0 + 1, _hl_start("5.10M", t0), now0)
TW["_twap_ingest"]("HL_TWAP", 12, t0 + 9, _hl_start("5.11M", t0 + 8), now0)
rA = [r for r in TW["twap_reg"].values() if r["usd"] == 5.10e6][0]
rB = [r for r in TW["twap_reg"].values() if r["usd"] == 5.11e6][0]
assert rA["state"] == "dropped" and rA["twap_id"] == 1001 and rB["state"] == "watch" and rB["twap_id"] == 1002
TW["_twap_ingest"]("TWAPx", 21, t0 + 2, _x_start("5,102,000", t0), now0)      # дубль X з другого каналу
assert rB["posts"] == ["HL_TWAP/12"] and len(TW["twap_reg"]) == 3, rB["posts"]  # НЕ злито у Y — окремий запис
TW["_twap_ingest"]("TWAPx", 22, t0 + 9, _X_CANCEL % 1001, now0, reply="", reply_pid=21)
assert rB["state"] == "watch" and rB["cancelled"] == 0, (rB["state"], rB["reason"])   # Y живий
st9 = TW["twap_stats"]
assert st9["dup_posts"] == 1 and st9["starts"] == 2 and st9["cancelled"] == 1 and st9["dropped"] == 1, st9
assert [r[1][-3] for r in TW["_sig_rows"]] == ["cancelled"], TW["_sig_rows"]   # один сигнальний рядок
# 9б) MEDIUM: дедуп по id досяжний через реальний ingest (звірка з посту дає slice)
TW, T = _mk9(); t0 = now0 - 20; T.now = now0; ex_reset(TW)
ex_twap(A, "HYPE", "sell", t0, 10, 60000, 777, status="terminated", sp=0.0, t_rev=t0 + 5)
TW["_twap_ingest"]("HL_TWAP", 11, t0 + 1, _hl_start("5.10M", t0), now0)
TW["_twap_ingest"]("TWAPx", 21, t0 + 2, _x_start("5,102,000", t0), now0 + 10)
TW["_twap_tick"](now0 + 30)
st9 = TW["twap_stats"]
assert st9["starts"] == 1 and st9["cancelled"] == 1 and st9["dropped"] == 1 and st9["dup_posts"] == 1, st9
assert len(TW["_sig_rows"]) == 1
# 9в) MEDIUM: єдиний кандидат без id — лише у межах одиниці 3-ї цифри суми ($5.10m ≠ $5,113,000)
TW, T = _mk9(); t0 = now0 - 30; T.now = now0; ex_reset(TW)
ex_twap(A, "HYPE", "sell", t0 + 8, 10, 60100, 1002, status="activated", sp=0.0)
TW["_twap_ingest"]("TWAPx", 21, t0 + 9, _x_start("5,113,000", t0 + 8), now0)
rY = list(TW["twap_reg"].values())[0]; assert rY["state"] == "watch"
_hl_cancel = ("⛔️ $5.10M TWAP with HYPE closed by user 0.00%\nFilled: 0.00/60000.0 HYPE\nTime: x")
TW["_twap_ingest"]("HL_TWAP", 31, t0 + 10, _hl_cancel, now0, "$5.10M selling HYPE 🟥 User: " + A)
assert rY["state"] == "watch" and TW["twap_stats"].get("fin_nomatch", 0) == 1, (rY["state"], TW["twap_stats"])
# 9г) reply-запис з ІНШИМ id біржі, ніж у пості, — не гаситься
TW, T = _mk9(); t0 = now0 - 30; T.now = now0; ex_reset(TW)
ex_twap(A, "HYPE", "sell", t0, 10, 60100, 1002, status="activated", sp=0.0)
TW["_twap_ingest"]("TWAPx", 21, t0 + 1, _x_start("5,102,000", t0), now0)
rZ = list(TW["twap_reg"].values())[0]; assert rZ["twap_id"] == 1002
TW["_twap_ingest"]("TWAPx", 22, t0 + 9, _X_CANCEL % 1001, now0, reply="", reply_pid=21)
assert rZ["state"] == "watch" and TW["twap_stats"].get("post_id_mismatch") == 1
print("9а-г) ідентичність: дубль скасованого не зливається у перестворену заявку; дедуп по id досяжний; сума/id як запобіжники")

# 9д) заморожений trade_row старої ширини перебудовується і пишеться; twfail > WFAIL_CAP -> дроп
tr9 = fake_tr("w1", [0.1] * 60, now0 - 61 * 60 - 3); tr9["trade_row"] = ["old"] * 95
writes = []
run_loop({"w1|" + T1: tr9}, [99.0], now0, writes)
assert [w[0] for w in writes] == ["t"] and len(writes[0][1]) == len(th) - 1 and tr9["trade_written"] == 1
tr9b = fake_tr("w2", [0.1] * 60, now0 - 61 * 60 - 3); tr9b["trade_row"] = ["old"] * 95
ro9 = {"w2|" + T1: tr9b}
ns9 = run_loop(ro9, [99.0] * 6, now0, [], extra={"_fail_paths": {"t"}, "WFAIL_CAP": 3})
assert "w2|" + T1 not in ns9["rev_open"], ns9["rev_open"].keys()          # дроп після WFAIL_CAP, не вічний ретрай
# 9е) twap_gaps у state: збереження і відновлення
_sv9 = src[src.index("def _save_state_locked"):src.index("def load_state")]
assert 'snap["twap_gaps"]' in _sv9
d9 = tempfile.mkdtemp()
ST9 = dict(ST, STATE_FILE=os.path.join(d9, "state.json"), REV_CSV=os.path.join(d9, "r.csv"),
           FOLLOW_CSV=os.path.join(d9, "f.csv"), TWAP_CSV=os.path.join(d9, "t.csv"),
           TWAP_CURVE_CSV=os.path.join(d9, "tc.csv"), REV_OUT_CSV=os.path.join(d9, "o.csv"),
           FOLLOW_OUT_CSV=os.path.join(d9, "fo.csv"))
json.dump({"saved_at": time.time() - 5, "twap_last_ids": {"X": 240},
           "twap_gaps": {"X": [[100, 111, time.time() - 20]], "Y": [[1, 2, 3.0]]}},
          open(ST9["STATE_FILE"], "w"))
L9 = load({"load_state", "_csv_written_keys", "_tracker_csv_key"}, dict(ST9,
    watchlist_lock=threading.Lock(), watchlist={}, sim_lock=threading.Lock(),
    sim_positions={}, sim_trackers={}, sim_closed=[], alerts_lock=threading.Lock(),
    recent_alerts=[], sent_alerts=set(), fill_cursor={}, close_episodes={},
    fc_lock=threading.Lock(), fc_positions={}, fc_episodes={},
    strat2_lock=threading.RLock(), rev_open={}, follow_open={}, follow_last_close={},
    vault_cache={}, twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
    strat_activated={}, _twap_ch_state={"X": {"ok_ts": 0.0, "err": 0}}))
L9["load_state"]()
assert L9["_twap_ch_state"]["X"]["gaps"] == [[100, 111, L9["_twap_ch_state"]["X"]["gaps"][0][2]]] \
    and L9["twap_last_ids"]["X"] == 240
# 9є) відповідь на пост усередині недогорнутого пропуску відкладається до його закриття
pages9 = {171: (151, 170), 151: (131, 150), 131: (111, 130), 111: (101, 110)}
def fake_fetch9(ch, before=None):
    if before is None:
        return [(pid, now0 - 100, "txt", "", (105 if pid == 185 else 0)) for pid in range(171, 191)]
    if before not in pages9: raise NEW["APIError"]("no messages in page")
    lo, hi = pages9[before]
    return [(pid, now0 - 100, "txt", "", 0) for pid in range(lo, hi + 1)]
ing = []; cyc["n"] = 0
WT = load({"run_twap_watcher"}, dict(C, TWAP_ENABLED=True, TWAP_CHANNELS=("X",), TWAP_POLL_S=30,
    TWAP_MAX_DUR_S=900, TWAP_ENTRY_LEAD=60.0, TWAP_MIN_MOVE=1.0, TWAP_BACKLOG_S=3600,
    _twap_ch_state={"X": {"ok_ts": 0.0, "err": 0}}, twap_lock=threading.Lock(),
    twap_last_ids={"X": 100}, twap_stats={"fetch_err": 0, "parse_err": 0},
    _tme_fetch=fake_fetch9, _twap_tick=lambda now: None,
    _twap_ingest=lambda ch, pid, ts, text, now, reply, seen=False, reply_pid=0: ing.append((cyc["n"], pid, seen)),
    time=type("T", (), {"time": staticmethod(time.time), "sleep": staticmethod(_sleep7)})()))
try: WT["run_twap_watcher"]()
except _Stop: pass
assert not [1 for c, p, s in ing if c == 1 and p == 185], "185 (reply->105 у пропуску) має чекати"
assert [(c, s) for c, p, s in ing if p == 185] == [(2, False), (3, True)], [(c, s) for c, p, s in ing if p == 185]
assert WT["_twap_ch_state"]["X"]["deferred"] == [] and WT["twap_last_ids"]["X"] == 190
print("9д-є) trade_row старої ширини перебудовано, дроп після WFAIL_CAP; twap_gaps у state; відкладена відповідь у пропуск")

print("\nALL v2.15 TESTS PASSED")

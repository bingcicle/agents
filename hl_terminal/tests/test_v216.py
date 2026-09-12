import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.16 — чесний paper-вхід/вихід, settlement, R8, епізодне правило
реверсу, грейс, періоди, когорти дампу, фікси рев'ю:
1 стакан: VWAP на $1000, partial, збій → None; _paper_px book/mid/mid+whale/none
2 follow_on_txs: вік філа (гейт 20 с), lag/detect_src/entry_src/grace/costs у
  трекері, full_close → force_exit_ts/force_fill_ts_ms; _fol_row ширина
3 rev_on_close: епізод (ref до першого філа, bucket, dur), стратегії R1/R2/R8,
  tp_px, довгий епізод → лише outcome, старий філ → outcome, busy = закрита
  заголовкова угода; _rev_row/_rev_out_row ширини; _twap_enter зі стакану
4 цикл трекерів: R8 TP зі стакану, m30 таймер зі стакану, m31 timer_late,
  no_price, R2 armed→open гірша з (стакан, тригер), follow-вихід зі стакану
  (full_close_late, silence), TWAP-вихід зі стакану у рядок угоди
5 витрати по обох боках стакану; _p_costs
6 API: офіційний = min(live, стрічка), пізні поза заголовком, грейс, без ціни,
  когорти дампу, періоди, допуск старих R-рядків за settlement, TWAP official,
  n_trades_total, settle-блок
7 settle.py: референс дампу зі стрічки (знак «у бік тиску»), заголовки
8 структурні: fast-retry/чанки sweep, поллер, /status, watchdog, UI, версії"""
import ast, threading, time, json, os, re, sys, math, tempfile, csv, glob, hashlib, shutil
import time as _tmod
import urllib.error, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HL)
from v28_shim import NEW, _median

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

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

R8 = "R8_тп80"
C = dict(PROFILE_ALGO_V=10, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.19",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005, SIM_SPREAD=0.0002,
         SIM_POSITION_USD=1000.0,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True,
         F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0, F5_NAME="F5_перший",
         F5_FIRST_SHOT_S=3600.0, F7_NAME="F7_без_ратіо", RATIO_GRACE_S=1800,
         FC_ENABLED=True, FC_MAX_EPISODE_S=300, R7_NAME="R7_одним", R7_MIN_TX_USD=100_000.0,
         R8_NAME=R8, R8_TP_FRAC=0.8, REV_HOLD_MIN=30, REV_EP_MAX_S=300.0, REV_EP_MIN_MAG=1.0,
         FOLLOW_MAX_FILL_AGE_S=20.0, REV_MAX_FILL_AGE_S=60.0, BOOK_MAX_AGE_S=5.0, REV_REF_TOL_S=12.0)
assert const("DATA_ALGO_V") == "2.20" and const("R8_NAME") == R8 and const("REV_HOLD_MIN") == 30
assert const("REV_EP_MAX_S") == 300.0 and const("REV_EP_MIN_MAG") == 1.0
assert const("FOLLOW_MAX_FILL_AGE_S") == 20.0 and const("REV_MAX_FILL_AGE_S") == 60.0
assert const("DEPTH_LIMIT") == 1000 and const("FAST_RETRY_MAX") == 3 and const("FAST_SWEEP_CHUNK") == 20
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
          assigns=("FOLLOW_HEADERS", "REV_HEADERS", "TWAP_HEADERS", "REV_OUT_HEADERS",
                   "FOLLOW_OUT_HEADERS", "TWAP_CURVE_HEADERS", "REV_SIG_HEADERS", "STRAT_SINCE",
                   "TAPE_SINCE", "STRAT2_TITLES", "STRAT2_DESC", "SYMBOL_MAP"))
fh, rh, th, oh, foh, sh = (FH["FOLLOW_HEADERS"], FH["REV_HEADERS"], FH["TWAP_HEADERS"],
                           FH["REV_OUT_HEADERS"], FH["FOLLOW_OUT_HEADERS"], FH["REV_SIG_HEADERS"])
assert len(rh) == 54 + 60 + 1 and len(fh) == 62 and len(th) == 53 + 60 + 1 and len(sh) == 27   # v2.19: +5 виконання у R/F/TWAP
assert len(oh) == 31 + 60 + 1 and len(foh) == 25 + 60 + 1
for k in ("entry_ts_ms", "fill_ts_ms", "lag_s", "detect_src", "entry_src", "entry_px_mid", "px_age_ms",
          "whale_px", "grace", "dump_first_ts_ms", "dump_last_ts_ms", "dump_dur_s", "dump_move_pct",
          "dump_bucket", "exit_min", "exit_reason", "exit_ts_ms", "exit_px", "exit_src", "tp_px"):
    assert k in rh, k
assert rh.index("algo_v") + 1 == rh.index("entry_ts_ms") and rh.index("tp_px") + 1 == rh.index("exch") and rh.index("trig_hash") + 1 == rh.index("m1")
assert all(FH["STRAT_SINCE"][k] == "2.17" for k in ("R1_загальний", "R3_великі",
                                                      "R4_великий", "R5_дуже", "R6_волт", "R7_одним"))
assert FH["STRAT_SINCE"]["R2_breakout"] == "2.20" and FH["STRAT_SINCE"][R8] == "2.20"   # v2.20: тригер/TP по Binance
assert FH["STRAT_SINCE"]["F1_1хв"] == "2.10" and FH["STRAT_SINCE"]["T1_твап_відкриття"] == "2.15"
assert R8 in FH["STRAT2_TITLES"] and R8 in FH["STRAT2_DESC"] and FH["SYMBOL_MAP"]["KLUNC"] == "1000LUNCUSDT"
A = "0x" + "a" * 40
now0 = time.time()

# ═══ 1. Стакан і ціна paper-угоди ════════════════════════════════════
BOOK = {"levels": [[{"px": "99.9", "sz": "3"}, {"px": "99.5", "sz": "20"}],      # bids
                   [{"px": "100.0", "sz": "5"}, {"px": "100.5", "sz": "20"}]],   # asks
        "time": 1700000000000}
calls = []
def fake_prio(body, retries=2, direct=False, max_wait=3.0):
    calls.append(body)
    if BOOK.get("fail"):
        raise RuntimeError("boom")
    if not BOOK.get("stale"):
        BOOK["time"] = int(time.time() * 1000)   # свіже котирування
    return BOOK
st1 = {}
B = load({"hl_book_exec", "_book_walk", "_book_quote", "_px_mid_age", "_paper_px", "_raw_book_get", "_raw_book_put",
          "_book_from_raw"},
         dict(C, hl_post_prio=fake_prio, _prio_opener=None, stats=st1, _raw_book_cache={}, BOOK_RAW_TTL_S=0.0,
              px_lock=threading.Lock(), px_hist={"AAA": [(now0 - 1.0, 100.2)]}))   # v2.19: без сирого кешу — кожен виклик = запит
r = B["hl_book_exec"]("AAA", "BUY")
# $1000 BUY: 5×100 = $500, решта $500 по 100.5 → qty 5 + 4.975...
qty = 5 + 500 / 100.5
assert abs(r["px"] - 1000 / qty) < 1e-9 and r["partial"] == 0 and abs(r["mid"] - 99.95) < 1e-9
assert r["bid"] == 99.9 and r["ask"] == 100.0 and calls[-1] == {"type": "l2Book", "coin": "AAA"}
r = B["hl_book_exec"]("AAA", "SELL")   # біди: 3×99.9 = 299.7, решта по 99.5
qty = 3 + (1000 - 299.7) / 99.5
assert abs(r["px"] - 1000 / qty) < 1e-9 and r["px"] < 99.9
r = B["hl_book_exec"]("AAA", "BUY", usd=5000)   # стакан тонший за розмір → partial: гірший пройдений рівень
assert r["partial"] == 1 and st1["book_ok"] == 3 and r["px"] == 100.5
BOOK["stale"] = True; BOOK["time"] = int(time.time() * 1000) - 9000   # котирування 9 с — не «зараз»
assert B["hl_book_exec"]("AAA", "BUY") is None and st1["book_stale"] == 1
BOOK["stale"] = False
BOOK["fail"] = True
assert B["hl_book_exec"]("AAA", "BUY") is None and st1["book_fail"] == 1
# _paper_px: стакан недоступний → мід; гірша з міда і ціни філа кита
px, s_, meta = B["_paper_px"]("AAA", "BUY", whale_px=100.9)
assert (px, s_) == (100.9, "mid+whale") and meta["mid"] == 100.2 and meta["px_age_ms"] < 5000
px, s_, meta = B["_paper_px"]("AAA", "SELL", whale_px=100.9)
assert (px, s_) == (100.2, "mid")            # для SELL гірша = нижча = мід
px, s_, meta = B["_paper_px"]("AAA", "SELL", whale_px=99.0)
assert (px, s_) == (99.0, "mid+whale")
B["px_hist"]["AAA"] = [(now0 - 40.0, 100.2)]   # мід старший за 20 с → нема ціни
assert B["_paper_px"]("AAA", "BUY")[:2] == (None, "none")
BOOK["fail"] = False
px, s_, meta = B["_paper_px"]("AAA", "BUY", whale_px=200.0)
assert s_ == "book" and abs(px - 1000 / (5 + 500 / 100.5)) < 1e-9 and meta["book_mid"] == 99.95
assert meta["quote_ts_ms"] == BOOK["time"]
# строгий режим (входи, аудит-2 №1): без стакану — no_book, мід і ціна філа не приймаються
BOOK["fail"] = True
assert B["_paper_px"]("AAA", "BUY", whale_px=100.9, strict=True)[:2] == (None, "no_book")
BOOK["fail"] = False
BOOK["levels"][1] = [{"px": "100.0", "sz": "1"}]   # $100 асків: partial → строго = нема стакану
assert B["_paper_px"]("AAA", "BUY", strict=True)[:2] == (None, "no_book") and st1["book_partial_skips"] == 1
assert B["_paper_px"]("AAA", "BUY")[1] == "book_partial"
BOOK["levels"][1] = [{"px": "100.0", "sz": "5"}, {"px": "100.5", "sz": "20"}]
BOOK["levels"][0] = []   # порожні біди: BUY працює (наш бік — аски), SELL — ні
assert B["hl_book_exec"]("AAA", "BUY") is not None and B["hl_book_exec"]("AAA", "SELL") is None
BOOK["levels"][0] = [{"px": "99.9", "sz": "3"}, {"px": "99.5", "sz": "20"}]
print("1) стакан: VWAP по рівнях на $1000, partial, збій; _paper_px book/mid/mid+whale/none")

# ═══ 2. follow_on_txs: вік філа, поля v2.16, грейс, витрати ═══════════
DEPTH = {"AAA": {"bid": 1e5, "ask": 2e4, "max": 1e5}}
def mk_fol(book_px=99.5, last_close=None):
    st = {}
    n = load({"follow_on_txs", "_paper_px", "_px_mid_age", "_sim_costs", "_sim_slip", "depth_for_side",
              "_fol_row", "_ms", "_rnd", "_p_costs"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={}, rev_open={},
        follow_last_close=({} if last_close is None else dict(last_close)),
        wallet_profiles={}, profiles_fetching=set(), stats=st,
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: None, _dt=_dt, _sig_seq=[0],
        hl_book_exec=(lambda coin, side, usd=None, qty=None, min_recv_ms=None: ({"px": book_px, "mid": 100.0, "bid": 99.9,
                                                     "ask": 100.1, "ts_ms": 0, "req_ms": 12, "partial": 0}
                                                    if book_px else None)),
        px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 2.0, 100.0)]},
        cache={"depth": DEPTH, "depth_prev": {}}, cache_lock=threading.Lock()))
    return n, st
OLD_F = {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5}
def opened(n): return sorted(p["strategy"] for p in n["follow_open"].values())
# (а) старий філ (25 с) → paper-вхід пропущено, лічильник; мітка пари стоїть
n, st = mk_fol()
old_ms = int((time.time() - 25) * 1000)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": old_ms, "hash": "0x1"}], False,
                   detect_src="sweep")
assert n["follow_open"] == {} and st["follow_stale_skips"] == 1 and A + ":AAA" in n["follow_last_close"]
# (б) свіжий філ (2 с): вхід зі стакану (SELL → 99.5), lag/джерела/грейс/витрати у трекері
n, st = mk_fol()
fresh_ms = int((time.time() - 2) * 1000)
n["follow_on_txs"](A, "AAA", dict(OLD_F, ratio=1.5, ratio_hi_ts=time.time() - 100),
                   [{"sz": 100.0, "px": 100.4, "ts": fresh_ms, "hash": "0x2"}], False, detect_src="ws")
assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший"], opened(n)
p = next(iter(n["follow_open"].values()))
assert p["our_side"] == "SHORT" and p["entry_px"] == 99.5 and p["entry_src"] == "book"
assert p["fill_ts_ms"] == fresh_ms and 1.5 <= p["lag_s"] <= 4 and p["detect_src"] == "ws"
assert p["whale_px"] == 100.4 and p["grace"] == 1 and p["entry_px_mid"] == 100.0
# витрати по обох боках: SHORT вхід у біди 1e5, вихід з асків 2e4
slip = lambda d: 0.0002 + 1000 / d * 0.005
exp_costs = (2 * 0.0005 + slip(1e5) + slip(2e4)) * 100.0
assert abs(p["costs"] - exp_costs) < 1e-9 and abs(p["open_ts"] - time.time()) < 5
# (в) стакан недоступний → paper-входу НЕМАЄ (аудит-2 №1: мід/ціна філа — не доказ), лічильник
n, st = mk_fol(book_px=None)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 99.2, "ts": fresh_ms, "hash": "0x3"}], False)
assert n["follow_open"] == {} and st["follow_no_book"] == 1 and A + ":AAA" in n["follow_last_close"]
# (в2) свіжість — за ТРИГЕРНОЮ транзакцією (аудит-2 №2): великий шматок 100 с + дрібний 1 с → пропуск
n, st = mk_fol()
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": old_ms - 75000, "hash": "0x5"},
                                     {"sz": 0.99, "px": 100.0, "ts": fresh_ms, "hash": "0x6"}], False)
assert n["follow_open"] == {} and st["follow_stale_skips"] == 1
n, st = mk_fol(book_px=99.3)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 99.2, "ts": fresh_ms, "hash": "0x3"}], False)
p = next(iter(n["follow_open"].values()))
assert p["entry_px"] == 99.3 and p["entry_src"] == "book"
# (г) повне закриття: force_exit + час, коли дізнались, + час останнього філа
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 900.0, "px": 99.0, "ts": fresh_ms + 500, "hash": "0x4"}], True)
assert p["force_exit"] == "full_close" and p["force_fill_ts_ms"] == fresh_ms + 500 \
    and abs(p["force_exit_ts"] - time.time()) < 5
# (д) _fol_row: ширина = FOLLOW_HEADERS-1, нові поля на місцях
p["exit_src"] = "book"; p["exit_px_mid"] = 98.9
row = n["_fol_row"](p, "t-1", p["open_ts"] + 61, 98.8, "full_close", 0.4)
assert len(row) == len(fh) - 1
g = lambda k: row[fh.index(k)]
assert g("exit_reason") == "full_close" and g("exit_px") == 98.8 and g("exit_src") == "book"
assert g("fill_ts_ms") == fresh_ms and g("detect_src") == "sweep" and g("grace") == 0
assert g("entry_src") == "book" and g("whale_px") == 99.2 and g("open_ts_ms") == int(round(p["open_ts"] * 1000))
assert g("close_ts_ms") == int(round((p["open_ts"] + 61) * 1000)) and abs(g("costs_pct") - p["costs"]) < 1e-9
assert g("trade_id") == "t-1" and g("hold_s") == 61.0
# аудит-2 №12: обидві ноги по стакану → net = gross − лише комісія (сліпаж уже в цінах)
assert abs(g("net_pct") - (0.4 - 0.1)) < 1e-9
p["exit_src"] = "mid"
row2 = n["_fol_row"](p, "t-1", p["open_ts"] + 61, 98.8, "full_close", 0.4)
slip_each = (p["costs"] - 0.1) / 2
assert abs(row2[fh.index("net_pct")] - (0.4 - 0.1 - slip_each)) < 1e-9
print("2) follow: гейт віку філа 20 с, вхід зі стакану/гірша з міда й філа, lag/грейс/витрати, _fol_row")

# ═══ 3. rev_on_close: епізод, R8, довгий/старий епізод, busy; ширини рядків ═══
def mk_rev(ref_px=102.0, book_px=100.3, ep=None, rev_open=None):
    st, calls_ = {}, []
    n = load({"rev_on_close", "_rev_ref_px", "_paper_px", "_px_mid_age", "_rev_trade_closed",
              "_sim_costs", "_sim_slip", "depth_for_side", "_rev_row", "_rev_out_row", "_rev_samples",
              "_rev_extra", "_ms", "_rnd", "_p_costs"}, dict(C,
        strat2_lock=threading.RLock(), rev_open=(rev_open if rev_open is not None else {}),
        fc_lock=threading.Lock(), fc_episodes=({} if ep is None else {(A, "AAA"): ep}),
        is_vault=lambda a: False, stats=st,
        _px_now=lambda c, max_age=None: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (101.0 if c != "BTC" else 50000.0),
        _px_at=lambda c, ts, tol=90.0: ref_px,
        _sim_depth=lambda c, s=None: 1e5,
        _dt=_dt, _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
        REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=sh,
        _strat_csv_append=lambda p, h, r: calls_.append((p, r)) or True,
        hl_book_exec=(lambda coin, side, usd=None, qty=None, min_recv_ms=None: ({"px": book_px, "mid": 100.05, "bid": 100.0,
                                                     "ask": 100.1, "ts_ms": 0, "req_ms": 9, "partial": 0}
                                                    if book_px else None)),
        px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 1.0, 100.0)]},
        cache={"depth": DEPTH, "depth_prev": {}}, cache_lock=threading.Lock()))
    return n, st, calls_
OLD_R = {"size": 1000.0, "side": "LONG", "val": 150_000, "ratio": 3}
t_now_ms = int(time.time() * 1000)
EP = {"first_ts": t_now_ms - 90_000, "first_px": 101.8, "last_ts": t_now_ms - 3000, "sum_usd": 150_000,
      "seen": [], "side": "LONG", "ratio": 3, "val": 150_000, "start_size": 1000.0,
      "max_sz": 600.0, "max_usd": 60_000, "max_liq": 0}
FILLS = [{"sz": 400.0, "px": 100.0, "hash": "0x9", "dir": "Close Long", "ts": t_now_ms - 3000}]
def strats_of(n): return {p["strategy"] for p in n["rev_open"].values()}
# (а) епізод 90 с: ref 102 (мід до першого філа) → останній філ 100 = −1.96% → R1/R2/R8, bucket 2
n, st, sig = mk_rev(ep=EP)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True, detect_src="ws")
assert strats_of(n) == {"_OUTCOME", "R1_загальний", "R2_breakout", R8}, strats_of(n)
r1 = [p for p in n["rev_open"].values() if p["strategy"] == "R1_загальний"][0]
mag = -(100.0 / 102.0 - 1.0) * 100.0
assert abs(r1["move_ep"] - mag) < 1e-9 and abs(r1["dump_move"] + mag) < 1e-9 and r1["dump_bucket"] == 2
assert abs(r1["dump_dur"] - 87.0) < 1e-6 and r1["dump_first_ts"] == EP["first_ts"] and r1["dump_last_ts"] == FILLS[0]["ts"]
assert r1["entry_px"] == 100.3 and r1["entry_src"] == "book" and r1["detect_src"] == "ws" and r1["grace"] == 0
assert r1["fill_ts_ms"] == FILLS[0]["ts"] and 2 <= r1["lag_s"] <= 5 and r1["whale_px"] == 100.0
assert abs(r1["move"] - (-(100.0 / 101.0 - 1) * 100)) < 1e-9      # довідковий 3-хв рух по кешу
r8 = [p for p in n["rev_open"].values() if p["strategy"] == R8][0]
assert abs(r8["tp_px"] - 100.3 * (1 + 0.8 * mag / 100.0)) < 1e-9 and r8["state"] == "open"
assert "costs" in r1 and abs(r1["costs"] - (2 * 0.0005 + slip(2e4) + slip(1e5)) * 100) < 1e-9   # LONG: вхід аски, вихід біди
assert len(sig) == 1 and len(sig[0][1]) == len(sh) - 1
sg = lambda k: sig[0][1][sh.index(k)]
assert sg("dump_bucket") == 2 and abs(sg("dump_dur_s") - 87.0) < 1e-6 and sg("detect_src") == "ws" and sg("grace") == 0
assert sg("opened") and "R8" in sg("opened") and abs(float(sg("dump_move_pct")) + round(mag, 3)) < 1e-6
# _rev_row / _rev_out_row: ширина і нові поля
r1["exit_reason"] = "timer"; r1["exit_min"] = 30; r1["exit_px"] = 101.0; r1["exit_src"] = "book"; r1["exit_ts_ms"] = 5
row = n["_rev_row"](r1, 1)
assert len(row) == len(rh) - 1
rg = lambda k: row[rh.index(k)]
assert rg("exit_px") == 101.0 and rg("exit_reason") == "timer" and rg("dump_bucket") == 2 and rg("entry_src") == "book"
assert rg("dump_first_ts_ms") == EP["first_ts"] and rg("fill_ts_ms") == FILLS[0]["ts"] and rg("grace") == 0
assert rg("tp_px") == "" and rg("entry_ts_ms") == int(round(r1["entry_ts"] * 1000))
outp = [p for p in n["rev_open"].values() if p["strategy"] == "_OUTCOME"][0]
orow = n["_rev_out_row"](outp)
assert len(orow) == len(oh) - 1 and orow[oh.index("dump_bucket")] == 2 and orow[oh.index("entry_src")] == "book"
# (б) одним пострілом: перший філ = останній, ref — мід до нього (одна tx уже рухає ціну)
n, st, sig = mk_rev(ref_px=101.5, ep=None)
n["rev_on_close"](A, "AAA", OLD_R, [{"sz": 1000.0, "px": 100.0, "hash": "0x1", "dir": "Close Long",
                                      "ts": t_now_ms - 2000}], True)
assert "R7_одним" in strats_of(n) and "R1_загальний" in strats_of(n)
r7 = [p for p in n["rev_open"].values() if p["strategy"] == "R7_одним"][0]
assert r7["dump_bucket"] == 1 and r7["dump_dur"] == 0.0 and abs(r7["move_ep"] - (-(100 / 101.5 - 1) * 100)) < 1e-9
# (в) без семпла до першого філа — референс = ціна першого філа (0% → нема сигналу)
n, st, sig = mk_rev(ref_px=None, ep=None)
n["rev_on_close"](A, "AAA", OLD_R, [{"sz": 1000.0, "px": 100.0, "hash": "0x1", "dir": "Close Long",
                                      "ts": t_now_ms - 2000}], True)
assert n["rev_open"] == {} and sig == []
# (г) епізод 6 хв → лише outcome (bucket 0); падіння 1.96% є, але довго
n, st, sig = mk_rev(ep=dict(EP, first_ts=t_now_ms - 360_000))
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert strats_of(n) == {"_OUTCOME"} and next(iter(n["rev_open"].values()))["dump_bucket"] == 0
assert sig[0][1][sh.index("dump_bucket")] == 0
# (д) 2%+ → R4; 1% рівно з епсилоном
n, st, sig = mk_rev(ref_px=102.5, ep=EP)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert "R4_великий" in strats_of(n) and "R5_дуже" not in strats_of(n)
n, st, sig = mk_rev(ref_px=100.0 / 0.99, ep=EP)   # рівно 1.0%
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert "R1_загальний" in strats_of(n), strats_of(n)
# (е) старий філ (>60 с) → лише outcome + лічильник; ratio<2 → grace=1 у трекері
n, st, sig = mk_rev(ep=dict(EP, last_ts=t_now_ms - 70_000))
n["rev_on_close"](A, "AAA", dict(OLD_R, ratio=1.4, ratio_hi_ts=time.time() - 300),
                  [dict(FILLS[0], ts=t_now_ms - 70_000)], True)
# v2.19 (рев'ю №8): запізніла детекція не породжує й тіньовий рядок — лише сигнальний рядок і лічильник
assert strats_of(n) == set() and st["rev_stale_skips"] == 1
assert sig[0][1][sh.index("grace")] == 1
# (є) busy: заголовкова R1-угода закрита (30 семплів) → нова R1 відкривається;
#     10 семплів → зайнята (лічильник); з exit_reason (TP) — теж вільна
def rtr(strat, samples, exit_reason=""):
    # entry_ts узгоджений із кількістю семплів: 10 семплів = 10 хв тому (угода ще триває)
    return {"sig_id": "old", "strategy": strat, "coin": "AAA", "state": "open",
            "entry_ts": time.time() - len(samples) * 60 - 5, "samples": samples, "exit_reason": exit_reason}
n, st, sig = mk_rev(ep=EP, rev_open={"old|R1_загальний": rtr("R1_загальний", [0.1] * 30),
                                     "old|R8_тп80": rtr(R8, [0.1] * 5, "tp"),
                                     "old|R2_breakout": rtr("R2_breakout", [0.1] * 10)})
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
new = {p["strategy"] for k, p in n["rev_open"].items() if not k.startswith("old|")}
assert new == {"_OUTCOME", "R1_загальний", R8} and st["rev_busy_skips"] == 1, (new, st)
RT = n["_rev_trade_closed"]
assert RT(rtr("R1", [0.1] * 30), time.time()) and RT(rtr("R1", [], "tp"), time.time())
assert not RT(rtr("R1", [0.1] * 29), time.time() - 60) and not RT({"state": "armed"}, time.time())
assert RT({"state": "open", "entry_ts": time.time() - 30 * 60 - 40, "samples": []}, time.time())
# (ж) стакан недоступний і мід старий → нема ціни входу → сигнал не пишеться, лічильник
n, st, sig = mk_rev(book_px=None, ep=EP)
n["px_hist"]["AAA"] = [(time.time() - 60, 100.0)]
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert n["rev_open"] == {} and st["rev_no_px"] == 1
# (з) _twap_enter: ціна входу зі стакану, витрати по обох боках
TWE = load({"_twap_enter"}, dict(C,
    T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення", TWAP_COHORTS=(1.0, 1.5, 2.0),
    _twap_cohort_name=lambda b, t: b + {1.0: "", 1.5: "_15", 2.0: "_20"}[t],
    _twap_trade_closed=lambda p, now: False, _sim_depth=lambda c, s=None: 1e5,
    _sim_costs=lambda coin, side: (0.17, 1e5, 2e4), _px_now=lambda c: 100.0, _px_ago=lambda c, s: 100.0,
    _twap_drop=lambda rec, why: rec.update(state="dropped", reason=why), _twap_sig_write=lambda r, w: None,
    twap_stats={"entered": 0, "busy_cohorts": 0}, save_state=lambda: None, rev_open={},
    strat2_lock=threading.RLock(), TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60,
    _paper_px=lambda coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, **kw: (98.7, "book", {"mid": 100.0, "px_age_ms": 300})))
rec = {"id": "tw1", "kind": "open", "side": "buy", "coin": "AAA", "addr": A, "src": "TWAPx", "usd": 1e5,
       "dur": 600, "pos_usd": 0, "move": 1.2, "p0": 100, "p1": 101.2, "state": "watch"}
TWE["_twap_enter"](rec, time.time(), 100.0)
tw = next(iter(TWE["rev_open"].values()))
assert tw["entry_px"] == 98.7 and tw["entry_src"] == "book" and tw["costs"] == 0.17 and tw["side"] == "SHORT"
assert rec["state"] == "entered" and rec["entry_px"] == 98.7
print("3) rev: епізод (мід до 1-го філа, bucket, dur), R1/R2/R8+tp_px, довгий/старий → outcome, busy=закрита угода, ширини; TWAP-вхід зі стакану")

# ═══ 4. Цикл трекерів: виходи зі стакану ═════════════════════════════
class FakeTime:
    def __init__(self): self.now = _tmod.time()
    def time(self): return self.now
    sleep = staticmethod(_tmod.sleep); localtime = staticmethod(_tmod.localtime)
    strftime = staticmethod(_tmod.strftime); gmtime = staticmethod(_tmod.gmtime)
    mktime = staticmethod(_tmod.mktime); strptime = staticmethod(_tmod.strptime)
class _Stop(Exception): pass
XBOOK = {"px": 101.9, "src": "book", "mid": 101.8}
def run_loop(rev_open, follow_open, px_seq, t_start, writes, extra=None):
    T_ = FakeTime(); T_.now = t_start
    ticks = {"i": 0}
    def _sleep(x):
        ticks["i"] += 1
        if ticks["i"] > len(px_seq): raise _Stop()
        T_.now += 3
    T_.sleep = _sleep
    def _append(p, hh, r):
        if len(r) != len(hh) - 1: return False
        writes.append((p, list(r))); return True
    pp = []
    def _paper(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, **kw):
        pp.append((coin, side))
        if XBOOK.get("none"):
            # стакану немає: нестрогий _paper_px віддає СВІЖИЙ мід (≤35 с) або нічого
            fm = XBOOK.get("fallback_mid")
            if fm and not strict:
                return fm, "mid", {"mid": fm, "px_age_ms": 5000}
            return None, "none", {"mid": None, "px_age_ms": None}
        return XBOOK["px"], XBOOK["src"], {"mid": XBOOK["mid"], "px_age_ms": 200}
    ns = load({"run_strat2_loop", "_rev_row", "_rev_samples", "_rev_out_row", "_fol_out_row", "_fol_row",
               "_rev_extra", "_ms", "_rnd", "_p_costs", "_twap_row", "_twap_exit", "_twap_curve_row",
               "_twap_close_min", "_twap_trade_closed", "_twap_freeze_exit", "_rev_trade_closed"}, dict(C,
        time=T_, strat2_lock=threading.RLock(), rev_open=rev_open, follow_open=follow_open,
        follow_last_close={}, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _px_now=lambda c, max_age=30.0: px_seq[min(ticks["i"], len(px_seq) - 1)],
        _strat_csv_append=_append, save_state=lambda: None, _paper_px=_paper,
        _sim_slip=lambda d: 0.0005, _dt=_dt, REV_CSV="r", REV_HEADERS=rh, REV_SIG_CSV="s",
        REV_SIG_HEADERS=[], REV_OUT_CSV="o", REV_OUT_HEADERS=oh, FOLLOW_CSV="f",
        FOLLOW_HEADERS=fh, FOLLOW_OUT_CSV="fo", FOLLOW_OUT_HEADERS=foh, TWAP_CSV="t",
        TWAP_HEADERS=th, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_EXIT_CAP_MIN=5,
        TWAP_CURVE_CSV="tc", TWAP_CURVE_HEADERS=FH["TWAP_CURVE_HEADERS"],
        stats={"delta_events": 0}, **{k: v for k, v in (extra or {}).items() if k != "follow_last_close"}))
    if extra and "follow_last_close" in extra:
        ns["follow_last_close"].update(extra["follow_last_close"])
    ns["_paper_calls"] = pp
    try:
        ns["run_strat2_loop"]()
    except _Stop:
        pass
    return ns
def rev_tr(strat, entry_ts, samples, side="LONG", state="open", **kv):
    d = {"sig_id": "s1", "strategy": strat, "coin": "AAA", "side": side, "addr": A, "src": "wallet",
         "detect_ts": entry_ts, "detect_px": 100.0, "entry_ts": entry_ts, "entry_px": 100.0, "state": state,
         "move": 1.96, "dur": 87.0, "sum_usd": 1e5, "usd_s": 1e3, "ratio": 3.0, "shtanga": 0, "vault": 0,
         "hour": 10, "btc_move": 0.01, "depth": 1e5, "algo_v": "2.19", "samples": list(samples),
         "peak": -999.0, "trough": 999.0}
    d.update(kv); return d
# (а) R8: мід перетнув TP (101.6) → вихід зі стакану (SELL 101.9), лімітка не краще за TP → 101.6
# цикл спить 3 с ПЕРЕД першим тиком → перше рішення у t0+3
t0 = now0; T1s = t0 + 3
ro = {"s1|" + R8: rev_tr(R8, t0 - 95, [0.2], tp_px=101.6)}
ns = run_loop(ro, {}, [101.7, 101.7], t0, [])
p = ro["s1|" + R8]
assert p["exit_reason"] == "tp" and p["exit_px"] == 101.6 and p["exit_src"] == "book" and 1.5 <= p["exit_min"] <= 1.7
assert p["exit_ts_ms"] == int(round(T1s * 1000)) and not p.get("done") and ns["_paper_calls"][0] == ("AAA", "SELL")
assert ns["_rev_trade_closed"](p, t0)
# SHORT R8: TP нижче, стакан BUY 98.3 гірший за TP 98.5 → max(98.3, 98.5) = 98.5
XBOOK.update(px=98.3, mid=98.4)
ro = {"s1|" + R8: rev_tr(R8, t0 - 95, [0.2], side="SHORT", tp_px=98.5)}
ns = run_loop(ro, {}, [98.4, 98.4], t0, [])
assert ro["s1|" + R8]["exit_px"] == 98.5 and ns["_paper_calls"][0] == ("AAA", "BUY")
# TP не перетнуто → нема виходу; TP після 30 хв не діє (таймер)
ro = {"s1|" + R8: rev_tr(R8, t0 - 95, [0.2], side="SHORT", tp_px=98.5)}
run_loop(ro, {}, [98.6, 98.6], t0, [])
assert not ro["s1|" + R8].get("exit_reason")
# (б) таймер m30: 29 семплів, 30-та хвилина настає → exit timer, ціна зі стакану
XBOOK.update(px=100.9, mid=100.95)
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 30 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [101.0, 101.0], t0, [])
p = ro["s1|R1_загальний"]
assert len(p["samples"]) == 30 and p["exit_reason"] == "timer" and p["exit_min"] == 30 and p["exit_px"] == 100.9
# стакан недоступний → мід (src mid)
XBOOK["none"] = True; XBOOK["fallback_mid"] = 101.0
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 30 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [101.0, 101.0], t0, [])
assert ro["s1|R1_загальний"]["exit_px"] == 101.0 and ro["s1|R1_загальний"]["exit_src"] == "mid"
# аудит-3 №11: ні стакану, ні СВІЖОГО міда → без фолбеку на мід із моменту рішення: виходу нема, повтор через 15 с
XBOOK["fallback_mid"] = None
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 30 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [101.0, 101.0], t0, [])
p = ro["s1|R1_загальний"]
assert "exit_px" not in p and not p.get("exit_reason") and p.get("exit_retry_at", 0) > t0, p.get("exit_retry_at")
XBOOK["none"] = False; XBOOK["fallback_mid"] = None
# (в) m30 без ціни, m31 з ціною → timer_late на 31; >33 без ціни → no_price
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 31 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [101.0, 101.0], t0, [])
p = ro["s1|R1_загальний"]
assert p["samples"][29] == "" and p["exit_reason"] == "timer_late" and p["exit_min"] == 31
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 35 * 60 - 5, [0.1] * 29)}   # m30..m34 без ціни
run_loop(ro, {}, [None, None], t0, [])
p = ro["s1|R1_загальний"]
assert p["exit_reason"] == "no_price" and p["exit_min"] == "" and p.get("exit_px") is None
# (г) R2 armed: тригер 100.3, стакан BUY 100.5 → вхід 100.5 (гірша з стакану/тригера), entry_src book
XBOOK.update(px=100.5, mid=100.45)
ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 300)}
del ro["s1|R2_breakout"]["entry_ts"]
run_loop(ro, {}, [100.35, 100.35], t0, [])
p = ro["s1|R2_breakout"]
assert p["state"] == "open" and p["entry_px"] == 100.5 and p["entry_src"] == "book" and p["entry_ts"] == T1s
XBOOK.update(px=100.1, mid=100.2)   # стакан кращий за тригер → тригер
ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 300)}
del ro["s1|R2_breakout"]["entry_ts"]
run_loop(ro, {}, [100.35, 100.35], t0, [])
assert ro["s1|R2_breakout"]["entry_px"] == 100.3
# (д) follow: full_close, філ 90 с тому → full_close_late; ціна виходу зі стакану (SHORT → BUY)
XBOOK.update(px=99.4, mid=99.5)
def fol_tr(**kv):
    d = {"coin": "AAA", "our_side": "SHORT", "entry_px": 100.0, "open_ts": t0 - 200, "strategy": "F1_1хв",
         "key": A + ":AAA", "addr": A, "timer": 60.0, "peak": -999.0, "trough": 999.0, "tx_pct": 10.0,
         "tx_usd": 5e4, "ratio": 3.0, "pos_usd": 5e5, "vault": 0, "hour": 12, "btc_move": 0.01, "depth": 1e5,
         "force_exit": None, "profile_gap": 0.0, "algo_v": "2.19", "prof": {}, "fill_ts_ms": 1, "lag_s": 2.0,
         "detect_src": "ws", "entry_src": "book", "entry_px_mid": 100.1, "px_age_ms": 300, "whale_px": 100.2,
         "grace": 0, "costs": 0.17}
    d.update(kv); return d
fo = {"f1": fol_tr(force_exit="full_close", force_exit_ts=t0 - 1, force_fill_ts_ms=int((t0 - 90) * 1000))}
w = []
ns = run_loop({}, fo, [99.6, 99.6], t0, w)
assert [x[0] for x in w] == ["f"] and "f1" not in fo
row = w[0][1]; g = lambda k: row[fh.index(k)]
assert g("exit_reason") == "full_close_late" and g("exit_px") == 99.4 and g("exit_src") == "book"
assert abs(g("gross_pct") - 0.6) < 1e-9 and abs(g("net_pct") - (0.6 - 0.1)) < 1e-9 and g("costs_pct") == 0.17   # обидві ноги по стакану: лише комісія
assert g("close_ts_ms") == int(round(T1s * 1000)) and ns["_paper_calls"][0] == ("AAA", "BUY")
# свіжий full_close (філ 5 с тому) → full_close; тиша → silence; вихід через ~5 хв → silence_late
fo = {"f1": fol_tr(force_exit="full_close", force_exit_ts=t0 - 1, force_fill_ts_ms=int((t0 - 5) * 1000))}
w = []; run_loop({}, fo, [99.6], t0, w); assert w[0][1][fh.index("exit_reason")] == "full_close"
fo = {"f1": fol_tr()}; w = []
run_loop({}, fo, [99.6], t0, w, {"follow_last_close": {A + ":AAA": t0 - 70}})
assert w[0][1][fh.index("exit_reason")] == "silence"
fo = {"f1": fol_tr()}; w = []
run_loop({}, fo, [99.6], t0, w, {"follow_last_close": {A + ":AAA": t0 - 300}})
assert w[0][1][fh.index("exit_reason")] == "silence_late"
# (е) TWAP: m60 → рядок угоди з ціною виходу зі стакану (SHORT → BUY 99.4): gross +0.6
def tw_tr(samples, entry_ts):
    return {"sig_id": "tw1", "strategy": "T1_твап_відкриття", "state": "open", "row_kind": "twap", "coin": "AAA",
            "side": "SHORT", "addr": A, "detect_ts": entry_ts, "detect_px": 100.0, "entry_ts": entry_ts,
            "entry_px": 100.0, "track_min": 120, "depth": 1e5, "btc_move": 0.1, "hour": 10, "algo_v": "2.19",
            "costs": 0.2, "entry_src": "book", "entry_px_mid": 100.1,
            "twap": {"src": "TWAPx", "twap_side": "sell", "usd": 1e5, "dur": 600, "kind": "open",
                     "kind_src": "slice", "twap_id": 5, "sp": 0.0, "pos_usd": 0, "move": 1.5, "p0": 100.0,
                     "p0_src": "candle", "p1": 98.5, "p1_src": "candle", "cohort": 1.0,
                     "cancel_after_entry": 0, "completed": 0, "exch_status": "activated",
                     "exec_pct": None, "exit_min": 0, "exit_reason": ""},
            "samples": list(samples), "peak": 2.0, "trough": -0.5}
ro = {"tw1|T1_твап_відкриття": tw_tr([0.1] * 59, t0 - 60 * 60 - 3)}
w = []
ns = run_loop(ro, {}, [99.0, 99.0], t0, w)
assert [x[0] for x in w] == ["t"] and len(w[0][1]) == len(th) - 1
row = w[0][1]; g = lambda k: row[th.index(k)]
assert g("exit_px") == 99.4 and g("exit_src") == "book" and g("exit_ts_ms") == int(round(T1s * 1000))
assert abs(g("net60_pct") - (0.6 - 0.1)) < 1e-9 and g("exit_min") == 60 and g("entry_src") == "book"   # обидві ноги по стакану
assert g("entry_ts_ms") == int(round((t0 - 3603) * 1000)) and ro["tw1|T1_твап_відкриття"]["trade_written"] == 1
# (є) хвилина виходу давно минула (рестарт): рядок з семплів, стакан не питаємо (рев'ю B1)
ro = {"tw1|T1_твап_відкриття": tw_tr([0.1] * 59 + [0.4] + [0.1] * 30, t0 - 95 * 60)}
w = []
ns = run_loop(ro, {}, [99.0, 99.0], t0, w)
row = w[0][1]; g = lambda k: row[th.index(k)]
assert [x[0] for x in w] == ["t"] and ns["_paper_calls"] == [] and g("exit_src") == "" and g("exit_px") == ""
assert abs(g("net60_pct") - (0.4 - 0.2)) < 1e-9 and g("exit_min") == 60
# (ж) R: тик проґавив 30..33 (рестарт), m30 є у семплах → вихід із семпла, не no_price (рев'ю B5)
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 40 * 60 - 5, [0.1] * 29 + [0.5] + [0.1] * 8)}
ns = run_loop(ro, {}, [101.0, 101.0], t0, [])
p = ro["s1|R1_загальний"]
assert p["exit_reason"] == "timer" and p["exit_src"] == "sample" and p["exit_min"] == 30 and abs(p["exit_px"] - 100.5) < 1e-9
assert ns["_paper_calls"] == []
print("4) цикл: R8 TP зі стакану (лімітка не краще TP), m30/m31/no_price, R2 гірша з (стакан, тригер), follow full_close_late/silence зі стакану, TWAP-вихід зі стакану")

# ═══ 5. Витрати по обох боках; _p_costs ══════════════════════════════
CS = load({"_sim_costs", "_sim_slip", "depth_for_side", "_p_costs"},
          dict(C, cache={"depth": DEPTH, "depth_prev": {}}, cache_lock=threading.Lock()))
c, d_in, d_out = CS["_sim_costs"]("AAA", "SHORT")
assert (d_in, d_out) == (1e5, 2e4) and abs(c - exp_costs) < 1e-9
c2, d_in, d_out = CS["_sim_costs"]("AAA", "LONG")
assert (d_in, d_out) == (2e4, 1e5) and abs(c2 - c) < 1e-9
c3, d_in, d_out = CS["_sim_costs"]("ZZZ", "LONG")      # без глибини: 0.12%/бік
assert (d_in, d_out) == (0, 0) and abs(c3 - 0.34) < 1e-9
assert CS["_p_costs"]({"costs": 0.19, "depth": 1e5}) == 0.19
assert abs(CS["_p_costs"]({"depth": 1e5}) - (2 * 0.0005 + 2 * slip(1e5)) * 100) < 1e-9   # старий трекер
print("5) витрати: вхід і вихід по своїх боках стакану; без глибини 0.34%; _p_costs зі збереженого")

# ═══ 6. API: офіційний net, пізні, грейс, когорти, періоди, допуск старих R ═══
import settle
d1 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d1, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def mkrow(headers, **kv):
    r = {k: "" for k in headers}; r["eol"] = "^"; r.update(kv)
    return [r[k] for k in headers]
today = _dt(now0); ago10 = _dt(now0 - 10 * 86400); ago40 = _dt(now0 - 40 * 86400); ago3 = _dt(now0 - 3 * 86400)
def revr(sig, date, algo="2.19", **kv):
    base = dict(sig_id=sig, strategy="R1_загальний", date=date, coin="AAA", our_side="LONG", whale_addr=A,
                src="wallet", detect_px="100", entry_px="100", entered="1", ratio="3", costs_pct="0.15",
                algo_v=algo, m30="0.5")
    base.update(kv); return mkrow(rh, **base)
wcsv("rev.csv", rh, [
    revr("r1", today, exit_px="101", exit_reason="timer", exit_min="30", grace="0", dump_bucket="2",
         dump_dur_s="90", dump_move_pct="-1.5"),
    revr("r2", ago10, exit_px="102", exit_reason="timer_late", exit_min="31", grace="1", ratio="1.5", dump_bucket="1"),
    revr("r3", ago40, exit_reason="no_price", exit_min="", grace="0", dump_bucket="4", m30=""),
    revr("r4", today, exit_px="99", exit_reason="timer", exit_min="30", grace="0", dump_bucket="2"),
    revr("r5", ago3, algo="2.13", m30="2.0"),          # старий рядок: допуск за settlement
    revr("r6", ago3, algo="2.13", m30="2.0"),          # старий рядок: епізод >5 хв → legacy
    revr("r7", today, strategy=R8, exit_px="101.6", exit_reason="tp", exit_min="4.5", grace="0", dump_bucket="1"),
])
def folr(tid, date, algo="2.19", **kv):
    base = dict(date_open=date, date_close=date, strategy="F1_1хв", coin="AAA", our_side="SHORT", whale_addr=A,
                entry_px="100", exit_px="99.5", exit_reason="silence", hold_s="70", gross_pct="0.5",
                costs_pct="0.15", net_pct="0.35", ratio="3", pos_usd="5e5", vault="0", hour="10",
                profile_gap_s="0", algo_v=algo, trade_id=tid, grace="0")
    base.update(kv); return mkrow(fh, **base)
wcsv("follow.csv", fh, [
    folr("t1", today),
    folr("t2", today, exit_reason="silence_late", net_pct="2.0"),
    folr("t3", ago10, algo="2.12", ratio="1.2", grace="", net_pct="-0.3"),
])
def twr(tid, **kv):
    base = dict(twap_id=tid, strategy="T1_твап_відкриття", date_entry=today, coin="AAA", our_side="SHORT",
                twap_side="buy", whale_addr=A, src="TWAPx", usd="1e5", dur_s="600", kind="open", kind_src="slice",
                entry_px="100", net60_pct="1.0", costs_pct="0.2", exit_min="60", exit_reason="timer_60m",
                completed="1", cancel_after_entry="0", exch_status="finished", algo_v="2.15")
    base.update(kv); return mkrow(th, **base)
wcsv("tw.csv", th, [twr("tw1")])
def setr(key, family, strategy, **kv):
    base = {k: "" for k in settle.HEADERS}
    base.update(key=key, family=family, strategy=strategy, coin="AAA", symbol="AAAUSDT", settle_v=settle.SETTLE_V,
                settled_at=today, eol="^")
    base.update(kv); return [base[k] for k in settle.HEADERS]
SETTLE_ROWS = [
    setr("r1|R1_загальний", "rev", "R1_загальний", net_live_pct="0.85", net_tape_pct="0.4", net_official_pct="0.4",
         dump_bucket="2", dump_move_pct="1.5", flags=""),
    setr("r4|R1_загальний", "rev", "R1_загальний", net_live_pct="-1.15", net_tape_pct="-0.5", net_official_pct="-1.15",
         dump_bucket="2", dump_move_pct="1.2", flags=""),
    setr("r5|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="3", dump_move_pct="1.7", dump_dur_s="150", lag_s="3", flags="dump_tape;full_close"),
    setr("r6|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="0", dump_move_pct="1.7", dump_dur_s="400", flags=""),
    setr("r7|" + R8, "rev", R8, net_live_pct="1.45", net_tape_pct="", net_official_pct="", flags="no_tape"),
    setr("t1", "fol", "F1_1хв", net_live_pct="0.35", net_tape_pct="0.9", net_official_pct="0.35", flags=""),
    setr("tw1|T1_твап_відкриття", "twap", "T1_твап_відкриття", net_live_pct="1.0", net_tape_pct="0.3",
         net_official_pct="0.3", flags=""),
]
wcsv("settlements.csv", settle.HEADERS, SETTLE_ROWS)
def mk_api(d=d1):
    return load({"strat2_api", "strat2_slice", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
                 "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key", "_twap_exit",
                 "_twap_curve_row", "_stat_small", "_period_stats", "_ts_local", "_load_settlements",
                 "_official_net", "_parse_curve", "_curves_of", "_agg_block", "_pub", "_leg_costs"}, dict(C,
        _strat2_full={},
        strat2_lock=threading.RLock(), rev_open={}, follow_open={}, wallet_profiles={},
        STRAT2_DESC={}, STRAT2_TITLES={}, STRAT_SINCE=FH["STRAT_SINCE"], TAPE_SINCE=FH["TAPE_SINCE"],
        _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={}, _settle_cache={"key": None, "data": {}},
        DATA_DIR=d, stats={}, FOLLOW_OUT_CSV=os.path.join(d, "fo.csv"), REV_CSV=os.path.join(d, "rev.csv"),
        REV_SIG_CSV=os.path.join(d, "sig.csv"), FOLLOW_CSV=os.path.join(d, "follow.csv"),
        REV_OUT_CSV=os.path.join(d, "out.csv"), TWAP_CSV=os.path.join(d, "tw.csv"),
        TWAP_CURVE_CSV=os.path.join(d, "tc.csv"), TWAP_SIG_CSV=os.path.join(d, "tws.csv"),
        TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_COHORTS=(1.0, 1.5, 2.0), F8_NAME="F8_ratio35",
        F9_NAME="F9_без_ратіо_90", T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення",
        strat_activated={}, _dt=_dt, _sim_slip=lambda d_: 0.0005), assigns=("TWAP_HEADERS",))
o = mk_api()["strat2_api"]()
r1s = o["strategies"]["R1_загальний"]
nets = sorted(t["net30"] for t in r1s["trades"] if t["net30"] is not None)
assert r1s["n"] == 3 and abs(r1s["median"] - 0.4) < 1e-9, (r1s["n"], r1s["median"])   # 0.4 / -1.15 / 1.0 (r5)
assert sorted(round(v, 4) for v in nets) == [-1.15, 0.4, 1.0, 1.0, 1.85], nets   # r2 (пізня) має net у списку, але поза n; v2.20: старий r6 без стрічки — у журналі (adm, поза n)
assert r1s["n_late"] == 1 and r1s["n_no_price"] == 1 and r1s["n_grace"] == 1 and r1s["n_adm_legacy"] == 1
assert r1s["n_settled"] == 4 and r1s["n_unsettled"] == 1     # r2 (пізня) live без стрічки; v2.20: + старий r6 (settlement є) у журналі
by = {t["date"] + "|" + str(t.get("adm")): t for t in r1s["trades"]}
tr1 = [t for t in r1s["trades"] if t["net_live"] is not None and abs(t["net_live"] - 0.85) < 1e-9][0]
assert tr1["net_tape"] == 0.4 and tr1["net30"] == 0.4 and tr1["settled"] == 1 and tr1["bucket"] == 2 and tr1["late"] == 0
tr4 = [t for t in r1s["trades"] if t["net_live"] is not None and abs(t["net_live"] + 1.15) < 1e-9][0]
assert abs(tr4["net30"] + 1.15) < 1e-9 and tr4["net_tape"] == -0.5          # гірший = live
tr2 = [t for t in r1s["trades"] if t["late"] == 1][0]
assert abs(tr2["net30"] - 1.85) < 1e-9 and tr2["grace"] == 1 and tr2["settled"] == 0 and tr2["exit_reason"] == "timer_late"
tr3 = [t for t in r1s["trades"] if t["exit_reason"] == "no_price"][0]
assert tr3["net30"] is None and tr3["entered"] == 1
tr5 = [t for t in r1s["trades"] if t["adm"] == 1][0]
assert tr5["net30"] == 1.0 and abs(tr5["net_live"] - 1.85) < 1e-9 and tr5["bucket"] == 3 and abs(tr5["dump_move"] + 1.7) < 1e-9
assert r1s["buckets"]["2"]["n"] == 2 and abs(r1s["buckets"]["2"]["median"] - (0.4 - 1.15) / 2) < 1e-9
assert r1s["buckets"]["3"]["n"] == 1 and r1s["buckets"]["1"]["n"] == 0 and r1s["buckets"]["4"]["n"] == 0
pr = r1s["periods"]
assert pr["today"]["n"] == 2 and pr["d7"]["n"] == 3 and pr["d30"]["n"] == 3 and pr["all"]["n"] == 3, pr
assert abs(pr["today"]["median"] - (0.4 - 1.15) / 2) < 1e-9 and pr["today"]["pnl_usd"] == round((0.4 - 1.15) * 10, 2)
assert r1s["live"]["n"] == 3 and abs(r1s["live"]["median"] - 0.85) < 1e-9 and r1s["tape"]["n"] == 3
assert r1s["n_trades_total"] == 6 and o["legacy_rows"] == 0, (r1s["n_trades_total"], o["legacy_rows"])   # r1..r5 + r6 у журналі (v2.20: старі R-рядки не приховані)
r8s = o["strategies"][R8]
# v2.20: STRAT_SINCE R8 = 2.20 (TP тепер по Binance-потоку) — рядок 2.16 у журналі як старий (adm), поза заголовком без підтвердженого сигналу
assert r8s["n"] == 0 and r8s["n_settled"] == 1 and r8s["n_paper"] == 1 and abs(r8s["paper"]["median"] - (1.6 - 0.15)) < 1e-9   # no_tape → live у журналі
assert r8s["trades"][0]["flags"] == "no_tape" and r8s["trades"][0]["net_tape"] is None
f1 = o["strategies"]["F1_1хв"]
assert f1["n"] == 2 and f1["n_late"] == 1 and f1["n_grace"] == 0 and f1["n_grace_unknown"] == 1 and f1["n_settled"] == 1 and f1["n_unsettled"] == 2, f1   # t3: старий рядок без колонки grace — невідомо
assert sorted(round(v, 4) for v in [t["net30"] for t in f1["trades"] if not t["late"]]) == [-0.3, 0.35]
t3 = [t for t in f1["trades"] if t["grace"] is None][0]
assert abs(t3["net30"] + 0.3) < 1e-9 and t3["settled"] == 0
t1 = [t for t in f1["trades"] if t["settled"] == 1][0]
assert t1["net_tape"] == 0.9 and t1["net30"] == 0.35             # гірший = live
assert f1["periods"]["today"]["n"] == 1 and f1["periods"]["d30"]["n"] == 2
tw1 = o["strategies"]["T1_твап_відкриття"]
assert tw1["n"] == 1 and abs(tw1["median"] - 0.3) < 1e-9 and tw1["n_settled"] == 1 and tw1["trades"][0]["net_live"] == 1.0
assert tw1["cohorts"]["1.0"]["n_trades_total"] == 1 and tw1["periods"]["today"]["n"] == 1
assert o["settle"]["n"] == 7
# кеш settlements по mtime/size; збій читання — порожньо, не краш
api = mk_api()
s1 = api["_load_settlements"](); assert len(s1) == 7 and api["_load_settlements"] () is s1
# рядок СТАРОЇ версії розрахунку (settle_v ≠ SETTLE_V) — не «verified», а pending до перерахунку
wcsv("settlements.csv", settle.HEADERS, [
    setr("r1|R1_загальний", "rev", "R1_загальний", net_live_pct="0.85", net_tape_pct="0.4", net_official_pct="0.4",
         dump_bucket="2", dump_move_pct="1.5", flags="", settle_v="1"),
    setr("r4|R1_загальний", "rev", "R1_загальний", net_live_pct="-1.15", net_tape_pct="-0.5", net_official_pct="-1.15",
         dump_bucket="2", dump_move_pct="1.2", flags="")])
api = mk_api(); s2 = api["_load_settlements"]()
assert set(s2) == {"r4|R1_загальний"} and api["stats"]["settle_old_v"] == 1, s2.keys()
o = api["strat2_api"](); r1s = o["strategies"]["R1_загальний"]
sts = [t["status"] for t in r1s["trades"]]
assert sts.count("verified") == 1 and sts.count("pending") >= 1 and r1s["n_verified"] == 1, sts
wcsv("settlements.csv", settle.HEADERS, SETTLE_ROWS)   # повернути фікстуру для наступних блоків
print("6) API: офіційний = min(live, стрічка), пізні поза заголовком, грейс/без ціни, когорти дампу, періоди, допуск старих R за стрічкою, TWAP, n_trades_total")

# ═══ 7. settle.py: референс дампу зі стрічки, знак «у бік тиску» ═════
from array import array
class FakeTape:
    def __init__(self, pts): self.pts = sorted(pts)
    def window(self, symbol, t0, t1):
        ts = array("q", [t for t, _ in self.pts if t0 <= t <= t1]); px = array("d", [p for t, p in self.pts if t0 <= t <= t1])
        return ts, px
class FakeHL:
    def __init__(self, fills): self.fills = fills
    def user_fills(self, addr, t0, t1): return self.fills
t_first, t_last = 1_700_000_000_000, 1_700_000_090_000
fills = [{"coin": "AAA", "time": t_first, "px": "101.0", "side": "A", "dir": "Close Long"},
         {"coin": "AAA", "time": t_last, "px": "100.0", "side": "A", "dir": "Close Long"}]
ctx = {"tape": FakeTape([(t_first - 5000, 102.0), (t_first + 1000, 101.0), (t_last - 500, 100.2), (t_last + 4000, 99.0)]),
       "hl": FakeHL(fills), "symbol_map": {}, "now_ms": t_last + 10 ** 6, "log": lambda *a: None}
out = {"flags": []}
settle._whale(out, {"whale_addr": A, "coin": "AAA"}, t_last + 2000, ctx, close_only=True, symbol="AAAUSDT")
# ref = останній трейд до першого філа (102.0), кінець = останній трейд на останньому філі (100.2):
# кит продає, ціна впала → додатний
assert abs(out["dump_move_pct"] - (100.2 / 102.0 - 1) * 100 * -1) < 1e-9 and out["dump_move_pct"] > 0
assert "dump_tape" in out["flags"] and out["dump_bucket"] == 2 and out["dump_dur_s"] == 90.0 and out["lag_s"] == 2.0
out = {"flags": []}
settle._whale(out, {"whale_addr": A, "coin": "AAA"}, t_last + 2000, dict(ctx, tape=FakeTape([])), close_only=True, symbol="AAAUSDT")
assert "dump_fills" in out["flags"] and abs(out["dump_move_pct"] - (100.0 / 101.0 - 1) * 100 * -1) < 1e-9
assert "net_official_pct" in settle.HEADERS and "dump_bucket" in settle.HEADERS and settle.HEADERS[-1] == "eol"
# транзиторний збій джерела (Transient) — рядок не фіксується, наступний прогін дописує; None = даних немає
d7 = tempfile.mkdtemp()
fh7 = ["date_open", "date_close", "strategy", "coin", "our_side", "whale_addr", "entry_px", "exit_px",
       "exit_reason", "hold_s", "gross_pct", "costs_pct", "net_pct", "algo_v", "trade_id", "open_ts_ms", "close_ts_ms", "eol"]
tt = 1_700_000_000_000
with open(os.path.join(d7, "follow_trades.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(fh7)
    w.writerow([settle._dt(tt), settle._dt(tt + 60000), "F1_1хв", "AAA", "SHORT", A, "100", "99", "silence", "60",
                "1.0", "0.15", "0.85", "2.19", "tA", str(tt), str(tt + 60000), "^"])
    w.writerow([settle._dt(tt), settle._dt(tt + 60000), "F1_1хв", "AAA", "SHORT", A, "100", "99", "silence", "60",
                "1.0", "0.15", "0.85", "2.9", "tOLD", str(tt), str(tt + 60000), "^"])   # < 2.10 — пропуск
def boom(url): raise settle.Transient("net down")
res = settle.settle_pending(d7, fetchers={"fetch_zip": boom, "fetch_rest": lambda u: None, "fetch_hl": lambda b: None},
                            now_ms=tt + 10 * 86400000, log=lambda *a: None)
assert res == (0, 0, 1) and not os.path.exists(os.path.join(d7, "settlements.csv")), res
# аудит-3 №7: після збою рядок у бекофі (10 хв) — той самий момент його не чіпає і не витрачає спробу
res = settle.settle_pending(d7, fetchers={"fetch_zip": lambda u: None, "fetch_rest": lambda u: None, "fetch_hl": lambda b: None},
                            now_ms=tt + 10 * 86400000, log=lambda *a: None)
assert res == (0, 0, 0) and settle.settle_pending.last_pending == 1 and settle.settle_pending.last_waiting == 1, res
res = settle.settle_pending(d7, fetchers={"fetch_zip": lambda u: None, "fetch_rest": lambda u: None, "fetch_hl": lambda b: None},
                            now_ms=tt + 10 * 86400000 + 11 * 60000, log=lambda *a: None)
assert res == (1, 0, 0) and settle.settle_pending.last_pending == 1 and not settle.settle_pending.retry, res
st7 = settle.load_settlements(d7)
assert set(st7) == {"tA"} and "no_tape" in st7["tA"]["flags"] and "no_fills" in st7["tA"]["flags"]
print("7) settle: референс дампу зі стрічки перед 1-м філом, знак у бік тиску, фолбек по філах")

# ═══ 8. Структурні: fast-retry/чанки, поллер, /status, watchdog, UI, версії ═══
mon = src[src.index("def run_realtime_monitor"):src.index("def check_position_changes")]
assert 'det_src = item[2] if len(item) > 2 else "sweep"' in mon
assert src[src.index("def _ingest_txs("):src.index("def run_realtime_monitor(")].count("detect_src=det_src") >= 2   # v2.19: 2 хуки + журнал у приймачі
assert 'fast_retry[(addr, coin)] = time.time() + FAST_RETRY_S' in mon and 'if det_src == "ws":' in mon
assert "def _process_fast():" in mon and 'items = [(a, c, "ws") for a, c in by_addr.items()]' in mon
assert "for ci in range(0, len(items), FAST_SWEEP_CHUNK):" in mon and mon.count("_process_fast()") >= 2
assert 'fast_retry_cnt.pop((addr, coin), None)' in mon
pol = src[src.index("def run_px_poller"):src.index("def run_strat2_loop")]
assert "_px_pending" in pol and "_hl_delisted" in pol and 'stats["px_fail"]' in pol and 'stats["px_last_ok"]' in pol
assert 'mids = sim_all_mids(retries=1)' in src and 'def sim_all_mids(retries=4):' in src
pxat = src[src.index("def _px_at"):src.index("_px_pending = {}")]
assert "if t > ts:" in pxat
assert 'time.sleep(1.5)' in src[src.index("def fetch_all_depth"):src.index("def fetch_all_depth") + 3000]
assert 'status") == "TRADING"' in src or '"status") == "TRADING"' in src or 'get("status") == "TRADING"' in src
loop = src[src.index("def run_strat2_loop"):src.index("# ── API")]
assert "rev_price, fol_exit = [], []" in loop and '_pxc[k] = _paper_px(coin, bside, max_mid_age_ms=35_000, strict=strict,' in loop
# рев'ю аудит-2: тик у власній функції під try/except; журнал — поза strat2_lock; R2 arm — строгий стакан
assert "def _strat2_tick():" in loop and "_strat2_tick()" in loop and 'stats["strat2_tick_err"]' in loop
assert 'xpx, xsrc, xmeta = _ppx(coin, bside, strict=(what == "arm"),' in loop
assert 'for _jk, _jf in _jrn:' in loop and loop.count("_journal(") == 1 and 'exit_kind=req["kind"]' in loop
assert 'p["arm_hit"] = req' in loop
assert 'reason = "full_close_late"' in loop and 'p["exit_pending"] = {"reason": reason, "ts": now, "mid": px}' in loop
for k in ('"px_age_s":', '"book_ok":', '"book_fail":', '"rev_busy_skips":', '"rev_stale_skips":',
          '"follow_stale_skips":', '"fast_retries":', '"settle_done":', '"settle_last_ok_s_ago":', '"px_fail":'):
    assert k in src, k
assert "threading.Thread(target=run_settle_worker,   daemon=True).start()" in src
sw = src[src.index("def run_settle_worker"):src.index("def main()")]
assert "settle_pending(" in sw and "symbol_map=SYMBOL_MAP" in sw and "limit=SETTLE_BATCH" in sw \
    and '"fetch_hl": _settle_fetch_hl' in sw and "_strat2_cache[\"ts\"] = 0.0" in sw
wd = open((_HL + "/watchdog.py"), encoding="utf-8").read()
assert 'px_age = st.get("px_age_s")' in wd and 'prev.get("px_stale") and not prev.get("px_alerted")' in wd
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for m in ("'R8_тп80'", "без грейсу", "setBk(", "sd-periods", "дамп: усі", "svSettle", "Net офіц.", "n_trades_total",
          "Пізні · старі F без гейта", "один трек на активну пару", "t.bucket===+rBk", "t.grace===(fGrace==='y'?1:0)",
          "/strat2_slice?", "Верифіковано (обидві оцінки)", "ОФІЦІЙНА: за хвилиною гірша", "ПЕРЕВІРЕНО (офіційний net R1", "НЕ доказ edge", "fetchSlice("):
    assert m in ui, m
assert "def _rev_adm_legacy" in src and "STRAT_SINCE.get(st, DATA_ALGO_V)):\n                continue" in src
print("8) структурні: fast-retry + чанки sweep, поллер (1 ретрай, outlier, delisted), /status, settle-воркер, watchdog px_age, UI, версії")

# ═══ 9. Аудит-2 (11.09): 16 знахідок ══════════════════════════════════
# (а) агрегація філів: VWAP лишається, але є СИРІ перша/остання ціни (№6)
FL = load({"get_recent_market_fills"}, dict(C, APIError=type("APIError", (Exception,), {}),
          hl_post=lambda body, **k: [
              {"coin": "AAA", "time": 1000, "px": "100.0", "sz": "9", "dir": "Close Long", "hash": "0xh1",
               "crossed": True, "startPosition": "100", "oid": 1, "tid": 1},
              {"coin": "AAA", "time": 1000, "px": "99.8", "sz": "9", "dir": "Close Long", "hash": "0xh1",
               "crossed": True, "startPosition": "100", "oid": 1, "tid": 2},
              {"coin": "AAA", "time": 1000, "px": "98.8", "sz": "1", "dir": "Close Long", "hash": "0xh1",
               "crossed": True, "startPosition": "100", "oid": 1, "tid": 3}]))
txs = FL["get_recent_market_fills"](A, "AAA", 0, "LONG")
assert len(txs) == 1 and txs[0]["px_first"] == 100.0 and txs[0]["px_last"] == 98.8
assert abs(txs[0]["px"] - (100 * 9 + 99.8 * 9 + 98.8) / 19) < 1e-9 and txs[0]["sp"] == 100.0
# (б) епізод FC: сира перша ціна, розрив ≤300 с І позиція не зростала (№5/№7)
FC = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={}))
oldp = {"side": "LONG", "ratio": 3, "val": 1e5, "size": 1000.0}
FC["fc_on_txs"](A, "AAA", oldp, [{"hash": "h1", "ts": 1000, "px": 99.9, "px_first": 100.0, "px_last": 99.8, "sz": 500.0, "sp": 1000.0}])
ep = FC["fc_episodes"][(A, "AAA")]
assert ep["first_px"] == 100.0 and ep["pos_after"] == 500.0 and ep["start_size"] == 1000.0
FC["fc_on_txs"](A, "AAA", oldp, [{"hash": "h2", "ts": 61000, "px": 99.5, "sz": 200.0, "sp": 500.0}])   # залишок збігся → той самий
assert FC["fc_episodes"][(A, "AAA")]["first_ts"] == 1000 and FC["fc_episodes"][(A, "AAA")]["pos_after"] == 300.0
FC["fc_on_txs"](A, "AAA", oldp, [{"hash": "h3", "ts": 121000, "px": 99.0, "sz": 500.0, "sp": 1000.0}])  # долив до 1000 → НОВИЙ епізод
ep = FC["fc_episodes"][(A, "AAA")]
assert ep["first_ts"] == 121000 and ep["start_size"] == 1000.0 and ep["max_sz"] == 500.0
# аудит-3 №2: той самий потік транзакцій — той самий епізод НЕЗАЛЕЖНО від пакування батчів
# (позиція 1000; продаж 100; долив до 2000; два продажі по 1000): одним пакетом і трьома
SEQ = [{"hash": "a1", "ts": 1000, "px": 100.0, "sz": 100.0, "sp": 1000.0, "dir": "Close Long"},
       {"hash": "a2", "ts": 61000, "px": 99.5, "sz": 1000.0, "sp": 2000.0, "dir": "Close Long"},
       {"hash": "a3", "ts": 121000, "px": 99.0, "sz": 1000.0, "sp": 1000.0, "dir": "Close Long"}]
def _ep_of(batches):
    ns = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={}))
    for b in batches:
        ns["fc_on_txs"](A, "AAA", oldp, b)
    e = ns["fc_episodes"][(A, "AAA")]
    return (e["first_ts"], e["start_size"], e["last_ts"], e["max_sz"], round(e["sum_usd"], 2), e["pos_after"])
one = _ep_of([SEQ]); three = _ep_of([[SEQ[0]], [SEQ[1]], [SEQ[2]]]); two = _ep_of([[SEQ[0], SEQ[1]], [SEQ[2]]])
assert one == three == two, (one, three, two)
assert one[0] == 61000 and one[1] == 2000.0 and one[2] == 121000 and one[3] == 1000.0 and one[5] == 0.0, one   # база 2000, тривалість 60 с
# рев'ю: у live лише тейкерські філи — невидиме мейкерське закриття між ними (позиція «незрозуміло» зменшилась)
# НЕ рве епізод: тейкер 100 → мейкер 200 (невидимий) → тейкер 700 = один епізод, база 1000, max 700 (70% → не R7)
MK = [{"hash": "m1", "ts": 1000, "px": 100.0, "sz": 100.0, "sp": 1000.0, "dir": "Close Long"},
      {"hash": "m3", "ts": 89000, "px": 99.0, "sz": 700.0, "sp": 700.0, "dir": "Close Long"}]
mk1 = _ep_of([MK]); mk2 = _ep_of([[MK[0]], [MK[1]]])
assert mk1 == mk2 and mk1[0] == 1000 and mk1[1] == 1000.0 and mk1[3] == 700.0 and mk1[5] == 0.0, (mk1, mk2)
# фолбек без settle.py: inline-правило (розрив >300 с або зростання → новий епізод), не «склеїти все»
ns = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={}, _close_episode=None))
ns["fc_on_txs"](A, "AAA", oldp, [dict(SEQ[0]), dict(SEQ[0], hash="z2", ts=1000 + 20 * 60000, sp=5000.0, sz=500.0)])
assert ns["fc_episodes"][(A, "AAA")]["first_ts"] == 1000 + 20 * 60000 and ns["fc_episodes"][(A, "AAA")]["start_size"] == 5000.0
# епізод зі старого state (без txs): знімок ratio/val на старті не губиться при першому батчі після деплою
ns = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_positions={},
          fc_episodes={(A, "AAA"): {"first_ts": 1000, "first_px": 100.0, "last_ts": 1000, "sum_usd": 1e4, "seen": {"a1"},
                                    "side": "LONG", "ratio": 4, "val": 2e5, "start_size": 1000.0, "max_sz": 100.0,
                                    "max_usd": 1e4, "max_liq": 0, "pos_after": 900.0}}))
ns["fc_on_txs"](A, "AAA", dict(oldp, val=5e3, ratio=0.5), [dict(SEQ[0], hash="a1b", ts=2000, sp=900.0)])
_e = ns["fc_episodes"][(A, "AAA")]
assert _e["ratio"] == 4 and _e["val"] == 2e5 and _e["first_ts"] == 1000 and _e["start_size"] == 1000.0, _e
# фліп (Long > Short) — теж закриття у правилі епізоду (settle._is_close)
ns = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={}))
ns["fc_on_txs"](A, "AAA", oldp, [dict(SEQ[0]), dict(SEQ[1], sp=900.0, dir="Long > Short", ts=31000)])
assert ns["fc_episodes"][(A, "AAA")]["first_ts"] == 1000 and ns["fc_episodes"][(A, "AAA")]["last_ts"] == 31000
# ratio/val знімок пари на старті епізоду успадковується, поки база та сама; нова база → поточний old
ns = load({"fc_on_txs"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={}))
ns["fc_on_txs"](A, "AAA", dict(oldp, val=5e5, ratio=4), [dict(SEQ[0])])
ns["fc_on_txs"](A, "AAA", dict(oldp, val=5e3, ratio=0.5), [dict(SEQ[0], hash="a1b", ts=2000, sp=900.0)])
assert ns["fc_episodes"][(A, "AAA")]["val"] == 5e5 and ns["fc_episodes"][(A, "AAA")]["ratio"] == 4
# (в) R6: лише повне закриття (№16); шматок волта ≥5% без повного — не сигнал; партіал ≥30% — тінь
n, st, sig = mk_rev(ep=EP)
n["is_vault"] = lambda a: True
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert strats_of(n) == {"_OUTCOME", "R6_волт"}, strats_of(n)
n, st, sig = mk_rev(ep=EP)
n["is_vault"] = lambda a: True
n["rev_on_close"](A, "AAA", OLD_R, [dict(FILLS[0], sz=100.0)], False)   # 10% — раніше R6, тепер нічого
assert n["rev_open"] == {} and sig == []
n, st, sig = mk_rev(ep=EP)
n["is_vault"] = lambda a: True
n["rev_on_close"](A, "AAA", OLD_R, [dict(FILLS[0], sz=400.0)], False)   # 40% → лише тінь partial
assert strats_of(n) == {"_OUTCOME"} and next(iter(n["rev_open"].values()))["src"] == "partial"
# (г) референс дампу: семпл лише ≤12 с до першого філа (№6); стакан обов'язковий для стратегій (№1)
n, st, sig = mk_rev(ep=EP)
n["_px_at"] = lambda c, ts, tol=90.0: (102.0 if tol <= 12.0 else None)   # старий 90-с допуск не діє
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert "R1_загальний" in strats_of(n)
n, st, sig = mk_rev(ep=EP)
n["_px_at"] = lambda c, ts, tol=90.0: None      # семпла нема → референс = сира перша ціна (101.8 → 100: 1.77%)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
r1 = [p for p in n["rev_open"].values() if p["strategy"] == "R1_загальний"][0]
assert abs(r1["move_ep"] - (-(100.0 / 101.8 - 1) * 100)) < 1e-9
n, st, sig = mk_rev(book_px=None, ep=EP)                     # стакану нема, мід свіжий → лише тінь
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert strats_of(n) == {"_OUTCOME"} and st["rev_no_book"] == 1 and sig[0][1][sh.index("opened")] == ""
# (д) кінець зливу — сирий останній філ, не VWAP (№6): VWAP 99.7 (−0.3%), останній 98.8 (−1.2%)
n, st, sig = mk_rev(ref_px=100.0, ep=None)
n["rev_on_close"](A, "AAA", OLD_R, [{"sz": 1000.0, "px": 99.7, "px_first": 99.8, "px_last": 98.8, "hash": "0x1",
                                      "dir": "Close Long", "ts": t_now_ms - 2000}], True)
assert "R1_загальний" in strats_of(n)
r1 = [p for p in n["rev_open"].values() if p["strategy"] == "R1_загальний"][0]
assert abs(r1["move_ep"] - 1.2) < 1e-9
# (е) витрати за ногою (№12)
LC = load({"_leg_costs"}, dict(C))
assert abs(LC["_leg_costs"](0.24, "book", "book") - 0.1) < 1e-9
assert abs(LC["_leg_costs"](0.24, "mid", "book") - 0.17) < 1e-9
assert abs(LC["_leg_costs"](0.24, None, None) - 0.24) < 1e-9 and abs(LC["_leg_costs"](0.24, "book_partial", "sample") - 0.17) < 1e-9
# (є) цикл: follow-вихід без поллера (№13) — ціна None, а стакан є
XBOOK.update(px=99.4, mid=99.5)
fo = {"f1": fol_tr(force_exit="full_close", force_exit_ts=t0 - 1, force_fill_ts_ms=int((t0 - 5) * 1000))}
w = []; ns = run_loop({}, fo, [None, None], t0, w)
assert [x[0] for x in w] == ["f"] and w[0][1][fh.index("exit_px")] == 99.4 and w[0][1][fh.index("exit_reason")] == "full_close"
# (ж) цикл: R2 — дедлайн перевіряється у момент фіксації (№14): запит стакану пережив вікно → входу немає
def run_loop_slow(rev_open, follow_open, px_seq, t_start, writes, delay):
    T_ = FakeTime(); T_.now = t_start
    ticks = {"i": 0}
    def _sleep(x):
        ticks["i"] += 1
        if ticks["i"] > len(px_seq): raise _Stop()
        T_.now += 3
    T_.sleep = _sleep
    def _append(p, hh, r):
        if len(r) != len(hh) - 1: return False
        writes.append((p, list(r))); return True
    def _paper(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, **kw):
        T_.now += delay
        return XBOOK["px"], "book", {"mid": XBOOK["mid"], "px_age_ms": 200}
    ns_ = load({"run_strat2_loop", "_rev_row", "_rev_samples", "_rev_out_row", "_fol_out_row", "_fol_row",
                "_rev_extra", "_ms", "_rnd", "_p_costs", "_twap_row", "_twap_exit", "_twap_curve_row",
                "_twap_close_min", "_twap_trade_closed", "_twap_freeze_exit", "_rev_trade_closed", "_leg_costs"}, dict(C,
        time=T_, strat2_lock=threading.RLock(), rev_open=rev_open, follow_open=follow_open,
        follow_last_close={}, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _px_now=lambda c, max_age=30.0: px_seq[min(ticks["i"], len(px_seq) - 1)],
        _strat_csv_append=_append, save_state=lambda: None, _paper_px=_paper,
        _sim_slip=lambda d: 0.0005, _dt=_dt, REV_CSV="r", REV_HEADERS=rh, REV_SIG_CSV="s",
        REV_SIG_HEADERS=[], REV_OUT_CSV="o", REV_OUT_HEADERS=oh, FOLLOW_CSV="f",
        FOLLOW_HEADERS=fh, FOLLOW_OUT_CSV="fo", FOLLOW_OUT_HEADERS=foh, TWAP_CSV="t",
        TWAP_HEADERS=th, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_EXIT_CAP_MIN=5,
        TWAP_CURVE_CSV="tc", TWAP_CURVE_HEADERS=FH["TWAP_CURVE_HEADERS"], stats={"delta_events": 0}))
    try:
        ns_["run_strat2_loop"]()
    except _Stop:
        pass
    return ns_
XBOOK.update(px=100.5, mid=100.45)
ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 4)}
del ro["s1|R2_breakout"]["entry_ts"]
w = []; run_loop_slow(ro, {}, [100.35, 100.35], t0, w, delay=6.0)
# рядок записано з entered=0 (входу не було), трекер знято
assert "s1|R2_breakout" not in ro and [x[0] for x in w] == ["r"] and w[0][1][rh.index("entered")] == 0
# (и) рев'ю аудит-2: R2 armed → open ЛИШЕ зі стакану: мід або partial не входять, arm_hit лишається,
# повторний запит стакану — не раніше ніж за 30 с (гард переставлення), не щотику
for _src in ("mid", "book_partial"):
    XBOOK.update(px=100.5, mid=100.45, src=_src)
    ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 300)}
    del ro["s1|R2_breakout"]["entry_ts"]
    ns_m = run_loop(ro, {}, [100.35] * 5, t0, [])          # 5 тиків = 15 с
    p = ro["s1|R2_breakout"]
    assert p["state"] == "armed" and p.get("arm_hit") and "entry_src" not in p, (_src, p)
    assert len(ns_m["_paper_calls"]) == 1, (_src, ns_m["_paper_calls"])
    ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 300)}
    del ro["s1|R2_breakout"]["entry_ts"]
    ns_m = run_loop(ro, {}, [100.35] * 12, t0, [])         # 12 тиків = 36 с → другий запит після 30 с
    assert ro["s1|R2_breakout"]["state"] == "armed" and len(ns_m["_paper_calls"]) == 2, ns_m["_paper_calls"]
XBOOK.update(px=100.5, mid=100.45, src="book")
ro = {"s1|R2_breakout": rev_tr("R2_breakout", None, [], state="armed", trigger_px=100.3, deadline=t0 + 300)}
del ro["s1|R2_breakout"]["entry_ts"]
run_loop(ro, {}, [100.35, 100.35], t0, [])
assert ro["s1|R2_breakout"]["state"] == "open" and ro["s1|R2_breakout"]["entry_src"] == "book"
# (к) один битий тик не вбиває потік стратегій: трекер без entry_px → виняток у тику → лічильник, наступний тик іде
import contextlib, io as _io
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 65, [0.1], entry_px=None)}
with contextlib.redirect_stderr(_io.StringIO()):
    ns_e = run_loop(ro, {}, [101.0, 101.0, 101.0], t0, [])
assert ns_e["stats"]["strat2_tick_err"] == 3, ns_e["stats"]
# (з) TWAP: пізній вихід поза заголовком (№15); журнал подій (№8); зріз на сервері (№4); статуси (№10/№11)
wcsv("tw.csv", th, [twr("tw1"), twr("tw2", exit_reason="timer_late", exit_min="62", net60_pct="-7.0")])
api = mk_api(); o = api["strat2_api"]()
tw1 = o["strategies"]["T1_твап_відкриття"]
assert tw1["n"] == 1 and tw1["n_late"] == 1 and abs(tw1["median"] - 0.3) < 1e-9
r1s = o["strategies"]["R1_загальний"]
assert r1s["n_verified"] == 3 and r1s["n_live_only"] == 1 and r1s["n_grace_unknown"] == 2   # v2.20: + старий r6 у журналі (grace невідомий)
assert sorted(t["status"] for t in r1s["trades"] if t["adm"] == 1) == ["verified", "verified"]   # v2.20: r5 і r6 (обидва старі, у журналі)
assert r1s["verified"]["n"] == 3 and abs(r1s["verified"]["median"] - 0.4) < 1e-9
sl = api["strat2_slice"]("R1_загальний", grace="n")
assert sl["n"] == 2 and abs(sl["median"] - (0.4 - 1.15) / 2) < 1e-9 and sl["n_trades_total"] == 3 and sl["filters"]["grace"] == "n"
sl = api["strat2_slice"]("R1_загальний", grace="y")
assert sl["n"] == 0 and sl["n_late"] == 1
sl = api["strat2_slice"]("R1_загальний", bucket="3")
assert sl["n"] == 1 and abs(sl["median"] - 1.0) < 1e-9 and "periods" in sl and sl["periods"]["d7"]["n"] == 1
sl = api["strat2_slice"]("R1_загальний", bucket="2", vault="n")
assert sl["n"] == 2
assert api["strat2_slice"]("NOPE")["error"]
# R2 без тригера на стрічці: записаний збиток лишається (official = live), статус live_only + розбіжність
wcsv("rev.csv", rh, [revr("q1", today, strategy="R2_breakout", exit_px="95", exit_reason="timer", exit_min="30", grace="0", dump_bucket="1")])
wcsv("settlements.csv", settle.HEADERS, [setr("q1|R2_breakout", "rev", "R2_breakout", net_live_pct="-5.15", net_tape_pct="",
                                                net_official_pct="-5.15", flags="no_trigger")])
api = mk_api(); o = api["strat2_api"]()
r2s = o["strategies"]["R2_breakout"]
# v2.20: STRAT_SINCE R2 = 2.20 (тригер по Binance-потоку) — рядок 2.16 старий (adm), без підтвердженого сигналу поза заголовком, у журналі
assert r2s["n"] == 0 and r2s["n_paper"] == 1 and abs(r2s["paper"]["median"] + 5.15) < 1e-9 and r2s["n_mismatch"] == 1 and r2s["trades"][0]["status"] == "live_only"
# live без ціни, стрічка є → tape_only: результат зі стрічки, статус окремо
wcsv("rev.csv", rh, [revr("q2", today, exit_reason="no_price", exit_min="", m30="", grace="0", dump_bucket="1")])
wcsv("settlements.csv", settle.HEADERS, [setr("q2|R1_загальний", "rev", "R1_загальний", net_live_pct="", net_tape_pct="0.7",
                                                net_official_pct="0.7", flags="")])
api = mk_api(); o = api["strat2_api"]()
r1s = o["strategies"]["R1_загальний"]
assert r1s["n"] == 1 and abs(r1s["median"] - 0.7) < 1e-9 and r1s["n_tape_only"] == 1 and r1s["verified"]["n"] == 0
# крива (№3, аудит-3 №5): ТЕ САМЕ правило, що офіційний net — за хвилиною ГІРША з live і стрічки,
# коли є обидві (curve_src=official), незалежно від кількості угод; live/tape окремо
rows3 = [revr(f"c{i}", today, exit_px="101", exit_reason="timer", exit_min="30", grace="0", dump_bucket="1",
              **{f"m{k}": ("0.5" if k <= 30 else "0.1") for k in range(1, 61)}) for i in range(3)]
wcsv("rev.csv", rh, rows3)
ct = ";".join(["0.2"] * 30 + ["0.3"] * 30)   # стрічка гірша до m30 (0.2 < 0.35), live гірший після (−0.05 < 0.3)
wcsv("settlements.csv", settle.HEADERS,
     [setr(f"c{i}|R1_загальний", "rev", "R1_загальний", net_live_pct="0.85", net_tape_pct="0.6", net_official_pct="0.6",
           flags="", curve_tape=ct) for i in range(3)])
api = mk_api(); o = api["strat2_api"]()
r1s = o["strategies"]["R1_загальний"]
assert r1s["curve_src"] == "official" and abs(r1s["curve"][29] - 0.2) < 1e-9 and abs(r1s["curve"][59] - (0.1 - 0.15)) < 1e-9, (r1s["curve_src"], r1s["curve"][29], r1s["curve"][59])
assert r1s["curve_common_n"] == 3 and r1s["curve_tape_n"] == 3 and r1s["curve_live_n"] == 3 and r1s["curve_both_n"] == 3
assert abs(r1s["curve_live"][29] - 0.35) < 1e-9 and abs(r1s["curve_tape"][59] - 0.3) < 1e-9
# знак офіційної медіани і кривої m30 збігається (одне правило): official(c) = min(0.85, 0.6) = 0.6 > 0; крива m30 = 0.2 > 0
assert r1s["median"] == 0.6 and r1s["curve"][29] > 0
# одна угода зі стрічкою — теж official (без перемикання методики на третій угоді)
wcsv("rev.csv", rh, rows3[:1]); wcsv("settlements.csv", settle.HEADERS,
     [setr("c0|R1_загальний", "rev", "R1_загальний", net_live_pct="0.85", net_tape_pct="0.6", net_official_pct="0.6", flags="", curve_tape=ct)])
api = mk_api(); o = api["strat2_api"](); r1s = o["strategies"]["R1_загальний"]
assert r1s["curve_src"] == "official" and abs(r1s["curve"][29] - 0.2) < 1e-9 and r1s["curve_both_n"] == 1
# без стрічки — official = live (те, що є); F без власних траєкторій — shadow
wcsv("settlements.csv", settle.HEADERS, [])
api = mk_api(); o = api["strat2_api"](); r1s = o["strategies"]["R1_загальний"]
assert r1s["curve_src"] == "official" and abs(r1s["curve"][29] - 0.35) < 1e-9 and r1s["curve_tape_n"] == 0
# журнал подій: JSON-рядки у DATA_DIR
dj = tempfile.mkdtemp()
J = load({"_journal"}, dict(C, DATA_DIR=dj, stats={}, _journal_lock=threading.Lock()))
J["_journal"]("close_batch", addr=A, coin="AAA", txs=[{"px": 1.5}])
fjs = glob.glob(os.path.join(dj, "events-*.jsonl"))
assert len(fjs) == 1
rec = json.loads(open(fjs[0], encoding="utf-8").read().strip())
assert rec["kind"] == "close_batch" and rec["txs"][0]["px"] == 1.5 and rec["addr"] == A
# рев'ю: поле події не конфліктує з іменем параметра (rev_exit несе exit_kind); kind= у полях не кидає
J["_journal"]("rev_exit", pid="x", exit_kind="tp", exit_min=12)
J["_journal"]("probe", kind="dup")
recs = [json.loads(l) for l in open(fjs[0], encoding="utf-8").read().splitlines() if l.strip()]
assert recs[1]["kind"] == "rev_exit" and recs[1]["exit_kind"] == "tp" and len(recs) == 3 and J["stats"].get("journal_err", 0) == 0
# структурні: скидання fc_episodes на доливі/реопені/фліпі/тихому закритті (№7)
mon = src[src.index("def run_realtime_monitor"):src.index("def check_position_changes")]
assert mon.count("fc_episodes.pop((addr, coin), None)") >= 3
ins = src[src.index("def _insert_flipped"):src.index("def run_realtime_monitor")]
assert "fc_episodes.pop((addr, coin), None)" in ins
assert 'strict=True' in src[src.index("def follow_on_txs"):src.index("def _profile_request")]
assert 'strict=True' in src[src.index("def rev_on_close"):src.index("def _rev_trade_closed")]
assert '_twap_drop(rec, "no_book")' in src and 'BOOK_MAX_AGE_S' in src and '"trunc": trunc' in src
assert 'settlement curve_tape' in src or '_parse_curve(srow.get("curve_tape")' in src
print("9) аудит-2: сирі ціни філів, епізод (позиція не зростала), R6 повне, референс ≤12 с, строгий стакан, кінець зливу, витрати за ногою, вихід без поллера, дедлайн R2, TWAP late, статуси, зріз, журнал")

# ═══ 10. Аудит-3 (12.09): 11 знахідок + залишкові ═══════════════════════
# (а) №9 follow-гейт: спершу СВІЖІ придатні tx, потім найбільша — стара велика не відкидає свіжу придатну
_tn = int(time.time() * 1000)
n, st = mk_fol()
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 250.0, "px": 80.0, "hash": "0xold", "dir": "Close Long", "ts": _tn - 100_000, "sp": 1000.0},
                                     {"sz": 125.0, "px": 80.0, "hash": "0xnew", "dir": "Close Long", "ts": _tn - 1_000, "sp": 1000.0}], False)
assert opened(n), "свіжа придатна tx має відкрити вхід"
p1 = next(iter(n["follow_open"].values()))
assert p1["tx_usd"] == 125.0 * 80.0 and p1["whale_px"] == 80.0 and 0 < p1["lag_s"] < 5.0 and p1["fill_ts_ms"] == _tn - 1_000, p1
assert st.get("follow_stale_skips", 0) == 0
n, st = mk_fol()
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 250.0, "px": 80.0, "hash": "0xold", "dir": "Close Long", "ts": _tn - 100_000, "sp": 1000.0}], False)
assert not opened(n) and st.get("follow_stale_skips") == 1
# (б) №8 мітка останнього закриття пари — БІРЖОВИЙ час філа, не час отримання
n, st = mk_fol()
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 250.0, "px": 80.0, "hash": "0xold", "dir": "Close Long", "ts": _tn - 90_000, "sp": 1000.0}], False)
assert abs(n["follow_last_close"][A + ":AAA"] - (_tn - 90_000) / 1000.0) < 1e-6
# таймер тиші відкритої угоди рахується від біржового часу: філ 93 с тому (отриманий щойно) → silence одразу,
# late = 33 с ≤ 60 → звичайна silence (не silence_late); філ 130 с тому → silence_late
for _ago, _exp in ((93.0, "silence"), (130.0, "silence_late")):
    fo = {"f1": fol_tr()}; w = []
    run_loop({}, fo, [99.6, 99.6], t0, w, extra={"follow_last_close": {A + ":AAA": t0 - _ago}})
    assert [x[0] for x in w] == ["f"] and w[0][1][fh.index("exit_reason")] == _exp, (_ago, w[0][1][fh.index("exit_reason")])
# (в) залишкове: лаг ВИКОНАННЯ після запиту стакану гейтить вхід (follow 20 с, rev 60 с) і пишеться у рядок
T5 = FakeTime()
n, st = mk_fol()
n["time"] = T5
def _slow_book(coin, side, usd=None, **kw):
    T5.now += 5.0
    return {"px": 99.5, "mid": 100.0, "bid": 99.9, "ask": 100.1, "ts_ms": 0, "req_ms": 5000, "partial": 0}
n["hl_book_exec"] = _slow_book
_t5 = int(T5.now * 1000)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 125.0, "px": 80.0, "hash": "0xf", "dir": "Close Long", "ts": _t5 - 17_000, "sp": 1000.0}], False)
assert not opened(n) and st.get("follow_stale_skips") == 1, "17 с при рішенні + 5 с стакану = 22 с > 20 → без входу"
n, st = mk_fol(); n["time"] = T5; n["hl_book_exec"] = _slow_book
_t5 = int(T5.now * 1000)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 125.0, "px": 80.0, "hash": "0xg", "dir": "Close Long", "ts": _t5 - 10_000, "sp": 1000.0}], False)
assert opened(n) and abs(next(iter(n["follow_open"].values()))["lag_s"] - 15.0) < 0.5
T6 = FakeTime()
n, st, sig = mk_rev(ep=None); n["time"] = T6
def _slow_book2(coin, side, usd=None, **kw):
    T6.now += 5.0
    return {"px": 100.3, "mid": 100.05, "bid": 100.0, "ask": 100.1, "ts_ms": 0, "req_ms": 5000, "partial": 0}
n["hl_book_exec"] = _slow_book2
_t6 = int(T6.now * 1000)
n["rev_on_close"](A, "AAA", OLD_R, [{"sz": 400.0, "px": 100.0, "hash": "0x9", "dir": "Close Long", "ts": _t6 - 57_000, "sp": 1000.0}], True)
assert strats_of(n) == set() and st.get("rev_stale_skips") == 1, strats_of(n)   # 57 + 5 = 62 с > 60; v2.19: без тіньового рядка
assert abs(float(sig[-1][1][sh.index("lag_s")]) - 62.0) < 0.5
# (г) №10 таймерний вихід R вирішується за ЧАСОМ: поллер мідів мертвий (семплів m30 немає), стакан є → вихід зі стакану
XBOOK.update(px=100.9, mid=100.95, src="book")
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 30 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [None, None], t0, [])
p = ro["s1|R1_загальний"]
assert p.get("exit_reason") == "timer" and p["exit_px"] == 100.9 and p["exit_src"] == "book" and p["exit_min"] == 30, p.get("exit_reason")
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 32 * 60 - 5, [0.1] * 29)}   # вікно 30..33 — timer_late m32
run_loop(ro, {}, [None, None], t0, [])
p = ro["s1|R1_загальний"]
assert p.get("exit_reason") == "timer_late" and p["exit_min"] == 32 and p["exit_px"] == 100.9, (p.get("exit_reason"), p.get("exit_min"))
# ні стакану, ні міда, ні семплів — після вікна no_price (як раніше)
XBOOK["none"] = True; XBOOK["fallback_mid"] = None
ro = {"s1|R1_загальний": rev_tr("R1_загальний", t0 - 35 * 60 - 5, [0.1] * 29)}
run_loop(ro, {}, [None, None], t0, [])
assert ro["s1|R1_загальний"]["exit_reason"] == "no_price"
XBOOK["none"] = False
# TWAP: рядок несе причину/хвилину, ВИРІШЕНІ у фазі 1 (ex), а не перерахунок за now2 після капу
TR = load({"_twap_row", "_twap_exit", "_twap_close_min", "_p_costs", "_leg_costs", "_sim_slip", "_ms", "_rnd"},
          dict(C, TWAP_HOLD_MIN=60, TWAP_EXIT_CAP_MIN=5, _dt=_dt), assigns=("TWAP_HEADERS",))
_tp = {"sig_id": "x", "strategy": "T1", "entry_ts": t0 - 66 * 60, "coin": "AAA", "side": "LONG", "entry_px": 100.0,
       "samples": [], "peak": -999.0, "trough": 999.0, "costs": 0.15, "entry_src": "book",
       "twap": {"exit_min": 0, "exit_px": 101.0, "exit_src": "book"}}
_row = TR["_twap_row"](_tp, t0, ex=(65, "timer_late"))
_TH = TR["TWAP_HEADERS"]
assert _row[_TH.index("exit_reason")] == "timer_late" and _row[_TH.index("exit_min")] == 65 and abs(_row[_TH.index("net60_pct")] - (1.0 - 0.1)) < 1e-9, (_row[_TH.index("exit_reason")], _row[_TH.index("net60_pct")])
_row2 = TR["_twap_row"](_tp, t0)   # без ex: за now — після капу це no_price
assert _row2[_TH.index("exit_reason")] == "no_price"
# TWAP: хвилина виходу настала без семпла → вихід вирішено за часом (mx None → ціна зі стакану)
TX = load({"_twap_exit", "_twap_close_min"}, dict(C, TWAP_HOLD_MIN=60, TWAP_EXIT_CAP_MIN=5))
_trw = {"entry_ts": t0 - 60 * 60 - 5, "samples": [], "twap": {"exit_min": 0}}
assert TX["_twap_exit"](_trw, t0) == (60, "timer_60m", None)
_trw2 = {"entry_ts": t0 - 62 * 60 - 5, "samples": [], "twap": {"exit_min": 0}}
assert TX["_twap_exit"](_trw2, t0) == (62, "timer_late", None)
_trw3 = {"entry_ts": t0 - 66 * 60, "samples": [], "twap": {"exit_min": 0}}
assert TX["_twap_exit"](_trw3, t0) == (60, "no_price", None)
assert TX["_twap_exit"]({"entry_ts": t0 - 59 * 60, "samples": [], "twap": {"exit_min": 0}}, t0) is None
# (д) залишкове: ratio не рахується по старій (>2 цикли) або обрізаній глибині; у скані така глибина = ratio 0
DP = load({"_discover_pairs", "_depth_ok", "depth_for_side"}, dict(C, COIN_BLACKLIST=set(), stats={}, REFRESH_S=1800,
          watchlist={}, watchlist_lock=threading.Lock(), _mark_ratio=lambda p, r, now=None: p.__setitem__("ratio", r),
          fill_cursor={}, cache={"depth": {}}, cache_lock=threading.Lock(), prio_stats={"added": 0}))
assert "_depth_ok(" in src[src.index("def _discover_pairs"):src.index("def _discover_pairs") + 3000]
assert src.count("_depth_ok(") >= 4
FR = load({"_fresh_ratio", "depth_for_side", "_depth_ok"}, dict(C, cache={"depth": {"AAA": {"bid": 1e5, "ask": 1e5, "ts": time.time()}}, "depth_prev": {}},
                                                  cache_lock=threading.Lock(), stats={}, REFRESH_S=1800))
assert abs(FR["_fresh_ratio"]("AAA", "LONG", 3e5) - 3.0) < 1e-9
FR["cache"]["depth"]["AAA"]["ts"] = time.time() - 3 * 1800
assert FR["_fresh_ratio"]("AAA", "LONG", 3e5) is None and FR["stats"]["ratio_stale_depth"] == 1
FR["cache"]["depth"]["AAA"].update(ts=time.time(), trunc=1)
assert FR["_fresh_ratio"]("AAA", "LONG", 3e5) is None and FR["stats"]["ratio_trunc_depth"] == 1
# (е) №1/№3 API: legacy-допуск R — лише повне закриття (settlement full_close), R7 — база за settlement;
#     F до DATA_ALGO_V без підтвердженого лагу — stale; ознаки епізоду — зі settlement для всіх рядків
wcsv("rev.csv", rh, [
    revr("r1", today, exit_px="101", exit_reason="timer", exit_min="30", grace="0", dump_bucket="1", dump_move_pct="-1.5"),
    revr("r5", ago3, algo="2.13", m30="2.0"),                       # legacy: full_close → допуск
    revr("r9", ago3, algo="2.16", m30="2.0"),                       # legacy 2.16: partial_close → ні
    revr("r10", ago3, algo="2.16", m30="2.0"),                      # legacy 2.16: close_unknown → ні
    revr("r11", ago3, algo="2.16", strategy="R7_одним", m30="2.0"), # legacy R7: найбільший філ 50% → ні
    revr("r12", ago3, algo="2.16", strategy="R7_одним", m30="2.0"), # legacy R7: 96% і $150k → допуск
])
wcsv("settlements.csv", settle.HEADERS, [
    setr("r1|R1_загальний", "rev", "R1_загальний", net_live_pct="0.85", net_tape_pct="0.4", net_official_pct="0.4",
         dump_bucket="3", dump_move_pct="0.4", dump_dur_s="150", lag_s="2", flags="dump_tape;full_close"),   # live bucket 1 ≠ settlement 3
    setr("r5|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="3", dump_move_pct="1.7", dump_dur_s="150", lag_s="3", flags="dump_tape;full_close"),
    setr("r9|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="1", dump_move_pct="1.7", dump_dur_s="50", lag_s="3", flags="dump_tape;partial_close"),
    setr("r10|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="1", dump_move_pct="1.7", dump_dur_s="50", lag_s="3", flags="dump_tape;close_unknown"),
    setr("r11|R7_одним", "rev", "R7_одним", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="1", dump_move_pct="1.7", dump_dur_s="50", lag_s="3", flags="dump_tape;full_close",
         whale_max_fill_pct="0.5", whale_max_fill_usd="150000"),
    setr("r12|R7_одним", "rev", "R7_одним", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0",
         dump_bucket="1", dump_move_pct="1.7", dump_dur_s="50", lag_s="3", flags="dump_tape;full_close",
         whale_max_fill_pct="0.96", whale_max_fill_usd="150000"),
    setr("t4", "fol", "F1_1хв", net_live_pct="0.35", net_tape_pct="0.9", net_official_pct="0.35", flags="no_fills"),
    setr("t5", "fol", "F1_1хв", net_live_pct="0.35", net_tape_pct="0.9", net_official_pct="0.35", lag_s="3", flags=""),
])
wcsv("follow.csv", fh, [folr("t4", today, algo="2.16"), folr("t5", today, algo="2.16"), folr("t6", today)])
api = mk_api(); o = api["strat2_api"]()
r1s = o["strategies"]["R1_загальний"]
assert r1s["n"] == 2 and r1s["n_adm_legacy"] == 1, (r1s["n"], r1s["n_adm_legacy"])          # r1 + r5; r9/r10 — ні
tr1 = [t for t in r1s["trades"] if t["adm"] == 0][0]
assert tr1["bucket"] == 3 and tr1["bucket_live"] == 1 and tr1["ep_mismatch"] == 1 and tr1["full_close"] == 1
assert abs(tr1["dump_move"] + 0.4) < 1e-9 and abs(tr1["dump_move_live"] + 1.5) < 1e-9   # знак live: LONG → −
assert r1s["n_ep_mismatch"] == 1 and r1s["buckets"]["3"]["n"] == 2 and r1s["buckets"]["1"]["n"] == 0
r7s = o["strategies"]["R7_одним"]
assert r7s["n"] == 1 and r7s["n_adm_legacy"] == 1 and r7s["trades"][0]["net30"] == 1.0
f1 = o["strategies"]["F1_1хв"]
_stale = {t["date"] + str(t["settled"]) + str(t["stale"]) for t in f1["trades"]}
assert f1["n_stale"] == 1 and f1["n"] == 2, (f1["n_stale"], f1["n"])      # t4: лаг невідомий → stale; t5 ок; t6 поточна
assert [t["stale"] for t in f1["trades"] if t["settled"] == 1 and t["net_tape"] == 0.9] == [1, 0]
# (є) №4 research: verified — на офіційних net R1 (та сама популяція, ознаки епізоду), observed — тіньова
#     стрічка ЛИШЕ поточної версії; старі outcome-рядки не змішуються
def outr(sig, algo="2.19", **kv):
    base = dict(sig_id=sig, date=today, coin="AAA", fade_side="LONG", whale_addr=A, src="wallet", detect_px="100",
                move_3m_pct="1.2", dur_s="60", sum_usd="1e5", usd_s="1e3", ratio="3", shtanga="0", vault="0", hour="10",
                btc_move_pct="0.1", btc_ok="1", would_open="1", depth_usd="1e5", costs_pct="0.15", algo_v=algo,
                dump_move_pct="-1.5", dump_bucket="1", m30="2.0")
    base.update(kv); return mkrow(oh, **base)
wcsv("out.csv", oh, [outr("o1"), outr("o2", algo="2.10", m30="9.0"), outr("o3", btc_ok="0", m30="-1.0"), outr("o4", dump_move_pct="-2.5", m30="3.0")])
api = mk_api(); api["RESEARCH_SINCE"] = "2.17"; o = api["strat2_api"]()
rs = o["research"]
assert set(rs) == {"verified", "observed"}
assert rs["verified"]["n_base"] == 2 and rs["verified"]["shtanga_0"]["n"] == 2 and abs(rs["verified"]["shtanga_0"]["median"] - 0.7) < 1e-9
assert rs["verified"]["mag_1_2"]["n"] == 1 and rs["verified"]["bucket_3_5"]["n"] == 2        # r1: 0.4% (settlement) → поза 1–2; r5: 1.7
assert rs["observed"]["n_base"] == 3 and rs["observed"]["n_excluded_old"] == 1                  # o2 (2.10) поза
assert rs["observed"]["btc_on"]["n"] == 2 and rs["observed"]["btc_off"]["n"] == 1 and rs["observed"]["mag_2p"]["n"] == 1
assert abs(rs["observed"]["btc_on"]["median"] - ((1.85 + 2.85) / 2)) < 1e-9
# (ж) структурні: журнал без обрізання на 40, версія 2.17, епізод через settle.close_episode
assert "mfills[:40]" not in src and "for f in txs[:500]" in src and "n_txs=len(txs)" in src   # v2.19: журнал у приймачі
assert "from settle import close_episode as _close_episode" in src and "_close_episode(fills, coin, t_last, True, grow_only=_grow_only)" in src   # v2.20: grow_only лише без пасивних філів
assert const("DATA_ALGO_V") == "2.20" and all(FH["STRAT_SINCE"][k] == "2.17" for k in ("R1_загальний", "R7_одним")) and FH["STRAT_SINCE"][R8] == "2.20"
assert 'if "full_close" not in fl:' in src and '"whale_max_fill_pct"' in src   # v2.19: у _sig_check_rev
assert "epx = xpx   # аудит-3 №11" in src and "base_px = xpx\n" in src and 'xsrc if xpx else "mid"' not in src
print("10) аудит-3: свіжі тригери спершу, біржовий час таймера, лаг виконання, таймер R/TWAP без міда, ratio по свіжій глибині, legacy лише повне закриття, епізод зі settlement, research verified/observed, журнал без обрізання")

print("\nУСІ РЕГРЕСІЇ v2.16 ЗЕЛЕНІ")

import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.20 — аудит v2.19 (10 знахідок + залишкові + рекомендації 6–7):
1  причинність котирувань: сирий кеш не віддає стакан до рішення (min_recv_ms),
   мід до події — не ціна, лаг < 0 = відмова + журнал, окремі мітки часу,
   _apply_delay проти рішення, _sig_check_* neg_lag
2  потік цін Binance: bn_px_hist/bookTicker, _tr_px_now по біржі трекера,
   тригер R2 / TP R8 лише по Binance, мід-фолбек тієї ж біржі, watchdog/status
3  settlement: Follow звіряється з тригерним філом (hash / ts / unmatched)
4  кеш v3 із доказом повноти: старі файли → повторний запит / без доказу
5  старий book_partial = частковий вихід (px_ok 0)
6  R8: tp_path_gap / tp_replay_thin → tape_thin, відкладення, фінал лише після
   перерахунку за 72 год
7  профілі: перший видимий долив = невизначений початок; пил у discovery;
   рефрешер (err / відкладені без профілю); ротація profiles_raw
8  скан не губить повне закриття: пара чекає sweep (_gone_ts), фліп, дроп, скидання
9  епізод на всіх філах (пасивні → лише в епізод), стабільний sig_id, ack
   споживачів (test_v219 блок 2 — повтор лише збійному)
10 статистики: групи через _head_ok, журнал зі старими R, TWAP «підтверджено» ∩
   заголовок, фандинг у кривій/net60/net_live, no_funding у кривій, eval-період,
   концентрація, pct_verified
11 структурні: версії, потоки, UI, watchdog, CLAUDE.md"""
import ast, threading, time, json, os, re, sys, math, tempfile, csv, glob, hashlib, shutil, gzip, types, io
import time as _tmod
import urllib.error, urllib.request
from collections import OrderedDict, deque
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HL)
from v28_shim import NEW, _median
import settle as S

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None, assigns=()):
    body = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in assigns for t in n.targets))]
    mod = ast.Module(body=body, type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "hashlib": hashlib, "shutil": shutil, "glob": glob,
          "urllib": urllib, "print": lambda *a, **k: None, "OrderedDict": OrderedDict}
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
C = dict(PROFILE_ALGO_V=11, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.20", F4_MIN_FAST_PCT=70.0,
         F9_MIN_FAST_PCT=90.0, PROFILE_ENTRY_LAT_MS=5000, PROFILE_ZERO_TOL=1e-6,
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005, SIM_SPREAD=0.0002,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True,
         F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0, F5_NAME="F5_перший",
         F5_FIRST_SHOT_S=3600.0, F7_NAME="F7_без_ратіо", RATIO_GRACE_S=1800,
         F8_NAME="F8_ratio35", F8_MIN_RATIO=3.5, F9_NAME="F9_без_ратіо_90", F10_NAME="F10_розумний_60",
         F_FIXED_TIMER_S=60.0, FC_ENABLED=True, FC_MAX_EPISODE_S=300, R7_NAME="R7_одним",
         R7_MIN_TX_USD=100_000.0, R8_NAME=R8, R8_TP_FRAC=0.8, REV_HOLD_MIN=30, REV_EP_MAX_S=300.0,
         REV_EP_MIN_MAG=1.0, FOLLOW_MAX_FILL_AGE_S=20.0, REV_MAX_FILL_AGE_S=60.0, BOOK_MAX_AGE_S=5.0,
         REV_REF_TOL_S=12.0, EXEC_EXCH="bn", BOOK_APPLY_MAX_S=5.0, BN_EXEC_LEVELS=100, FUND_CACHE_S=60.0,
         HEAD_STRICT=True, _REV_THR={"R4_великий": 2.0, "R5_дуже": 3.0}, EVAL_SINCE="2026-09-13",
         GONE_MAX_S=7200.0, PRIO_AGG_MIN_USD=0.0, BOOK_RAW_TTL_S=2.0, INGEST_RETRY_MAX=50)
assert const("DATA_ALGO_V") == "2.20" and const("PROFILE_ALGO_V") == 11 and const("EVAL_SINCE") == "2026-09-13"
assert const("PRIO_AGG_MIN_USD") == 0.0 and const("GONE_MAX_S") == 7200 and const("BOOK_RAW_TTL_S") == 2.0
assert S.SETTLE_V == "5" and S.CACHE_V == 3 and S.THIN_FLAGS == ("tp_path_gap", "tp_replay_thin")
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
          assigns=("FOLLOW_HEADERS", "REV_HEADERS", "TWAP_HEADERS", "STRAT_SINCE", "EXEC_COLS", "REV_SIG_HEADERS",
                   "REV_OUT_HEADERS", "TWAP_CURVE_HEADERS", "FOLLOW_OUT_HEADERS", "TAPE_SINCE"))
fh, rh, th = FH["FOLLOW_HEADERS"], FH["REV_HEADERS"], FH["TWAP_HEADERS"]
assert FH["EXEC_COLS"][-5:] == ["decision_ms", "detect_ms", "exit_decision_ms", "exit_px_filled", "trig_hash"]
assert FH["STRAT_SINCE"]["R2_breakout"] == "2.20" and FH["STRAT_SINCE"][R8] == "2.20" and FH["STRAT_SINCE"]["R1_загальний"] == "2.17"
assert "trig_match" in S.HEADERS
A = "0x" + "a" * 40
now0 = time.time()
print("0) константи v2.20 (версії, EVAL_SINCE, пил discovery, GONE_MAX_S), колонки виконання +5, STRAT_SINCE R2/R8")

# ═══ 1. Причинність котирувань ═══════════════════════════════════════════
RB = load({"_raw_book_get", "_raw_book_put", "_book_from_raw", "_book_walk", "_book_quote", "bn_book_exec",
           "hl_book_exec"},
          dict(C, stats={}, _depth_universe=[set()], _bn_backoff=[0.0], _raw_book_cache={},
               get_bn_symbol=lambda c: c.upper() + "USDT", _prio_opener=None))
t_fetch = time.time() - 0.5
raw = RB["_raw_book_put"]("bn", "AAA", t_fetch - 0.1, t_fetch, [(99.9, 5.0)], [(100.1, 5.0)], int(t_fetch * 1000))
assert RB["_raw_book_get"]("bn", "AAA") is raw                                       # без вимоги — свіжий кеш
assert RB["_raw_book_get"]("bn", "AAA", min_recv_ms=int((t_fetch - 0.2) * 1000)) is raw   # рішення ДО отримання — ок
assert RB["_raw_book_get"]("bn", "AAA", min_recv_ms=int((t_fetch + 0.2) * 1000)) is None  # рішення ПІСЛЯ отримання — ні
net = []
class _Resp(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): return False
def _fake_urlopen(req, timeout=4):
    net.append(req.full_url)
    return _Resp(json.dumps({"T": int(time.time() * 1000), "bids": [["99.8", "50"]], "asks": [["100.2", "50"]]}).encode())
RB["urllib"] = types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request, urlopen=_fake_urlopen))
r_old = RB["bn_book_exec"]("AAA", "BUY", usd=1000, min_recv_ms=int((t_fetch - 0.2) * 1000))
assert r_old["px"] == 100.1 and not net                                          # кеш придатний
r_new = RB["bn_book_exec"]("AAA", "BUY", usd=1000, min_recv_ms=int((t_fetch + 0.2) * 1000))
assert r_new["px"] == 100.2 and len(net) == 1 and r_new["recv_ms"] >= int((t_fetch + 0.2) * 1000)   # новий запит
# _paper_px: мід-фолбек лише ТІЄЇ Ж біржі і не старший за рішення
PX = load({"_paper_px", "_px_mid_age", "_bn_px_mid_age", "_bn_px_now", "_mid_age_exch"},
          dict(C, stats={}, px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 1.0, 100.9)]},
               bn_px_lock=threading.Lock(), bn_px_hist={"AAAUSDT": [(time.time() - 1.0, 99.9, 99.8, 100.0)]},
               get_bn_symbol=lambda c: c.upper() + "USDT",
               bn_book_exec=lambda *a, **k: None, hl_book_exec=lambda *a, **k: None))
px, srcx, meta = PX["_paper_px"]("AAA", "BUY", strict=False, exch="bn")
assert px == 99.9 and srcx == "mid" and meta["exch"] == "bn", (px, srcx)          # Binance-мід, не HL 100.9
px, srcx, meta = PX["_paper_px"]("AAA", "BUY", strict=False, exch="hl")
assert px == 100.9 and srcx == "mid"                                              # старий трекер — HL
px, srcx, meta = PX["_paper_px"]("AAA", "BUY", strict=False, exch="bn", min_recv_ms=int(time.time() * 1000))
assert px is None and srcx == "none" and PX["stats"]["px_pre_decision"] == 1     # семпл ДО рішення — не ціна
px, srcx, meta = PX["_paper_px"]("AAA", "BUY", strict=False, exch="bn", min_recv_ms=int((time.time() - 5) * 1000))
assert px == 99.9 and abs(meta["recv_ms"] - PX["bn_px_hist"]["AAAUSDT"][-1][0] * 1000) < 2   # recv = момент семпла
# _sig_check: нижня межа лага
SC = load({"_sig_check_rev", "_sig_check_fol"}, dict(C))
fnum = lambda v, d=None: (float(v) if v not in (None, "") else d)
base_s = {"flags": "full_close;dump_tape", "dump_bucket": "2", "dump_move_pct": "1.5", "lag_s": "-0.5"}
assert SC["_sig_check_rev"]("R1_загальний", base_s, fnum) == (0, "neg_lag")
assert SC["_sig_check_fol"]({"flags": "full_close", "lag_s": "-0.1"}, fnum) == (0, "neg_lag")
assert SC["_sig_check_fol"]({"flags": "trigger_unmatched"}, fnum) == (None, "trig_unmatched")
assert SC["_sig_check_fol"]({"flags": "full_close", "lag_s": "3"}, fnum) == (1, "")
# follow_on_txs: котирування, отримане ДО філа кита → лаг < 0 → відмова + журнал; мітки часу й тригер у трекері
jrn = []
def mk_fol(recv_off):
    def _pp(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
        return 100.0, "book", {"mid": 100.0, "px_age_ms": 50, "recv_ms": int((time.time() - recv_off) * 1000),
                               "fill_frac": 1.0, "exch": "bn", "qty": 10.0, "quote_ts_ms": 1}
    return load({"follow_on_txs", "_sim_costs", "_sim_slip", "depth_for_side", "_entry_exec_fields", "_ratio_ok",
                 "_grace_val"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={}, follow_last_close={}, rev_open={}, wallet_profiles={},
        profiles_fetching=set(), profile_retry_at={}, stats={}, _journal=lambda k, **f: jrn.append((k, f)),
        _paper_px=_pp, _px_now=lambda c, max_age=20: 100.0, _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        is_vault=lambda a: False, cache={"depth": {"AAA": {"bid": 1e5, "ask": 1e5, "ts": time.time()}}, "depth_prev": {}},
        cache_lock=threading.Lock(), _profile_request=lambda a: None, _dt=_dt, bn_funding_info=lambda c: (None, None),
        _depth_ok=lambda d, count=True: True, REFRESH_S=30, _sig_retry_lock=threading.Lock(), _sig_retry_q=[]))
OLD_F = {"size": 1000.0, "val": 5e5, "side": "LONG", "ratio": 3.0, "ratio_hi_ts": time.time()}
fill = {"sz": 100.0, "px": 100.0, "hash": "0xtrig", "dir": "Close Long", "ts": int((time.time() - 2) * 1000), "sp": 1000.0}
n = mk_fol(recv_off=5.0)                      # ціна отримана за 5 с до «зараз» = до філа (2 с тому)
n["follow_on_txs"](A, "AAA", OLD_F, [fill], False, detect_src="ws", detect_ms=int((time.time() - 1.5) * 1000))
assert not n["follow_open"] and n["stats"]["follow_neg_lag"] == 1 and jrn[-1][0] == "follow_skip" and jrn[-1][1]["why"] == "neg_lag"
n = mk_fol(recv_off=-0.05)                    # ціна отримана ПІСЛЯ рішення → вхід; мітки часу й тригер у трекері
n["follow_on_txs"](A, "AAA", OLD_F, [fill], False, detect_src="ws", detect_ms=int((time.time() - 1.5) * 1000))
p = next(iter(n["follow_open"].values()))
assert p["trig_hash"] == "0xtrig" and p["trig_dir"] == "Close Long" and p["trig_sz"] == 100.0 and p["trig_sp"] == 1000.0
assert p["detect_ms"] and p["decision_ms"] and p["detect_ms"] <= p["decision_ms"] <= p["open_ts"] * 1000 + 1 \
       and p["lag_s"] > 0
FR = load({"_fol_row", "_exec_extra", "_p_costs", "_ms", "_rnd", "_leg_costs", "_sim_slip"}, dict(C, _dt=_dt))
row = FR["_fol_row"](dict(p, costs=0.2), "fid", time.time(), 99.5, "silence", 0.5)
assert len(row) == len(fh) - 1 and row[fh.index("trig_hash")] == "0xtrig" and row[fh.index("decision_ms")] == p["decision_ms"] \
       and row[fh.index("detect_ms")] == p["detect_ms"]
# rev_on_close: тіньова стрічка від міда лише ПІСЛЯ події (останнього філа), hash тригера у трекері
def mk_rev(mid_ts_off, book=None):
    def _pp(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
        if book is None:
            return None, "no_book", {"mid": None, "px_age_ms": None, "exch": "bn", "recv_ms": None}
        return book, "book", {"mid": book, "px_age_ms": 50, "recv_ms": int(time.time() * 1000), "fill_frac": 1.0,
                              "exch": "bn", "qty": 10.0, "book_mid": book}
    n = load({"rev_on_close", "_rev_ref_px", "_px_at", "_sim_costs", "_sim_slip", "depth_for_side", "_entry_exec_fields",
              "_ratio_ok", "_grace_val", "_rev_trade_closed", "_depth_ok"}, dict(C,
        strat2_lock=threading.RLock(), rev_open={}, follow_open={}, fc_lock=threading.Lock(), fc_episodes={},
        stats={}, _journal=lambda k, **f: jrn.append((k, f)), _paper_px=_pp, px_lock=threading.Lock(),
        px_hist={"BTC": [(time.time() - 1, 100.0)]}, px_min={}, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5, is_vault=lambda a: False,
        cache={"depth": {"AAA": {"bid": 1e5, "ask": 1e5, "ts": time.time()}}, "depth_prev": {}}, cache_lock=threading.Lock(),
        _dt=_dt, _strat_csv_append=lambda *a, **k: True, REV_SIG_CSV="s", REV_SIG_HEADERS=FH["REV_SIG_HEADERS"],
        _mid_age_exch=lambda ex, c: (99.5, 1000, time.time() - mid_ts_off), bn_funding_info=lambda c: (None, None),
        wallet_profiles={}, _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _vt=NEW["_vt"], PART_OUT_PCT=0.30,
        VAULT_PART_PCT=0.05, REV_OUT_MIN_MAG=0.5, _rev_extra=NEW["_rev_extra"], REFRESH_S=30))
    return n
OLD_R = {"size": 1000.0, "val": 5e5, "side": "LONG", "ratio": 3.0, "ratio_hi_ts": time.time()}
t_now_ms = int(time.time() * 1000)
FILLS = [{"sz": 1000.0, "px": 99.0, "px_first": 100.0, "px_last": 98.5, "hash": "0xlast", "dir": "Close Long",
          "ts": t_now_ms - 3000, "sp": 1000.0}]
EP = {"first_ts": t_now_ms - 60_000, "first_px": 101.8, "last_ts": t_now_ms - 3000, "sum_usd": 1e5, "val": 5e5, "ratio": 3.0,
      "txs": [], "seen": set(), "side": "LONG", "start_size": 1000.0, "max_sz": 1000.0, "max_usd": 1e5, "max_liq": 0}
n = mk_rev(mid_ts_off=10.0); n["fc_episodes"][(A, "AAA")] = dict(EP)   # мід знято за 10 с до «зараз» = ДО філа (3 с тому)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert not n["rev_open"] and n["stats"]["rev_no_px"] == 1
n = mk_rev(mid_ts_off=1.0); n["fc_episodes"][(A, "AAA")] = dict(EP)    # мід після філа → тінь є
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert [p["strategy"] for p in n["rev_open"].values()] == ["_OUTCOME"]
n = mk_rev(mid_ts_off=1.0, book=99.6); n["fc_episodes"][(A, "AAA")] = dict(EP)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True, detect_src="ws", detect_ms=t_now_ms - 2500)
r1 = [p for p in n["rev_open"].values() if p["strategy"] == "R1_загальний"][0]
assert r1["trig_hash"] == "0xlast" and r1["detect_ms"] == t_now_ms - 2500 and r1["decision_ms"] >= r1["detect_ms"] \
       and r1["sig_id"].startswith(f"{FILLS[0]['ts']}-AAA-") and r1["sig_id"].endswith("-last")   # стабільний id
# _apply_delay (тик): ціна, отримана ДО рішення фази 1, не застосовується
_tk = src[src.index("        def _apply_delay(xmeta, now2, req=None):"):src.index("        _jrn = []   # події журналу")]
ns_ad = {"BOOK_APPLY_MAX_S": 5.0, "stats": {}}
exec(compile("\n".join(l[8:] for l in _tk.split("\n")), "ad", "exec"), ns_ad)
nowx = time.time()
assert ns_ad["_apply_delay"]({"recv_ms": int((nowx - 1) * 1000)}, nowx, {"ts": nowx - 2})[0] is not None
assert ns_ad["_apply_delay"]({"recv_ms": int((nowx - 3) * 1000)}, nowx, {"ts": nowx - 2}) == (None, None) \
       and ns_ad["stats"]["book_pre_decision"] == 1
assert ns_ad["_apply_delay"]({"recv_ms": int((nowx - 6) * 1000)}, nowx, {"ts": nowx - 7}) == (None, None) \
       and ns_ad["stats"]["book_apply_stale"] == 1
print("1) причинність: кеш стакану/мід не раніше за рішення (події), лаг<0 = відмова, мітки часу й hash тригера у трекері/рядку, стабільний sig_id, _apply_delay")

# ═══ 2. Потік цін Binance ═════════════════════════════════════════════════
BN = load({"_bn_px_mid_age", "_bn_px_now", "_tr_exch", "_tr_px_now", "_mid_age_exch", "_px_now", "_px_mid_age",
           "run_bn_px_poller", "_bn_px_age_s"},
          dict(C, stats={}, px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 1.0, 100.9)]},
               bn_px_lock=threading.Lock(), bn_px_hist={}, get_bn_symbol=lambda c: c.upper() + "USDT",
               _bn_backoff=[0.0], BN_PX_POLL_S=3.0, BN_PX_URL="https://x/bookTicker"))
calls_bn = []
class _Stop(Exception): pass
def _fake_bt(req, timeout=5):
    calls_bn.append(req.full_url)
    return _Resp(json.dumps([{"symbol": "AAAUSDT", "bidPrice": "99.8", "askPrice": "100.0", "time": 1},
                             {"symbol": "BBBUSDT", "bidPrice": "nan", "askPrice": "1"},
                             {"symbol": "CCCUSDT", "bidPrice": "0", "askPrice": "1"}]).encode())
BN["urllib"] = types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request, urlopen=_fake_bt))
BN["time"] = types.SimpleNamespace(time=time.time, sleep=lambda s: (_ for _ in ()).throw(_Stop()))
try:
    BN["run_bn_px_poller"]()
except _Stop:
    pass
assert len(calls_bn) == 1 and set(BN["bn_px_hist"]) == {"AAAUSDT"} and BN["bn_px_hist"]["AAAUSDT"][-1][1] == 99.9 \
       and BN["stats"]["bn_px_n"] == 1 and BN["_bn_px_age_s"]() < 2
assert BN["_bn_px_now"]("AAA") == 99.9 and BN["_bn_px_now"]("ZZZ") is None
assert BN["_tr_px_now"]({"coin": "AAA", "exch": "bn"}) == 99.9 and BN["_tr_px_now"]({"coin": "AAA", "algo_v": "2.18"}) == 100.9 \
       and BN["_tr_px_now"]({"coin": "AAA", "algo_v": "2.20"}) == 99.9     # без поля exch, ≥2.19 → EXEC_EXCH=bn
assert BN["_mid_age_exch"]("bn", "AAA")[0] == 99.9 and BN["_mid_age_exch"]("hl", "AAA")[0] == 100.9
# 429 → бекоф, без винятку
def _bt429(req, timeout=5):
    e = urllib.error.HTTPError(req.full_url, 429, "rl", {"Retry-After": "45"}, None); raise e
BN["urllib"] = types.SimpleNamespace(request=types.SimpleNamespace(Request=urllib.request.Request, urlopen=_bt429))
try:
    BN["run_bn_px_poller"]()
except _Stop:
    pass
assert BN["_bn_backoff"][0] >= time.time() + 40 and BN["stats"]["bn_px_fail"] == 1
# тик: тригер R2 і TP R8 Binance-трекера — лише по Binance-міду (HL 100.9 не озброює; Binance 99.9 не б'є TP)
class FakeTime:
    def __init__(self, t): self.now = t
    def time(self): return self.now
    sleep = staticmethod(_tmod.sleep); localtime = staticmethod(_tmod.localtime)
    strftime = staticmethod(_tmod.strftime); gmtime = staticmethod(_tmod.gmtime)
    mktime = staticmethod(_tmod.mktime); strptime = staticmethod(_tmod.strptime)
T8 = FakeTime(1_800_000_000.0)
def _paper_none(*a, **k):
    return None, "no_book", {"mid": None, "px_age_ms": None, "recv_ms": None, "exch": "bn"}
TK = load({"_strat2_tick", "_fol_row", "_exec_extra", "_p_costs", "_ms", "_rnd", "_leg_costs", "_funding_pct", "_vt",
           "_rev_trade_closed", "_tr_exch", "_tr_px_now", "_bn_px_now", "_bn_px_mid_age", "_px_now", "_mid_age_exch",
           "_px_mid_age"}, dict(C,
    time=T8, stats={}, strat2_lock=threading.RLock(), _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
    rev_open={}, follow_open={}, follow_last_close={}, px_lock=threading.Lock(), px_hist={"AAA": [(T8.now, 100.9)]},
    bn_px_lock=threading.Lock(), bn_px_hist={"AAAUSDT": [(T8.now, 99.9, 99.8, 100.0)]},
    get_bn_symbol=lambda c: c.upper() + "USDT", _sim_slip=lambda d: 0.0005, _dt=_dt, _paper_px=_paper_none,
    _journal=lambda *a, **k: None, _strat_csv_append=lambda p, h, r: True, _rev_row=lambda p, e: ["rev-row"],
    _rev_out_row=lambda p: ["out-row"], _fol_out_row=lambda p: ["fo-row"], FOLLOW_OUT_CSV="fo.csv",
    FOLLOW_OUT_HEADERS=[], save_state=lambda: None, REV_CSV="r.csv", REV_OUT_CSV="o.csv", FOLLOW_CSV="f.csv",
    REV_HEADERS=[], REV_OUT_HEADERS=[], FOLLOW_HEADERS=fh, FOLLOW_TIMERS={"F1_1хв": 60}))
armed = {"sig_id": "s2", "strategy": "R2_breakout", "state": "armed", "coin": "AAA", "side": "LONG", "addr": "0xa",
         "detect_ts": T8.now - 10, "entry_ts": None, "entry_px": None, "trigger_px": 100.8, "samples": [],
         "deadline": T8.now + 500, "algo_v": "2.20", "exch": "bn", "peak": -999.0, "trough": 999.0, "track_min": 60}
r8t = {"sig_id": "s8", "strategy": R8, "state": "open", "coin": "AAA", "side": "LONG", "addr": "0xa",
       "detect_ts": T8.now - 120, "entry_ts": T8.now - 120, "entry_px": 100.0, "tp_px": 100.8, "samples": [],
       "algo_v": "2.20", "exch": "bn", "peak": -999.0, "trough": 999.0, "track_min": 60, "entry_qty": 10.0}
TK["rev_open"]["s2|R2"] = dict(armed, samples=[]); TK["rev_open"]["s8|R8"] = dict(r8t, samples=[])
TK["_strat2_tick"]()
assert TK["rev_open"]["s2|R2"]["state"] == "armed" and "arm_hit" not in TK["rev_open"]["s2|R2"]   # HL 100.9 > 100.8 — ігнор
assert "exit_req" not in TK["rev_open"]["s8|R8"] and not TK["rev_open"]["s8|R8"].get("exit_reason")   # TP по Binance 99.9 < 100.8
TK["bn_px_hist"]["AAAUSDT"].append((T8.now, 100.85, 100.8, 100.9))                # Binance перетнув
def _paper_book(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
    return 100.86, "book", {"mid": 100.85, "px_age_ms": 50, "recv_ms": int(T8.now * 1000), "fill_frac": 1.0,
                            "exch": exch or "bn", "qty": qty, "px_filled": 100.86}
TK["_paper_px"] = _paper_book
TK["_strat2_tick"]()
assert TK["rev_open"]["s2|R2"]["state"] == "open" and TK["rev_open"]["s2|R2"]["entry_px"] == 100.86 \
       and TK["rev_open"]["s2|R2"]["decision_ms"] == int(T8.now * 1000)               # озброєння по Binance-міду 100.85
assert TK["rev_open"]["s8|R8"].get("exit_reason") == "tp" and TK["rev_open"]["s8|R8"]["exit_decision_ms"] == int(T8.now * 1000) \
       and TK["rev_open"]["s8|R8"]["exit_px_filled"] == 100.86
# старий трекер (до 2.19, HL) семплюється по HL
oldt = dict(r8t, sig_id="s9", strategy="R1_загальний", algo_v="2.18", exch=None, tp_px=None,
            entry_ts=T8.now - 61, detect_ts=T8.now - 61, samples=[])
TK["rev_open"]["s9|R1"] = oldt
T8.now += 3
TK["px_hist"]["AAA"].append((T8.now, 100.9)); TK["bn_px_hist"]["AAAUSDT"].append((T8.now, 100.85, 100.8, 100.9))
TK["_strat2_tick"]()
assert TK["rev_open"]["s9|R1"]["samples"][0] == round((100.9 / 100.0 - 1) * 100, 4)   # HL-мід (старий трекер)
assert TK["rev_open"]["s8|R8"]["samples"] == ["", round((99.9 / 100.0 - 1) * 100, 4)]   # m2 на 1-му тику — Binance 99.9, не HL 100.9
print("2) Binance-потік: bookTicker-поллер (nan/0 відкинуто, 429 → бекоф), _tr_px_now по біржі, R2/R8 лише по Binance, старі — HL")

# ═══ 3. Settlement: Follow звіряється з тригерним філом ═══════════════════
BASE = (int(time.time() * 1000) // S.DAY_MS) * S.DAY_MS - 3 * S.HOUR_MS
t_in = BASE + 10 * S.MIN_MS
def hfill(t, d, sz, sp, crossed=True, h=None, coin="AAA"):
    return {"time": t, "coin": coin, "px": "100", "sz": str(sz), "dir": d, "crossed": crossed,
            "startPosition": str(sp), "hash": h or f"0x{t}", "side": "A", "tid": t}
fills = [hfill(t_in - 100_000, "Close Long", 500, 1000, True, "0xtrig"),     # тригер: тейкерське закриття за 100 с
         hfill(t_in - 1_000, "Open Long", 10, 500, False, "0xmaker")]        # мейкерське відкриття за 1 с
assert S._trigger_fill(fills, {"coin": "AAA", "trig_hash": "0xtrig"})[1] == "hash"
assert S._trigger_fill(fills, {"coin": "AAA", "fill_ts_ms": str(t_in - 100_000)})[1] == "ts"
assert S._trigger_fill(fills, {"coin": "AAA", "trig_hash": "0xnone"}) == (None, "unmatched")
assert S._trigger_fill(fills, {"coin": "AAA", "fill_ts_ms": str(t_in - 50_000)}) == (None, "unmatched")
assert S._trigger_fill(fills, {"coin": "AAA"}) == (None, "none")
class HLF:
    def __init__(self, fs): self.fs = fs
    def user_fills(self, addr, t0, t1): return [f for f in self.fs if t0 <= f["time"] <= t1]
    def saturated(self, *a): return False
ctx3 = {"hl": HLF(fills), "tape": None}
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xtrig"}, t_in, ctx3, close_only=False, symbol=None, trigger=True)
assert o["trig_match"] == "hash" and abs(o["lag_s"] - 100.0) < 1e-9 and o["whale_pos_start"] == 1000 \
       and "partial_close" in o["flags"]                                            # лаг від ТРИГЕРА, не від мейкера (1 с)
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xzzz"}, t_in, ctx3, close_only=False, symbol=None, trigger=True)
assert "trigger_unmatched" in o["flags"] and "lag_s" not in o and o["trig_match"] == "unmatched"
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA"}, t_in, ctx3, close_only=False, symbol=None, trigger=True)
assert "trigger_unmatched" in o["flags"]                                            # без ідентичності — неповна перевірка
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xtrig"}, t_in, {"hl": HLF([]), "tape": None}, close_only=False, symbol=None, trigger=True)
assert "no_whale_fill" in o["flags"] and "trigger_unmatched" not in o["flags"]      # філів нема — як і раніше
print("3) settlement Follow: тригер за hash / за часом; інший філ не підставляється; без ідентичності — trigger_unmatched")

# ═══ 4. Кеш v3 із доказом повноти ════════════════════════════════════════
dd4 = tempfile.mkdtemp()
b0 = (BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS
now4 = BASE + 2 * S.HOUR_MS
ROWS4 = [(k, BASE + 1000 + k * 100, 100.0 + k * 0.01) for k in range(20)]
calls4 = []
def rest4(url):
    calls4.append(url)
    q = dict(p.split("=") for p in url.split("?")[1].split("&"))
    if "fromId" in q:
        return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in ROWS4 if a >= int(q["fromId"])][:1000]
    st, en = int(q["startTime"]), int(q["endTime"])
    return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in ROWS4 if st <= t <= en][:1000]
tape4 = S.Tape(dd4, fetch_zip=lambda u: None, fetch_rest=rest4, now_ms=now4, rest_pace_s=0)
# (а) старий формат (список) у REST-вікні → видалено й перетягнуто, записано v3
jp = os.path.join(tape4.rest_dir, "XUSDT-%d.json" % b0)
json.dump([[BASE + 1000, 99.0, 0]], open(jp, "w"))          # обрізане відро v2.17 (1 з 20)
ts_, px_ = tape4.window("XUSDT", BASE, BASE + 9 * S.MIN_MS)
assert len(ts_) == 20 and len(calls4) >= 1 and tape4._bucket_ok[("XUSDT", b0)] and tape4.stats["cache_legacy_refetch"] == 1
meta4 = json.load(open(jp))
assert meta4["v"] == 3 and meta4["complete"] is True and meta4["n"] == 20 and meta4["last_id"] == 19
# (б) v3 читається без запиту і з доказом
tape4b = S.Tape(dd4, fetch_zip=lambda u: None, fetch_rest=rest4, now_ms=now4, rest_pace_s=0)
n_before = len(calls4)
assert len(tape4b.window("XUSDT", BASE, BASE + 9 * S.MIN_MS)[0]) == 20 and len(calls4) == n_before and tape4b._bucket_ok[("XUSDT", b0)]
# (в) старий формат ПОЗА REST-вікном → лишається як ціни, але БЕЗ доказу повноти (window_complete False)
b_old = b0 - 60 * S.HOUR_MS
jp_old = os.path.join(tape4.rest_dir, "XUSDT-%d.json" % b_old)
json.dump([[b_old + 1000, 99.0, 0], [b_old + 2000, 99.5, 1]], open(jp_old, "w"))
ts_, px_ = tape4b.window("XUSDT", b_old, b_old + 9 * S.MIN_MS)
assert len(ts_) == 2 and tape4b._bucket_ok.get(("XUSDT", b_old)) is False and tape4b.stats["cache_legacy_kept"] == 1 \
       and not tape4b.window_complete("XUSDT", b_old, b_old + 5 * S.MIN_MS)
# (г) HL: старий список → перетягнуто, записано v3 із doказом
page = [{"tid": k, "time": BASE + 1000 + k, "coin": "AAA", "px": "1", "sz": "1", "dir": "Close Long", "crossed": True,
         "startPosition": "5", "hash": "0x%d" % k, "side": "A"} for k in range(5)]
hd4 = tempfile.mkdtemp()
hl4 = S.HLFills(hd4, fetch_hl=lambda body: [f for f in page if body["startTime"] <= f["time"] <= body["endTime"]],
                now_ms=now4, pace_s=0)
hp = os.path.join(hl4.dir, "0xw-%d.json" % (BASE // S.HOUR_MS))
json.dump(page[:2], open(hp, "w"))
assert len(hl4.user_fills("0xw", BASE, BASE + 2000)) == 5 and hl4.stats["req"] == 1 and hl4.stats["cache_legacy_refetch"] == 1
hm = json.load(open(hp))
assert hm["v"] == 3 and hm["complete"] is True and hm["sat"] is False and hm["n"] == 5
hl4b = S.HLFills(hd4, fetch_hl=lambda body: (_ for _ in ()).throw(RuntimeError("no net")), now_ms=now4, pace_s=0)
assert len(hl4b.user_fills("0xw", BASE, BASE + 2000)) == 5                          # v3 читається без мережі
print("4) кеш v3: старий REST-список у вікні → перезапит і доказ повноти; поза вікном → без доказу; HL-година → перезапит")

# ═══ 5. Старий book_partial = частковий вихід ═══════════════════════════
EP5 = load({"_exit_partial_of", "_px_check"}, dict(C))
assert EP5["_exit_partial_of"]({"exit_fill_frac": "", "exit_src": "book_partial"}, fnum) == 1
assert EP5["_exit_partial_of"]({"exit_fill_frac": "1", "exit_src": "book", "entry_src": "book_partial"}, fnum) == 1
assert EP5["_exit_partial_of"]({"exit_fill_frac": "0.6", "exit_src": "book"}, fnum) == 1
assert EP5["_exit_partial_of"]({"exit_fill_frac": "1", "exit_src": "book", "entry_src": "book"}, fnum) == 0
assert EP5["_exit_partial_of"]({"exit_src": "mid"}, fnum) == 0
print("5) частковість: старий exit_src/entry_src book_partial — частковий, verified не рятує")

# ═══ 6. R8: діра до перетину / без ціни після виявлення → tape_thin; фінал ═
o6 = {"flags": ["tp_tape_hit", "tp_path_gap"]}
S._result(o6, True, 100.0, 101.0, 100.0, 0.8, 0.15)
assert o6["status"] == "tape_thin" and abs(o6["net_official_pct"] - 0.8) < 1e-9          # офіційний = гірший
o6 = {"flags": ["tp_tape_hit"]}
S._result(o6, True, 100.0, 101.0, 100.0, 0.8, 0.15)
assert o6["status"] == "verified"
assert S._tape_pending({"flags": ["tp_replay_thin"], "entry_ts_ms": BASE, "entry_px_tape": 1, "exit_px_tape": 1}, BASE + S.HOUR_MS)
assert not S._tape_pending({"flags": ["tp_replay_thin"], "entry_ts_ms": BASE, "entry_px_tape": 1, "exit_px_tape": 1}, BASE + 80 * S.HOUR_MS)
srow6 = {"settle_v": S.SETTLE_V, "family": "rev", "entry_ts_ms": str(BASE), "settled_at": S._dt(BASE + 2 * S.HOUR_MS),
         "curve_n": "54", "flags": ""}
assert not S._settle_final(srow6, now_ms=BASE + 80 * S.HOUR_MS) and S._resettle_due(srow6, BASE + 80 * S.HOUR_MS)
assert S._settle_final(dict(srow6, settled_at=S._dt(BASE + 73 * S.HOUR_MS)), now_ms=BASE + 80 * S.HOUR_MS)
assert S._settle_final(dict(srow6, curve_n="60"), now_ms=BASE + 3 * S.HOUR_MS)     # повна крива — остаточний одразу
# реплей: шлях до перетину не покритий → tp_path_gap (фейкова стрічка з дірою у 20 хв)
tr6 = [(k, BASE + 5000 + k * 1000, 100.0) for k in range(60)] + [(100 + k, BASE + 25 * S.MIN_MS + k * 1000, 100.6 + k * 0.001) for k in range(60)]
b6_gap = (BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS + S.REST_BUCKET_MS   # друге відро — збій збору (діра даних)
def rest6(url):
    q = dict(p.split("=") for p in url.split("?")[1].split("&"))
    if "startTime" in q and int(q["startTime"]) == b6_gap:
        return None
    if "fromId" in q:
        return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in tr6 if a >= int(q["fromId"])][:1000]
    st, en = int(q["startTime"]), int(q["endTime"])
    return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in tr6 if st <= t <= en][:1000]
ctx6 = S.make_ctx(tempfile.mkdtemp(), {"X": "XUSDT"}, {"fetch_zip": lambda u: None, "fetch_rest": rest6, "fetch_hl": lambda b: [],
                                                        "hl_pace_s": 0, "rest_pace_s": 0}, now_ms=BASE + 2 * S.HOUR_MS,
                  log=lambda *a: None)
out6 = {"flags": [], "entry_src": "tape3s"}
x, xt, why = S._r8_tp_replay(out6, {"tp_px": "100.5", "dump_move_pct": "-1"}, "XUSDT", BASE + 5000, 100.0, True, 0.15, "SELL", ctx6)
assert why == "tp_replay" and "tp_tape_hit" in out6["flags"] and "tp_path_gap" in out6["flags"], out6["flags"]
print("6) R8: tp_path_gap/tp_replay_thin → tape_thin (не verified), відкладення ≤72 год, фінал лише після перерахунку за горизонтом")

# ═══ 7. Профілі / discovery / рефрешер ════════════════════════════════════
P7 = load({"_wilson_lb", "_profile_txs", "_profile_lifecycles", "_profile_episode", "_build_profile"},
          dict(C, _median=_median, _sim_depth=lambda c, s=None: 1e5))
T7 = 1_750_000_000_000; NOW7 = T7 + 30 * 86_400_000
_seq = [0]
def fl(t, px, sz, sp, d="Close Long", coin="AAA", crossed=True):
    _seq[0] += 1
    return {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir": d, "coin": coin, "crossed": crossed,
            "twapId": None, "hash": f"0x{coin}{t}{_seq[0]}", "tid": _seq[0]}
def opn(t, px, sz, sp=0.0, coin="AAA"): return fl(t, px, sz, sp, d="Open Long", coin=coin)
def pos_full(c, t):   # відкрито 1000; закрито 100 на 100-й с; долив 100 на 690-й; усе закрито на 700-й
    return [opn(t, 100.0, 1000, 0, coin=c), fl(t + 100_000, 100.0, 100, 1000, coin=c),
            opn(t + 690_000, 100.0, 100, 900, coin=c), fl(t + 700_000, 100.0, 1000, 1000, coin=c)]
def pos_trunc(c, t):  # історія починається з ДОЛИВУ (startPosition 900 ≠ 0)
    return pos_full(c, t)[2:]
full7 = [x for k in range(5) for x in pos_full(f"C{k}", T7 + k * 3_600_000)]
trunc7 = [x for k in range(5) for x in pos_trunc(f"C{k}", T7 + k * 3_600_000)]
pf = P7["_build_profile"](full7, now_ms=NOW7, depth_fn=lambda c, s=None: 1e5)
pt = P7["_build_profile"](trunc7, now_ms=NOW7, depth_fn=lambda c, s=None: 1e5)
assert pf["n_fast"] == 0 and pf["n_slow"] == 5 and pf["status"] == "no", (pf["n_fast"], pf["n_slow"], pf["status"])
assert pt["n_fast"] == 0 and pt["n_uncertain"] == 5 and pt["status"] != "ok", (pt["n_fast"], pt["n_uncertain"], pt["status"])
by7, _ = P7["_profile_txs"](pos_trunc("C0", T7))
lf = P7["_profile_lifecycles"](by7["C0"])
assert len(lf) == 1 and lf[0]["start_known"] is False and lf[0]["n_add_in"] == 1
by7, _ = P7["_profile_txs"](pos_full("C0", T7))
assert P7["_profile_lifecycles"](by7["C0"])[0]["start_known"] is True
# discovery: 100 × $200 у вікні 15 с = та сама подія, що $20k одним
q = deque(); ev = threading.Event()
PN = load({"_prio_note"}, dict(C, COIN_BLACKLIST={"BTC"}, PRIO_FLOOR_USD=15_000.0, PRIO_K_DEPTH=0.10,
          PRIO_COOLDOWN_S=600, PRIO_MAX_PER_MIN=10, PRIO_AGG_S=15.0, PRIO_AGG_MIN_USD=0.0, _prio_acc={},
          _prio_lock=threading.Lock(), _prio_seen={}, _prio_minute=deque(), _prio_q=q, _prio_event=ev,
          prio_stats={"triggers": 0, "added": 0, "dropped": 0, "errors": 0}, _sim_depth=lambda c, s=None: 1e5))
for i in range(100):
    PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 2.0})     # $200 × 100
    if len(q): break
assert len(q) == 1 and q[0][3] >= 15_000 and i == 74, (len(q), i)
# рефрешер: err поза watchlist після TTL і відкладені адреси без профілю — у черзі
RP = load({"_profile_refresh_pick"}, dict(C, watchlist_lock=threading.Lock(), watchlist={},
          strat2_lock=threading.RLock(), profiles_fetching=set(), PROFILE_REFRESH_BATCH=10,
          profile_retry_at={"0xdeferred": time.time() - 10, "0xsoon": time.time() + 600},
          wallet_profiles={"0xerr": {"ok": False, "err": 1, "v": 11, "fetched": time.time() - 7 * 3600, "status": "err"},
                           "0xerr_fresh": {"ok": False, "err": 1, "v": 11, "fetched": time.time() - 100, "status": "err"}}))
assert RP["_profile_refresh_pick"]() == ["0xerr", "0xdeferred"]
# ротація сирих філів — структурно
_pr = src[src.index("_rd = os.path.join(DATA_DIR, \"profiles_raw\")"):src.index("_rd = os.path.join(DATA_DIR, \"profiles_raw\")") + 1500]
assert 'os.path.join(_rd, f"{addr}.{_g}.json.gz")' in _pr and "for _g in (2, 1):" in _pr
print("7) профілі: перший видимий долив → uncertain, не fast; пил накопичується (100×$200); рефрешер бере err/відкладених; ротація raw")

# ═══ 8. Скан не губить повне закриття ════════════════════════════════════
def mk_uw(wl, tomb=None):
    return load({"update_watchlist", "_depth_ok"}, dict(C, COIN_BLACKLIST=set(), watchlist_lock=threading.Lock(),
        watchlist=wl, scan_tombstones=(tomb or {}), sent_alerts=set(), close_episodes={}, delta_seen={},
        fill_cursor={}, stats={}, depth_for_side=lambda d, s: (d or {}).get("bid", 0), REFRESH_S=30))
depth10 = {f"C{i}": {"bid": 1e5, "ts": time.time()} for i in range(10)}
depth10["AAA"] = {"bid": 1e5, "ts": time.time()}; depth10["BBB"] = {"bid": 1e5, "ts": time.time()}
tsc = time.time()
wl = {"0xw": {"AAA": {"size": 1.0, "val": 5e5, "side": "LONG", "ratio": 5.0, "entry": 1.0, "ratio_hi_ts": tsc},
              "BBB": {"size": 1.0, "val": 5e5, "side": "LONG", "ratio": 5.0, "entry": 1.0, "ratio_hi_ts": tsc - 5000}}}
n = mk_uw({a: {c: dict(p) for c, p in cs.items()} for a, cs in wl.items()})
# знімок: AAA зникла (закрита), BBB є, але ratio впав і грейс минув (жива — звичайний дроп)
n["update_watchlist"]({"BBB": [{"addr": "0xw", "size": 1.0, "val": 1e4, "side": "LONG", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
w = n["watchlist"]
assert set(w.get("0xw", {})) == {"AAA"} and w["0xw"]["AAA"].get("_gone_ts") and n["stats"]["scan_gone_carried"] == 1, w
# фліп у знімку без tombstone — старий бік лишається з міткою (sweep доставить закриття + вставить новий бік)
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"])}})
n["update_watchlist"]({"AAA": [{"addr": "0xw", "size": 2.0, "val": 5e5, "side": "SHORT", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
assert n["watchlist"]["0xw"]["AAA"]["side"] == "LONG" and n["watchlist"]["0xw"]["AAA"].get("_gone_ts")
# tombstone після старту скану — realtime уже закрив → пара знята
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"])}}, tomb={"0xw:AAA": tsc + 1})
n["update_watchlist"]({}, depth10, tsc, None, {"0xw": tsc})
assert n["watchlist"] == {}
# мітка старіша за GONE_MAX_S → дроп із лічильником
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"], _gone_ts=tsc - 8000)}})
n["update_watchlist"]({}, depth10, tsc, None, {"0xw": tsc})
assert n["watchlist"] == {} and n["stats"]["scan_gone_dropped"] == 1
# пара знову у знімку — мітка знімається
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"], _gone_ts=tsc - 100)}})
n["update_watchlist"]({"AAA": [{"addr": "0xw", "size": 1.0, "val": 5e5, "side": "LONG", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
assert "_gone_ts" not in n["watchlist"]["0xw"]["AAA"]
_rt = src[src.index("def run_realtime_monitor("):src.index("def check_position_changes(")]
assert '_w.pop("_gone_ts", None)' in _rt
print("8) скан: закрита пара чекає sweep (_gone_ts), жива без ratio — дроп, фліп — старий бік, tombstone/вік — зняття, мітка скидається")

# ═══ 9. Епізод на всіх філах; пасивні лише в епізод ══════════════════════
GF = load({"get_recent_market_fills"}, dict(C, stats={}, RateLimited=type("RL", (Exception,), {}),
          APIError=type("AE", (Exception,), {})))
raw9 = [{"time": 1000, "coin": "AAA", "px": "120", "sz": "100", "dir": "Close Long", "crossed": False, "startPosition": "1000", "hash": "0xm", "oid": 1},
        {"time": 300_000, "coin": "AAA", "px": "119.5", "sz": "100", "dir": "Close Long", "crossed": True, "startPosition": "900", "hash": "0xt1", "oid": 2},
        {"time": 360_000, "coin": "AAA", "px": "118", "sz": "800", "dir": "Close Long", "crossed": True, "startPosition": "800", "hash": "0xt2", "oid": 3},
        {"time": 361_000, "coin": "AAA", "px": "118", "sz": "5", "dir": "Close Long", "crossed": True, "startPosition": "0", "hash": "0xtw", "oid": 4, "twapId": 7}]
GF["hl_post"] = lambda body, **k: [f for f in raw9 if f["time"] >= body.get("startTime", 0)]
GF["hl_post_prio"] = GF["hl_post"]
agg = GF["get_recent_market_fills"]("0xw", "AAA", 0, "LONG")
assert sorted(t["hash"] for t in agg) == ["0xt1", "0xt2"] and all(t["agg"] == 1 for t in agg)   # контракт без змін
agg, pas = GF["get_recent_market_fills"]("0xw", "AAA", 0, "LONG", with_passive=True)
assert sorted(t["hash"] for t in pas) == ["0xm", "0xtw"] and all(t["agg"] == 0 for t in pas)     # мейкер і TWAP — пасивні
FC = load({"fc_on_txs", "_fc_rebuild"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={},
          _close_episode=S.close_episode, stats={}))
OLD9 = {"size": 1000.0, "val": 1.2e5, "side": "LONG", "ratio": 3.0}
FC["fc_on_txs"]("0xw", "AAA", OLD9, [], passive=[t for t in pas if t["hash"] == "0xm"])   # спершу лише мейкерське закриття
FC["fc_on_txs"]("0xw", "AAA", OLD9, agg, passive=None)              # потім тейкерські
ep9 = FC["fc_episodes"][("0xw", "AAA")]
assert ep9["first_ts"] == 1000 and ep9["last_ts"] == 360_000 and (ep9["last_ts"] - ep9["first_ts"]) / 1000 == 359.0   # як у settlement
assert ep9["max_sz"] == 800.0 and ep9["start_size"] == 1000.0 and len(ep9["txs"]) == 3 and ep9["txs"][0]["agg"] == 0
# без пасивних (як до 2.20) епізод був би 60 с → хибний швидкий дамп
FC2 = load({"fc_on_txs", "_fc_rebuild"}, dict(C, fc_lock=threading.Lock(), fc_episodes={}, fc_positions={},
           _close_episode=S.close_episode, stats={}))
FC2["fc_on_txs"]("0xw", "AAA", OLD9, agg)
assert (FC2["fc_episodes"][("0xw", "AAA")]["last_ts"] - FC2["fc_episodes"][("0xw", "AAA")]["first_ts"]) / 1000 == 60.0
# структурно: обидва шляхи детекції тягнуть пасивні, приймач — з підтвердженням споживачів
assert src.count('with_passive=True)') == 2 and 'fc_on_txs(addr, coin, old, [], passive=_passive)' in src
assert '_release("rev", todo["rev"], False)' in src and '_ingest_retry' in src and 'd["busy"].add(c)' in src
print("9) епізод: мейкерське закриття входить в епізод (359 с, не 60), R7-база лише агресивна; пасивні тягнуться обома шляхами; приймач із ack/retry")

# ═══ 10. Статистики ══════════════════════════════════════════════════════
AG = load({"_agg_block", "_group_stats", "_eval_ts", "_head_ok", "_count_by", "_curves_of", "_stat_small",
           "_period_stats", "_parse_curve"}, dict(C, _median=_median))
ts_new = time.mktime(time.strptime("2026-09-14 12:00:00", "%Y-%m-%d %H:%M:%S"))
ts_old = time.mktime(time.strptime("2026-09-10 12:00:00", "%Y-%m-%d %H:%M:%S"))
def tr(net, sig=1, px=1, wallet="0xaaaa", coin="AAA", date="2026-09-10", ts=None, **kw):
    d = {"net30": net, "sig_ok": sig, "px_ok": px, "exit_ok": 1, "status": "verified", "entered": 1, "settled": 1,
         "wallet": wallet, "coin": coin, "date": date + " 12:00:00", "ts": ts or ts_old, "_ct": None, "_cl": None}
    d.update(kw); return d
trs = [tr(0.5, sig=0, wallet="0xbbbb"), tr(1.0), tr(0.2, coin="BBB", wallet="0xcccc"),
       tr(0.3, date="2026-09-14", ts=ts_new), tr(-0.4, px=0, status="live_only")]
blk = AG["_agg_block"](trs, time.time(), 60)
assert blk["n"] == 3 and blk["n_paper"] == 5 and blk["pct_verified"] == 60.0
assert blk["by_wallet"]["n_groups"] == 2 and blk["by_wallet"]["top_share"] == 66.7 and blk["by_wallet"]["top_key"] == "0xaaaa"   # sig=0 і px=0 — поза групами
assert blk["by_coin"]["n_groups"] == 2 and blk["by_day"]["n_groups"] == 2
assert blk["eval_since"] == "2026-09-13" and blk["eval"]["n"] == 1 and blk["eval"]["median"] == 0.3 and blk["eval_paper"]["n"] == 1
assert AG["_group_stats"]([tr(0.5, sig=0)], lambda t: t["wallet"])["n_groups"] == 0
# settle: фандинг у кривій, net60, net_live мінус фандинг рядка; no_funding у кривій
FUND_T = BASE + 20 * S.MIN_MS
class FT:
    """фейкова стрічка: ціна 100 весь час, одне нарахування +0.01% на 20-й хвилині"""
    def __init__(self, fund=True): self.fund = fund
    def window(self, symbol, t0, t1):
        from array import array
        ts = [t for t in range(int(t0), int(t1) + 1, 1000)]
        return array("q", ts), array("d", [100.0] * len(ts))
    def window_complete(self, *a): return True
    def is_unlisted(self, s): return False
    def funding(self, symbol, t0, t1):
        if not self.fund: raise RuntimeError("no fund")
        return [(FUND_T, 0.0001)] if int(t0) < FUND_T <= int(t1) else []
out10 = {"flags": []}
ctx10 = {"tape": FT(), "hl": None, "symbol_map": {}, "now_ms": BASE + 3 * S.HOUR_MS, "log": lambda *a: None}
S._curve(out10, "XUSDT", BASE, 100.0, "SELL", True, 0.15, 60, ctx10)
cv = [float(v) for v in out10["curve_tape"].split(";")]
assert abs(cv[18] + 0.15) < 1e-9 and abs(cv[19] + 0.16) < 1e-9 and abs(cv[59] + 0.16) < 1e-9 and "no_funding" not in out10["flags"]
out10b = {"flags": []}
S._curve(out10b, "XUSDT", BASE, 100.0, "SELL", True, 0.15, 60, dict(ctx10, tape=FT(fund=False)))
assert "no_funding" in out10b["flags"] and abs(float(out10b["curve_tape"].split(";")[59]) + 0.15) < 1e-9
row10 = {"sig_id": "z", "strategy": "R1_загальний", "date": S._dt(BASE), "coin": "X", "our_side": "LONG", "entered": "1",
         "entry_px": "100.0", "entry_ts_ms": str(BASE), "exit_ts_ms": str(BASE + 30 * S.MIN_MS), "exit_px": "100.5",
         "exit_reason": "timer", "exit_min": "30", "costs_pct": "0.15", "funding_pct": "0.02", "entry_src": "book",
         "exit_src": "book", "whale_addr": ""}
o10 = S.settle_rev(row10, dict(ctx10, hl=None, symbol_map={"X": "XUSDT"}))
assert abs(o10["net_live_pct"] - (0.5 - 0.10 - 0.02)) < 1e-9 and abs(o10["net_tape_pct"] + 0.16) < 1e-9 \
       and abs(o10["net60_tape_pct"] + 0.16) < 1e-9 and o10["funding_pct"] == 0.01      # live: комісії 2×0.05 (обидві ноги зі стакану), мінус фандинг рядка 0.02
# API: старий R без settlement — у журналі; TWAP «підтверджено» ∩ заголовок; старий book_partial → px_ok 0
d10 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d10, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def mkrow(headers, **kv):
    r = {k: "" for k in headers}; r["eol"] = "^"; r.update(kv)
    return [r[k] for k in headers]
today = _dt(now0)
def revr(sig, algo="2.20", st="R1_загальний", **kv):
    base = dict(sig_id=sig, strategy=st, date=today, coin="AAA", our_side="LONG", whale_addr=A, src="wallet",
                detect_px="100", entry_px="100", entered="1", ratio="3", costs_pct="0.15", algo_v=algo,
                exit_px="101", exit_reason="timer", exit_min="30", grace="0", dump_bucket="2", dump_move_pct="-1.5",
                exch="bn", entry_qty="10", exit_fill_frac="1", funding_pct="0", exec_delay_s="0.2", entry_src="book", exit_src="book")
    base.update(kv); return mkrow(rh, **base)
wcsv("rev.csv", rh, [revr("a1"), revr("a2", algo="2.15", exit_px="", exit_reason="", exit_min="", m30="-2.0"),   # старий, без settlement
                     revr("a3", algo="2.17", exit_src="book_partial", exit_fill_frac="")])                       # старий partial + verified
def twr(tid, **kv):
    base = dict(twap_id=tid, strategy="T1_твап_відкриття", date_entry=today, coin="AAA", our_side="SHORT",
                twap_side="buy", whale_addr=A, src="TWAPx", usd="1e5", dur_s="600", kind="open", kind_src="slice",
                entry_px="100", net60_pct="2.0", costs_pct="0.2", exit_min="60", exit_reason="timer_60m",
                completed="1", cancel_after_entry="0", exch_status="finished", algo_v="2.20", exch="bn",
                entry_qty="10", exit_fill_frac="1", funding_pct="0", exec_delay_s="0.3")
    base.update(kv); return mkrow(th, **base)
wcsv("tw.csv", th, [twr("tw1")])
for nm, hdr in (("out.csv", FH["REV_OUT_HEADERS"]), ("tc.csv", FH["TWAP_CURVE_HEADERS"]), ("sig.csv", FH["REV_SIG_HEADERS"]),
                ("tws.csv", FH["REV_SIG_HEADERS"]), ("fo.csv", FH["FOLLOW_OUT_HEADERS"]), ("follow.csv", fh)):
    wcsv(nm, hdr, [])
def setr(key, family, strategy, **kv):
    base = {k: "" for k in S.HEADERS}
    base.update(key=key, family=family, strategy=strategy, coin="AAA", symbol="AAAUSDT", settle_v=S.SETTLE_V,
                settled_at=today, eol="^", lag_s="3", dump_bucket="2", dump_move_pct="1.5", flags="full_close;dump_tape",
                net_live_pct="0.85", net_tape_pct="0.6", net_official_pct="0.6", curve_n="60", curve_final="1")
    base.update(kv); return [base[k] for k in S.HEADERS]
wcsv("settlements.csv", S.HEADERS, [setr("a1|R1_загальний", "rev", "R1_загальний"), setr("a3|R1_загальний", "rev", "R1_загальний")])
api = load({"strat2_api", "strat2_slice", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
            "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key", "_twap_exit",
            "_twap_curve_row", "_stat_small", "_period_stats", "_ts_local", "_load_settlements",
            "_official_net", "_parse_curve", "_curves_of", "_agg_block", "_pub", "_leg_costs",
            "_sig_check_rev", "_sig_check_fol", "_px_check", "_head_ok", "_count_by", "_pending_rev_rows",
            "_p_costs", "_sim_slip", "_rev_trade_closed", "_exit_partial_of", "_group_stats", "_eval_ts"}, dict(C,
    _strat2_full={}, strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, profiles_fetching=set(), STRAT2_DESC={}, STRAT2_TITLES={},
    STRAT_SINCE=FH["STRAT_SINCE"], TAPE_SINCE=FH["TAPE_SINCE"], _strat2_cache={"ts": 0.0, "data": None},
    _legacy_csv_cache={}, _settle_cache={"key": None, "data": {}}, DATA_DIR=d10, stats={},
    FOLLOW_OUT_CSV=os.path.join(d10, "fo.csv"), REV_CSV=os.path.join(d10, "rev.csv"),
    REV_SIG_CSV=os.path.join(d10, "sig.csv"), FOLLOW_CSV=os.path.join(d10, "follow.csv"),
    REV_OUT_CSV=os.path.join(d10, "out.csv"), TWAP_CSV=os.path.join(d10, "tw.csv"),
    TWAP_CURVE_CSV=os.path.join(d10, "tc.csv"), TWAP_SIG_CSV=os.path.join(d10, "tws.csv"),
    TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_COHORTS=(1.0, 1.5, 2.0),
    T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення", strat_activated={}, _dt=_dt,
    _median=_median, RESEARCH_SINCE="2.17"), assigns=("TWAP_HEADERS",))
o = api["strat2_api"]()
r1 = o["strategies"]["R1_загальний"]
assert r1["n"] == 1 and r1["n_paper"] == 3 and o["legacy_rows"] == 0 and r1["n_adm_cand"] == 0 and r1["n_exit_partial"] == 1
t_a3 = [t for t in r1["trades"] if t.get("exit_partial")][0]
assert t_a3["px_ok"] == 0 and t_a3["status"] == "verified" and t_a3["fill_frac"] is None      # старий book_partial: verified не рятує
t_a2 = [t for t in r1["trades"] if t.get("adm") and not t.get("exit_partial")][0]
assert t_a2["settled"] == 0 and t_a2["px_ok"] is None and abs(t_a2["net30"] + 2.15) < 1e-9     # у журналі, поза заголовком
t1 = o["strategies"]["T1_твап_відкриття"]
assert t1["n_confirmed"] == 0 and t1["n"] == 0 and t1["n_paper"] == 1                         # completed без settlement — не «підтверджено»
assert "by_coin" in r1 and "pct_verified" in r1 and "eval" in r1
# рев'ю v2.20 №1: status tape_thin із settlement (R8, tp_path_gap) — НЕ verified: px_ok 0, поза
# заголовком, окремий лічильник n_tape_thin; tp_tape_miss + tp_path_gap — «не перетнув» не доведено
ON = api["_official_net"]
thin = {"net_tape_pct": "0.5", "status": "tape_thin", "flags": "full_close;tp_tape_hit;tp_path_gap"}
off, nt, sd, fl, vs = ON(1.0, thin, fnum)
assert vs == "tape_thin" and off == 0.5 and sd == 1 and api["_px_check"](vs, 0) == 0
assert ON(None, thin, fnum)[4] == "tape_thin"                                                  # і без live
assert ON(1.0, {"net_tape_pct": "0.5", "status": "verified", "flags": "full_close;tp_tape_hit"}, fnum)[4] == "verified"
assert ON(1.0, {"net_tape_pct": "0.5", "status": "", "flags": "full_close;tp_replay_thin"}, fnum)[4] == "tape_thin"   # за прапорцем, без status
wcsv("rev.csv", rh, [revr("a1"), revr("a3", algo="2.17", exit_src="book_partial", exit_fill_frac=""),
                     revr("r8a", st=FH["R8_NAME"], exit_reason="tp", tp_px="100.8"),
                     revr("r8b", st=FH["R8_NAME"], exit_reason="timer", tp_px="100.8")])
wcsv("settlements.csv", S.HEADERS, [setr("a1|R1_загальний", "rev", "R1_загальний"), setr("a3|R1_загальний", "rev", "R1_загальний"),
                                    setr("r8a|" + FH["R8_NAME"], "rev", FH["R8_NAME"], status="tape_thin", flags="full_close;dump_tape;tp_tape_hit;tp_path_gap"),
                                    setr("r8b|" + FH["R8_NAME"], "rev", FH["R8_NAME"], status="tape_thin", flags="full_close;dump_tape;tp_tape_miss;tp_path_gap")])
api["_strat2_cache"]["ts"] = 0.0; api["_settle_cache"]["key"] = None
o = api["strat2_api"]()
r8 = o["strategies"][FH["R8_NAME"]]
ta, tb = [t for t in r8["trades"] if t["exit_reason"] == "tp"][0], [t for t in r8["trades"] if t["exit_reason"] == "timer"][0]
assert ta["status"] == "tape_thin" and ta["px_ok"] == 0 and ta["settled"] == 1 and ta["exit_ok"] == 1                # hit доведено; ціна — ні
assert tb["status"] == "tape_thin" and tb["px_ok"] == 0 and tb["exit_ok"] is None and tb["exit_why"] == "tape_thin"  # miss з дірою — не доведено
assert r8["n"] == 0 and r8["n_paper"] == 2 and r8["n_tape_thin"] == 2 and r8["n_verified_all"] == 0 and r8["n_tape_only"] == 0
assert r8["n_settled"] == r8["n_verified_all"] + r8["n_live_only"] + r8["n_tape_only"] + r8["n_tape_thin"]
r1 = o["strategies"]["R1_загальний"]
assert r1["n"] == 1 and r1["n_tape_thin"] == 0 and r1["n_settled"] == r1["n_verified_all"] + r1["n_live_only"] + r1["n_tape_only"] + r1["n_tape_thin"]
# n_sig_unmatched окремо від «чекає»
blk_u = AG["_agg_block"]([tr(0.5, sig=None, sig_why="trig_unmatched"), tr(0.4, sig=None, sig_why="pending"), tr(0.3)], time.time(), 60)
assert blk_u["n_sig_unmatched"] == 1 and blk_u["n_sig_pending"] == 1 and blk_u["n"] == 1
print("10) статистики: групи через _head_ok, eval-період, pct_verified, старі R у журналі, старий partial → px_ok 0, TWAP confirmed ∩ заголовок, фандинг у кривій/net60/net_live, no_funding")

# ═══ 12. Рев'ю дифу v2.20 (settlement) ══════════════════════════════════
# №2: TWAP-рядок тригера не записує — епізод кита за останнім філом, як раніше (без trigger_unmatched);
#     Follow — лише з trigger=True
o12 = {"flags": []}
S._whale(o12, {"whale_addr": "0xw", "coin": "AAA"}, t_in, ctx3, close_only=False, symbol=None)     # як settle_twap
assert "trigger_unmatched" not in o12["flags"] and "trig_match" not in o12 and abs(o12["lag_s"] - 1.0) < 1e-9, o12   # останній філ (мейкер, 1 с) — як до v2.20
o12 = {"flags": []}
S._whale(o12, {"whale_addr": "0xw", "coin": "AAA"}, t_in, ctx3, close_only=False, symbol=None, trigger=True)   # як settle_follow
assert "trigger_unmatched" in o12["flags"] and o12["trig_match"] == "none"
src_settle = open((_HL + "/settle.py"), encoding="utf-8").read()
assert src_settle.count("symbol=symbol, trigger=True)") == 1 \
       and src_settle.index("def settle_follow") < src_settle.index("symbol=symbol, trigger=True)") < src_settle.index("def settle_twap")
# №3: відро, зняте ще ВІДКРИТИМ, — не «ок»; після закриття відра знімок у пам'яті перетягується
b12 = ((BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS) + 3 * S.HOUR_MS
ROWS12 = [(k, b12 + 1000 + k * 1000, 100.0) for k in range(20)] + [(20 + k, b12 + 7 * S.MIN_MS + k * 1000, 101.0) for k in range(10)]
now12 = [b12 + 5 * S.MIN_MS]; calls12 = []
def rest12(url):
    calls12.append(url)
    q = dict(p.split("=") for p in url.split("?")[1].split("&"))
    vis = [r for r in ROWS12 if r[1] <= now12[0]]                       # біржа віддає лише те, що вже сталось
    if "fromId" in q:
        return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in vis if a >= int(q["fromId"])][:1000]
    st, en = int(q["startTime"]), int(q["endTime"])
    return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in vis if st <= t <= en][:1000]
tape12 = S.Tape(tempfile.mkdtemp(), fetch_zip=lambda u: None, fetch_rest=rest12, now_ms=now12[0], rest_pace_s=0)
ts12, _ = tape12.window("XUSDT", b12, b12 + 4 * S.MIN_MS)
assert len(ts12) == 20 and tape12._bucket_ok.get(("XUSDT", b12)) is False and ("XUSDT", b12) in tape12._partial
assert not tape12.window_complete("XUSDT", b12, b12 + 4 * S.MIN_MS)
n12 = len(calls12)
assert len(tape12.window("XUSDT", b12, b12 + 4 * S.MIN_MS)[0]) == 20 and len(calls12) == n12   # поки відро триває — з пам'яті
now12[0] = b12 + 20 * S.MIN_MS; tape12.now_ms = now12[0]                                    # відро закрилось
assert tape12.window_complete("XUSDT", b12, b12 + 9 * S.MIN_MS) and len(calls12) > n12           # перетягнуто, обхід повний
assert len(tape12.window("XUSDT", b12, b12 + 9 * S.MIN_MS)[0]) == 30 and tape12._bucket_ok[("XUSDT", b12)] \
       and ("XUSDT", b12) not in tape12._partial
meta12 = json.load(open(os.path.join(tape12.rest_dir, "XUSDT-%d.json" % b12)))
assert meta12["complete"] is True and meta12["n"] == 30
# детекція R у мс (detect_ms, v2.20) — перша у списку колонок часу
assert S._ts({"detect_ms": str(BASE + 1000), "detect_ts_ms": "", "entry_ts_ms": str(BASE + 2000)},
             ("detect_ms", "detect_ts_ms", "entry_ts_ms"), "date") == (BASE + 1000, "ms")
assert [f for f in S.FAMILIES if f[0] == "rev_trades.csv"][0][3][0] == "detect_ms"
# причинність у settlement: тригер ПІСЛЯ входу знаходиться (вікно +120 с) → lag_s<0, neg_lag → sig_ok=0 (F і R)
late = hfill(t_in + 2000, "Close Long", 100, 500, True, "0xlate")
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xlate"}, t_in, {"hl": HLF(fills + [late]), "tape": None}, close_only=False, symbol=None, trigger=True)
assert o["trig_match"] == "hash" and "neg_lag" in o["flags"] and abs(o["lag_s"] + 2.0) < 1e-9, o
assert api["_sig_check_fol"]({"flags": ";".join(o["flags"]), "lag_s": str(o["lag_s"])}, fnum) == (0, "neg_lag")
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xlate"}, t_in, {"hl": HLF(fills + [late]), "tape": None}, close_only=True, symbol=None)   # реверс
assert "neg_lag" in o["flags"] and abs(o["lag_s"] + 2.0) < 1e-9, o
assert api["_sig_check_rev"]("R1_загальний", {"flags": "full_close;neg_lag", "dump_bucket": "2", "dump_move_pct": "1.5", "lag_s": "-2.0"}, fnum) == (0, "neg_lag")
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA", "trig_hash": "0xtrig"}, t_in, {"hl": HLF(fills + [late]), "tape": None}, close_only=True, symbol=None)   # тригер до входу — як раніше
assert "neg_lag" not in o["flags"] and abs(o["lag_s"] - 100.0) < 1e-9, o
o = {"flags": []}
S._whale(o, {"whale_addr": "0xw", "coin": "AAA"}, t_in, {"hl": HLF(fills + [late]), "tape": None}, close_only=True, symbol=None)   # без тригера — вікно лише до входу
assert "neg_lag" not in o["flags"] and abs(o["lag_s"] - 100.0) < 1e-9, o
# №5: _seen_pairs з боком — фліп у НЕкваліфікований бік (ratio<2) = старий бік закрито → carried, не дроп
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"], ratio_hi_ts=tsc - 5000)}})
n["update_watchlist"]({"AAA": [{"addr": "0xw", "size": 1.0, "val": 1e4, "side": "SHORT", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
assert n["watchlist"]["0xw"]["AAA"]["side"] == "LONG" and n["watchlist"]["0xw"]["AAA"].get("_gone_ts") and n["stats"]["scan_gone_carried"] == 1, n["watchlist"]
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"], ratio_hi_ts=tsc - 5000)}})
n["update_watchlist"]({"AAA": [{"addr": "0xw", "size": 1.0, "val": 1e4, "side": "LONG", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
assert n["watchlist"] == {} and not n["stats"].get("scan_gone_carried")                    # той самий бік без ratio — звичайний дроп
# №6: стала мітка — live-запис новіший за знімок (merge лишає його), пара жива → мітка знята з ЖИВОГО запису
n = mk_uw({"0xw": {"AAA": dict(wl["0xw"]["AAA"], _gone_ts=tsc - 3 * 3600, upd=tsc + 5)}})
n["update_watchlist"]({"AAA": [{"addr": "0xw", "size": 1.0, "val": 5e5, "side": "LONG", "entry": 1.0}]}, depth10, tsc, None, {"0xw": tsc})
assert "_gone_ts" not in n["watchlist"]["0xw"]["AAA"] and n["watchlist"]["0xw"]["AAA"]["upd"] == tsc + 5, n["watchlist"]
n["update_watchlist"]({}, depth10, tsc + 10, None, {"0xw": tsc + 10})                     # тепер справді зникла → carried, не dropped
assert n["watchlist"]["0xw"]["AAA"].get("_gone_ts") and not n["stats"].get("scan_gone_dropped"), n["watchlist"]
assert src.count('watchlist[addr][coin].pop("_gone_ts", None)') == 3                        # усі гілки sweep, де позиція оновлюється
# №7: частковий сигнал (тінь) і повне закриття того самого батчу — різні id (-p)
n = mk_rev(mid_ts_off=1.0); n["fc_episodes"][(A, "AAA")] = dict(EP)
n["rev_on_close"](A, "AAA", OLD_R, [dict(FILLS[0], sz=400.0)], False)                      # 40% → тіньовий сигнал
assert n["rev_open"] and all(k.split("|")[0].endswith("-p") for k in n["rev_open"]), list(n["rev_open"])
n["fc_episodes"][(A, "AAA")] = dict(EP)
n["rev_on_close"](A, "AAA", OLD_R, FILLS, True)
assert len(n["rev_open"]) == 2 and any(not k.split("|")[0].endswith("-p") for k in n["rev_open"]), list(n["rev_open"])
# дрібне: протерміновані записи _ingest_retry зникають зі словника
from collections import OrderedDict as _OD
FAIL12 = {"sim"}
def _stub12(name):
    def f(*a, **k):
        if name in FAIL12: raise RuntimeError("boom " + name)
    return f
ING = load({"_ingest_txs", "_tx_key", "_grace_val"}, dict(C, _ingested=_OD(), _ingested_lock=threading.Lock(), INGEST_LRU=20000,
           INGEST_CONSUMERS=("sim", "fc", "rev", "fol"), INGEST_RETRY_MAX=50, _fc_done={}, _ingest_retry={}, stats={},
           _journal=lambda *a, **k: None, fc_lock=threading.Lock(), fc_episodes={}, sim_on_market_txs=_stub12("sim"),
           fc_on_txs=_stub12("fc"), rev_on_close=_stub12("rev"), follow_on_txs=_stub12("fol"), fc_on_full_close=_stub12("fcfull")))
txi = lambda h, ts: {"hash": h, "ts": ts, "px": 100.0, "sz": 10.0, "sp": 100.0, "dir": "Close Long", "agg": 1}
ING["_ingest_txs"]("0xw", "AAA", OLD_R, [txi("0x1", 1000)], 900.0, False, "ws", 1500)
assert [f["hash"] for f in ING["_ingest_retry"][("0xw", "AAA")]] == ["0x1"]
ING["_ingest_retry"][("0xw", "AAA")][0]["_rt_ts"] -= 4000                                    # протерміновано (>1 год)
FAIL12.clear()
ING["_ingest_txs"]("0xw", "AAA", OLD_R, [txi("0x2", 2000)], 800.0, False, "sweep", 2500)
assert ("0xw", "AAA") not in ING["_ingest_retry"], ING["_ingest_retry"]
assert src.count('p["exit_retry_at"] = now2 + 15.0') == 4 and src.count('if p["_stale_n"] >= 2:') == 2   # повторно застарілий стакан — бекоф 15 с
print("12) рев'ю дифу: TWAP без trigger_unmatched (тригер лише у Follow), відкрите відро — не «ок» і перетягується після закриття, detect_ms у settlement, "
      "neg_lag через тригер після входу, _seen_pairs з боком, стала мітка _gone_ts, -p для часткового сигналу, retry-очищення, бекоф виходу")

# ═══ 11. Структурні ═══════════════════════════════════════════════════════
assert '"bn_px_fail":     stats.get("bn_px_fail", 0)' in src and '"settle_prev_v":  stats.get("settle_prev_v", 0)' in src   # рев'ю №2
assert "threading.Thread(target=run_bn_px_poller" in src and '"bn_px_age_s":    _bn_px_age_s()' in src
assert "def _tr_px_now(p, max_age=30.0):" in src and src.count("px = _tr_px_now(p, 30.0)") == 2
assert "min_recv_ms=decision_ms" in src and src.count('"decision_ms": decision_ms') >= 3
assert 'if lag_s is not None and lag_s < 0:' in src and 'stats["follow_neg_lag"]' in src and 'stats["rev_neg_lag"]' in src
assert 'old = [k for k, r in data.items() if (r.get("settle_v") or "").strip() != str(cur)]' in src
assert 'def _trigger_fill(fills, row):' in open((_HL + "/settle.py"), encoding="utf-8").read()
wd = open((_HL + "/watchdog.py"), encoding="utf-8").read()
assert 'bn_px_age_s' in wd and "bn_px_stale" in wd
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for m in ("tape_thin", "neg_lag:", "trig_unmatched", "no_tp:", "pct_verified", "eval_since", "by_coin", "bookTicker",
          "n_tape_thin", "n_sig_unmatched", "стрічка неповна"):
    assert m in ui, m
_js = "\n".join(re.findall(r"<script(?![^>]*src)[^>]*>(.*?)</script>", ui, re.S))
_jp = os.path.join(tempfile.mkdtemp(), "inline.js"); open(_jp, "w", encoding="utf-8").write(_js)
assert os.system(f"node --check {_jp} >/dev/null 2>&1") == 0
md = open((_HL + "/CLAUDE.md"), encoding="utf-8").read()
assert "## v2.20" in md and "EVAL_SINCE" in md and "tape_thin" in md and "_gone_ts" in md
print("11) структурні: потік цін у main/status/watchdog, причинність у коді, settlement лише поточної версії, UI-маркери й синтаксис, CLAUDE.md v2.20")
print("\nУСІ РЕГРЕСІЇ v2.20 ЗЕЛЕНІ")

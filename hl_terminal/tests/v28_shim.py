import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Спільні доповнення до неймспейсів СТАРИХ тест-сюїт після v2.8:
нові константи/стаби, без яких екстраговані функції падають на NameError.
Значення — як у server.py, крім _sim_depth (стаб: глибина $10k, щоб
історичні фікстури $100k+ проходили ratio-гейт ≥2)."""
import threading, time as _time
from collections import deque

def _median(v):
    if not v: return None
    s = sorted(v); n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0

class RateLimited(Exception):
    pass

class APIError(Exception):
    pass

def _no_proxy(body, retries=2, direct=False):
    raise RuntimeError("no proxy in tests")

NEW = dict(
    F5_NAME="F5_перший", F5_FIRST_SHOT_S=3600.0,
    F4_MIN_FAST_PCT=70.0, F4_MIN_RATIO=2.0, F4_FULL_PCT=0.95,
    PROFILE_WINDOW_D=90, PROFILE_PAGES=6, REST_PROXY="",
    PROFILE_HARD_TTL_S=48 * 3600,
    PROFILE_QUEUE_MAX=100, PROFILE_429_RETRY_S=300, PROFILE_FAIL_RETRY_S=1800,
    WS_STALE_S=180, WS_STALE_MAX_S=1800,
    RateLimited=RateLimited, APIError=APIError, deque=deque,
    _profile_sem=threading.Semaphore(1),
    profile_retry_at={}, stats={},
    _prio_proxy_state={"streak": 0, "dead_since": 0.0},
    _profile_proxy={"dead_until": 0.0},
    PROFILE_W_PER_MIN={"proxy": 600, "direct": 150},
    _profile_w={"proxy": deque(), "direct": deque()},
    _profile_w_lock=threading.Lock(),
    _profile_budget_wait=lambda via, w_next=120: None,
    _profile_budget_add=lambda via, batch: None,
    _sim_depth=lambda c, s=None: 1e4,
    _median=_median, time=_time,
    hl_post_prio=_no_proxy,
)

# ── v2.9 (ТЗ 04.09): нові стратегії і константи ──
NEW.update(
    F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0,
    F7_NAME="F7_без_ратіо",
    R7_NAME="R7_одним", R7_MIN_TX_USD=100_000.0,
    VIP_TOP_N=2000,
)

# ── v2.11 (ТЗ 08.09): грейс ratio, версіонування, F8/F9, TWAP, проксі скану ──
import glob as _glob, re as _re, calendar as _calendar, datetime as _dtmod, html as _htmlmod
RATIO_GRACE_S = 1800
def _mark_ratio(p, r, now=None):
    p["ratio"] = r
    if r >= 2.0: p["ratio_hi_ts"] = now if now is not None else _time.time()
    return p
def _ratio_ok(p, now=None):
    if (p.get("ratio") or 0) >= 2.0: return True
    return ((now if now is not None else _time.time()) - (p.get("ratio_hi_ts") or 0)) < RATIO_GRACE_S
def _vt(v):
    try: return tuple(int(x) for x in str(v).strip().split("."))
    except (ValueError, AttributeError): return (0,)
def _v_ok(row_v, since):
    return _vt(row_v) >= _vt(since) and _vt(row_v) != (0,)
_F8, _F9, _T1, _T2 = "F8_ratio35", "F9_без_ратіо_90", "T1_твап_відкриття", "T2_твап_скорочення"
NEW.update(
    F8_NAME=_F8, F8_MIN_RATIO=3.5, F9_NAME=_F9, F9_MIN_FAST_PCT=90.0,
    T1_NAME=_T1, T2_NAME=_T2,
    STRAT_SINCE={"R1_загальний": "2.10", "R2_breakout": "2.10", "R3_великі": "2.10",
                 "R4_великий": "2.10", "R5_дуже": "2.10", "R6_волт": "2.10", "R7_одним": "2.10",
                 "F1_1хв": "2.10", "F2_2хв": "2.10", "F3_3хв": "2.10", "F6_1хв_перший": "2.10",
                 "F4_розумний": "2.10", "F5_перший": "2.10", "F7_без_ратіо": "2.10",
                 _F8: "2.11", _F9: "2.11", _T1: "2.11", _T2: "2.11"},
    TAPE_SINCE="2.10", _vt=_vt, _v_ok=_v_ok, _legacy_csv_cache={}, glob=_glob, re=_re,
    calendar=_calendar, _dtmod=_dtmod, _htmlmod=_htmlmod,
    RATIO_GRACE_S=RATIO_GRACE_S, _mark_ratio=_mark_ratio, _ratio_ok=_ratio_ok,
    SCAN_PROXY="", SCAN_W_PER_MIN=1000, SCAN_DEAD_S=1800,
    _scan_via_proxy=lambda: False, _scan_budget_wait=lambda w=2: None,
    _scan_state={"dead_until": 0.0, "streak": 0, "req": 0, "err": 0, "rl": 0, "fallback": 0, "last_ok": 0.0},
    TWAP_CSV="/nonexistent/twap_trades.csv", TWAP_SIG_CSV="/nonexistent/twap_signals.csv",
    TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_COHORTS=(1.0, 1.5, 2.0),
    twap_lock=threading.Lock(), twap_reg={}, twap_last_ids={},
    twap_stats={"posts": 0, "starts": 0, "eligible": 0, "cancelled": 0, "entered": 0,
                "dropped": 0, "fetch_err": 0, "parse_err": 0},
    _twap_ch_state={"HL_TWAP": {"ok_ts": 0.0, "err": 0}, "TWAPx": {"ok_ts": 0.0, "err": 0}},
    px_min={}, PX_MIN_KEEP=240,
)

# ── v2.12: грейс від останнього великого закриття, якір big_val, F8=F6-копія,
#    інкрементальний скан, TWAP-звірка з біржею ──
import hashlib as _hashlib, signal as _signal, atexit as _atexit
def _mark_close(p, ts=None):
    t = ts if ts is not None else _time.time()
    if t > (p.get("ratio_hi_ts") or 0): p["ratio_hi_ts"] = t
    return p
def _grace_val(p):
    v = float(p.get("val") or 0)
    if (p.get("ratio") or 0) < 2.0: v = max(v, float(p.get("big_val") or 0))
    return v
NEW["STRAT_SINCE"].update({_F8: "2.12", _T1: "2.12", _T2: "2.12"})
NEW.update(
    _mark_close=_mark_close, _grace_val=_grace_val, hashlib=_hashlib,
    signal=_signal, atexit=_atexit,
    scan_metrics={"next_scan_at": 0.0, "last_duration_s": 0.0, "new_pairs": 0},
    _discover_pairs=lambda *a, **k: 0, _schedule_scan=lambda d: None,
    _scan_timer=None, _scan_sched_lock=threading.Lock(),
    TWAP_MIN_DUR_S=120, TWAP_VERIFY_S=60, TWAP_CONFIRM_S=300, _twap_api_cache={},
    _twap_candle_miss={}, TWAP_CANDLE_TRIES=3,
    # v2.13
    TWAP_COHORT_SUFFIX={1.0: "", 1.5: "_15", 2.0: "_20"},
    _twap_cohort_name=lambda b, t: b + {1.0: "", 1.5: "_15", 2.0: "_20"}[t],
    strat_activated={}, _state_bak_ts=[0.0], shutil=__import__("shutil"),
    # v2.14 (аудит v2.13): константи й дефолтні заглушки — справжні функції,
    # завантажені через load(), перекривають їх
    TWAP_FIRST_SLICE_S=25.0, TWAP_VERIFY_MAX_AGE_S=90.0, TWAP_XCH_START_S=20.0,
    TWAP_XCH_DUR_S=30.0, TWAP_POLL_S=30, glob=__import__("glob"),
    _profile_budget_wait=lambda via, w_next=120, max_wait=65.0: True,
    # v2.15: рядок угоди при виході + крива окремо; заглушки перекриваються
    # справжніми функціями через load()
    TWAP_EXIT_CAP_MIN=5, TWAP_CURVE_CSV="/nonexistent/twap_curves.csv",
    TWAP_CURVE_HEADERS=(["twap_id", "strategy", "date_entry", "coin", "our_side", "exit_min", "algo_v"]
                        + [f"m{i}" for i in range(1, 121)] + ["eol"]),
    _twap_exit=lambda tr, now: None, _twap_curve_row=lambda p: [],
    _twap_freeze_exit=lambda p: None,
    _twap_chain_head=lambda mine: None,
    _profile_budget_add=lambda via, batch, body=None, reserved=0: None,
    _hl_weight=lambda body, resp=None: (2 if (isinstance(body, dict) and body.get("type") in
                                             ("clearinghouseState", "l2Book", "allMids"))
                                        else 20 + (len(resp) // 20 if isinstance(resp, list) else 0)),
    _csv_written_keys=lambda: set(),
    _tracker_csv_key=lambda coll, pid, p: ("fol", str(pid)) if coll == "fol" else ("rev", str(p.get("sig_id")), str(p.get("strategy"))),
    _twap_close_min=lambda tr: (lambda xm: xm if 1 <= xm <= 60 else 60)(int((tr.get("twap") or {}).get("exit_min") or 0)),
    _twap_trade_closed=lambda tr, now: bool(tr.get("entry_ts")) and now - tr["entry_ts"] >= (lambda xm: xm if 1 <= xm <= 60 else 60)(int((tr.get("twap") or {}).get("exit_min") or 0)) * 60.0,
    _twap_row=lambda p, now=None: [],
)
NEW["STRAT_SINCE"].update({_T1: "2.13", _T2: "2.13", _T1 + "_15": "2.13", _T1 + "_20": "2.13",
                           _T2 + "_15": "2.13", _T2 + "_20": "2.13"})
import os as _os
NEW["os"] = _os
# аудит-3 №4: гейт версії спостережень research — старі сюїти мають фікстури 2.3–2.5,
# у проді RESEARCH_SINCE = "2.17"; test_v216 перевизначає явно
NEW["RESEARCH_SINCE"] = "2.0"

# ── v2.16: чесний paper-вхід (стакан), settlement, R8, епізодне правило ──
# Старі сюїти перевіряють стару семантику входу «за мідом», тож стаб
# _paper_px повертає чистий мід із _px_now ТЕСТОВОГО неймспейсу (через
# кадр викликача); гейти віку філа вимкнені (1e12) — їх перевіряє test_v216.
import ast as _ast, math as _math, sys as _sys
_SRC216 = (_HL + "/server.py")
_tree216 = _ast.parse(open(_SRC216, encoding="utf-8").read())
def _dt216(ts):
    return _time.strftime("%Y-%m-%d %H:%M:%S", _time.localtime(ts)) if ts else ""
_ns216 = {"math": _math, "time": _time, "os": _os, "REV_HOLD_MIN": 30, "_PRIV": ("_ct", "_cl"),
          "REFRESH_S": 1800, "stats": {},
          # v2.18: константи профілю v10 для хелперів реконструктора
          "F4_CHUNK_PCT": 0.05, "F4_MIN_NOTIONAL": 100_000.0, "F4_FULL_PCT": 0.95,
          "F4_MAX_UNLOAD_S": 300.0, "F4_MIN_EPISODES": 5, "F4_MIN_FAST_PCT": 70.0,
          "PROFILE_ENTRY_LAT_MS": 5000, "PROFILE_ZERO_TOL": 1e-6,
          "SIM_COMMISSION": 0.0005, "SIM_SPREAD": 0.0002, "SIM_POSITION_USD": 1000.0,
          "_dt": _dt216, "_median": _median, "print": lambda *a, **k: None}
def _pull216(names, ns):
    body = [n for n in _tree216.body if isinstance(n, _ast.FunctionDef) and n.name in names]
    exec(compile(_ast.Module(body=body, type_ignores=[]), "shim216", "exec"), ns)
    return ns
_pull216({"_sim_slip", "_ms", "_rnd", "_rev_extra", "_fol_row", "_rev_trade_closed",
          "_p_costs", "_stat_small", "_period_stats", "_ts_local", "_official_net",
          "_leg_costs", "_parse_curve", "_curves_of", "_agg_block", "_pub"}, _ns216)
_ns216["_journal"] = lambda *a, **k: None
# аудит-3 №2: live-епізод — спільна функція settle.close_episode (та сама, що у settlement)
if _HL not in _sys.path:
    _sys.path.insert(0, _HL)
import settle as _settle_mod
NEW["_close_episode"] = _settle_mod.close_episode
_pull216({"_fc_rebuild", "_depth_ok", "_wilson_lb", "_profile_txs", "_profile_lifecycles",
          "_profile_episode"}, _ns216)
NEW["_fc_rebuild"] = _ns216["_fc_rebuild"]
NEW["_depth_ok"] = _ns216["_depth_ok"]     # рев'ю аудит-3: придатність глибини для ratio у скані
# v2.18: реконструктор життєвих циклів профілю (сюїти, що вантажать _build_profile)
for _k in ("_wilson_lb", "_profile_txs", "_profile_lifecycles", "_profile_episode"):
    NEW[_k] = _ns216[_k]
NEW.update(F10_NAME="F10_розумний_60", F_FIXED_TIMER_S=60.0, PROFILE_ENTRY_LAT_MS=5000,
           PROFILE_ZERO_TOL=1e-6, F4_MIN_FAST_PCT=70.0, F9_MIN_FAST_PCT=90.0,
           F4_MIN_EPISODES=5, profiles_fetching=set(), profile_retry_at={})

def with_opens(fills):
    """v2.18: старі фікстури профілю містять ЛИШЕ закриття (старий будівник
    відкриття ігнорував). Профіль v10 читає життєві цикли: закриття без
    видимого відкриття = позиція існувала до історії → епізод «невизначений».
    Щоб старі сценарії описували те, що мали на увазі (повністю
    спостережена позиція), додаємо синтетичне відкриття перед першим
    закриттям кожної позиції (і перед зростанням позиції = долив)."""
    import copy
    out = []
    rem = {}
    seq = 0
    for f in sorted(fills, key=lambda x: (x.get("time", 0) if isinstance(x.get("time"), (int, float)) else 0)):
        try:
            d = str(f.get("dir", ""))
            sp = abs(float(f.get("startPosition")))
            sz = float(f.get("sz"))
            t = int(f.get("time"))
            coin = f.get("coin")
            float(f.get("px"))   # битий філ (px не число) — лишаємо як є, відкриття не синтезуємо
        except (TypeError, ValueError):
            out.append(f); continue
        if not coin or not (d.startswith("Close") or ">" in d or "Liquidat" in d):
            if d.startswith("Open"):
                rem[coin] = sp + sz
            out.append(f); continue
        r = rem.get(coin, 0.0)
        if sp > r * 1.01 + 1e-9:
            seq += 1
            side = "Short" if (d.startswith("Close Short") or d.startswith("Short >")) else "Long"
            out.append({"coin": coin, "px": f.get("px"), "sz": str(sp - r), "startPosition": str(r),
                        "dir": f"Open {side}", "crossed": False, "twapId": None, "time": t - 1,
                        "hash": f"0xopen{seq}", "tid": -seq})
        rem[coin] = max(0.0, sp - min(sz, sp))
        out.append(f)
    return out

def with_opens_wrap(fn):
    def _w(fills, *a, **k):
        return fn(with_opens(fills), *a, **k)
    return _w

def _p_costs_stub(p):
    """Витрати трекера: збережені (v2.16) або стара формула по _sim_slip
    ТЕСТОВОГО неймспейсу (тести підміняють _sim_slip)."""
    c = p.get("costs")
    if isinstance(c, (int, float)) and _math.isfinite(c):
        return float(c)
    f = _sys._getframe(1)
    for _ in range(6):
        g = f.f_globals
        if "_sim_slip" in g and g is not _ns216:
            return (2 * g.get("SIM_COMMISSION", 0.0005) + 2 * g["_sim_slip"](p.get("depth") or 0)) * 100.0
        if f.f_back is None: break
        f = f.f_back
    return (2 * 0.0005 + 2 * _ns216["_sim_slip"](p.get("depth") or 0)) * 100.0
_ns216["_p_costs"] = _p_costs_stub
def _paper_px_stub(coin, side, whale_px=None, max_mid_age_ms=20_000, strict=False, qty=None, exch=None, min_recv_ms=None):
    """Старі сюїти: мід тестового неймспейсу видається як «стакан» (src book),
    щоб входи відбувались; строгість стакану перевіряє test_v216."""
    g = _sys._getframe(1).f_globals
    pxnow = g.get("_px_now")
    mid = pxnow(coin) if pxnow else None
    _tm = g.get("time", _time)
    meta = {"mid": mid, "px_age_ms": 0, "whale_px": whale_px, "book_ms": None, "partial": 0,
            "quote_ts_ms": None, "book_mid": mid, "exch": exch or "hl", "fill_frac": 1.0,
            "recv_ms": _tm.time() * 1000, "qty": qty}   # float: recv/1000 == time тесту
    if not mid:
        return None, ("no_book" if strict else "none"), meta
    return mid, "book", meta
def _sim_costs_stub(coin, our_side):
    g = _sys._getframe(1).f_globals
    d = g["_sim_depth"](coin, our_side) if "_sim_depth" in g else 1e4
    slip = g["_sim_slip"](d or 0) if "_sim_slip" in g else _ns216["_sim_slip"](d or 0)
    return ((2 * g.get("SIM_COMMISSION", 0.0005) + 2 * slip) * 100.0, d or 0, d or 0)
_R8 = "R8_тп80"
NEW["STRAT_SINCE"][_R8] = "2.17"

def _rev_ref_px_stub(coin, first_ts_ms, first_px):
    """Старі rev-фікстури задають рух через _px_now/_px_ago (3-хв кеш) і філи
    з «нереальною» ціною 150 при міді 100: відтворюємо старий рух епізоду
    (last/ref = now/ago), щоб пороги/гейти тестувались як раніше."""
    g = _sys._getframe(1).f_globals
    try:
        now_ = g["_px_now"](coin); ago = g["_px_ago"](coin, g.get("REV_WINDOW_S", 180))
    except Exception:
        now_, ago = None, None
    if now_ and ago and first_px:
        return first_px * ago / now_
    return first_px
NEW["_rev_ref_px"] = _rev_ref_px_stub
NEW["_px_at"] = lambda coin, ts, tol=90.0: None
NEW.update(
    R8_NAME=_R8, R8_TP_FRAC=0.8, REV_HOLD_MIN=30, REV_EP_MAX_S=300.0, REV_EP_MIN_MAG=1.0,
    FOLLOW_MAX_FILL_AGE_S=1e12, REV_MAX_FILL_AGE_S=1e12,
    _hl_delisted=set(), _px_pending={}, fast_retry={}, fast_retry_cnt={},
    FAST_RETRY_S=1.5, FAST_RETRY_MAX=3, FAST_SWEEP_CHUNK=20,
    _settle_cache={"key": None, "data": {}}, _load_settlements=lambda: {},
    _prio_opener=None, SIM_POSITION_USD=1000.0,
    hl_book_exec=lambda coin, side, usd=None: None,
    _paper_px=_paper_px_stub, _sim_costs=_sim_costs_stub, _px_age_s=lambda: None,
    _px_mid_age=lambda coin: (None, None),
    _ms=_ns216["_ms"], _rnd=_ns216["_rnd"], _rev_extra=_ns216["_rev_extra"],
    _fol_row=_ns216["_fol_row"], _rev_trade_closed=_ns216["_rev_trade_closed"],
    _p_costs=_p_costs_stub, _stat_small=_ns216["_stat_small"],
    _period_stats=_ns216["_period_stats"], _ts_local=_ns216["_ts_local"],
    _official_net=_ns216["_official_net"], _leg_costs=_ns216["_leg_costs"],
    _parse_curve=_ns216["_parse_curve"], _curves_of=_ns216["_curves_of"],
    _agg_block=_ns216["_agg_block"], _pub=_ns216["_pub"], _PRIV=("_ct", "_cl"),
    _strat2_full={}, _journal=lambda *a, **k: None, _journal_lock=threading.Lock(),
    BOOK_MAX_AGE_S=5.0, REV_REF_TOL_S=12.0,
    depth_for_side=lambda d, side: ((d or {}).get("bid" if side == "LONG" else "ask", 0)
                                    or (d or {}).get("max", 0)) if d else 0,
    cache={"depth": {}, "depth_prev": {}}, cache_lock=threading.Lock(),
    _strat2_cache={"ts": 0.0, "data": None},
)
# старі сюїти пишуть рядки зі СВОЄЮ версією (2.4, 2.5…): у них since не
# має відкидати нічого; test_v211 передає справжні STRAT_SINCE/TAPE_SINCE
NEW["TAPE_SINCE"] = "0.1"
NEW["STRAT_SINCE"] = {k: "0.1" for k in NEW["STRAT_SINCE"]}

# ── v2.19: виконання на Binance, приймач філів, статуси сигнал/ціна ──
import types as _types
_pull216({"_vt", "_exec_extra", "_funding_pct", "_book_walk", "_sig_check_rev", "_sig_check_fol",
          "_px_check", "_head_ok", "_count_by", "_tx_key", "_ingest_txs", "_pending_rev_rows"}, _ns216)
for _k in ("_vt", "_exec_extra", "_funding_pct", "_book_walk", "_sig_check_rev", "_sig_check_fol",
           "_px_check", "_head_ok", "_count_by", "_tx_key"):
    NEW[_k] = _ns216[_k]
NEW.update(EXEC_EXCH="hl",            # старі сюїти перевіряють семантику стакану HL; test_v219 — "bn"
           BOOK_APPLY_MAX_S=5.0, BN_EXEC_LEVELS=100, FUND_CACHE_S=60.0,
           SIM_POSITION_USD=1000.0,
           EXEC_COLS=["exch", "entry_qty", "exit_fill_frac", "funding_pct", "exec_delay_s",
                      "decision_ms", "detect_ms", "exit_decision_ms", "exit_px_filled", "trig_hash"],
           _prio_acc={}, PRIO_AGG_S=15.0, PRIO_AGG_MIN_USD=0.0, INGEST_LRU=20000)
NEW["OrderedDict"] = OrderedDict = __import__("collections").OrderedDict
NEW["_ingested"] = OrderedDict()
NEW["_ingested_lock"] = __import__("threading").Lock()
NEW["_fund_cache"] = {}
def _entry_exec_fields(px_entry, pmeta, coin):
    """Стаб без мережі: біржа з meta (або EXEC_EXCH сюїти), кількість на $1000, без фандингу."""
    exch = (pmeta or {}).get("exch") or "hl"
    return {"exch": exch, "entry_qty": (1000.0 / px_entry if px_entry else None),
            "entry_recv_ms": (pmeta or {}).get("recv_ms"), "entry_fill_frac": (pmeta or {}).get("fill_frac"),
            "fund_rate": None, "fund_next_ms": None}
NEW["_entry_exec_fields"] = _entry_exec_fields
NEW["bn_funding_info"] = lambda coin: (None, None)
NEW["bn_book_exec"] = lambda coin, side, usd=None, qty=None, min_recv_ms=None: None
def _rebind(fn_name):
    """Функція сервера, ВИКОНУВАНА у глобалах ВИКЛИКАЧА (тестового неймспейсу):
    приймач філів кличе хуки (rev/follow/fc/sim), які кожна сюїта стабить у
    своєму ns — код беремо з server.py, а імена — з кадру, що викликав."""
    real = _ns216[fn_name]
    def _w(*a, **k):
        g = _sys._getframe(1).f_globals
        g.setdefault("_ingested", OrderedDict()); g.setdefault("_ingested_lock", NEW["_ingested_lock"])
        g.setdefault("INGEST_LRU", 20000); g.setdefault("_tx_key", _ns216["_tx_key"])
        g.setdefault("_journal", lambda *a, **k: None); g.setdefault("OrderedDict", OrderedDict)
        g.setdefault("stats", {})
        g.setdefault("_ingest_retry", {}); g.setdefault("INGEST_RETRY_MAX", 50)   # v2.20
        g.setdefault("_fc_done", {}); g.setdefault("INGEST_CONSUMERS", ("sim", "fc", "rev", "fol"))
        f = _types.FunctionType(real.__code__, g, fn_name, real.__defaults__, real.__closure__)
        return f(*a, **k)
    _w.__name__ = fn_name
    return _w
NEW["_ingest_txs"] = _rebind("_ingest_txs")
NEW["_pending_rev_rows"] = _rebind("_pending_rev_rows")

NEW["HEAD_STRICT"] = False   # старі сюїти: заголовок без вимоги settlement (test_v219 — True)

_ns216["HEAD_STRICT"] = False          # _head_ok/_agg_block, витягнуті у _ns216: стара семантика заголовку
_ns216["BOOK_MAX_AGE_S"] = 5.0
_pull216({"_book_quote"}, _ns216)
NEW["_book_quote"] = _ns216["_book_quote"]

_ns216.update(_REV_THR={"R4_великий": 2.0, "R5_дуже": 3.0}, REV_MAX_FILL_AGE_S=60.0, FOLLOW_MAX_FILL_AGE_S=20.0,
              R7_NAME="R7_одним", F4_FULL_PCT=0.95, R7_MIN_TX_USD=100_000.0, R8_NAME="R8_тп80")
NEW["_REV_THR"] = _ns216["_REV_THR"]

# ── v2.20: причинність котирувань, потік цін Binance, ідемпотентний приймач, групи ──
_pull216({"_tr_exch", "_mid_age_exch", "_bn_px_mid_age", "_bn_px_now", "_bn_px_age_s",
          "_exit_partial_of", "_group_stats", "_eval_ts", "_raw_book_get", "_raw_book_put",
          "_book_from_raw"}, _ns216)
for _k in ("_tr_exch", "_mid_age_exch", "_bn_px_mid_age", "_bn_px_now", "_bn_px_age_s",
           "_exit_partial_of", "_group_stats", "_eval_ts", "_raw_book_get", "_raw_book_put", "_book_from_raw"):
    NEW[_k] = _ns216[_k]
_ns216.update(EVAL_SINCE="2026-09-13", bn_px_hist={}, bn_px_lock=__import__("threading").Lock(),
              _raw_book_cache={}, BOOK_RAW_TTL_S=2.0)
_ns216.setdefault("EXEC_EXCH", "hl"); _ns216.setdefault("px_hist", {}); _ns216.setdefault("px_lock", __import__("threading").Lock())
NEW.update(EVAL_SINCE="2026-09-13", bn_px_hist=_ns216["bn_px_hist"], bn_px_lock=_ns216["bn_px_lock"],
           _raw_book_cache={}, BOOK_RAW_TTL_S=2.0, GONE_MAX_S=7200.0, INGEST_CONSUMERS=("sim", "fc", "rev", "fol"),
           _fc_done={}, BN_PX_POLL_S=3.0)
# старі сюїти стабили _px_now у своїх ns: _tr_px_now (HL-трекери без exch → _px_now) бере його з кадру виклику
def _tr_px_now_stub(p, max_age=30.0):
    g = _sys._getframe(1).f_globals
    ex = (p.get("exch") or ("hl" if _ns216["_vt"](p.get("algo_v")) < (2, 19) else g.get("EXEC_EXCH", "hl")))
    if ex == "bn" and "_bn_px_now" in g:
        return g["_bn_px_now"](p["coin"], max_age)
    return g["_px_now"](p["coin"], max_age=max_age)
NEW["_tr_px_now"] = _tr_px_now_stub

# v2.20: _mid_age_exch — з кадру виклику (старі сюїти стабили _px_mid_age у своїх ns)
def _mid_age_exch_stub(exch, coin):
    g = _sys._getframe(1).f_globals
    if (exch or g.get("EXEC_EXCH", "hl")) == "bn" and "_bn_px_mid_age" in g:
        return g["_bn_px_mid_age"](coin)
    f = g.get("_px_mid_age") or _ns216.get("_px_mid_age")
    mid, age = f(coin) if f else (None, None)
    ts = (_time.time() - age / 1000.0) if age is not None else None
    return mid, age, ts
NEW["_mid_age_exch"] = _mid_age_exch_stub

# v2.20: потік цін Binance у тестах — з неймспейсу ВИКЛИКАЧА (bn_px_hist / get_bn_symbol сюїти)
def _bn_px_mid_age_stub(coin, _g=None):
    g = _g if _g is not None else _sys._getframe(1).f_globals
    sym = (g.get("get_bn_symbol") or (lambda c: c.upper() + "USDT"))(coin)
    h = (g.get("bn_px_hist") or {}).get(sym)
    if not h:
        return None, None, None
    ts, mid = h[-1][0], h[-1][1]
    return mid, int((_time.time() - ts) * 1000), ts
def _bn_px_now_stub(coin, max_age=20.0, _g=None):
    g = _g if _g is not None else _sys._getframe(1).f_globals
    mid, age, _ = _bn_px_mid_age_stub(coin, _g=g)
    return mid if (mid is not None and (age or 0) <= max_age * 1000) else None
def _tr_px_now_stub(p, max_age=30.0):
    g = _sys._getframe(1).f_globals
    ex = (p.get("exch") or ("hl" if _ns216["_vt"](p.get("algo_v")) < (2, 19) else g.get("EXEC_EXCH", "hl")))
    if ex == "bn":
        return _bn_px_now_stub(p["coin"], max_age, _g=g)
    return g["_px_now"](p["coin"], max_age=max_age)
def _mid_age_exch_stub(exch, coin):
    g = _sys._getframe(1).f_globals
    if (exch or g.get("EXEC_EXCH", "hl")) == "bn" and "bn_px_hist" in g:
        return _bn_px_mid_age_stub(coin, _g=g)
    f = g.get("_px_mid_age") or _ns216.get("_px_mid_age")
    mid, age = f(coin) if f else (None, None)
    ts = (_time.time() - age / 1000.0) if age is not None else None
    return mid, age, ts
NEW["_bn_px_mid_age"] = _bn_px_mid_age_stub
NEW["_bn_px_now"] = _bn_px_now_stub
NEW["_tr_px_now"] = _tr_px_now_stub
NEW["_mid_age_exch"] = _mid_age_exch_stub

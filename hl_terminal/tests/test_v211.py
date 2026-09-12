import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.11 (ТЗ 08.09): (1) грейс-вікно ratio 30 хв; (2) статистика
переживає апдейти — since-версії і .legacy-файли; (3) швидкість
угод/день; (4) F8 ratio≥3.5 і F9 90% швидких; (5) TWAP-реверс —
парсер двох каналів на живих постах, реєстр, стан-машина, трекер 120
хв, CSV; (6) мобільний CSS; (7) проксі скану — бюджет, фолбек."""
import ast, threading, time, json, os, re, sys, math, textwrap, tempfile
import calendar, datetime as _dtmod, html as _htmlmod, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from v28_shim import NEW, _median

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)
HERE = os.path.dirname(os.path.abspath(__file__))

def load(names, extra=None, assigns=()):
    body = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in assigns
                        for t in n.targets))]
    mod = ast.Module(body=body, type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "print": lambda *a, **k: None}
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
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.13",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True)
assert const("DATA_ALGO_V") == "2.20"

# ═══ 1. Грейс-вікно ratio ═════════════════════════════════════════
g = load({"_mark_ratio", "_ratio_ok"}, {"RATIO_GRACE_S": 1800})
mk, ok = g["_mark_ratio"], g["_ratio_ok"]
now = 1_000_000.0
p = mk({}, 2.5, now)
assert p["ratio"] == 2.5 and p["ratio_hi_ts"] == now
mk(p, 1.2, now + 600)                       # впала нижче 2 через 10 хв
assert p["ratio_hi_ts"] == now              # мітка НЕ рухається вниз
assert ok(p, now + 600) and ok(p, now + 1799)   # грейс 30 хв від останнього ≥2
assert not ok(p, now + 1800)                    # рівно пів години — кінець
mk(p, 2.1, now + 5000)                       # повернулась ≥2 -> мітка знову
assert ok(p, now + 5000 + 1000)
assert not ok({"ratio": 1.5})                # ніколи не була великою
# rev/follow: пара з ratio 1.4, що була ≥2 10 хв тому — сигнал ЖИВЕ
def mk_rev(old_extra=None):
    ns = load({"rev_on_close", "_mark_ratio", "_ratio_ok"}, dict(C,
        RATIO_GRACE_S=1800, strat2_lock=threading.RLock(), rev_open={},
        fc_lock=threading.Lock(), fc_episodes={},
        is_vault=lambda a: False,
        _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
        _sim_depth=lambda c, s: 1e5, _dt=_dt,
        _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
        REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
        _strat_csv_append=lambda p, h, r: True))
    return ns
OLD_G = {"size": 1000.0, "side": "LONG", "val": 150_000, "ratio": 1.4,
         "ratio_hi_ts": time.time() - 600}
r = mk_rev()
r["rev_on_close"]("0xa", "AAA", OLD_G,
                  [{"sz": 1000.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"}],
                  True)
assert "R1_загальний" in {p["strategy"] for p in r["rev_open"].values()}
r = mk_rev()
r["rev_on_close"]("0xa", "AAA", dict(OLD_G, ratio_hi_ts=time.time() - 1900),
                  [{"sz": 1000.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"}],
                  True)
assert r["rev_open"] == {}                   # >30 хв — знову мала
def mk_fol(old):
    n = load({"follow_on_txs", "_ratio_ok"}, dict(C,
        RATIO_GRACE_S=1800, strat2_lock=threading.RLock(), follow_open={},
        rev_open={}, follow_last_close={}, wallet_profiles={},
        profiles_fetching=set(), is_vault=lambda a: False,
        _px_now=lambda c, max_age=20: 100.0, _px_ago=lambda c, s: 100.0,
        _sim_depth=lambda c, s=None: 1e5, _profile_request=lambda a: None,
        stats={}))
    n["follow_on_txs"]("0xabc", "AAA", old,
                       [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
    return n
assert len(mk_fol(OLD_G)["follow_open"]) == 4        # F1-F3 + F6 у грейсі
assert mk_fol(dict(OLD_G, ratio_hi_ts=time.time() - 2000))["follow_open"] == {}
# алерт-шлях: обидва гейти через _ratio_ok, розмір події при full_close
# — по епізоду (_ev_val), як у rev
assert "if big_txs and (not _ratio_ok(old) or _ev_val < MIN_POS_USD):" in src
assert "if not big_txs or not _ratio_ok(new_pos, _now)" in src
assert src.count("_mark_ratio(") >= 4          # усі місця запису ratio
assert src.count('"ratio_hi_ts": ') >= 4        # усі місця вставки пари
# скан-діф: пара з ratio 1.5 лишається в базі діфу лише в грейсі
cpc = load({"check_position_changes", "_ratio_ok"}, dict(C,
    RATIO_GRACE_S=1800, COIN_BLACKLIST=set(), tracking_lock=threading.Lock(),
    prev_positions={"0xw": {"AAA": {"size": 10.0, "val": 1e5, "side": "LONG",
                                    "ratio": 2.5, "ratio_hi_ts": time.time() - 300,
                                    "entry": 1.0},
                            "BBB": {"size": 10.0, "val": 1e5, "side": "LONG",
                                    "ratio": 2.5, "ratio_hi_ts": time.time() - 3000,
                                    "entry": 1.0}}},
    fill_cursor={}, get_recent_market_fills=lambda *a, **k: ([], []),
    depth_for_side=lambda d, s: (d or {}).get("bid", 0),
    alerted_txs={}, sent_alerts=set(), close_episodes={}, scan_tombstones={},
    delta_seen={}, watchlist_lock=threading.Lock(), watchlist={},
    stats={"delta_events": 0, "fills_confirmed": 0, "fills_empty": 0}))
res = {"AAA": [{"addr": "0xw", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}],
       "BBB": [{"addr": "0xw", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}],
       "CCC": [{"addr": "0xw", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}]}
depth = {c: {"bid": 1e5} for c in ("AAA", "BBB", "CCC")}
depth.update({f"D{i}": {"bid": 1e5} for i in range(10)})
cpc["check_position_changes"](res, depth)
pp = cpc["prev_positions"]["0xw"]
assert "AAA" in pp and pp["AAA"]["ratio"] == 1.5      # ratio 1.5, грейс 5 хв тому
assert "BBB" not in pp                                # грейс минув (50 хв)
assert "CCC" not in pp                                # ніколи не була ≥2
print("1) грейс ratio: 30 хв після падіння нижче 2 у rev/follow/алертах/скан-діфі")

# ═══ 2. Версіонування: since-версії і .legacy ═════════════════════
vt, vok = load({"_vt", "_v_ok"})["_vt"], load({"_vt", "_v_ok"})["_v_ok"]
_ss = load(set(), {}, assigns=("STRAT_SINCE", "TAPE_SINCE"))
REAL_SINCE, REAL_TAPE = _ss["STRAT_SINCE"], _ss["TAPE_SINCE"]
assert REAL_TAPE == "2.10" and REAL_SINCE["F8_ratio35"] == "2.12" and REAL_SINCE["R1_загальний"] == "2.17"
assert vt("2.10") > vt("2.9") and vt("2.11") > vt("2.10")   # не рядки!
assert vok("2.10", "2.10") and vok("2.11", "2.10") and not vok("2.9", "2.10")
assert not vok("", "2.10") and not vok(None, "2.10")        # legacy без версії
ss = const("STRAT_SINCE") if False else None
assert '"R1_загальний": "2.17"' in src and 'F8_NAME: "2.12"' in src
assert 'TAPE_SINCE = "2.10"' in src
# strat2_api читає поточний файл + .legacy і фільтрує по since стратегії
d2 = tempfile.mkdtemp()
FH = const("FOLLOW_HEADERS") if False else None
fh = ["date_open", "date_close", "strategy", "coin", "our_side", "whale_addr",
      "entry_px", "exit_px", "exit_reason", "hold_s", "gross_pct", "costs_pct",
      "net_pct", "peak_pct", "trough_pct", "tx_pct_of_pos", "tx_usd", "ratio",
      "pos_usd", "vault", "hour", "btc_move_pct", "profile_gap_s", "algo_v",
      "trade_id", "first_shot", "pair_gap_s", "prof_n_fast", "prof_n_slow",
      "prof_fast_pct", "prof_unload_med_s", "prof_unload_mean_s",
      "prof_window_d", "eol"]
def frow(strat, net, ver, tid, date="2026-09-01 10:00:00"):
    r = [""] * len(fh)
    r[0] = date; r[2] = strat; r[3] = "AAA"; r[4] = "LONG"; r[5] = "0xw"
    r[11] = "0.15"; r[12] = str(net); r[23] = ver; r[24] = tid; r[-1] = "^"
    return ",".join(r)
open(os.path.join(d2, "follow_trades.csv"), "w").write(
    ",".join(fh) + "\n" + frow("F1_1хв", 1.0, "2.11", "t1") + "\n"
    + frow("F8_ratio35", 2.0, "2.12", "t2") + "\n")
# legacy-файл СТАРОГО формату (без prof_*-колонок): F1 з 2.10 — має жити,
# F8 з 2.10 — ні (стратегія з'явилась у 2.11), F1 з 2.9 — ні
fh_old = fh[:25] + ["eol"]
def frow_old(strat, net, ver, tid):
    r = [""] * len(fh_old)
    r[0] = "2026-08-25 10:00:00"; r[2] = strat; r[3] = "AAA"; r[4] = "LONG"
    r[5] = "0xw"; r[11] = "0.15"; r[12] = str(net); r[23] = ver; r[24] = tid
    r[-1] = "^"
    return ",".join(r)
open(os.path.join(d2, "follow_trades.csv.legacy-1725000000.csv"), "w").write(
    ",".join(fh_old) + "\n" + frow_old("F1_1хв", 3.0, "2.10", "t3") + "\n"
    + frow_old("F8_ratio35", 4.0, "2.10", "t4") + "\n"
    + frow_old("F1_1хв", 5.0, "2.9", "t5") + "\n")
api = load({"strat2_api", "_median", "_vt", "_v_ok"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
    STRAT_SINCE=REAL_SINCE, TAPE_SINCE=REAL_TAPE,
    _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
    FOLLOW_OUT_CSV=os.path.join(d2, "fo.csv"), REV_CSV=os.path.join(d2, "rev.csv"),
    REV_SIG_CSV=os.path.join(d2, "sig.csv"), FOLLOW_CSV=os.path.join(d2, "follow_trades.csv"),
    REV_OUT_CSV=os.path.join(d2, "out.csv"), TWAP_CSV=os.path.join(d2, "tw.csv"),
    TWAP_SIG_CSV=os.path.join(d2, "tws.csv"), F5_NAME="F5_перший",
    F6_NAME="F6_1хв_перший", F7_NAME="F7_без_ратіо"))
out = api["strat2_api"]()
f1 = out["strategies"]["F1_1хв"]
assert f1["n"] == 2 and sorted(t["net30"] for t in f1["trades"]) == [1.0, 3.0], f1["n"]
assert out["strategies"]["F8_ratio35"]["n"] == 1     # 2.10-рядок F8 відкинуто
assert out["legacy_rows"] == 2                        # F8@2.10 + F1@2.9
assert f1["since"] == "2.10" and out["strategies"]["F8_ratio35"]["since"] == "2.12"
# кеш legacy: другий виклик не перечитує файл (mtime/size ті самі)
api["_strat2_cache"]["ts"] = 0
cache = api["_legacy_csv_cache"]
assert len(cache) == 1
out2 = api["strat2_api"]()
assert out2["strategies"]["F1_1хв"]["n"] == 2
print("2) since-версії: незмінені стратегії переживають апдейт, .legacy читається")

# ═══ 3. Швидкість угод/день ═══════════════════════════════════════
# F1: перший рядок 25.08, зараз — тест: n=2 / дні
days = (time.time() - time.mktime(time.strptime("2026-08-25 10:00:00", "%Y-%m-%d %H:%M:%S"))) / 86400
assert abs(f1["days"] - round(days, 1)) < 0.2 and abs(f1["per_day"] - round(2 / max(1.0, days), 2)) < 0.05
assert out["strategies"]["F2_2хв"]["per_day"] is None and out["strategies"]["F2_2хв"]["days"] == 0.0
print("3) швидкість: угод/день від першого рядка стратегії, мінімум 1 день")

# ═══ 4. F8 ratio≥3.5 і F9 90% швидких ════════════════════════════
PROF_OK = {"ok": True, "v": 9, "fetched": time.time() - 100, "n_big": 7,
           "n_fast": 6, "n_slow": 1, "fast_pct": 85.7, "unload_med_s": 95.0,
           "unload_mean_s": 110.5, "window_d": 61.3, "avg_gap_s": 40.0,
           "nr": {"ok": True, "n_big": 8, "n_fast": 7, "n_slow": 1,
                  "fast_pct": 87.5, "unload_med_s": 120.0,
                  "unload_mean_s": 130.0, "avg_gap_s": 50.0}}
def mk_f(prof, old):
    n = load({"follow_on_txs", "_ratio_ok"}, dict(C,
        RATIO_GRACE_S=1800, strat2_lock=threading.RLock(), follow_open={},
        rev_open={}, follow_last_close={},
        wallet_profiles={"0xabc": dict(prof)}, profiles_fetching=set(),
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: None, stats={}))
    n["follow_on_txs"]("0xabc", "AAA", old,
                       [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
    return sorted(p["strategy"] for p in n["follow_open"].values())
OLD3 = {"size": 1000.0, "side": "LONG", "ratio": 3.0, "val": 5e5}
st = mk_f(PROF_OK, OLD3)
assert "F5_перший" in st and "F8_ratio35" not in st and "F9_без_ратіо_90" not in st, st
st = mk_f(PROF_OK, dict(OLD3, ratio=3.6))
assert "F8_ratio35" in st and "F7_без_ратіо" in st and "F9_без_ратіо_90" not in st, st
# v2.12: поріг по ТОЧНИХ лічильниках (n_fast/n_slow), не по округленому fast_pct
# v2.18: nr-гілка = той самий профіль; F9 читає точні лічильники з кореня (n_fast/n_big)
p9 = dict(PROF_OK, fast_pct=92.0, n_fast=9, n_slow=1, n_big=10)
st = mk_f(p9, OLD3)
assert "F9_без_ратіо_90" in st and "F7_без_ратіо" in st, st
p9b = dict(PROF_OK, fast_pct=92.0, n_fast=4, n_slow=0, n_big=4)   # <5 швидких
assert "F9_без_ратіо_90" not in mk_f(p9b, OLD3)
p9c = dict(PROF_OK, fast_pct=90.0, n_fast=7, n_slow=1, n_big=8)   # 87.5% насправді
assert "F9_без_ратіо_90" not in mk_f(p9c, OLD3)
p9d = dict(PROF_OK, n_fast=9, n_slow=0, n_uncertain=1, n_big=10)   # 90% при невизначеному=повільний
assert "F9_без_ратіо_90" in mk_f(p9d, OLD3)
p9e = dict(PROF_OK, n_fast=8, n_slow=0, n_uncertain=2, n_big=10)   # 80% — невизначені НЕ рахуються швидкими
assert "F9_без_ратіо_90" not in mk_f(p9e, OLD3)
assert "for st in list(FOLLOW_TIMERS) + [F6_NAME, F4_NAME, F5_NAME, F8_NAME," in src
print("4) F8 (ratio пари ≥3.5) і F9 (nr ≥90% швидких) — гейти і API")

# ═══ 5. TWAP-реверс ═══════════════════════════════════════════════
# ── фейкова біржа для звірки TWAP (v2.12): twapHistory / userTwapSliceFills /
#    candleSnapshot з тим самим форматом, що віддає api.hyperliquid.xyz (перевірено
#    живими запитами 08.09) ──
EX = {"hist": {}, "slices": {}, "candles": {}}
def _fake_prio(body):
    t = body.get("type")
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
            executed=0.0, sp=0.0, px=100.0):
    EX["hist"].setdefault(addr, []).append(
        {"time": int(start), "twapId": twap_id,
         "state": {"coin": coin, "user": addr, "side": "B" if side == "buy" else "A",
                   "sz": str(sz), "executedSz": str(executed), "minutes": minutes,
                   "reduceOnly": False, "timestamp": int(start * 1000)},
         "status": {"status": status}})
    EX["slices"].setdefault(addr, []).append(
        {"twapId": twap_id, "fill": {"coin": coin, "side": "B" if side == "buy" else "A",
                                    "time": int(start * 1000) + 1500, "startPosition": str(sp),
                                    "px": str(px), "sz": "1", "tid": 1, "dir": "x"}})
def ex_reset():
    EX["hist"].clear(); EX["slices"].clear(); EX["candles"].clear()
    TW["_twap_api_cache"].clear()
TW = load({"twap_parse", "_twap_usd", "_twap_hl_dt", "_iso_ts", "_tme_parse",
           "_tme_text", "_coin_canon",
           "_twap_find", "_twap_register", "_twap_sig_write", "_twap_drop",
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
               TWAP_LATE_S=90.0, TWAP_BACKLOG_S=3600, TWAP_HOLD_MIN=60,
               TWAP_TRACK_MIN=120, TWAP_SIG_CSV="tws.csv", TWAP_CSV="tw.csv",
               TWAP_SIG_HEADERS=None, TWAP_HEADERS=None,
               _strat_csv_append=lambda p, h, r: True, _dt=_dt,
               strat2_lock=threading.RLock(), rev_open={},
               check_one_wallet=lambda a, post=None: {},
               hl_post_prio=lambda b, retries=2, direct=False, **kw: _fake_prio(b),
               _hl_post_prio_direct=lambda b, retries=2, **kw: _fake_prio(b),
               _prio_opener=object(), _twap_api_cache={},
               _twap_candle_miss={}, TWAP_CANDLE_TRIES=3,
               TWAP_MIN_DUR_S=120, TWAP_VERIFY_S=30, TWAP_CONFIRM_S=300, _px_now=lambda c, max_age=20: 100.0,
               _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
               _sim_slip=lambda d: 0.0005, save_state=lambda: None,
               px_lock=threading.Lock(), px_hist={}, px_min={}),
          assigns=("_HL_START", "_HL_USER", "_HL_USER_PFX", "_HL_PERIOD", "_HL_ETA",
                   "_HL_PRICE", "_HL_CLOSED", "_X_START", "_X_ADDR", "_X_PRICE",
                   "_X_CREATED", "_X_SIZE", "_TWAP_MONTHS", "TWAP_SIG_HEADERS",
                   "TWAP_HEADERS", "_X_TWAPID", "_X_EXEC", "_X_STATUS",
                   "_HL_FILLED", "_HL_TIME", "TWAP_CURVE_HEADERS"))
tp, tme = TW["twap_parse"], TW["_tme_parse"]
# (а) живі сторінки обох каналів (08.09): усі 20 постів розбираються;
# парсер повертає (pid, ts, text, reply) — reply = цитата поста-відповіді
# (рев'ю v2.11: регекс без js-суфікса брав цитату замість тексту, і всі
# скасування читались як старти)
fx = {}
for ch, fn in (("HL_TWAP", "tme_hl_twap.html"), ("TWAPx", "tme_twapx.html")):
    posts = tme(open(os.path.join(HERE, fn), encoding="utf-8").read())
    assert len(posts) == 20 and posts == sorted(posts, key=lambda x: x[0])
    assert all(len(x) == 5 for x in posts)              # v2.12: + reply_pid
    fx[ch] = [(pid, ts, tp(ch, txt, ts, rp)) for pid, ts, txt, rp, _ in posts]
    assert all(p is not None for _, _, p in fx[ch]), ch
    n_reply = sum(1 for _, _, _, rq, _ in posts if rq)
    assert n_reply >= 7, (ch, n_reply)                  # відповіді з цитатою є
    # цитата і reply_pid ідуть разом: відповідь -> є id поста, на який відповіли
    assert all((rq != "") == (rpid > 0) for _, _, _, rq, rpid in posts), ch
    assert all(rpid < pid for pid, _, _, _, rpid in posts if rpid), ch
hl = [p for _, _, p in fx["HL_TWAP"]]
assert sum(p["kind"] == "start" for p in hl) == 2
assert sum(p["kind"] == "done" for p in hl) == 2
assert sum(p["kind"] == "cancel" for p in hl) == 16
s1714 = [p for pid, _, p in fx["HL_TWAP"] if pid == 1714][0]
assert s1714["side"] == "sell" and s1714["coin"] == "LIT" and s1714["usd"] == 2.85e6
assert s1714["dur"] == 4 * 3600 and s1714["exact"] and s1714["addr"].startswith("0xe0ffc829")
assert time.gmtime(s1714["start"])[3:6] == (12, 20, 45)     # 15:20:45 UTC+3
# «closed by user» (відповідь на стартовий пост): текст без адреси,
# бік і адреса — з цитати; exact=False (старту тут немає)
c1695 = [p for pid, _, p in fx["HL_TWAP"] if pid == 1695][0]
assert c1695["kind"] == "cancel" and not c1695["exact"] and c1695["addr"]
assert c1695["coin"] == "HYPE" and c1695["side"] == "buy" and c1695["usd"] == 5.10e6
assert c1695["addr"].startswith("0x186db447")
xs = [p for _, _, p in fx["TWAPx"]]
assert sum(p["kind"] == "start" for p in xs) == 10
assert sum(p["kind"] == "done" for p in xs) == 6         # «✅ TWAP завершён»
assert sum(p["kind"] == "cancel" for p in xs) == 4       # «❌ TWAP отменён»
short = [p for p in xs if p["kind"] == "start" and p["dur"] <= 900]
assert len(short) == 3 and {p["dur"] for p in short} == {300.0, 600.0}   # 5 і 10 хв
x44673 = [p for pid, _, p in fx["TWAPx"] if pid == 44673][0]
x44674 = [p for pid, _, p in fx["TWAPx"] if pid == 44674][0]     # відповідь: скасування 44673
assert x44674["kind"] == "cancel" and x44674["addr"] == x44673["addr"]
assert x44674["coin"] == "HYPE" and x44674["side"] == "sell"       # бік — із цитати
assert time.gmtime(x44673["start"])[3:6] == (13, 25, 49) and x44673["usd"] == 165_570.0
x44671 = [p for pid, _, p in fx["TWAPx"] if pid == 44671][0]     # 4.5 часа
assert x44671["dur"] == 4.5 * 3600
x44681 = [p for pid, _, p in fx["TWAPx"] if pid == 44681][0]
assert x44681["kind"] == "done" and x44681["coin"] == "HYPE" and x44681["addr"].startswith("0xb079")
# монети: як у Hyperliquid (kPEPE), не KPEPE — канонізація по поллеру цін
TW["px_hist"].update({"kPEPE": [], "HYPE": []})
assert TW["_coin_canon"]("KPEPE") == "kPEPE" and TW["_coin_canon"]("hype") == "HYPE"
assert TW["_coin_canon"]("ZZZ") == "ZZZ" and TW["_coin_canon"](None) is None
assert tp("TWAPx", "🟩 $100K покупка kPEPE в течение 5 минут Цена: $0.01 Субъект: 0x"
          + "a" * 40 + " Создан в: 10:00:00 (UTC)", 0)["coin"] == "kPEPE"   # «течение» теж
assert ".upper()" not in src[src.index("def twap_parse"):src.index("def _iso_ts")]
# приклади з ТЗ
assert tp("HL_TWAP", "⛔️ $2.84m TWAP with INJ closed by user 19.34%\nFilled: 1/2 INJ\nTime: 07 Sep 2026 17:35:03 GMT", 0)["coin"] == "INJ"
xc = tp("TWAPx", "❌ TWAP отменён (частично)\nСтатус: terminated\nИсполнено: 9.52%\nРазмер: 190.47 / 2000.00 HYPE\nTwapId: 2197027\nСубъект: 0xb079d72d18c4b706f318b8fb6b85354929d0e9dd\n\nЦена в начале:  $82.78\nЦена в конце: $82.81 🔴 (+0.03%)", 0)
assert xc["kind"] == "cancel" and xc["coin"] == "HYPE" and xc["addr"].startswith("0xb079")
assert tp("TWAPx", "якийсь інший пост без твапу", 0) is None
print("5а) парсер: 40 живих постів обох каналів + приклади з ТЗ")
# (б) реєстр: два пости одного твапу (різні канали) -> один запис; 2–15 хв
# -> eligible; поля звірки з біржею є (v2.12)
now0 = time.time()
reg = TW["twap_reg"]; reg.clear(); ex_reset()
TW["twap_stats"].update({k: 0 for k in TW["twap_stats"]})
pS = dict(x44673, start=now0 + 30, end=now0 + 630)   # стартує за 30с, 10 хв
r1, new1 = TW["_twap_register"]("TWAPx", 1, pS, now0)
r2, new2 = TW["_twap_register"]("HL_TWAP", 2, dict(pS, start=pS["start"] + 3), now0)
assert new1 and not new2 and r1 is r2 and r1["src"] == "TWAPx+HL_TWAP"
assert r1["eligible"] == 1 and r1["state"] == "watch" and r1["kind"] is None
assert r1["twap_id"] is None and r1["kind_src"] == "" and r1["confirm_done"] == 0
pL = dict(x44671, start=now0, end=now0 + 4.5 * 3600)
rL, _ = TW["_twap_register"]("TWAPx", 3, pL, now0)
assert rL["eligible"] == 0 and rL["state"] == "ineligible" and rL["reason"] == "dur>15m"
rM, _ = TW["_twap_register"]("TWAPx", 31, dict(pS, start=now0 + 5000, end=now0 + 5060,
                                                dur=60.0), now0)
assert rM["state"] == "ineligible" and rM["reason"] == "dur<2m"   # v2.12: <2 хв
# ТОЙ ЖЕ канал, новий пост з тими ж addr/coin/side/start±2хв — НОВИЙ твап
rX, newX = TW["_twap_register"]("TWAPx", 99, dict(pS, usd=pS["usd"] * 1.01), now0)
assert newX and rX is not r1 and rX["id"] != r1["id"] and rX["id"].endswith("-99")
rX["state"] = "dropped"
# (в) вид — зі startPosition ПЕРШОГО слайсу на біржі (v2.12): продаж при
# лонгу ≥ розміру -> reduce; при 0 -> open; при лонгу < розміру -> flip;
# при шорті -> increase; без історії — вид невідомий
A = x44673["addr"]
ex_reset(); ex_twap(A, "HYPE", "sell", pS["start"], 10, sz=2000, twap_id=777, sp=3000.0, px=80.0)
assert TW["_twap_verify"](r1, now0 + 40)
assert r1["kind"] == "reduce" and r1["kind_src"] == "slice" and r1["twap_id"] == 777
assert r1["exch"] == "activated" and r1["sp"] == 3000.0 and abs(r1["pos_usd"] - 240_000) < 1e-6
for sp, want in ((0.0, "open"), (500.0, "flip"), (-3000.0, "increase")):
    ex_reset(); ex_twap(A, "HYPE", "sell", pS["start"], 10, sz=2000, twap_id=777, sp=sp)
    r1["kind_src"] = ""
    TW["_twap_verify"](r1, now0 + 40)
    assert r1["kind"] == want, (sp, r1["kind"])
ex_reset(); r1.update(kind=None, kind_src="", twap_id=None)
assert not TW["_twap_verify"](r1, now0 + 40) and r1["kind"] is None
assert TW["_twap_resolve_kind"](r1) is None
# історія вже є, слайсу ще нема -> twapId відомий, вид — ні
ex_twap(A, "HYPE", "sell", pS["start"], 10, 2000, 777, sp=0.0); EX["slices"][A] = []
TW["_twap_api_cache"].clear()
assert TW["_twap_verify"](r1, now0 + 40) and r1["twap_id"] == 777 and r1["kind_src"] == ""
# (г) стан-машина: до останньої хвилини — чекаємо; в останню хвилину при
# русі у бік твапу ≥1% — вхід ПРОТИ (продаж -> наш LONG), трекер 120 хв у
# rev_open; без свічок — ціни поллера (p0_src=hist, p1_src=live)
ex_reset(); ex_twap(A, "HYPE", "sell", pS["start"], 10, 2000, 777, sp=0.0)
TW["_px_at"] = lambda c, ts, tol=90.0: 100.0        # p0 = 100
TW["_px_now"] = lambda c, max_age=20: 98.5           # ціна впала на 1.5%
TW["_twap_tick"](now0 + 100)
assert r1["state"] == "watch" and r1["kind"] == "open"   # звірено, ще не остання хвилина
TW["_twap_tick"](r1["end"] - 50)
assert r1["state"] == "entered" and r1["strategy"].startswith("T1_твап_відкриття"), r1
assert r1["strategy"] == "T1_твап_відкриття+T1_твап_відкриття_15"   # v2.13: когорти ≥1 і ≥1.5 (рух 1.5%)
assert abs(r1["move"] - 1.5) < 1e-6 and r1["p0"] == 100.0 and r1["p1"] == 98.5
assert r1["p0_src"] == "hist" and r1["p1_src"] == "live"
tr = TW["rev_open"][f"{r1['id']}|T1_твап_відкриття"]
assert tr["side"] == "LONG" and tr["track_min"] == 120 and tr["row_kind"] == "twap"
assert tr["twap"]["kind"] == "open" and tr["twap"]["kind_src"] == "slice" and tr["twap"]["twap_id"] == 777
assert tr["entry_px"] == 98.5 and tr["twap"]["completed"] == 0
# після кінця біржа каже finished + виконано повністю -> completed=1 у записі й трекері
ex_reset(); ex_twap(A, "HYPE", "sell", pS["start"], 10, 2000, 777, status="finished",
                    executed=2000, sp=0.0)
TW["_twap_tick"](r1["end"] + 40)
assert r1["completed"] == 1 and r1["confirm_done"] == 1 and tr["twap"]["completed"] == 1
# ціни зі свічок біржі (v2.12): p0 = закриття свічки перед стартом, p1 —
# перед останньою хвилиною; продаж + падіння 3% -> вхід LONG за ЖИВОЮ ціною
B = "0x" + "b" * 40
pS2 = dict(pS, coin="ZEC", start=now0 + 40, end=now0 + 640, addr=B)   # інша монета: HYPE зайнята T1
r3, _ = TW["_twap_register"]("TWAPx", 4, pS2, now0)
ex_reset(); ex_twap(B, "ZEC", "sell", pS2["start"], 10, 2000, 778, sp=0.0)
bar0 = (int(pS2["start"]) // 60 - 1) * 60 * 1000
bar1 = (int(pS2["end"] - 60) // 60 - 1) * 60 * 1000
EX["candles"]["ZEC"] = {bar0: 100.0, bar1: 97.0}
TW["_px_now"] = lambda c, max_age=20: 99.5           # поллер каже -0.5% — не він вирішує
TW["_twap_tick"](r3["end"] - 50)
assert r3["state"] == "entered" and r3["p0_src"] == "candle" and r3["p1_src"] == "candle", r3
assert r3["p0"] == 100.0 and r3["p1"] == 97.0 and abs(r3["move"] - 3.0) < 1e-6
assert TW["rev_open"][f"{r3['id']}|T1_твап_відкриття"]["entry_px"] == 99.5
# рух замалий -> пропуск
C_ = "0x" + "c" * 40
pS3 = dict(pS, start=now0 + 50, end=now0 + 650, addr=C_)
r3b, _ = TW["_twap_register"]("TWAPx", 5, pS3, now0)
ex_reset(); ex_twap(C_, "HYPE", "sell", pS3["start"], 10, 2000, 779, sp=0.0)
TW["_twap_tick"](r3b["end"] - 50)
assert r3b["state"] == "dropped" and r3b["reason"] == "move_small"
# купівля при ШОРТІ ≥ розміру -> reduce -> T2, наш SHORT при рості 2%
D = "0x" + "d" * 40
pS4 = dict(pS, side="buy", start=now0 + 60, end=now0 + 660, addr=D)
r4, _ = TW["_twap_register"]("TWAPx", 6, pS4, now0)
ex_reset(); ex_twap(D, "HYPE", "buy", pS4["start"], 10, 1000, 780, sp=-5000.0)
TW["_px_now"] = lambda c, max_age=20: 102.0
TW["_twap_tick"](r4["end"] - 45)
assert r4["state"] == "entered" and r4["strategy"].startswith("T2_твап_скорочення") and r4["kind"] == "reduce"
assert f"{r4['id']}|T2_твап_скорочення_20" in TW["rev_open"]            # рух 2% -> і когорта ≥2%
assert TW["rev_open"][f"{r4['id']}|T2_твап_скорочення"]["side"] == "SHORT"
# долив (той самий бік) -> не сигнал, ще до останньої хвилини
E = "0x" + "e" * 40
pS5 = dict(pS, side="buy", start=now0 + 70, end=now0 + 670, addr=E)
r5i, _ = TW["_twap_register"]("TWAPx", 7, pS5, now0)
ex_reset(); ex_twap(E, "HYPE", "buy", pS5["start"], 10, 1000, 781, sp=4000.0)
TW["_twap_tick"](now0 + 80)
assert r5i["state"] == "dropped" and r5i["reason"] == "not_new_or_reduce" and r5i["kind"] == "increase"
# спізнились більш як на 90с після кінця -> late
F_ = "0x" + "f" * 40
pS6 = dict(pS, start=now0 - 700, end=now0 - 100, addr=F_)
r5, _ = TW["_twap_register"]("TWAPx", 8, pS6, now0 - 700)
ex_reset()
TW["_twap_tick"](now0)
assert r5["state"] == "dropped" and r5["reason"] == "late"
# без слайсу до кінця -> kind_unknown (вид ЛИШЕ зі звірки з біржею)
G = "0x" + "9" * 40
pS7 = dict(pS, start=now0 + 80, end=now0 + 680, addr=G)
r7, _ = TW["_twap_register"]("TWAPx", 9, pS7, now0)
ex_reset(); TW["_px_now"] = lambda c, max_age=20: 98.0
TW["_twap_tick"](r7["end"] - 50)
assert r7["state"] == "watch"                         # ще чекаємо слайс
TW["_twap_tick"](r7["end"] - 10)
assert r7["state"] == "dropped" and r7["reason"] == "kind_unknown"
# скасування біржею (terminated) до входу -> dropped cancelled
H = "0x" + "8" * 40
pS8 = dict(pS, start=now0 + 90, end=now0 + 690, addr=H)
r8, _ = TW["_twap_register"]("TWAPx", 10, pS8, now0)
ex_reset(); ex_twap(H, "HYPE", "sell", pS8["start"], 10, 2000, 782, status="terminated", sp=0.0)
TW["_twap_tick"](now0 + 100)
assert r8["state"] == "dropped" and r8["reason"] == "cancelled" and r8["cancelled"] == 1
# дедуп по twapId: другий запис того ж твапу (старт розійшовся на 150с —
# поза ±2 хв реєстру) -> dup_twapid, пости й канал зливаються у старший
I_ = "0x" + "7" * 40
pS9 = dict(pS, start=now0 + 100, end=now0 + 700, addr=I_)
r9, _ = TW["_twap_register"]("TWAPx", 11, pS9, now0)
r9b, _ = TW["_twap_register"]("HL_TWAP", 12, dict(pS9, start=pS9["start"] + 150,
                                                   end=pS9["end"] + 150), now0 + 1)
assert r9 is not r9b
ex_reset(); ex_twap(I_, "HYPE", "sell", pS9["start"], 10, 2000, 783, sp=0.0)
ex_twap(I_, "HYPE", "sell", pS9["start"] + 150, 10, 2000, 783, sp=-1.0)   # той самий 783; другий філ продовжує ланцюг позицій
TW["_twap_tick"](now0 + 260)
assert r9["state"] == "watch" and r9b["state"] == "dropped" and r9b["reason"] == "dup_twapid"
assert "HL_TWAP/12" in r9["posts"] and "HL_TWAP" in r9["src"]
# (д) cancel: TWAPx-скасування по адресі+монеті гасить активний твап;
# після входу — лише позначка cancel_after_entry; reply_pid зіставляє ТОЧНО
J = "0x" + "6" * 40
pS10 = dict(pS, start=now0 + 120, end=now0 + 720, addr=J)
r6, _ = TW["_twap_register"]("TWAPx", 13, pS10, now0)
TW["_twap_ingest"]("TWAPx", 14, now0 + 130,
                   "❌ TWAP отменён (частично)\nСтатус: terminated\nРазмер: 1 / 2 HYPE\n"
                   "Субъект: " + J, now0 + 130)
assert r6["state"] == "dropped" and r6["reason"] == "cancelled" and r6["cancelled"] == 1
TW["_twap_ingest"]("TWAPx", 15, now0 + 700,
                   "❌ TWAP отменён (частично)\nРазмер: 1 / 2 HYPE\nСубъект: " + A,
                   now0 + 700)
assert r1["cancel_after_entry"] == 1 and tr["twap"]["cancel_after_entry"] == 1
# reply_pid (v2.12): скасування-відповідь на пост старту гасить САМЕ той
# запис, навіть коли сума в тексті інша (евристика по сумі не потрібна)
K = "0x" + "5" * 40
_hl = ("$9.00M selling HYPE 🟥 Frequency: $1 every 60 seconds (10 cycles) ETA: 9m "
       "Price: $85 User: " + K + " Period: "
       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(now0 + 200)) + " - "
       + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(now0 + 800)) + " UTC+0")
TW["_twap_ingest"]("HL_TWAP", 5001, now0, _hl, now0)
rK = [r for r in reg.values() if r["addr"] == K][0]
assert rK["state"] == "watch"
TW["_twap_ingest"]("HL_TWAP", 5002, now0 + 10,
                   "⛔️ $1.00m TWAP with HYPE closed by user 0.00%\nFilled: 0/1 HYPE\nTime: x",
                   now0 + 10, "", reply_pid=5001)
assert rK["state"] == "dropped" and rK["reason"] == "cancelled"
assert TW["_twap_by_post"]("HL_TWAP", 5001) is rK and TW["_twap_by_post"]("HL_TWAP", 1) is None
# HL «successful completed» -> completed=1 у запису (по тексту, без біржі)
r1["completed"] = 0
TW["_twap_ingest"]("HL_TWAP", 16, now0 + 640,
                   "✅ TWAP is successful completed [1/1] $165.57K selling HYPE 🟥 "
                   "User: " + A + " Period: "
                   + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(pS["start"]))
                   + " - " + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(pS["end"]))
                   + " UTC+0", now0 + 640)
assert r1["completed"] == 1
# (е) ingest старту: старіший за годину / вже завершений — ігнор;
# сплющений HL-репост без Period — ігнор
n_before = len(reg)
TW["_twap_ingest"]("TWAPx", 17, now0 - 5000,
                   "🟥 $130.01K продажа JUP в течении 5 минут Цена: $0.24 "
                   "Субъект: 0x" + "f" * 40 + " Создан в: 00:00:00 (UTC)", now0)
assert len(reg) == n_before
TW["_twap_ingest"]("HL_TWAP", 18, now0,
                   "$2.85M selling LIT 🟥 Frequency: $1 every 60 seconds (10 cycles) "
                   "ETA: 9m Price: $4.6 User: 0x" + "a" * 40 + " Period: 08…", now0)
assert len(reg) == n_before                     # exact=False
# (є) рядки CSV — рівно під заголовки (eol додає письменник)
sig_h = TW["TWAP_SIG_HEADERS"]
rows = []
TW["_strat_csv_append"] = lambda p, h, r: rows.append((p, r)) or True
r1["sig_written"] = 0
TW["_twap_sig_write"](r1, "entered")
assert len(rows[-1][1]) == len(sig_h) - 1
assert rows[-1][1][sig_h.index("exch_id")] == 777 and rows[-1][1][sig_h.index("kind_src")] == "slice"
tw_h = TW["TWAP_HEADERS"]
assert len(tw_h) == 53 + 60 + 1   # v2.19: +5 виконання; v2.20: +5 (мітки часу, px_filled, trig_hash)                      # v2.15: рядок угоди = 36 полів + m1..m60; крива — окремий файл
tr["samples"] = [0.5] * 120; tr["peak"] = 1.0; tr["trough"] = -0.2
row = TW["_twap_row"](tr)
assert len(row) == len(tw_h) - 1
assert row[1] == "T1_твап_відкриття" and row[4] == "LONG" and row[5] == "sell"
assert row[tw_h.index("exch_id")] == 777 and row[tw_h.index("completed")] == 1
assert row[tw_h.index("kind_src")] == "slice" and row[tw_h.index("p1_src")] == "live"
assert row[tw_h.index("exit_reason")] == "cancelled" and row[tw_h.index("exit_min")] == 1   # скасовано після входу -> вихід m1
assert abs(row[21] - (0.5 - row[22])) < 1e-9     # net60 = m60 - costs (v2.12 індекси)
# (ж) трекер-цикл: горизонт із track_min, row_kind twap -> TWAP_CSV
assert '_tm = int(p.get("track_min") or REV_TRACK_MIN)' in src
assert 'if len(p["samples"]) >= _tm:' in src
assert 'elif kind == "twap":\n            ok = _strat_csv_append(TWAP_CSV, TWAP_HEADERS, r)' in src   # тіло тику у _strat2_tick (рев'ю аудит-2)
# (з) _px_at: хвилинна історія + 5с-історія, толеранс 90с
TWp = load({"_px_at"}, {"px_lock": threading.Lock(),
                        "px_min": {"AAA": [(1000.0, 10.0), (1060.0, 11.0)]},
                        "px_hist": {"AAA": [(1100.0, 12.0)]}})
assert TWp["_px_at"]("AAA", 1005.0) == 10.0 and TWp["_px_at"]("AAA", 1095.0) == 11.0   # v2.16: лише семпли ДО ts (без підглядання)
assert TWp["_px_at"]("AAA", 2000.0) is None and TWp["_px_at"]("ZZZ", 1000.0) is None
# (и) state: реєстр і last_ids персистяться; прюнінг старих записів
assert 'snap["twap_reg"]' in src and 'snap.get("twap_reg", {})' in src
assert "twap_last_ids" in src[src.index("def _prune_leaks"):src.index("def run_state_saver")] or \
       "twap_reg.pop(k, None)" in src[src.index("def _prune_leaks"):src.index("def run_state_saver")]
assert "threading.Thread(target=run_twap_watcher," in src
print("5б) реєстр/стан-машина/вхід проти твапу/скасування/CSV/персист")

# ═══ 6. Мобільна версія ═══════════════════════════════════════════
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
assert "@media (max-width:900px)" in ui and "@media (max-width:480px)" in ui
assert ".scards{grid-template-columns:1fr}" in ui
assert '<nav class="mobile-nav"' in ui and 'body[data-pane="positions"]' in ui   # v2.12: панелі замість стрічки монет
assert "style.display=v==='term'?'':'none'" in ui        # display вирішує CSS
assert "curveTouch" in ui and "ontouchstart" in ui
for k in ("F8_ratio35", "F9_без_ратіо_90", "T1_твап_відкриття", "T2_твап_скорочення",
          "setCoh", "per_day", "svTwap", "maxM"):
    assert k in ui, k
print("6) мобільна версія: медіа-запити, одна колонка, тач на кривій; UI нових стратегій")

# ═══ 7. Проксі скану ══════════════════════════════════════════════
np_ = load({"_norm_proxy"})["_norm_proxy"]
assert np_("1.2.3.4:5555:user:pw") == "user:pw@1.2.3.4:5555"
S = load({"_scan_via_proxy", "_scan_budget_wait", "hl_post_scan"},
         {"SCAN_W_PER_MIN": 6, "SCAN_DEAD_S": 1800, "SCAN_PROXY": "u:p@h:1",
          "_scan_opener": None,
          "_scan_state": {"dead_until": 0.0, "streak": 0, "req": 0, "err": 0,
                          "rl": 0, "fallback": 0, "last_ok": 0.0},
          "_scan_w": __import__("collections").deque(), "_scan_w_lock": threading.Lock(),
          "urllib": __import__("urllib.request"), "RateLimited": NEW["RateLimited"],
          "APIError": NEW["APIError"]})
assert not S["_scan_via_proxy"]()                     # opener None -> напряму
# бюджет: 3 запити по 2 = 6 проходять миттєво, 4-й чекає (перевіряємо
# через підміну sleep, щоб не спати насправді)
slept = []
S["time"] = type("T", (), {"time": staticmethod(time.time),
                           "sleep": staticmethod(lambda s: slept.append(s) or (_ for _ in ()).throw(RuntimeError("wait")))})()
for _ in range(3): S["_scan_budget_wait"](2)
try:
    S["_scan_budget_wait"](2); raise SystemExit("FAIL: 4-й запит мав чекати")
except RuntimeError:
    pass
assert slept and 0 < slept[0] <= 2.0
# мертва проксі: 3 мережеві збої поспіль -> dead_until, фолбек напряму
class _Op:
    def open(self, req, timeout=0): raise OSError("conn reset")
S["_scan_opener"] = _Op(); S["time"] = time
assert S["_scan_via_proxy"]()
for i in range(3):
    try: S["hl_post_scan"]({"type": "x"}, retries=1)
    except NEW["APIError"]: pass
assert S["_scan_state"]["dead_until"] > time.time() and not S["_scan_via_proxy"]()
assert S["_scan_state"]["fallback"] == 1 and S["_scan_state"]["err"] == 3
# уже мертва: воркери «в польоті» падають з "dead", streak/fallback НЕ ростуть
for i in range(5):
    try: S["hl_post_scan"]({"type": "x"}, retries=1)
    except NEW["APIError"] as e: assert "dead" in str(e)
assert S["_scan_state"]["fallback"] == 1 and S["_scan_state"]["streak"] == 0
# свіжий успіх (<20с) + 3 збої пачкою (паралельні воркери) — НЕ смерть
S["_scan_state"].update(dead_until=0.0, streak=0, fallback=0)
class _OpOK:
    def open(self, req, timeout=0):
        import io
        class R(io.BytesIO):
            def __enter__(s): return s
            def __exit__(s, *a): pass
        return R(b'{"ok":1}')
S["_scan_opener"] = _OpOK()
assert S["hl_post_scan"]({"type": "x"}, retries=1) == {"ok": 1}
assert time.time() - S["_scan_state"]["last_ok"] < 2
S["_scan_opener"] = _Op()
for i in range(4):
    try: S["hl_post_scan"]({"type": "x"}, retries=1)
    except NEW["APIError"]: pass
assert S["_scan_state"]["fallback"] == 0 and S["_scan_via_proxy"]()
S["_scan_state"]["last_ok"] = time.time() - 30          # успіх давній -> смерть
try: S["hl_post_scan"]({"type": "x"}, retries=1)
except NEW["APIError"]: pass
assert S["_scan_state"]["fallback"] == 1 and not S["_scan_via_proxy"]()
# fetch_one: проксі-гілка без fast_hold і БЕЗ пейсера (він у process()
# ДО знімка часу _t_fetch — інакше курсор філів випереджав запит на
# хвилину); /status має поля
_fo = src[src.index("def fetch_one(addr_str)"):src.index("def should_skip")]
assert "if _scan_via_proxy():" in _fo and "_scan_budget_wait" not in _fo
_pr = src[src.index("        def process(w):"):src.index("            positions = fetch_one(w[\"addr\"])")]
assert "if _scan_via_proxy():\n                _scan_budget_wait(2)" in _pr
assert _pr.index("_scan_budget_wait(2)") < _pr.index("_t_fetch = time.time()")
assert '"scan_proxy_alive": _scan_via_proxy(),' in src
assert "scan_proxy.txt" in open((_HL + "/.gitignore")).read()
print("7) проксі скану: формат, бюджет ваги/хв, мертва проксі -> фолбек, без fast_hold, "
      "смерть лише без успіху >20с, пейсер до знімка часу")

# ═══ 8. Рев'ю v2.11 (Workflow: 5 лінз + 5 скептиків) ══════════════
# (а) грейс у повному скані: пара з ratio<2 у знімку, що була ≥2 5 хв
# тому (мітка в живому watchlist) — ЛИШАЄТЬСЯ; 50 хв тому — ні; мітка
# для ratio≥2 = час читання гаманця (fetch_times), не старт скану;
# злиття бере свіжішу мітку з двох джерел
_wl = {"0xa": {"AAA": {"size": 10.0, "val": 1e5, "side": "LONG", "ratio": 2.5,
                       "ratio_hi_ts": time.time() - 300, "entry": 1.0},
               "BBB": {"size": 10.0, "val": 1e5, "side": "LONG", "ratio": 2.5,
                       "ratio_hi_ts": time.time() - 3000, "entry": 1.0},
               "CCC": {"size": 10.0, "val": 3e5, "side": "LONG", "ratio": 3.0,
                       "ratio_hi_ts": time.time() - 2000, "upd": 0, "entry": 1.0},
               "EEE": {"size": 10.0, "val": 3e5, "side": "LONG", "ratio": 3.0,
                       "ratio_hi_ts": time.time() - 10, "upd": 0, "entry": 1.0}}}
uw = load({"update_watchlist"}, dict(C, RATIO_GRACE_S=1800, COIN_BLACKLIST=set(),
    watchlist_lock=threading.Lock(), watchlist=_wl, scan_tombstones={},
    sent_alerts=set(), close_episodes={}, delta_seen={}, fill_cursor={},
    depth_for_side=lambda d, s: (d or {}).get("bid", 0)))
_scan0 = time.time() - 1200            # скан стартував 20 хв тому
_ft = {"0xa": time.time() - 100}       # гаманець зчитано 100с тому
res8 = {"AAA": [{"addr": "0xa", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}],
        "BBB": [{"addr": "0xa", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}],
        "CCC": [{"addr": "0xa", "size": 9.0, "val": 2.5e5, "side": "LONG", "entry": 1.0}],
        "DDD": [{"addr": "0xa", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}],
        "EEE": [{"addr": "0xa", "size": 9.0, "val": 2.5e5, "side": "LONG", "entry": 1.0}]}
uw["update_watchlist"](res8, {c: {"bid": 1e5} for c in list("ABCDEFGHIJKL")}
                       | {c: {"bid": 1e5} for c in ("AAA", "BBB", "CCC", "DDD", "EEE")},
                       _scan0, set(), _ft)
w8 = uw["watchlist"]["0xa"]
assert "AAA" in w8 and w8["AAA"]["ratio"] == 1.5                 # грейс (5 хв)
assert abs(w8["AAA"]["ratio_hi_ts"] - (time.time() - 300)) < 2  # мітка успадкована
assert "BBB" not in w8 and "DDD" not in w8                       # 50 хв / ніколи ≥2
assert abs(w8["CCC"]["ratio_hi_ts"] - _ft["0xa"]) < 2            # = час читання, не старт скану
assert abs(w8["EEE"]["ratio_hi_ts"] - (time.time() - 10)) < 2    # злиття: свіжіша з двох
# (б) скан-діф бере мітку і з живого watchlist (sweep бачив ≥2 5 хв
# тому, а попередній знімок скану — ні)
cpc2 = load({"check_position_changes", "_ratio_ok"}, dict(C,
    RATIO_GRACE_S=1800, COIN_BLACKLIST=set(), tracking_lock=threading.Lock(),
    prev_positions={"0xw": {"AAA": {"size": 10.0, "val": 1e5, "side": "LONG",
                                    "ratio": 1.2, "ratio_hi_ts": 0, "entry": 1.0}}},
    fill_cursor={}, get_recent_market_fills=lambda *a, **k: ([], []),
    depth_for_side=lambda d, s: (d or {}).get("bid", 0),
    alerted_txs={}, sent_alerts=set(), close_episodes={}, scan_tombstones={},
    delta_seen={}, watchlist_lock=threading.Lock(),
    watchlist={"0xw": {"AAA": {"ratio": 1.5, "ratio_hi_ts": time.time() - 300}}},
    stats={"delta_events": 0, "fills_confirmed": 0, "fills_empty": 0}))
cpc2["check_position_changes"](
    {"AAA": [{"addr": "0xw", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}]},
    {**{f"D{i}": {"bid": 1e5} for i in range(10)}, "AAA": {"bid": 1e5}})
assert "AAA" in cpc2["prev_positions"]["0xw"]
assert abs(cpc2["prev_positions"]["0xw"]["AAA"]["ratio_hi_ts"] - (time.time() - 300)) < 2
print("8а) грейс: скан не обнуляє мітку (fetch_times / живий watchlist / злиття)")
# (в) версії: карантин legacy — окремо; той самий ключ у legacy і в
# поточному файлі — виграє ПЕРШИЙ записаний (legacy: результат завершеної
# угоди незмінний, v2.14); рядки нижче мінімальної since-версії не
# тримаються в пам'яті, але рахуються; видалений legacy — геть із кешу
d3 = tempfile.mkdtemp()
open(os.path.join(d3, "follow_trades.csv"), "w").write(
    ",".join(fh) + "\n" + frow("F1_1хв", 1.0, "2.11", "t1") + "\n"
    + frow("F1_1хв", 7.0, "2.11", "tX") + "\n")            # tX і в legacy
lp3 = os.path.join(d3, "follow_trades.csv.legacy-1725000000.csv")
open(lp3, "w").write(
    ",".join(fh_old) + "\n" + frow_old("F1_1хв", 3.0, "2.10", "t3") + "\n"
    + frow_old("F1_1хв", 5.0, "2.9", "t5") + "\n"
    + frow_old("F1_1хв", 9.0, "2.10", "tX") + "\n"          # дубль ключа
    + "битий,рядок\n")                                       # карантин legacy
api3 = load({"strat2_api", "_median", "_vt", "_v_ok"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={},
    STRAT_SINCE=REAL_SINCE, TAPE_SINCE=REAL_TAPE,
    _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
    FOLLOW_OUT_CSV=os.path.join(d3, "fo.csv"), REV_CSV=os.path.join(d3, "rev.csv"),
    REV_SIG_CSV=os.path.join(d3, "sig.csv"), FOLLOW_CSV=os.path.join(d3, "follow_trades.csv"),
    REV_OUT_CSV=os.path.join(d3, "out.csv"), TWAP_CSV=os.path.join(d3, "tw.csv"),
    TWAP_SIG_CSV=os.path.join(d3, "tws.csv"), F5_NAME="F5_перший",
    F6_NAME="F6_1хв_перший", F7_NAME="F7_без_ратіо"))
o3 = api3["strat2_api"]()
f13 = o3["strategies"]["F1_1хв"]
assert f13["n"] == 3 and sorted(t["net30"] for t in f13["trades"]) == [1.0, 3.0, 9.0], f13
assert o3["dup_rows"] == 1                                          # дубль tX порахований
assert o3["quarantined"] == 0 and o3["legacy_quarantined"] == 1   # битий рядок — у legacy
assert o3["legacy_rows"] == 1 and o3["legacy_files"] == 1          # t5@2.9
c3 = api3["_legacy_csv_cache"][lp3]
assert len(c3[1]) == 2 and c3[4] == 1          # t5 не в пам'яті, порахований
os.remove(lp3)
api3["_strat2_cache"]["ts"] = 0
o3b = api3["strat2_api"]()
assert lp3 not in api3["_legacy_csv_cache"] and o3b["legacy_files"] == 0
assert o3b["strategies"]["F1_1хв"]["n"] == 2
# rev per_day: 1 вхід із 3 сигналів за N днів = 1/N, а не 1/1
rh = ["sig_id", "strategy", "date", "coin", "our_side", "entered", "costs_pct",
      "m30", "algo_v", "eol"]
def rrow(sid, ent, date):
    r = [""] * len(rh); r[0] = sid; r[1] = "R1_загальний"; r[2] = date
    r[3] = "AAA"; r[4] = "LONG"; r[5] = str(ent); r[6] = "0.15"; r[7] = "1.0"
    r[8] = "2.17"; r[-1] = "^"; return ",".join(r)   # v2.17: R since 2.17
open(os.path.join(d3, "rev.csv"), "w").write(
    ",".join(rh) + "\n" + rrow("s1", 0, "2026-08-25 10:00:00") + "\n"
    + rrow("s2", 0, "2026-08-26 10:00:00") + "\n" + rrow("s3", 1, "2026-09-05 10:00:00") + "\n")
api3["_strat2_cache"]["ts"] = 0
r1s = api3["strat2_api"]()["strategies"]["R1_загальний"]
assert r1s["signals"] == 3 and r1s["entered"] == 1
assert abs(r1s["days"] - round(days, 1)) < 0.2 and abs(r1s["per_day"] - round(1 / days, 2)) < 0.02
print("8б) версії: legacy-карантин окремо, поточний виграє дедуп, min-since, кеш; rev per_day")
# (г) TWAP: HL «closed by user» (без адреси в тексті) гасить твап через
# цитату; TWAPx «завершён» -> completed; вже бачений пост (редагування
# старту в cancelled) — ЛИШЕ як cancel/done, повтор ідемпотентний;
# _twap_find по префіксу адреси і найближчій сумі
reg.clear(); TW["rev_open"].clear()
TW["twap_stats"].update({k: 0 for k in TW["twap_stats"]})
TW["px_hist"].update({"HYPE": [], "LIT": []})
nowA = time.time()
_hl_start = ("$5.10M buying HYPE 🟩 Frequency: $24,192 every 60 seconds (10 cycles) "
             "ETA: 9m Price: $85.07 User: 0x186db447f4de2f258ec100aa194b6e9974a46482 "
             "Period: " + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(nowA + 60))
             + " - " + time.strftime("%d %b %Y %H:%M:%S", time.gmtime(nowA + 660)) + " UTC+0")
TW["_twap_ingest"]("HL_TWAP", 2001, nowA, _hl_start, nowA)
rA = [r for r in reg.values() if r["coin"] == "HYPE"][0]
assert rA["state"] == "watch" and rA["addr"].startswith("0x186db447") and rA["side"] == "buy"
# сусідній твап того ж кита: $5.11M — окремий запис (той самий канал)
TW["_twap_ingest"]("HL_TWAP", 2002, nowA, _hl_start.replace("$5.10M", "$5.11M"), nowA)
assert sum(1 for r in reg.values() if r["coin"] == "HYPE" and r["state"] == "watch") == 2
# «closed by user» $5.11m — відповідь із цитатою (адреса обрізана)
TW["_twap_ingest"]("HL_TWAP", 2003, nowA + 10,
                   "⛔️ $5.11m TWAP with HYPE closed by user 0.00%\nFilled: 0.00/60000.0 HYPE\nTime: x",
                   nowA + 10,
                   "⛔️ TWAP is cancelled [0.00/60000.0] $5.11M buying HYPE 🟩 ETA: 9m "
                   "Price: $85.16 User: 0x186db447f4de2f258ec100a")
rB = [r for r in reg.values() if r["coin"] == "HYPE" and r["usd"] == 5.11e6][0]
assert rB["state"] == "dropped" and rB["reason"] == "cancelled" and rA["state"] == "watch"
assert TW["twap_stats"]["cancelled"] == 1
# вже бачений стартовий пост, відредагований у «TWAP is cancelled» — гасить
TW["_twap_ingest"]("HL_TWAP", 2001, nowA, "⛔️ TWAP is cancelled [0/1] " + _hl_start,
                   nowA + 20, seen=True)
assert rA["state"] == "dropped" and TW["twap_stats"]["cancelled"] == 2
TW["_twap_ingest"]("HL_TWAP", 2001, nowA, "⛔️ TWAP is cancelled [0/1] " + _hl_start,
                   nowA + 25, seen=True)
assert TW["twap_stats"]["cancelled"] == 2                       # ідемпотентно
# бачений пост як старт — ігнор (не воскрешає запис)
n_reg = len(reg)
TW["_twap_ingest"]("HL_TWAP", 2001, nowA, _hl_start, nowA + 30, seen=True)
assert len(reg) == n_reg and TW["twap_stats"]["posts"] == 3
# TWAPx «завершён» -> completed=1 (бік із цитати, адреса повна)
_x_start = ("🟩 $150.11K покупка NEAR в течении 5 минут Цена: $2.37 Субъект: 0x"
            + "9" * 40 + " Создан в: " + time.strftime("%H:%M:%S", time.gmtime(nowA)) + " (UTC)")
TW["_twap_ingest"]("TWAPx", 3001, nowA, _x_start, nowA)
rN = [r for r in reg.values() if r["coin"] == "NEAR"][0]
assert rN["state"] == "watch" and rN["dur"] == 300.0
TW["_twap_ingest"]("TWAPx", 3002, nowA + 300,
                   "✅ TWAP завершён\nСтатус: finished\nРазмер: 1 / 1 NEAR\nСубъект: 0x" + "9" * 40,
                   nowA + 300, _x_start)
assert rN["completed"] == 1 and rN["state"] == "watch"
assert TW["_twap_find"]("0x999", "NEAR") is rN                   # префікс
assert TW["_twap_find"]("0x998", "NEAR") is None
# сторінка без постів — збій, не тиша
assert 'raise APIError(f"no messages in page' in src
assert "with twap_lock:\n                _twap_active = sum(" in src
print("8в) TWAP: closed-by-user через цитату, edit-cancel бачених постів, завершён, префікс адреси")
# (д) UI (мобільна): без горизонтального скролу сторінки, підпис
# останньої хвилини, ширина графіка від контейнера, підказка ліворуч у
# правій половині, стани у стрічці монет, кнопки ≥36px, тач без preventDefault
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for k in ("grid-template-columns:minmax(0,1fr)", "text-anchor=\"${m===maxM?'end':'middle'}\"",
          "const L=34,R=16", "cw=Math.min(680,Math.max(320,", "tip.style.right=(box.width-px+12)",
          ".mobile-nav button{flex:1;min-height:44px", "#sortBy{min-height:44px",   # v2.12: панелі
          "touch-action:pan-y", 'ontouchcancel="hideTip()"',
          "старіших за since-версію"):
    assert k in ui, k
assert "ev.preventDefault()" not in ui[ui.index("function curveTouch"):ui.index("function hideTip")]
assert "перейменовано у *.legacy" not in ui
print("8г) UI: мобільні правки рев'ю")

print("\nALL v2.11 TESTS PASSED")

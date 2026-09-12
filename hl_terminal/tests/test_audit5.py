import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.5: хард-фільтри, пріоритетний фетч, тіньові стрічки,
фікси 5-го аудиту, спостережуваність."""
import math
import ast, threading, time, json, os, re, sys, tempfile, csv

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
          "math": math,
          "print": lambda *a, **k: None}
    ns.update(_V28)   # v2.8: нові константи/стаби
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

C = dict(PROFILE_ALGO_V=5, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=2, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.5",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS={"ZEC", "HYPE"},
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600,
         FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0),
         PRIO_K_DEPTH=0.10, PRIO_FLOOR_USD=15_000.0, PRIO_COOLDOWN_S=600,
         PRIO_MAX_PER_MIN=10, PRIO_DIRECT_PER_MIN=4,
         COIN_BLACKLIST={"BTC", "ETH", "XRP", "BNB"})

def _dt(ts): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# ── 1. Логи: мітка часу + flush у shadow-print ──────────
assert "_print_raw = print" in src
assert 'time.strftime("%d.%m %H:%M:%S")' in src
assert 'kwargs.setdefault("flush", True)' in src
n_silent = 0
for m in re.finditer(r"except Exception:\n(\s+)pass", src):
    n_silent += 1
# легітимні мовчазні: 4 опційні файли (ws_proxy, rest_proxy, tg_token,
# tg_chat) + best-effort читання тіла HTTP-помилки
assert n_silent <= 6, f"тихих except лишилось {n_silent} (очікуємо <=6 легітимних: 5 опційних файлів + best-effort body; v2.11 додав scan_proxy.txt)"
print("1) print з часом і flush; тихі except розковтані")

# ── 2. Хард-фільтри follow: кейс MET ────────────────────
def mk_fol(extra=None):
    ns = dict(C, STRAT2_ENABLED=True,
        strat2_lock=threading.RLock(), follow_last_close={}, follow_open={},
        rev_open={}, _px_now=lambda c: 10.0, _px_ago=lambda c, s: 10.0,
        is_vault=lambda a: False, _sim_depth=lambda c, s: 1e5,
        wallet_profiles={}, _profile_request=lambda a: None)
    ns.update(_V28)   # v2.8: нові константи/стаби
    ns.update(extra or {})
    return load({"follow_on_txs"}, ns)
f2 = mk_fol()
f2["follow_on_txs"]("0xMET", "MET",
    {"size": 100.0, "side": "LONG", "ratio": 0.44, "val": 17_311},
    [{"sz": 10.0, "px": 100.0, "hash": "0x1"}], False)   # поза $17k
assert f2["follow_open"] == {} and f2["rev_open"] == {}
f2["follow_on_txs"]("0xB", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 100.0, "hash": "0x2"}], False)   # tx $1k < $5k
assert f2["follow_open"] == {}
f2["follow_on_txs"]("0xB", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x3"}], False)  # tx $10k — ок
assert len(f2["follow_open"]) == 3   # F1-F3; F6 ні: $1k-tx вище вже
                                     # поставила мітку пари (не 1-й постріл)
print("2) MET-кейс закритий: позиція <$50k і tx <$5k не входять у стратегії")

# ── 3. Хард-фільтр rev + тіньова стрічка часткових ──────
def mk_rev():
    calls = []
    ns = load({"rev_on_close"}, dict(C, STRAT2_ENABLED=True,
        strat2_lock=threading.RLock(), rev_open={},
        fc_lock=threading.Lock(), fc_episodes={},
        is_vault=lambda a: False,
        _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
        _sim_depth=lambda c, s: 1e5,
        _dt=_dt, _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
        REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
        _strat_csv_append=lambda p, h, r: calls.append(r) or True))
    return ns, calls
r3, sig3 = mk_rev()
r3["rev_on_close"]("0xa", "PUMP", {"size": 100.0, "side": "LONG",
    "val": 30_000, "ratio": 3}, [{"sz": 100.0, "px": 100.0, "hash": "0xq"}], True)
assert r3["rev_open"] == {} and sig3 == []
print("3а) rev: позиція $30k — ні трекерів, ні сигнального рядка")
r3b, sig3b = mk_rev()
r3b["rev_on_close"]("0xa", "PUMP", {"size": 1000.0, "side": "LONG",
    "val": 100_000, "ratio": 3},
    [{"sz": 400.0, "px": 100.0, "hash": "0xw"}], False)   # 40% партіал
strats3 = {p["strategy"] for p in r3b["rev_open"].values()}
assert strats3 == {"_OUTCOME"}, strats3
outp = next(iter(r3b["rev_open"].values()))
assert outp["src"] == "partial" and outp["would_open"] == ""
assert len(sig3b) == 1   # сигнальний рядок для partial теж пишеться
print("3б) часткове закриття 40%: лише тіньова стрічка (src=partial)")

# ── 4. _norm_proxy: усі формати ─────────────────────────
np_ = load({"_norm_proxy"})["_norm_proxy"]
assert np_("1.2.3.4:5555:user:pw") == "user:pw@1.2.3.4:5555"
assert np_("user:pw@1.2.3.4:5555") == "user:pw@1.2.3.4:5555"
assert np_("1.2.3.4:5555") == "1.2.3.4:5555"
assert np_("") == ""
print("4) rest_proxy: host:port:user:pass нормалізується")

# ── 5. _prio_note: по-монетний поріг, кулдаун, стеля ────
from collections import deque
def mk_note(depth):
    q = deque()
    ns = load({"_prio_note"}, dict(C,
        _prio_lock=threading.Lock(), _prio_q=q,
        _prio_event=threading.Event(), _prio_seen={},
        _prio_minute=deque(), prio_stats={"triggers": 0, "dropped": 0},
        _sim_depth=lambda c, s: depth.get(c, 0), deque=deque))
    return ns, q
n5, q5 = mk_note({"PUMP": 100_000.0, "HYPE": 5_000_000.0})
n5["_prio_note"]("0xa", "PUMP", "A", {"px": 1.0, "sz": 9_000})    # $9k < $15k флор
assert not q5
n5["_prio_note"]("0xa", "PUMP", "A", {"px": 1.0, "sz": 20_000})   # $20k > max(15k, 10k)
assert len(q5) == 1
n5["_prio_note"]("0xa", "PUMP", "A", {"px": 1.0, "sz": 30_000})   # кулдаун адреси
assert len(q5) == 1
n5["_prio_note"]("0xb", "HYPE", "A", {"px": 1.0, "sz": 300_000})  # $300k < 10% від $5M
assert len(q5) == 1
n5["_prio_note"]("0xb", "HYPE", "A", {"px": 1.0, "sz": 600_000})  # $600k > $500k
assert len(q5) == 2
n5["_prio_note"]("0xc", "BTC", "A", {"px": 1.0, "sz": 9e9})       # блекліст
n5["_prio_note"]("0xd", "NOCOIN", "A", {"px": 1.0, "sz": 9e9})    # без глибини
assert len(q5) == 2
print("5) prio-поріг = max($15k, 10% глибини); кулдаун/блекліст/без-глибини")

# ── 6. run_prio_fetcher: ratio>=2 -> у watchlist, як у скана ──
prio_src = src[src.index("def run_prio_fetcher"):src.index("def get_recent_market_fills")]
one_pass = prio_src.replace("while True:", "for _pass in range(1):", 1)
q6 = deque([("0xw", "PUMP", "LONG", 50_000.0, 100_000.0, 15_000.0)])
logs6, wl6, fc6 = [], {}, {}
ns6 = dict(C, RateLimited=_V28["RateLimited"], REST_PROXY="user:pw@1.2.3.4:5", _prio_lock=threading.Lock(),
           strat2_lock=threading.RLock(), follow_last_close={},   # v2.8: мітка пари для F5

           _prio_q=q6, _prio_event=threading.Event(), _prio_direct=deque(),
           _prio_proxy_state={"streak": 0, "dead_since": 0.0},
           _prio_seen={},
           _hl_post_prio_direct=lambda b, retries=2: None,
           prio_stats={"triggers": 0, "added": 0, "dropped": 0, "errors": 0},
           check_one_wallet=lambda a, post=None: {
               "PUMP": {"size": 3000.0, "val": 300_000.0, "side": "LONG",
                        "entry": 95.0, "liq": 0},
               "NIL": {"size": 10.0, "val": 1_000.0, "side": "SHORT",
                       "entry": 1.0, "liq": 0}},
           hl_post_prio=lambda b, retries=2: None,
           _sim_depth=lambda c, s: 1e5, watchlist_lock=threading.Lock(),
           watchlist=wl6, sent_alerts=set(), close_episodes={},
           delta_seen={}, fill_cursor=fc6,
           _prio_log=lambda *a: logs6.append(a),
           threading=threading, time=time, print=lambda *a, **k: None)
ns6["_prio_event"].set()
exec(compile(one_pass, "prio", "exec"), ns6)
ns6["run_prio_fetcher"]()
assert "PUMP" in wl6.get("0xw", {}), wl6          # ratio 3.0 -> доданий
assert "NIL" not in wl6.get("0xw", {})            # ratio 0.01 -> ні
assert wl6["0xw"]["PUMP"]["ratio"] == 3.0
assert fc6.get("0xw:PUMP", 0) > 0                 # курсор ініціалізовано
assert logs6 and logs6[-1][6] == "added", logs6
assert ns6["prio_stats"]["added"] == 1
print("6) prio-фетч: невідомий кит з ratio>=2 у watchlist за один прохід")

# ── 7. Follow-стрічка: трекер створюється, без дублю, рядок у fo.csv ──
f7 = mk_fol()
f7["follow_on_txs"]("0xB", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x3"}], False)
fo = [p for p in f7["rev_open"].values() if p["strategy"] == "_FOLLOW_OUT"]
assert len(fo) == 1 and fo[0]["entry_px"] == 10.0 and fo[0]["side"] == "SHORT"
f7["follow_on_txs"]("0xB", "X",
    {"size": 90.0, "side": "LONG", "ratio": 3, "val": 9e5},
    [{"sz": 9.0, "px": 1000.0, "hash": "0x4"}], False)
assert sum(1 for p in f7["rev_open"].values()
           if p["strategy"] == "_FOLLOW_OUT") == 1   # fo_busy: без дублю
print("7а) follow-стрічка: один тіньовий трекер на активну пару")
# завершення 60 хв -> рядок у FOLLOW_OUT_CSV
loop_src = src[src.index("def run_strat2_loop"):src.index("# ── API")]
one_tick = loop_src.replace("while True:", "for _tick in range(1):") \
                   .replace("time.sleep(3)", "pass")
writes7 = []
ns7 = dict(C, STRAT2_ENABLED=True, strat2_lock=threading.RLock(),
           _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
           rev_open={}, follow_open={}, follow_last_close={},
           _px_now=lambda c, max_age=None: 10.0,
           _sim_slip=lambda d: 0.0005, _dt=_dt,
           _strat_csv_append=lambda p, h, r: writes7.append((p, len(h), r))
                                             or True,
           save_state=lambda: None,
           REV_CSV="r.csv", REV_OUT_CSV="o.csv", FOLLOW_CSV="f.csv",
           FOLLOW_OUT_CSV="fo.csv",
           REV_HEADERS=[], REV_OUT_HEADERS=[],
           FOLLOW_HEADERS=[], FOLLOW_OUT_HEADERS=list("h" * 78),
           threading=threading, time=time, print=lambda *a, **k: None)
for fn in ("_fol_out_row", "_rev_samples", "_rev_out_row", "_rev_row", "_p_costs", "_ms", "_rnd", "_rev_extra", "_fol_row"):
    seg = src[src.index(f"def {fn}("):]
    seg = seg[:seg.index("\ndef ")]
    exec(compile(seg, fn, "exec"), ns7)
ns7["_paper_px"] = _V28["_paper_px"]; ns7["math"] = math
tr7 = {"sig_id": "fo-1", "strategy": "_FOLLOW_OUT", "state": "open",
       "coin": "X", "side": "SHORT", "addr": "0xb",
       "detect_ts": time.time() - 4000, "detect_px": 10.0,
       "entry_ts": time.time() - 4000, "entry_px": 10.0,
       "tx_pct": 10.0, "tx_usd": 1e4, "ratio": 3.0, "pos_usd": 1e6,
       "vault": 0, "hour": 12, "btc_move": 0.01, "depth": 1e5,
       "algo_v": "2.5", "samples": [0.1] * 60, "peak": 0.5, "trough": -0.2,
       "done": 0}
ns7["rev_open"]["fo-1"] = tr7
exec(compile(one_tick, "loop", "exec"), ns7)
ns7.update(_tr_px_now=_V28["_tr_px_now"], _tr_exch=_V28["_tr_exch"], _mid_age_exch=_V28["_mid_age_exch"], _bn_px_now=_V28["_bn_px_now"], _bn_px_mid_age=_V28["_bn_px_mid_age"], bn_px_hist={}, bn_px_lock=threading.Lock(), stats=ns7.get("stats") or {})   # v2.20
ns7["run_strat2_loop"]()
fo_writes = [w for w in writes7 if w[0] == "fo.csv"]
assert len(fo_writes) == 1 and fo_writes[0][2][0] == "fo-1"
assert fo_writes[0][2][17] == "2.5"   # algo_v на своєму місці
assert "fo-1" not in ns7["rev_open"]
print("7б) follow-стрічка: 60 хв -> рядок у follow_outcomes.csv")

# ── 8. Flat-спліт: $100k позиція після $10M не губиться ──
bp = load({"_build_profile", "_grade_episode"}, dict(C))
bp["_build_profile"] = _v28_with_opens(bp["_build_profile"])   # v2.18
def mkf(t, px, sz, sp, coin="AAA", tid=None):
    return {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir":
            "Close Long", "coin": coin, "crossed": True, "twapId": None,
            "hash": f"0x{coin}{t}", "tid": tid or t}
T0 = 1_700_000_000_000
small_after_big = [mkf(T0, 1000.0, 10_000, 10_000),          # $10M -> 0
                   mkf(T0 + 60_000, 1000.0, 100, 100)]       # $100k -> 0
p8 = bp["_build_profile"](small_after_big)
assert p8["n_ep"] == 2 and p8["ok"], p8
print("8) нова позиція у 100 разів менша за попередню — обидві пораховані")

# ── 9. 100-денний ok-профіль НЕ відкриває F4; 47-годинний — так ──
f9 = mk_fol(dict(wallet_profiles={"0xold": {
    "ok": True, "v": 5, "n_ep": 3, "avg_gap_s": 30.0,
    "fetched": time.time() - 100 * 86400}}))
f9["follow_on_txs"]("0xOLD", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x5"}], False)
sts9 = {p["strategy"] for p in f9["follow_open"].values()}
assert "F4_розумний" not in sts9 and len(sts9) == 4, sts9   # F1-F3 + F6 (v2.9)
f9b = mk_fol(dict(wallet_profiles={"0xok": {
    "ok": True, "v": 5, "n_ep": 3, "avg_gap_s": 30.0,
    "fetched": time.time() - 47 * 3600}}))
f9b["follow_on_txs"]("0xOK", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x6"}], False)
assert "F4_розумний" in {p["strategy"] for p in f9b["follow_open"].values()}
print("9) вік профілю: >48г — F4 закритий до рефрешу, 47г — працює")

# ── 10. API: follow_tape, partial-когорта, карантин, назви ──
m = re.search(r'REV_SIG_HEADERS = \[(.*?)\]', src, re.S)
SIGH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
REVH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
m = re.search(r"FOLLOW_HEADERS = \[(.*?)\]", src, re.S)
FOLH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_OUT_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
OUTH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
m = re.search(r'FOLLOW_OUT_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
FOUTH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
d10 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d10, name), "w", newline="") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def dictrow(headers, d, fill=0.5):
    return [d.get(h, fill) for h in headers]
wcsv("rev_trades.csv", REVH, [])
wcsv("rev_signals.csv", SIGH, [
    dictrow(SIGH, {"sig_id": "s1", "src": "wallet", "move_3m_pct": 1.5,
                   "btc_move_pct": 0.01, "btc_ok": 1, "algo_v": "2.5", "eol": "^"}),
    dictrow(SIGH, {"sig_id": "s2", "src": "partial", "move_3m_pct": 1.5,
                   "btc_move_pct": 0.01, "btc_ok": 1, "algo_v": "2.5", "eol": "^"}),
])
wcsv("rev_outcomes.csv", OUTH, [
    dictrow(OUTH, {"sig_id": "o1", "src": "wallet", "move_3m_pct": 1.5, "dump_move_pct": "",
                   "btc_move_pct": 0.01, "btc_ok": 1, "shtanga": 1,
                   "m30": 2.0, "costs_pct": 0.15, "algo_v": "2.5", "eol": "^"}),
    dictrow(OUTH, {"sig_id": "o2", "src": "partial", "move_3m_pct": 1.5, "dump_move_pct": "",
                   "btc_move_pct": 0.01, "btc_ok": 1, "shtanga": 0,
                   "m30": -1.0, "costs_pct": 0.15, "algo_v": "2.5", "eol": "^"}),
])
fo_rows = [dictrow(FOUTH, {"fo_id": f"fo{i}", "costs_pct": 0.15,
                           "algo_v": "2.5", "m30": 1.0}) for i in (1, 2)]
wcsv("follow_outcomes.csv", FOUTH, fo_rows)
with open(os.path.join(d10, "follow_trades.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow(FOLH)
    w.writerow(dictrow(FOLH, {"trade_id": "t1", "strategy": "F1_1хв",
                              "net_pct": 0.5, "algo_v": "2.5", "eol": "^"}))
    f.write("2026-08-30,partial,row\r\n")   # огризок: 3 колонки з 25
api = load({"strat2_api", "_median"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, STRAT2_DESC={}, STRAT2_TITLES={"R1_загальний": "x"},
    _strat2_cache={"ts": 0.0, "data": None},
    REV_CSV=os.path.join(d10, "rev_trades.csv"),
    REV_SIG_CSV=os.path.join(d10, "rev_signals.csv"),
    FOLLOW_CSV=os.path.join(d10, "follow_trades.csv"),
    REV_OUT_CSV=os.path.join(d10, "rev_outcomes.csv"),
    FOLLOW_OUT_CSV=os.path.join(d10, "follow_outcomes.csv")))
out = api["strat2_api"]()
assert out["signals_total"] == 1 and out["signals_partial"] == 1
assert out["quarantined"] == 1                      # огризок у карантині
assert out["research"]["observed"]["partial"]["n"] == 1
assert abs(out["research"]["observed"]["partial"]["median"] - (-1.15)) < 1e-9
assert out["research"]["observed"]["mag_1_2"]["n"] == 1         # partial НЕ в mag-когорті
assert out["follow_tape"]["n"] == 2
assert out["follow_tape"]["curve_common_n"] == 2
assert abs(out["follow_tape"]["curve"][29][0]
           if isinstance(out["follow_tape"]["curve"][29], list)
           else out["follow_tape"]["curve"][29] - 0.85) < 1e-9
assert out["titles"] == {"R1_загальний": "x"}
assert out["strategies"]["F1_1хв"]["n"] == 1        # цілий рядок живий
print("10) API: follow_tape, partial-когорта, карантин огризків, назви")

# ── 11. Файли спостережуваності на місці ────────────────
H = _HL
for fn in ("watchdog.py", "whale-watchdog.service", "whale-watchdog.timer",
           "whale-terminal.logrotate"):
    assert os.path.exists(os.path.join(H, fn)), fn
import py_compile
py_compile.compile(os.path.join(H, "watchdog.py"), doraise=True)
wd = open(os.path.join(H, "watchdog.py"), encoding="utf-8").read()
assert "down_alerted" in wd and "ws_alive" in wd and "disk_usage" in wd
print("11) watchdog/logrotate/timer у комплекті, watchdog компілюється")

# ── 12. Рев'ю-фікси v2.5 ────────────────────────────────
# 12а: ВІДОМА пара не перезаписується prio-фетчем
q12 = deque([("0xw", "PUMP", "LONG", 50_000.0, 100_000.0, 15_000.0)])
wl12 = {"0xw": {"PUMP": {"size": 100.0, "val": 10_000.0, "side": "LONG",
                         "ratio": 5.0, "entry": 90.0, "liq": 0}}}
fc12, logs12 = {"0xw:PUMP": 777}, []
ns12 = dict(ns6, _prio_q=q12, watchlist=wl12, fill_cursor=fc12,
            _prio_log=lambda *a: logs12.append(a),
            _prio_lock=threading.Lock(), _prio_event=threading.Event(),
            _prio_direct=deque(), _prio_seen={},
            _prio_proxy_state={"streak": 0, "dead_since": 0.0},
            prio_stats={"triggers": 0, "added": 0, "dropped": 0,
                        "errors": 0})
ns12["_prio_event"].set()
exec(compile(one_pass, "prio", "exec"), ns12)
ns12["run_prio_fetcher"]()
assert wl12["0xw"]["PUMP"]["size"] == 100.0        # НЕ перезаписано
assert fc12["0xw:PUMP"] == 777                     # курсор не рухано
assert logs12[-1][6] == "known", logs12
print("12а) prio: відома пара недоторкана — конвеєр підтвердження живий")
# 12б: мертва проксі -> фолбек на прямий канал після 3 збоїв
def boom(a, post=None): raise RuntimeError("proxy down")
q12b = deque([(f"0x{i}", "PUMP", "LONG", 5e4, 1e5, 1.5e4)
              for i in range(4)])
st12 = {"streak": 0, "dead_since": 0.0}
logs12b = []
ns12b = dict(ns6, _prio_q=q12b, watchlist={}, fill_cursor={},
             check_one_wallet=boom,
             _hl_post_prio_direct=lambda b, retries=2: None,
             _prio_log=lambda *a: logs12b.append(a),
             _prio_lock=threading.Lock(), _prio_event=threading.Event(),
             _prio_direct=deque(), _prio_seen={}, _prio_proxy_state=st12,
             prio_stats={"triggers": 0, "added": 0, "dropped": 0,
                         "errors": 0})
ns12b["_prio_event"].set()
exec(compile(one_pass, "prio", "exec"), ns12b)
ns12b["run_prio_fetcher"]()
assert st12["dead_since"] > 0 and st12["streak"] >= 3
assert logs12b[-1][8] == 0 or logs12b[-1][-1] == 0   # 4-та пішла напряму
print("12б) prio: 3 збої проксі -> прямий канал, стан dead зафіксовано")
# 12в: волт-партіал без великого шматка = ТІНЬ, а не R6
rv, sigv = mk_rev()
rv["is_vault"] = lambda a: True
ns_v = load({"rev_on_close"}, dict(C, STRAT2_ENABLED=True,
    strat2_lock=threading.RLock(), rev_open={},
    fc_lock=threading.Lock(), fc_episodes={}, is_vault=lambda a: True,
    _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
    _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
    _sim_depth=lambda c, s: 1e5, _dt=_dt,
    _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
    REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
    _strat_csv_append=lambda p, h, r: True))
ns_v["rev_on_close"]("0xV", "PUMP",
    {"size": 1000.0, "side": "LONG", "val": 100_000, "ratio": 3},
    [{"sz": 45.0, "px": 100.0, "hash": f"0x{i}"} for i in range(8)],
    False)   # батч 36%, кожна tx $4.5k < $5k -> big_any False
sts_v = {p["strategy"] for p in ns_v["rev_open"].values()}
assert sts_v == {"_OUTCOME"}, sts_v
pv = next(iter(ns_v["rev_open"].values()))
assert pv["src"] == "partial" and pv["vault"] == 1
# і серійний дедуп: другий батч того ж розвантаження — без нового треку
ns_v["rev_on_close"]("0xV", "PUMP",
    {"size": 640.0, "side": "LONG", "val": 64_000, "ratio": 3},
    [{"sz": 45.0, "px": 100.0, "hash": f"0y{i}"} for i in range(5)],
    False)
assert len(ns_v["rev_open"]) == 1
print("12в) волт-партіал = тінь (vault=1, без R6); серійні батчі не дублюються")
# 12г: доларовий поріг у big_any волта
ns_d = load({"rev_on_close"}, dict(C, STRAT2_ENABLED=True,
    strat2_lock=threading.RLock(), rev_open={},
    fc_lock=threading.Lock(), fc_episodes={}, is_vault=lambda a: True,
    _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
    _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
    _sim_depth=lambda c, s: 1e5, _dt=_dt,
    _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
    REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
    _strat_csv_append=lambda p, h, r: True))
ns_d["rev_on_close"]("0xV", "PUMP",
    {"size": 600.0, "side": "LONG", "val": 60_000, "ratio": 3},
    [{"sz": 30.0, "px": 100.0, "hash": "0xz"}], False)  # 5%, але $3k
assert ns_d["rev_open"] == {}, ns_d["rev_open"]
print("12г) волт: шматок 5% на $3k не відкриває R6 (MIN_TX_USD)")
# 12д: курсор prio ставиться ДО запиту (порядок у коді)
i_now = prio_src_now = src.index("now_ms = int(time.time() * 1000)   # курсор = час ДО запиту")
i_call = src.index("check_one_wallet(", i_now)
assert i_now < i_call
# 12е: _prio_seen чиститься у _prune_leaks
pl = src[src.index("def _prune_leaks"):src.index("def is_vault")]
assert "_prio_seen.pop" in pl
print("12д-е) курсор до запиту; кулдаун-кеш не тече")

print("\nУСІ РЕГРЕСІЇ v2.5 ЗАКРИТІ")

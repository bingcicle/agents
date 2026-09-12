import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.12 (рішення користувача 08.09 + перенесене з CH-версії):
(1) грейс від ОСТАННЬОГО великого закриття + якір big_val для порогу $50k
+ скидання при перевідкритті; (2) F8 = копія F6 + ratio≥3.5, F9 по точних
лічильниках; (3) скан: нові пари у watchlist одразу, каденс від старту,
розподіл порожніх гаманців; (4) стан: SIGTERM→save, трекери після довгого
простою, main(); (5) UI: три панелі на телефоні; (6) версії 2.12."""
import ast, threading, time, json, os, re, sys, math, tempfile
import calendar, datetime as _dtmod, html as _htmlmod, glob, hashlib
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
          "math": math, "hashlib": hashlib, "print": lambda *a, **k: None}
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
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True,
         F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0, F5_NAME="F5_перший",
         F5_FIRST_SHOT_S=3600.0, F7_NAME="F7_без_ратіо", RATIO_GRACE_S=1800)
assert const("DATA_ALGO_V") == "2.20"

# ═══ 1. Грейс від останнього великого закриття + якір big_val ═══════
g = load({"_mark_ratio", "_mark_close", "_grace_val", "_ratio_ok"})
mk, mc, gv, ok = g["_mark_ratio"], g["_mark_close"], g["_grace_val"], g["_ratio_ok"]
now = 1_000_000.0
p = {"val": 200_000.0}
mk(p, 2.5, now)
assert p["ratio_hi_ts"] == now and p["big_val"] == 200_000.0     # якір = вартість, коли велика
p["val"] = 100_000.0
mk(p, 1.3, now + 600)                    # злив половини: ratio впав, якір лишився
assert p["ratio_hi_ts"] == now and p["big_val"] == 200_000.0
assert ok(p, now + 1799) and not ok(p, now + 1800)
# велике закриття о now+1500 (пара ще у грейсі) -> мітка = час закриття
mc(p, now + 1500)
assert p["ratio_hi_ts"] == now + 1500 and ok(p, now + 1500 + 1799)
mc(p, now + 1000)                        # назад мітка не рухається
assert p["ratio_hi_ts"] == now + 1500
mc(p)                                    # без ts — зараз
assert p["ratio_hi_ts"] > now + 1500
# якір для порогу $50k: у грейсі — max(залишок, big_val); при ratio≥2 — залишок
p["val"] = 30_000.0; p["ratio"] = 1.3
assert gv(p) == 200_000.0
p["ratio"] = 2.2
assert gv(p) == 30_000.0
assert gv({"val": 30_000.0, "ratio": 1.0}) == 30_000.0          # без якоря
# rev: залишок $30k у грейсі з якорем $200k — сигнал ЖИВЕ; без якоря — ні
def mk_rev():
    return load({"rev_on_close", "_mark_ratio", "_ratio_ok", "_grace_val"}, dict(C,
        strat2_lock=threading.RLock(), rev_open={},
        fc_lock=threading.Lock(), fc_episodes={},
        is_vault=lambda a: False,
        _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
        _sim_depth=lambda c, s: 1e5, _dt=_dt,
        _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
        REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
        _strat_csv_append=lambda p, h, r: True))
OLD_G = {"size": 1000.0, "side": "LONG", "val": 30_000.0, "ratio": 1.4,
         "ratio_hi_ts": time.time() - 600, "big_val": 200_000.0}
r = mk_rev()
r["rev_on_close"]("0xa", "AAA", OLD_G,
                  [{"sz": 1000.0, "px": 30.0, "hash": "0x1", "dir": "Close Long"}], True)
assert "R1_загальний" in {p["strategy"] for p in r["rev_open"].values()}
r = mk_rev()
r["rev_on_close"]("0xa", "AAA", dict(OLD_G, big_val=0),
                  [{"sz": 1000.0, "px": 30.0, "hash": "0x1", "dir": "Close Long"}], True)
assert r["rev_open"] == {}                                        # $30k без якоря — пил
# follow: те саме через _grace_val
def mk_fol(old, prof=None):
    n = load({"follow_on_txs", "_ratio_ok", "_grace_val"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={},
        rev_open={}, follow_last_close={},
        wallet_profiles=({"0xabc": dict(prof)} if prof else {}),
        profiles_fetching=set(), is_vault=lambda a: False,
        _px_now=lambda c, max_age=20: 100.0, _px_ago=lambda c, s: 100.0,
        _sim_depth=lambda c, s=None: 1e5, _profile_request=lambda a: None,
        stats={}))
    n["follow_on_txs"]("0xabc", "AAA", old,
                       [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
    return n
assert len(mk_fol(OLD_G)["follow_open"]) == 4                    # F1-F3 + F6 (якір $200k)
assert mk_fol(dict(OLD_G, big_val=0))["follow_open"] == {}      # $30k без якоря
# місця запису: sweep ставить мітку по ОСТАННІЙ великій tx батчу (не при
# повному закритті — пара зникає), скан-діф — теж; перевідкриття скидає
_ws = src[src.index("def check_wallet_worker"):src.index("def check_position_changes")]
assert "if big_txs and _ratio_ok(old) and not full_close:" in _ws
assert '_ts_close = max((f.get("ts") or 0) for f in big_txs) / 1000.0' in _ws
assert "_mark_close(_wc, _ts_close or time.time())" in _ws
assert '_wr.pop("ratio_hi_ts", None)' in _ws and '_wr.pop("big_val", None)' in _ws
_cp = src[src.index("def check_position_changes"):src.index("def send_close_alert")]
assert "_mark_close(new_pos, _ts_close)" in _cp and "_grace_val(new_pos)" in _cp
assert '"ratio_hi_ts": hi_ts, "big_val": big_val,' in _cp
assert src.count('"big_val": ') >= 5                             # усі місця вставки пари
assert "ev_val = _grace_val(old)" in src and "_event_val = _grace_val(old)" in src   # v2.19: у приймачі _ingest_txs
assert "if _grace_val(old) < MIN_POS_USD: return" in src
# скан-діф успадковує big_val із prev/живого watchlist
cpc = load({"check_position_changes", "_ratio_ok", "_grace_val", "_mark_close"}, dict(C,
    COIN_BLACKLIST=set(), tracking_lock=threading.Lock(),
    prev_positions={"0xw": {"AAA": {"size": 10.0, "val": 1e5, "side": "LONG",
                                    "ratio": 2.5, "ratio_hi_ts": time.time() - 300,
                                    "big_val": 3e5, "entry": 1.0}}},
    fill_cursor={}, get_recent_market_fills=lambda *a, **k: ([], []),
    depth_for_side=lambda d, s: (d or {}).get("bid", 0),
    alerted_txs={}, sent_alerts=set(), close_episodes={}, scan_tombstones={},
    delta_seen={}, watchlist_lock=threading.Lock(), watchlist={},
    stats={"delta_events": 0, "fills_confirmed": 0, "fills_empty": 0}))
cpc["check_position_changes"](
    {"AAA": [{"addr": "0xw", "size": 9.0, "val": 1.5e5, "side": "LONG", "entry": 1.0}]},
    {**{f"D{i}": {"bid": 1e5} for i in range(10)}, "AAA": {"bid": 1e5}})
assert cpc["prev_positions"]["0xw"]["AAA"]["big_val"] == 3e5      # ratio 1.5 -> якір з prev
print("1) грейс: мітка від останнього великого закриття, якір big_val, скидання при перевідкритті")

# ═══ 2. F8 = копія F6 + ratio≥3.5; F9 — точні лічильники ══════════
n = mk_fol({"size": 1000.0, "side": "LONG", "ratio": 3.6, "val": 5e5})   # профілю НЕМАЄ
st = {p["strategy"]: p for p in n["follow_open"].values()}
assert set(st) == {"F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший", "F8_ratio35"}, set(st)
assert st["F8_ratio35"]["timer"] == 60.0 and st["F8_ratio35"]["profile_gap"] == 0.0
n = mk_fol({"size": 1000.0, "side": "LONG", "ratio": 3.4, "val": 5e5})
assert "F8_ratio35" not in {p["strategy"] for p in n["follow_open"].values()}
# не перший постріл пари — ні F6, ні F8
n2 = load({"follow_on_txs", "_ratio_ok", "_grace_val"}, dict(C,
    strat2_lock=threading.RLock(), follow_open={}, rev_open={},
    follow_last_close={"0xabc:AAA": time.time() - 100}, wallet_profiles={},
    profiles_fetching=set(), is_vault=lambda a: False,
    _px_now=lambda c, max_age=20: 100.0, _px_ago=lambda c, s: 100.0,
    _sim_depth=lambda c, s=None: 1e5, _profile_request=lambda a: None, stats={}))
n2["follow_on_txs"]("0xabc", "AAA", {"size": 1000.0, "side": "LONG", "ratio": 3.6, "val": 5e5},
                    [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
assert {p["strategy"] for p in n2["follow_open"].values()} == {"F1_1хв", "F2_2хв", "F3_3хв"}
# F8 більше не в profile-гілці
_fo = src[src.index("def follow_on_txs"):src.index("def _build_profile") if "def _build_profile" in src[src.index("def follow_on_txs"):] else len(src)]
assert "_timers.append((F8_NAME, F6_TIMER_S))" in _fo
assert "(F8_NAME, timer4" not in _fo
# F9: 90.0 у профілі, але 899/1000 = 89.9% — не проходить; 9/10 — проходить
PROF = {"ok": True, "v": 9, "fetched": time.time() - 100, "n_big": 1000,
        "n_fast": 899, "n_slow": 101, "fast_pct": 90.0, "unload_med_s": 95.0,
        "unload_mean_s": 110.5, "window_d": 61.3, "avg_gap_s": 40.0,
        "nr": {"ok": True, "n_big": 1000, "n_fast": 899, "n_slow": 101,
               "fast_pct": 90.0, "unload_med_s": 120.0, "unload_mean_s": 130.0,
               "avg_gap_s": 50.0}}
OLD3 = {"size": 1000.0, "side": "LONG", "ratio": 3.0, "val": 5e5}
st = {p["strategy"] for p in mk_fol(OLD3, PROF)["follow_open"].values()}
assert "F7_без_ратіо" in st and "F9_без_ратіо_90" not in st, st
P9 = dict(PROF, n_fast=9, n_slow=1, n_big=10)   # v2.18: F9 читає точні лічильники з кореня профілю
st = {p["strategy"] for p in mk_fol(OLD3, P9)["follow_open"].values()}
assert "F9_без_ратіо_90" in st, st
# версії/титули
_ss = load(set(), {}, assigns=("STRAT_SINCE", "TAPE_SINCE"))
assert _ss["STRAT_SINCE"]["F8_ratio35"] == "2.12"
assert _ss["STRAT_SINCE"]["T1_твап_відкриття"] == "2.15" and _ss["STRAT_SINCE"]["T2_твап_скорочення"] == "2.15"   # v2.14: completed за фактом, timer_late, закриття≠спостереження
assert _ss["STRAT_SINCE"]["F9_без_ратіо_90"] == "2.18" and _ss["STRAT_SINCE"]["R1_загальний"] == "2.17"   # v2.18: F9 рахує невизначені як повільні
assert '"F8_ratio35":   "За китом · тиша 1 хв · перший постріл · ratio ≥3.5"' in src
print("2) F8 = F6-копія + ratio≥3.5 (без профілю, таймер 60с); F9 по точних лічильниках")

# ═══ 3. Скан: пари у watchlist одразу, каденс від старту, розподіл ══
D = load({"_discover_pairs"}, dict(C, COIN_BLACKLIST={"BAD"},
    cache={"depth": {**{f"D{i}": {"bid": 1e5, "ask": 1e5} for i in range(10)},
                     "AAA": {"bid": 1e5, "ask": 1e5}, "BBB": {"bid": 1e5, "ask": 1e5},
                     "CCC": {"bid": 1e5, "ask": 1e5}, "DDD": {"bid": 1e5, "ask": 1e5},
                     "BAD": {"bid": 1e3, "ask": 1e3}}},
    cache_lock=threading.Lock(), depth_for_side=lambda d, s: (d or {}).get("bid", 0),
    watchlist_lock=threading.Lock(), watchlist={"0xw": {"BBB": {"marker": 1}}},
    scan_tombstones={"0xw:CCC": 5000.0}, sent_alerts={"0xw:AAA"}, close_episodes={"0xw:AAA": {}},
    delta_seen={"0xw:AAA": 1}, fill_cursor={"0xw:AAA": 1000},
    scan_metrics={"next_scan_at": 0.0, "last_duration_s": 0.0, "new_pairs": 0}))
added = D["_discover_pairs"]("0xw", [
    {"coin": "AAA", "size": 10.0, "val": 3e5, "side": "LONG", "entry": 1.0, "liq": 0},
    {"coin": "BBB", "size": 10.0, "val": 3e5, "side": "LONG", "entry": 1.0},   # уже є
    {"coin": "CCC", "size": 10.0, "val": 3e5, "side": "LONG", "entry": 1.0},   # tombstone після знімка
    {"coin": "DDD", "size": 10.0, "val": 1e5, "side": "LONG", "entry": 1.0},   # ratio 1 (D-глибина)
    {"coin": "BAD", "size": 10.0, "val": 3e5, "side": "LONG", "entry": 1.0},   # блекліст
], 4000.0)
assert added == 1 and D["scan_metrics"]["new_pairs"] == 1
w = D["watchlist"]["0xw"]
assert set(w) == {"AAA", "BBB"} and w["BBB"] == {"marker": 1}
assert w["AAA"]["ratio"] == 3.0 and w["AAA"]["ratio_hi_ts"] == 4000.0 and w["AAA"]["big_val"] == 3e5
assert w["AAA"]["upd"] == 4000.0
assert D["fill_cursor"]["0xw:AAA"] == 4_000_000 and "0xw:AAA" not in D["sent_alerts"]
assert "0xw:AAA" not in D["close_episodes"] and "0xw:AAA" not in D["delta_seen"]
# tombstone ПІСЛЯ знімка — не воскрешаємо; ДО знімка — можна
D["scan_tombstones"]["0xw:CCC"] = 3000.0
assert D["_discover_pairs"]("0xw", [{"coin": "CCC", "size": 1.0, "val": 3e5, "side": "LONG"}], 4000.0) == 1
# без глибини (<10 монет) — нічого
D2 = dict(D); D2["cache"] = {"depth": {"AAA": {"bid": 1e5}}}
assert D["_discover_pairs"].__globals__ is D  # той самий namespace
D["cache"]["depth"] = {"AAA": {"bid": 1e5}}
assert D["_discover_pairs"]("0xz", [{"coin": "AAA", "size": 1.0, "val": 3e5, "side": "LONG"}], 4000.0) == 0
# process() кличе discover одразу після читання гаманця; каденс — від старту
_pr = src[src.index("        def process(w):"):src.index("def _schedule_scan")]
assert "_discover_pairs(k, positions, _t_fetch)" in _pr
assert "_schedule_scan(max(5.0, REFRESH_S - (time.time() - scan_start)))" in _pr
assert "threading.Timer(REFRESH_S" not in src
S = load({"_schedule_scan"}, {"scan_metrics": {"next_scan_at": 0.0}, "_scan_timer": None,
                              "_scan_sched_lock": threading.Lock(), "run_scan": lambda: None})
S["_schedule_scan"](3600); t1 = S["_scan_timer"]; assert t1 is not None and t1.daemon
S["_schedule_scan"](3600); t2 = S["_scan_timer"]
assert t2 is not t1 and not t1.is_alive() or t1.finished.is_set()   # попередній скасовано
t2.cancel()
assert abs(S["scan_metrics"]["next_scan_at"] - (time.time() + 3600)) < 5
# розподіл порожніх: кожен хронічно порожній гаманець перевіряється рівно в
# одному з 5 послідовних сканів; активні — завжди
SK = load({"should_skip"}, {"SKIP_AFTER": 3, "CHECK_EVERY": 5, "stats_lock": threading.Lock(),
                            "wallet_stats": {f"0x{i:040x}": {"empty_streak": 5, "last_checked": 1}
                                             for i in range(50)}})
SK["wallet_stats"]["0xactive"] = {"empty_streak": 1, "last_checked": 1}
for a in list(SK["wallet_stats"]):
    n_checked = sum(not SK["should_skip"](a, sn) for sn in range(10, 15))
    assert n_checked == (5 if a == "0xactive" else 1), (a, n_checked)
per_scan = [sum(not SK["should_skip"](a, sn) for a in SK["wallet_stats"] if a != "0xactive")
            for sn in range(10, 15)]
assert sum(per_scan) == 50 and max(per_scan) <= 20, per_scan   # розмазано, не всі в одному
# /status показує планувальник
assert '"scan_next_in_s"' in src and '"scan_new_pairs_live": scan_metrics["new_pairs"]' in src
print("3) скан: нові пари одразу (курсор на знімок), каденс від старту, розподіл порожніх")

# ═══ 4. Стан: SIGTERM→save, трекери після довгого простою, main() ═══
assert 'if __name__ == "__main__":\n    main()' in src
assert "signal.signal(signal.SIGTERM, _on_stop)" in src and "atexit.register(save_state)" in src
assert src.index("def main():") < src.index('if __name__ == "__main__":')
assert "ThreadingHTTPServer" in src[src.index("def main():"):]
d = tempfile.mkdtemp()
def mk_state(age):
    snap = {"saved_at": time.time() - age,
            "watchlist": {"0xw": {"AAA": {"size": 1.0, "val": 1e5, "side": "LONG", "ratio": 2.5}}},
            "rev_open": {"s1|R1_загальний": {"strategy": "R1_загальний", "samples": [], "entry_ts": 1}},
            "follow_open": {"f1": {"strategy": "F1_1хв", "open_ts": 1}},
            "twap_reg": {"tw-1": {"id": "tw-1", "state": "entered"}},
            "twap_last_ids": {"HL_TWAP": 1700},
            "fill_cursor": {"0xw:AAA": 5}, "sent_alerts": ["0xw:AAA"],
            "fc_positions": [], "fc_episodes": [], "sim_positions": [], "sim_trackers": []}
    json.dump(snap, open(os.path.join(d, "state.json"), "w"))
def ld():
    ns = load({"load_state"}, {"STATE_FILE": os.path.join(d, "state.json"), "STATE_MAX_AGE_S": 3600,
        "watchlist_lock": threading.Lock(), "watchlist": {}, "sim_lock": threading.Lock(),
        "sim_positions": {}, "sim_trackers": {}, "sim_closed": [], "alerts_lock": threading.Lock(),
        "recent_alerts": [], "sent_alerts": set(), "fill_cursor": {}, "close_episodes": {},
        "fc_lock": threading.Lock(), "fc_positions": {}, "fc_episodes": {},
        "strat2_lock": threading.RLock(), "rev_open": {}, "follow_open": {},
        "follow_last_close": {}, "vault_cache": {}, "twap_lock": threading.Lock(),
        "twap_reg": {}, "twap_last_ids": {}})
    ns["load_state"](); return ns
mk_state(120); fresh = ld()
assert fresh["watchlist"] and fresh["rev_open"] and fresh["fill_cursor"] and fresh["twap_reg"]
mk_state(2 * 3600); stale = ld()
assert stale["watchlist"] == {} and stale["fill_cursor"] == {} and stale["sent_alerts"] == set()
assert set(stale["rev_open"]) == {"s1|R1_загальний"} and set(stale["follow_open"]) == {"f1"}
assert stale["twap_reg"] == {"tw-1": {"id": "tw-1", "state": "entered"}}
assert stale["twap_last_ids"] == {"HL_TWAP": 1700}
print("4) стан: SIGTERM/atexit зберігають, після >1 год — трекери й TWAP-реєстр без ринкового знімка")

# ═══ 5. UI: три панелі на телефоні, F8 без профілю ═════════════════
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for k in ('<body data-pane="positions">', '<nav class="mobile-nav"', 'onclick="mobilePane(\'coins\')"',
          'function mobilePane(name)', "mobilePane('positions');   // вибір монети",
          'body[data-pane="coins"] #termView>.center', 'body[data-pane="watch"] #termView>.coins',
          '.mobile-nav,.mobile-back{display:none}', '.mobile-nav button{flex:1;min-height:44px',
          'touch-action:manipulation', 'prefers-reduced-motion', 'class="mobile-back fbtn"',
          "header{height:auto;position:sticky;top:0;z-index:15",
          "'F6_1хв_перший',\n               'F8_ratio35','F4_розумний'",
          "matchMedia('(max-width:900px)').matches"):
    assert k in ui, k
assert "'F8_ratio35'" not in ui[ui.index("const S_PROF="):ui.index("const S_FIRST=")]
assert "'F8_ratio35'" in ui[ui.index("const S_FIRST="):ui.index("function secTxt")]
assert ".coin-scroll{display:flex;overflow-x:auto" not in ui        # стрічку монет замінили панелі
print("5) UI: панелі монети/позиції/watch, 44px, липкий хедер, F8 у порядку карток після F6")

# ═══ 6. TWAP: reply-посилання у прев'ю, версії/заголовки ═════════════
TWp = load({"_tme_parse", "_tme_text", "_iso_ts"}, {"re": re, "_dtmod": _dtmod, "_htmlmod": _htmlmod})
posts = TWp["_tme_parse"](open(os.path.join(HERE, "tme_hl_twap2.html"), encoding="utf-8").read())
byid = {p[0]: p for p in posts}
assert byid[1695][4] == 1694 and byid[1696][4] == 1693 and byid[1713][4] == 0
assert byid[1695][3].startswith("⛔️ TWAP is cancelled")                  # цитата — теж
posts_x = TWp["_tme_parse"](open(os.path.join(HERE, "tme_twapx2.html"), encoding="utf-8").read())
assert sum(1 for p in posts_x if p[4]) == 7 and all(p[4] < p[0] for p in posts_x if p[4])
assert const("TWAP_MIN_DUR_S") == 120 and const("TWAP_VERIFY_S") == 60 and const("TWAP_CONFIRM_S") == 300
assert 'TWAP 2–15 хв, яким кит ВІДКРИВАЄ НОВУ позицію' in src
assert "def _twap_verify(rec, now, slices=True):" in src and 'dist = abs(ts0 - rec["start"])' in src \
    and "if dist > 10 or (want_min and abs(mins - want_min) > 1):" in src
assert 'kind = "open"' in src and 'kind = "reduce"' in src and 'kind = "flip"' in src and 'kind = "increase"' in src
assert "check_one_wallet(rec[" not in src[src.index("def _twap_resolve_kind"):src.index("def _twap_sig_write")]
print("6) TWAP: reply_pid із прев'ю, вид лише зі startPosition, константи 2.12")

print("\nALL v2.12 TESTS PASSED")

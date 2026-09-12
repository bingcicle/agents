import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки третього аудиту (v2.2 -> v2.3)."""
import math
import ast, threading, time, json, os, re, sys, tempfile, csv, io

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

C = dict(PROFILE_ALGO_V=3, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=2,
         F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0, F4_CHUNK_PCT=0.05,
         PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.3", WFAIL_CAP=200,
         REV_TRACK_MIN=60, SIM_COMMISSION=0.0005)

def _dt(ts): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

# ── 1. Заморожений рядок: ретрай пише ТІ САМІ байти ─────
loop_src = src[src.index("def run_strat2_loop"):src.index("# ── API")]
one_tick = loop_src.replace("while True:", "for _tick in range(1):") \
                   .replace("time.sleep(3)", "pass")
px_feed = {"X": 110.0}
appended, csv_ok = [], {"ok": False}
def fake_append(path, headers, row):
    appended.append((path, list(row)))
    return csv_ok["ok"]
ns1 = dict(C, STRAT2_ENABLED=True, strat2_lock=threading.RLock(),
           _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
           rev_open={}, follow_open={}, follow_last_close={},
           _px_now=lambda c, max_age=None: px_feed.get(c),
           _sim_slip=lambda d: 0.0005, _dt=_dt,
           _strat_csv_append=fake_append,
           _rev_row=lambda p, e: ["rev-row"], _rev_out_row=lambda p: ["out-row"],
           _fol_out_row=lambda p: ["fo-row"],
           FOLLOW_OUT_CSV="fo.csv", FOLLOW_OUT_HEADERS=[],
           save_state=lambda: None,
           REV_CSV="r.csv", REV_OUT_CSV="o.csv", FOLLOW_CSV="f.csv",
           REV_HEADERS=[], REV_OUT_HEADERS=[], FOLLOW_HEADERS=[],
           threading=threading, time=time, print=lambda *a, **k: None)
for _fn in ("_fol_row", "_exec_extra", "_p_costs", "_ms", "_rnd"):   # v2.16: вихід follow через _fol_row/стакан
    _seg = src[src.index(f"def {_fn}("):]; _seg = _seg[:_seg.index("\ndef ")]
    exec(compile(_seg, _fn, "exec"), ns1)
ns1["_paper_px"] = _V28["_paper_px"]; ns1["math"] = math
ns1["_journal"] = _V28["_journal"]; ns1["_leg_costs"] = _V28["_leg_costs"]
ns1.update(_vt=_V28["_vt"], stats=ns1.get("stats", {}), EXEC_EXCH="hl", BOOK_APPLY_MAX_S=5.0, _funding_pct=_V28["_funding_pct"], SIM_POSITION_USD=1000.0)   # v2.19
exec(compile(one_tick, "loop", "exec"), ns1)
ns1.update(_tr_px_now=_V28["_tr_px_now"], _tr_exch=_V28["_tr_exch"], _mid_age_exch=_V28["_mid_age_exch"], _bn_px_now=_V28["_bn_px_now"], _bn_px_mid_age=_V28["_bn_px_mid_age"], bn_px_hist={}, bn_px_lock=threading.Lock(), stats=ns1.get("stats") or {})   # v2.20
now0 = time.time()
tr = {"coin": "X", "our_side": "LONG", "entry_px": 100.0, "open_ts": now0 - 100,
      "strategy": "F1_1хв",
      "key": "0xa:X", "addr": "0xa", "timer": 60.0, "peak": -999.0,
      "trough": 999.0, "tx_pct": 10.0, "tx_usd": 5e4, "ratio": 3.0,
      "pos_usd": 5e5, "vault": 0, "hour": 12, "btc_move": 0.01, "depth": 1e5,
      "force_exit": None, "profile_gap": 0.0, "algo_v": "2.3", "eol": "^"}
ns1["follow_open"]["fid1"] = tr
ns1["follow_last_close"]["0xa:X"] = now0 - 100   # тиша 100с > 60с -> вихід
ns1["run_strat2_loop"]()                          # тик 1: запис ПАДАЄ
first = [r for p, r in appended if p == "f.csv"]
assert len(first) == 1 and first[0][7] == 110.0, first  # exit_px @110
assert "fid1" in ns1["follow_open"] and tr.get("done") == 1
px_feed["X"] = 90.0; csv_ok["ok"] = True
ns1["run_strat2_loop"]()                          # тик 2: ціна ВЖЕ 90
second = [r for p, r in appended if p == "f.csv"][1]
assert second == first[0], (second, first[0])     # ті самі байти!
assert second[7] == 110.0 and "fid1" not in ns1["follow_open"]
print("1) done-угода заморожена: ретрай пише exit=110, а не перераховує по 90")

# ── 2. Outbox: сигнальний рядок, що впав, дописується ретраєм ──
sig_writes = []
def sig_append(path, headers, row):
    sig_writes.append(path)
    return len(sig_writes) > 1   # перший виклик падає, далі ок
ns2 = dict(ns1)
ns2.update(_strat_csv_append=sig_append, _sig_retry_q=[],
           _sig_retry_lock=threading.Lock(), follow_open={}, rev_open={})
exec(compile(one_tick, "loop", "exec"), ns2)
ns2["_sig_retry_q"].append(["sig.csv", ["h"], ["row"], 0])
sig_writes.append("warmup")      # щоб перший реальний ретрай пройшов
ns2["run_strat2_loop"]()
assert ns2["_sig_retry_q"] == [], ns2["_sig_retry_q"]
assert "sig.csv" in sig_writes
# і сам rev_on_close ставить у чергу при відмові
rev_seg = src[src.index("def rev_on_close"):src.index("def _rev_samples")]
assert "_sig_retry_q.append" in rev_seg and "_sig_retry_lock" in rev_seg
print("2) outbox: сигнал не губиться при відмові диска, дописується у циклі")

# ── 3. Пагінація профілю: межа однакової мс не губить філи ──
T = int(time.time() * 1000) - 10_000
def mk(t, i):
    return {"time": t, "px": 2.0, "sz": 1.0, "startPosition": 1.0,
            "dir": "Close Long", "coin": "A", "hash": f"0x{i}", "tid": i,
            "crossed": True, "twapId": None}
# 3а: 1999 філів @T, 3 філи @T+5 -> сторінка 2000 ріже мс T+5 навпіл
fills_a = [mk(T, i) for i in range(1999)] + [mk(T + 5, 2000 + j) for j in range(3)]
def hl_mock(fills):
    def post(body, retries=2):
        st = body["startTime"]
        return sorted([f for f in fills if f["time"] >= st],
                      key=lambda f: f["time"])[:2000]
    return post
got = {}
ns3 = load({"_fetch_profile", "_fetch_profile_locked", "_profile_post", "_fill_key"}, dict(C,
    strat2_lock=threading.Lock(), _prof_save_lock=threading.Lock(),
    profiles_fetching={"0xa"}, wallet_profiles={},
    PROFILES_FILE=os.path.join(tempfile.mkdtemp(), "p.json"),
    _build_profile=lambda fl, **kw: got.update(n=len(fl)) or
        {"ok": True, "n_ep": 2, "avg_gap_s": 10.0},
    hl_post=hl_mock(fills_a)))
ns3["_fetch_profile"]("0xa")
assert got["n"] == 2002, got   # УСІ філи, включно з 2001-м і 2002-м
assert not ns3["wallet_profiles"]["0xa"].get("truncated")
# 3б: >2000 філів в ОДНІЙ мс -> чесний truncated, не "ok і повна"
fills_b = [mk(T, i) for i in range(2001)]
got.clear()
ns3b = load({"_fetch_profile", "_fetch_profile_locked", "_profile_post", "_fill_key"}, dict(C,
    strat2_lock=threading.Lock(), _prof_save_lock=threading.Lock(),
    profiles_fetching={"0xb"}, wallet_profiles={},
    PROFILES_FILE=os.path.join(tempfile.mkdtemp(), "p.json"),
    _build_profile=lambda fl, **kw: {"ok": True, "n_ep": 2, "avg_gap_s": 10.0},
    hl_post=hl_mock(fills_b)))
ns3b["_fetch_profile"]("0xb")
assert ns3b["wallet_profiles"]["0xb"].get("truncated") == 1
print("3) межа мс: 2002/2002 філів зібрано; >2000 в одній мс = truncated")

# ── 4. truncated-профіль НЕ відкриває F4 (але F1-F3 живі) ──
fol_ns = load({"follow_on_txs"}, dict(C, STRAT2_ENABLED=True,
    strat2_lock=threading.RLock(), follow_last_close={}, follow_open={}, rev_open={},
    _px_now=lambda c: 10.0, _px_ago=lambda c, s: 10.0,
    is_vault=lambda a: False, _sim_depth=lambda c, s: 1e5,
    wallet_profiles={"0xabc": {"ok": True, "v": 3, "n_ep": 3,
                               "avg_gap_s": 30.0, "truncated": 1,
                               "fetched": time.time()}},
    FOLLOW_TX_PCT=0.05, FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120,
                                       "F3_3хв": 180},
    F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0),
    _profile_request=lambda a: None, REV_WINDOW_S=180))
fol_ns["follow_on_txs"]("0xABC", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x1"}], False)  # $10k
sts = {p["strategy"] for p in fol_ns["follow_open"].values()}
assert "F4_розумний" not in sts and len(sts) == 4, sts   # F1-F3 + F6 (v2.9)
print("4) truncated-профіль: F4 закритий, F1-F3 відкрились")

# ── 5. Персист серіалізований (структурно) ──────────────
st_seg = src[src.index("def save_state"):src.index("def load_state")]
assert "_state_save_lock" in st_seg and "_save_state_locked" in st_seg
pf_seg = src[src.index("def _fetch_profile"):src.index("def run_strat2_loop")]
i_lock = pf_seg.index("_prof_save_lock")
assert pf_seg.index("os.replace", i_lock) > i_lock  # replace ПІД локом
assert pf_seg.index("snapshot = dict", i_lock) > i_lock  # знімок ПІД локом
print("5) state/profiles: знімок+запис+replace під одним локом")

# ── 6. algo_v: версія трекера, а не константа запису ────
row_ns = load({"_rev_row", "_rev_samples"}, dict(C,
    _sim_slip=lambda d: 0.0005, _dt=_dt))
base = {"sig_id": "s", "strategy": "R1", "detect_ts": 0, "coin": "X",
        "side": "LONG", "addr": "0xa", "src": "wallet", "detect_px": 1.0,
        "entry_px": 1.0, "move": 1.5, "dur": 10.0, "sum_usd": 1e5,
        "usd_s": 1e4, "ratio": 3.0, "shtanga": 1, "vault": 0, "hour": 3,
        "btc_move": 0.0, "depth": 1e5, "peak": 1.0, "trough": -1.0,
        "samples": [0.1] * 60}
r_new = row_ns["_rev_row"](dict(base, algo_v="2.3"), 1)
r_old = row_ns["_rev_row"](dict(base), 1)   # відновлений без версії
assert r_new[23] == "2.3" and r_old[23] == "", (r_new[23], r_old[23])
print("6) algo_v з трекера: новий '2.3', відновлений старий -> '' (legacy)")

# ── 7. API: фільтр версії, дедуп outcomes, research-блок ──
d7 = tempfile.mkdtemp()
def wcsv(path, headers, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
m = re.search(r"REV_SIG_HEADERS = \[(.*?)\]", src, re.S)
SIGH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
REVH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
m = re.search(r"FOLLOW_HEADERS = \[(.*?)\]", src, re.S)
FOLH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_OUT_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
OUTH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
def outrow(sig, mag, btc_ok, shtanga, m30, v="2.3"):
    base = {"sig_id": sig, "date": "2026-08-29 10:00:00", "coin": "X",
            "fade_side": "LONG", "whale_addr": "0xa", "src": "wallet",
            "detect_px": 1, "move_3m_pct": mag, "dur_s": 10, "sum_usd": 1e5,
            "usd_s": 1e4, "ratio": 3, "shtanga": shtanga, "vault": 0,
            "hour": 3, "btc_move_pct": 0.01, "btc_ok": btc_ok,
            "would_open": "R1", "depth_usd": 1e5, "costs_pct": 0.15,
            "peak_pct": 1, "trough_pct": -1, "algo_v": v, "eol": "^",
            "dump_move_pct": ""}   # аудит-3: рух епізоду невідомий → research бере move_3m
    return [base.get(h, (m30 if h == "m30" else 0.5)) for h in OUTH]
wcsv(os.path.join(d7, "rev_trades.csv"), REVH, [])
wcsv(os.path.join(d7, "rev_signals.csv"), SIGH, [
    {"sig_id": "s1", "move_3m_pct": 1.5, "btc_ok": 1, "algo_v": "2.3", "eol": "^"},
    {"sig_id": "s2", "move_3m_pct": 1.5, "btc_ok": 0, "algo_v": "2.2", "eol": "^"},
][0:2] and [[r.get(h, 0) for h in SIGH] for r in [
    {"sig_id": "s1", "move_3m_pct": 1.5, "btc_ok": 1, "algo_v": "2.3", "eol": "^"},
    {"sig_id": "s2", "move_3m_pct": 1.5, "btc_ok": 0, "algo_v": "2.2", "eol": "^"}]])
wcsv(os.path.join(d7, "follow_trades.csv"), FOLH, [])
wcsv(os.path.join(d7, "rev_outcomes.csv"), OUTH, [
    outrow("o1", 1.5, 1, 1, 2.0),
    outrow("o1", 1.5, 1, 1, 2.0),          # ДУБЛЬ (краш-рестарт)
    outrow("o2", 1.5, 0, 0, -1.0),
    outrow("o3", 0.7, 1, 1, 0.5),
    outrow("o4", 2.5, 1, 0, 3.0, v="2.2"), # стара версія -> за бортом
])
api_ns = load({"strat2_api", "_median"}, dict(C, TAPE_SINCE=C["DATA_ALGO_V"], STRAT_SINCE={},   # v2.11: емуляція «лише поточна версія»
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, F4_NAME="F4_розумний",
    FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
    STRAT2_DESC={}, STRAT2_TITLES={},
    _strat2_cache={"ts": 0.0, "data": None},
    FOLLOW_OUT_CSV=os.path.join(d7, "follow_outcomes.csv"),
    REV_CSV=os.path.join(d7, "rev_trades.csv"),
    REV_SIG_CSV=os.path.join(d7, "rev_signals.csv"),
    FOLLOW_CSV=os.path.join(d7, "follow_trades.csv"),
    REV_OUT_CSV=os.path.join(d7, "rev_outcomes.csv")))
out = api_ns["strat2_api"]()
assert out["outcomes_total"] == 3, out["outcomes_total"]  # дедуп + версія
assert out["legacy_rows"] == 2, out["legacy_rows"]        # s2 + o4
rs = out["research"]["observed"]   # аудит-3 №4: спостереження окремо від перевірених
assert rs["btc_on"]["n"] == 1 and rs["btc_on"]["median"] == 2.0 - 0.15
assert rs["btc_off"]["n"] == 1 and rs["btc_off"]["median"] == -1.0 - 0.15
assert rs["mag_05_1"]["n"] == 1 and rs["mag_1_2"]["n"] == 2
assert rs["mag_2p"]["n"] == 0
assert rs["shtanga_1"]["n"] == 1 and rs["shtanga_0"]["n"] == 1
assert out["signals_total"] == 1 and out["signals_sub"] == 0
print("7) API: лише поточна версія, дедуп outcomes, research-когорти вірні")

# ── 8. truncated — ТИМЧАСОВИЙ стан: рефетч після TTL ────
calls8 = []
class T8:
    def __init__(self, target=None, args=(), daemon=None): self.t = (target, args)
    def start(self): self.t[0](*self.t[1])
th8 = type(sys)("th"); th8.Thread = T8; th8.Lock = threading.Lock
pr_ns = load({"_profile_request"}, dict(C, strat2_lock=threading.Lock(),
    profiles_fetching=set(),
    wallet_profiles={
        "0xtrunc_old": {"ok": True, "v": 3, "truncated": 1,
                        "fetched": time.time() - 7 * 3600},
        "0xtrunc_new": {"ok": True, "v": 3, "truncated": 1,
                        "fetched": time.time()},
        "0xfull":      {"ok": True, "v": 3, "fetched": time.time()},  # свіжий
    },
    _fetch_profile=lambda a: calls8.append(a)))
pr_ns["threading"] = th8
for a in ("0xTRUNC_OLD", "0xTRUNC_NEW", "0xFULL"):
    pr_ns["_profile_request"](a)
assert calls8 == ["0xtrunc_old"], calls8
# і follow ставить need_profile для truncated (гейт TTL — у _profile_request)
req8 = []
fol8 = load({"follow_on_txs"}, dict(C, STRAT2_ENABLED=True,
    strat2_lock=threading.RLock(), follow_last_close={}, follow_open={}, rev_open={},
    _px_now=lambda c: 10.0, _px_ago=lambda c, s: 10.0,
    is_vault=lambda a: False, _sim_depth=lambda c, s: 1e5,
    wallet_profiles={"0xabc": {"ok": True, "v": 3, "n_ep": 3,
                               "avg_gap_s": 30.0, "truncated": 1,
                               "fetched": time.time()}},
    FOLLOW_TX_PCT=0.05, FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120,
                                       "F3_3хв": 180},
    F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0),
    _profile_request=lambda a: req8.append(a), REV_WINDOW_S=180))
fol8["follow_on_txs"]("0xABC", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x1"}], False)  # $10k
assert req8 == ["0xABC"], req8
print("8) truncated: рефетч після TTL, свіжий і повний — ні; follow питає профіль")

# ── 9. profiles.ok не рахує truncated; UI показує legacy без когорт ──
api_seg = src[src.index("def strat2_api"):]
assert 'not v.get("truncated")' in api_seg
html = open((_HL + "/hyperliquid-terminal.html"),
            encoding="utf-8").read()
assert "else if(legacy||sData.read_errors)" in html and "приховано" in html
print("9) хедер чесний: truncated != ok; legacy-лічильник видно і без когорт")

print("\nУСІ РЕГРЕСІЇ ТРЕТЬОГО АУДИТУ ЗАКРИТІ")

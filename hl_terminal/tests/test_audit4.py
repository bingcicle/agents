import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки четвертого аудиту (v2.3 -> v2.4)."""
import math
import ast, threading, time, json, os, re, sys, tempfile, csv

from v28_shim import with_opens_wrap as _v28_with_opens
from v28_shim import NEW as _V28
SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None):
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

C = dict(PROFILE_ALGO_V=4, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=2, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.4",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005)
T = 1_700_000_000_000

def mkfill(t, px, sz, sp, d="Close Long", coin="AAA", tid=None):
    return {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir": d,
            "coin": coin, "crossed": True, "twapId": None,
            "hash": f"0x{coin}{t}", "tid": tid if tid is not None else t}

bp = load({"_build_profile", "_grade_episode"}, dict(C))
bp["_build_profile"] = _v28_with_opens(bp["_build_profile"])   # v2.18: фікстури лише із закриттями

# ── 1. Одна повільна позиція != два швидкі епізоди ──────
# $2M: злив $1.9M (95%) миттєво, хвіст $100k через 10 хв. Це ОДНЕ
# розвантаження (сценарій аудиту: не ДВА епізоди). v2.8: «повністю»
# = ≥95% від старту, тож епізод один і швидкий; кваліфікації (≥2
# швидких у цьому тесті) все одно немає
slow = [mkfill(T, 1000.0, 1900, 2000), mkfill(T + 600_000, 1000.0, 100, 100)]
p1 = bp["_build_profile"](slow)
assert p1["n_big"] == 1 and p1["n_ep"] == 1 and not p1["ok"], p1
# а РЕОПЕН (нова позиція) досі ділить: два швидкі повні зливи
reop = [mkfill(T, 1000.0, 200, 200, coin="BBB"),
        mkfill(T + 400_000, 1000.0, 200, 200, coin="BBB", tid=T + 1)]
p1b = bp["_build_profile"](reop)
assert p1b["n_ep"] == 2 and p1b["ok"], p1b
# два шматки ОДНІЄЇ позиції в межах 5 хв — один епізод, кваліфікується
fast = [mkfill(T, 1000.0, 100, 200, coin="CCC"),
        mkfill(T + 100_000, 1000.0, 100, 100, coin="CCC")]
p1c = bp["_build_profile"](fast)
assert p1c["n_ep"] == 1, p1c
print("1) епізод = позиція: повільний хвіст не створює другий 'швидкий' епізод")

# ── 2. Malformed close-філ -> fail-closed (err, не ok) ──
good2 = ([mkfill(T, 2.0, 50000, 100000, coin="DDD"),
          mkfill(T + 60_000, 2.0, 50000, 50000, coin="DDD")] +
         [mkfill(T + 10**7, 2.0, 60000, 60000, coin="EEE")])
ok2 = bp["_build_profile"](good2)
assert ok2["ok"] and ok2["n_ep"] == 2
p2 = bp["_build_profile"](good2 + [mkfill(T + 2 * 10**7, "not-a-number",
                                          10, 100, coin="FFF")])
assert not p2["ok"] and p2.get("err") == 1 and p2.get("bad_rows") == 1, p2
print("2) битий філ: профіль = 'невідомо' (err+TTL), а не тиха кваліфікація")

# ── 3. Композитний ключ дедупу: різні монети з одним tid ──
fk = load({"_fill_key"})["_fill_key"]
a = mkfill(T, 2.0, 1, 1, coin="HYPE", tid=777)
b = mkfill(T + 5, 2.0, 1, 1, coin="ZEC", tid=777)
assert fk(a) != fk(b)
assert fk(a) == fk(dict(a))   # стабільний для того самого філа
print("3) tid-ключ композитний (time, coin, tid): чужий tid не з'їдає філ")

# ── 4. Здоровий профіль СТАРІЄ: рефетч після PROFILE_TTL_S ──
calls4 = []
class T4:
    def __init__(self, target=None, args=(), daemon=None): self.t = (target, args)
    def start(self): self.t[0](*self.t[1])
th4 = type(sys)("th"); th4.Thread = T4; th4.Lock = threading.Lock
pr = load({"_profile_request"}, dict(C, strat2_lock=threading.Lock(),
    profiles_fetching=set(),
    wallet_profiles={
        "0xok_old":  {"ok": True,  "v": 4, "fetched": time.time() - 25 * 3600},
        "0xok_new":  {"ok": True,  "v": 4, "fetched": time.time()},
        "0xneg_old": {"ok": False, "v": 4, "fetched": time.time() - 100 * 86400},
        "0xerr_new": {"ok": False, "v": 4, "err": 1, "fetched": time.time()},
    },
    _fetch_profile=lambda a: calls4.append(a)))
pr["threading"] = th4
for a in ("0xOK_OLD", "0xOK_NEW", "0xNEG_OLD", "0xERR_NEW"):
    pr["_profile_request"](a)
assert calls4 == ["0xok_old", "0xneg_old"], calls4
# follow теж просить рефреш для здорового, але старого профілю
req4 = []
fol4 = load({"follow_on_txs"}, dict(C, STRAT2_ENABLED=True,
    strat2_lock=threading.RLock(), follow_last_close={}, follow_open={}, rev_open={},
    _px_now=lambda c: 10.0, _px_ago=lambda c, s: 10.0,
    is_vault=lambda a: False, _sim_depth=lambda c, s: 1e5,
    wallet_profiles={"0xabc": {"ok": True, "v": 4, "n_ep": 3,
                               "avg_gap_s": 30.0,
                               "fetched": time.time() - 25 * 3600}},
    FOLLOW_TX_PCT=0.05, FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120,
                                       "F3_3хв": 180},
    F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0),
    _profile_request=lambda a: req4.append(a), REV_WINDOW_S=180))
fol4["follow_on_txs"]("0xABC", "X",
    {"size": 100.0, "side": "LONG", "ratio": 3, "val": 1e6},
    [{"sz": 10.0, "px": 1000.0, "hash": "0x1"}], False)  # $10k
assert req4 == ["0xABC"], req4
sts4 = {p["strategy"] for p in fol4["follow_open"].values()}
assert "F4_розумний" in sts4   # кешем ще користуємось, рефреш фоном
print("4) профілі старіють: ok/негативний рефетчиться після TTL, кеш працює")

# ── 5. CSV: обірваний хвіст закривається, дедуп бере ОСТАННІЙ ──
csvw = load({"_strat_csv_append"}, dict(C, _csv_lock=threading.Lock()))
d5 = tempfile.mkdtemp()
p5 = os.path.join(d5, "t.csv")
open(p5, "w", newline="").write("sig_id,m30,algo_v\r\n")
with open(p5, "a") as f:
    f.write("s1,-5")   # обірваний минулим ENOSPC рядок БЕЗ \n
assert csvw["_strat_csv_append"](p5, ["sig_id", "m30", "algo_v"],
                                 ["s1", -5, "2.4"]) is True
rows5 = list(csv.DictReader(open(p5, newline="")))
# битий огризок лишився ОКРЕМИМ рядком, повний ретрай — цілий
assert rows5[-1] == {"sig_id": "s1", "m30": "-5", "algo_v": "2.4"}, rows5
print("5) ретрай не клеїться до обірваного рядка")

# ── 6. API: btc_na окремо; вето лише виміряне; keep-last дедуп ──
m = re.search(r'REV_SIG_HEADERS = \[(.*?)\]', src, re.S)
SIGH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
REVH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
m = re.search(r"FOLLOW_HEADERS = \[(.*?)\]", src, re.S)
FOLH = eval("[" + m.group(1) + "]")
m = re.search(r'REV_OUT_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', src, re.S)
OUTH = eval("[" + m.group(1) + "]") + [f"m{i}" for i in range(1, 61)]
d6 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d6, name), "w", newline="") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def outrow(sig, mag, btc_move, btc_ok, m30):
    base = {"sig_id": sig, "date": "2026-08-30 10:00:00", "coin": "X",
            "fade_side": "LONG", "whale_addr": "0xa", "src": "wallet",
            "detect_px": 1, "move_3m_pct": mag, "dur_s": 10, "sum_usd": 1e5,
            "usd_s": 1e4, "ratio": 3, "shtanga": 1, "vault": 0, "hour": 3,
            "btc_move_pct": btc_move, "btc_ok": btc_ok, "would_open": "R1",
            "depth_usd": 1e5, "costs_pct": 0.15, "peak_pct": 1,
            "trough_pct": -1, "algo_v": "2.4", "eol": "^",
            "dump_move_pct": ""}   # аудит-3: рух епізоду невідомий → research бере move_3m
    return [base.get(h, (m30 if h == "m30" else 0.5)) for h in OUTH]
wcsv("rev_trades.csv", REVH, [])
wcsv("rev_signals.csv", SIGH, [[{"sig_id": "s1", "move_3m_pct": 1.5,
    "btc_move_pct": "", "btc_ok": 0, "algo_v": "2.4", "eol": "^"}.get(h, 0)
    for h in SIGH]])   # BTC н/д: НЕ вето
wcsv("follow_trades.csv", FOLH, [])
wcsv("rev_outcomes.csv", OUTH, [
    outrow("oA", 1.5, 0.01, 1, 1.0),    # PASS
    outrow("oB", 1.5, 0.30, 0, -1.0),   # справжнє VETO
    outrow("oC", 1.5, "", 0, -10.0),    # BTC н/д (btc_ok=0!)
    outrow("oD", 1.5, 0.01, 1, 5.55),   # перший ВАЛІДНИЙ запис oD...
    outrow("oD", 1.5, 0.01, 1, 2.0),    # ...і аварійний повтор: ПЕРШИЙ виграє (v2.14)
])
api = load({"strat2_api", "_median"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={}, follow_open={},
    wallet_profiles={}, F4_NAME="F4_розумний",
    FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
    STRAT2_DESC={}, STRAT2_TITLES={},
    _strat2_cache={"ts": 0.0, "data": None},
    FOLLOW_OUT_CSV=os.path.join(d6, "follow_outcomes.csv"),
    REV_CSV=os.path.join(d6, "rev_trades.csv"),
    REV_SIG_CSV=os.path.join(d6, "rev_signals.csv"),
    FOLLOW_CSV=os.path.join(d6, "follow_trades.csv"),
    REV_OUT_CSV=os.path.join(d6, "rev_outcomes.csv")))
out = api["strat2_api"]()
rs = out["research"]["observed"]   # аудит-3 №4: спостереження окремо від перевірених
assert rs["btc_off"]["n"] == 1 and rs["btc_off"]["median"] == -1.0 - 0.15, rs["btc_off"]
assert rs["btc_na"]["n"] == 1 and rs["btc_na"]["median"] == -10.0 - 0.15
assert rs["btc_on"]["n"] == 2   # oA + oD(перший валідний: 5.55)
assert abs(rs["btc_on"]["median"] - ((1.0 - 0.15 + 5.55 - 0.15) / 2)) < 1e-9
assert out["btc_veto"] == 0 and out["btc_unknown"] == 1   # сигнал s1: н/д
assert out["outcomes_total"] == 4 and out["dup_rows"] == 1
print("6) BTC: PASS/VETO/UNKNOWN розділені; дубль-ключ бере ПЕРШИЙ валідний запис (v2.14)")

# ── 7. profiles.ok = ті самі умови, що f4_ok ────────────
i_p = src.index('def _prof_ok')   # v2.9: умови у спільному хелпері
seg = src[i_p:src.index('out["profiles"]', i_p) + 700]
seg = src[src.index('def _pstat(v)'):src.index('out["profiles"] = {"total"') + 900]   # v2.18: статуси профілів
for cond in ('PROFILE_ALGO_V', 'v.get("err")',
             'v.get("truncated")', 'PROFILE_HARD_TTL_S',
             '"uncertain"', '"migration"'):
    assert cond in seg, cond
print("7) хедер 'Профілів F4' рахує лише реально придатні зараз")

# ── 8. Спільна когорта кривої ───────────────────────────
apiseg = src[src.index("def strat2_api"):]
assert '"curve_common"' in apiseg and '"curve_common_n"' in apiseg
html = open((_HL + "/hyperliquid-terminal.html"),
            encoding="utf-8").read()
assert "curve_common_n" in html and "спільна когорта" in html
assert "різні набори угод" in html   # чесний підпис у фолбеку
print("8) крива: спільна когорта (ті самі угоди) або чесний підпис фолбеку")

print("\nУСІ РЕГРЕСІЇ ЧЕТВЕРТОГО АУДИТУ ЗАКРИТІ")

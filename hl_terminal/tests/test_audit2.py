import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки другого аудиту (v2.1 -> v2.2)."""
import math
import ast, threading, time, json, os, re, sys, tempfile

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

C = dict(PROFILE_ALGO_V=2, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600, F4_MIN_EPISODES=2,
         F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0, F4_CHUNK_PCT=0.05,
         PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.2")

# ── 1+2. Старі профілі (без "v" / err-less negative) перераховуються ──
calls = []
class T:
    def __init__(self, target=None, args=(), daemon=None): self.t=(target,args)
    def start(self): self.t[0](*self.t[1])
thmod = type(sys)("th"); thmod.Thread = T; thmod.Lock = threading.Lock
ns = load({"_profile_request"}, dict(C, strat2_lock=threading.Lock(),
    profiles_fetching=set(),
    wallet_profiles={
        "0xlegacyok":  {"ok": True,  "n_ep": 2, "avg_gap_s": 10.0, "fetched": 1},   # v2.0, без v
        "0xlegacyneg": {"ok": False, "n_ep": 0, "avg_gap_s": 0.0,  "fetched": 1},   # v2.0 timeout, без err/v
        "0xv2ok":      {"ok": True,  "v": 2, "n_ep": 2, "avg_gap_s": 10.0,
                        "fetched": time.time()},  # свіжий: v2.4 — здорові профілі старіють за TTL
        "0xv2errold":  {"ok": False, "v": 2, "err": 1, "fetched": time.time() - 7*3600},
        "0xv2errnew":  {"ok": False, "v": 2, "err": 1, "fetched": time.time()},
    },
    _fetch_profile=lambda a: calls.append(a)))
ns["threading"] = thmod
for a in ("0xLEGACYOK", "0xLEGACYNEG", "0xV2OK", "0xV2ERROLD", "0xV2ERRNEW"):
    ns["_profile_request"](a)
assert calls == ["0xlegacyok", "0xlegacyneg", "0xv2errold"], calls
print("1) старі v2.0-профілі (обидва типи) перераховуються; валідний v2 і свіжий err — ні")

# follow не відкриває F4 на профілі без v
seg = src[src.index("def follow_on_txs"):src.index("def _profile_request")]
assert 'prof.get("v") == PROFILE_ALGO_V' in seg and "prof_valid" in seg
print("2) follow: F4 лише при профілі поточної версії")

# ── 3. Реопен ділить епізоди навіть із короткою паузою ──
ns3 = load({"_build_profile", "_grade_episode"}, dict(C))
ns3["_build_profile"] = _v28_with_opens(ns3["_build_profile"])   # v2.18: фікстури лише із закриттями
def unload(coin, t0, start=100000.0):
    out = []
    rem = start
    for i in range(10):
        out.append({"time": t0 + i * 10_000, "px": 2.0, "sz": start / 10,
                    "startPosition": rem, "dir": "Close Long", "coin": coin,
                    "hash": f"0x{coin}{t0}{i}", "crossed": True, "twapId": None})
        rem -= start / 10
    return out
T0 = 1_700_000_000_000
short_pause = unload("AAA", T0) + unload("AAA", T0 + 130_000)   # пауза 40с, але реопен!
p3 = ns3["_build_profile"](short_pause)
assert p3["ok"] and p3["n_ep"] == 2, p3
print("3) реопен позиції = новий епізод навіть із паузою <5хв")

# ── 4. Фліп: рахується лише закрита частина ──
flips = []
for k, t0 in ((0, T0), (1, T0 + 10**7)):
    # закрито лише $50k (sp=25000*2.0), а виконано $150k
    flips.append({"time": t0, "px": 2.0, "sz": 75000.0, "startPosition": 25000.0,
                  "dir": "Long > Short", "coin": f"C{k}", "hash": f"0xf{k}",
                  "crossed": True, "twapId": None})
p4 = ns3["_build_profile"](flips)
assert not p4["ok"] and p4["n_ep"] == 0, p4   # $50k < порога $100k
print("4) фліп: закрите зрізається до розміру позиції, $50k != $150k")

# ── 5. _px_ago не видає стару ціну за 3-хвилинну ──
ns5 = load({"_px_ago", "_px_now"}, dict(C, px_lock=threading.Lock(), px_hist={}))
now = time.time()
ns5["px_hist"]["X"] = [(now - 900, 100.0), (now - 5, 98.0)]
assert ns5["_px_ago"]("X", 180) is None       # найближчий семпл на 720с далі цілі
ns5["px_hist"]["Y"] = [(now - 240, 100.0), (now - 5, 98.0)]
assert ns5["_px_ago"]("Y", 180) == 100.0       # 60с від цілі — ок (толеранс 90с)
print("5) застаріла ціна не маскується під '3 хвилини тому'")

# ── 6. Не-list відповідь історії = err (ретрай), а не вирок ──
ns6 = load({"_fetch_profile", "_fetch_profile_locked", "_profile_post", "_build_profile", "_grade_episode"}, dict(C,
    strat2_lock=threading.Lock(), profiles_fetching={"0xa"},
    wallet_profiles={}, PROFILES_FILE=os.path.join(tempfile.mkdtemp(), "p.json"),
    hl_post=lambda body, retries=2: {"error": "rate limited"}))
ns6["_fetch_profile"]("0xa")
prof = ns6["wallet_profiles"]["0xa"]
assert prof.get("err") == 1 and prof.get("v") == 2, prof
print("6) '{error:...}' замість історії -> err=1 (ретрай після TTL), v=2")

# ── 7. Запис CSV: ротація старого формату + підтвердження запису ──
import threading as _th
ns7 = load({"_strat_csv_append"}, dict(C, _csv_lock=_th.Lock()))
d = tempfile.mkdtemp()
path = os.path.join(d, "t.csv")
open(path, "w").write("old_a,old_b\n1,2\n")
ok = ns7["_strat_csv_append"](path, ["new_a", "new_b", "new_c"], [1, 2, 3])
assert ok is True
legacy = [f for f in os.listdir(d) if "legacy" in f]
assert len(legacy) == 1, legacy
head = open(path).readline().strip()
assert head == "new_a,new_b,new_c"
bad = ns7["_strat_csv_append"](d, ["a"], [1])   # шлях-директорія -> збій
assert bad is False
print("7) ротація legacy-формату + повернення успіху запису")

# ── 8. Цикл: трекер видаляється лише після успішного запису ──
loop = src[src.index("def run_strat2_loop"):src.index("# ── API")]
assert "written_rev" in loop and "written_fol" in loop
assert "del rev_open[pid]" not in loop and "del follow_open[fid]" not in loop
assert "rev_open.pop(pid, None)" in loop
print("8) commit-before-delete у циклі")

# ── 9. Outcome від 0.5%, стратегії від 1% ──
rev = src[src.index("def rev_on_close"):src.index("def _rev_samples")]
assert "REV_OUT_MIN_MAG" in rev and "below_threshold = (mag + 1e-9 < REV_EP_MIN_MAG)" in rev
assert "strats = []" in rev
print("9) outcome-стрічка від 0.5%, стратегії від 1%")

# ── 10. follow: BTC н/д -> порожньо в CSV ──
fol = src[src.index("def follow_on_txs"):src.index("def _profile_request")]
assert "else None" in fol
loop2 = src[src.index("def _fol_row"):]   # v2.16: рядок follow — у _fol_row
assert 'round(_bm, 3) if _bm is not None else ""' in loop2
print("10) follow: btc_move н/д пишеться порожнім")

# ── 11. API: curve_n і тіні окремо ──
api = src[src.index("def strat2_api"):]
assert '"curve_n"' in api and '"shadow_open"' in api
assert 'not p["strategy"].startswith("_")' in api  # v2.5: тіні = всі _-стратегії
print("11) API: покриття хвилин + тіні не в 'відкрито зараз'")

# ── 12. rev_outcomes.csv у списку міграції; algo_v у заголовках ──
assert '"*.csv"' in src[:3500] and '"state.json.bak"' in src[:3500]   # v2.14: міграція — усі CSV/legacy/state
for h in ("REV_SIG_HEADERS", "REV_HEADERS", "FOLLOW_HEADERS", "REV_OUT_HEADERS"):
    i = src.index(h + " =")
    assert '"algo_v"' in src[i:i + 900], h
print("12) міграція outcome-файла + algo_v у всіх CSV")
print("\nУСІ РЕГРЕСІЇ ДРУГОГО АУДИТУ ЗАКРИТІ")

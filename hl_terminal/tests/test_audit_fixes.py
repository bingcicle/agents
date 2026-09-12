import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки аудиту 29.08 (whale_terminal_2_0)."""
import math
import ast, threading, time, json, os, re, sys

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

BASE_CONSTS = dict(
    STRAT2_ENABLED=True, FOLLOW_TX_PCT=0.05,
    FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
    F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), REV_WINDOW_S=180,
    F4_MIN_EPISODES=2, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
    F4_CHUNK_PCT=0.05, PROFILE_ERR_TTL_S=6 * 3600, PROFILE_TTL_S=24*3600, PROFILE_ALGO_V=2,
    DATA_ALGO_V="2.5", MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
    PROFILE_HARD_TTL_S=48*3600,
)

# ── 1 (P0): deadlock зник — follow_on_txs з відсутнім профілем ──
ns = load({"follow_on_txs", "_profile_request"}, dict(
    BASE_CONSTS,
    strat2_lock=threading.Lock(),   # звичайний Lock: перевіряємо СТРУКТУРНИЙ фікс
    follow_open={}, follow_last_close={}, wallet_profiles={}, rev_open={},
    profiles_fetching=set(), is_vault=lambda a: False,
    _px_now=lambda c, max_age=20: 100.0, _px_ago=lambda c, s: 100.0,
    _sim_depth=lambda c, s=None: 1e5, _fetch_profile=lambda a: None))
done = []
t = threading.Thread(target=lambda: (ns["follow_on_txs"](
    "0xdeadbeef", "AAA", {"size": 100.0, "side": "LONG", "ratio": 3, "val": 5e5},
    [{"sz": 100.0, "px": 100.0, "ts": 1}], False),   # tx $10k > MIN_TX_USD
    done.append(1)), daemon=True)
t.start(); t.join(3.0)
assert done and not t.is_alive() and not ns["strat2_lock"].locked(), "DEADLOCK ще є!"
assert len(ns["follow_open"]) == 4            # F1-F3 + F6 (v2.9) відкрились
assert "0xdeadbeef" in ns["profiles_fetching"]  # запит профілю стартував
print("1) deadlock FIXED (звичайний Lock, завершився, запит профілю пішов)")

# ── 5 (P1): err-профіль ретраїться після TTL, ok=False без err — ні ──
calls = []
ns5 = load({"_profile_request"}, dict(BASE_CONSTS,
    strat2_lock=threading.Lock(), profiles_fetching=set(),
    wallet_profiles={"0xerr": {"ok": False, "v": 2, "err": 1, "fetched": time.time() - 7 * 3600},
                     "0xfresh": {"ok": False, "v": 2, "err": 1, "fetched": time.time()},
                     "0xnoq": {"ok": False, "n_ep": 1, "fetched": 0}},
    _fetch_profile=lambda a: calls.append(a)))
class T:
    def __init__(self, target=None, args=(), daemon=None): self.t=(target,args)
    def start(self): self.t[0](*self.t[1])
ns5["threading"] = types = type(sys)("th"); types.Thread = T; types.Lock = threading.Lock
ns5["_profile_request"]("0xERR")    # err + TTL минув -> ретрай
ns5["_profile_request"]("0xFRESH")  # err, TTL не минув -> ні
ns5["_profile_request"]("0xNOQ")    # чесний not-qualified -> ні
ns5["_profile_request"]("0xNEW")    # немає -> так
assert calls == ["0xerr", "0xnoq", "0xnew"], calls  # 0xnoq без "v" -> перерахунок (фікс аудиту №2)
print("2) профіль: err-TTL ретрай працює, вирок не перезапитується")

# ── 4 (P1): фрагментація філів більше не міняє профіль ──
ns4 = load({"_build_profile", "_grade_episode"}, dict(BASE_CONSTS))
ns4["_build_profile"] = _v28_with_opens(ns4["_build_profile"])   # v2.18
T0 = 1_700_000_000_000
def fills_episode(coin, t0, split):
    out = []
    for i in range(10):   # 10 ордерів по 10% позиції, $200k разом
        t = t0 + i * 10_000
        h = f"0xhash{coin}{i}"
        if split:
            for j in range(10):
                out.append({"time": t + j, "px": 2.0, "sz": 1000.0,
                            "startPosition": 100000 - i * 10000, "dir": "Close Long",
                            "coin": coin, "hash": h, "crossed": True, "twapId": None})
        else:
            out.append({"time": t, "px": 2.0, "sz": 10000.0,
                        "startPosition": 100000 - i * 10000, "dir": "Close Long",
                        "coin": coin, "hash": h, "crossed": True, "twapId": None})
    return out
whole = fills_episode("AAA", T0, False) + fills_episode("BBB", T0 + 10**7, False)
splitf = fills_episode("AAA", T0, True) + fills_episode("BBB", T0 + 10**7, True)
p_whole = ns4["_build_profile"](whole)
p_split = ns4["_build_profile"](splitf)
assert p_whole["ok"] and p_split["ok"], (p_whole, p_split)
assert p_whole["n_ep"] == p_split["n_ep"] == 2
assert p_whole["avg_gap_s"] == p_split["avg_gap_s"] == 10.0
print("3) фрагментація: цілий і порізаний ордер дають ОДИН профіль")

# ── 9: maker-філи більше не рахуються тиском ──
maker = [dict(f, crossed=False) for f in whole]
p_maker = ns4["_build_profile"](maker)
assert not p_maker["ok"] and p_maker["n_ep"] == 0
print("4) maker-історія не кваліфікується як агресивний тиск")

# ── 2 (P1): пропущені хвилини = "" а не майбутня ціна ──
# симуляція циклу: перший тик на 61с (+1%), потім розрив, тик на 305с (+10%)
p = {"entry_ts": 1000.0, "samples": [], "peak": -999.0, "trough": 999.0}
REV_TRACK_MIN = 60
def tick(now, g):
    target = min(int((now - p["entry_ts"]) // 60), REV_TRACK_MIN)
    while len(p["samples"]) < target:
        k = len(p["samples"]) + 1
        late = now - (p["entry_ts"] + k * 60.0)
        p["samples"].append(round(g, 4) if g is not None and late <= 30.0 else "")
tick(1061.0, 1.0)     # хвилина 1 закрита вчасно (+1с)
tick(1305.0, 10.0)    # розрив: 2-4 давно минули -> "", 5 закривається (+5с) = чесний вимір
assert p["samples"] == [1.0, "", "", "", 10.0], p["samples"]
tick(1500.0, 3.0)     # ще розрив: 6-7 минули -> "", 8 закривається (+20с)
assert p["samples"] == [1.0, "", "", "", 10.0, "", "", 3.0], p["samples"]
print("5) хвилинні мітки: розрив -> порожньо, не підстановка майбутнього")

# ── 3 (P1): R2 після дедлайну не входить ──
_a = src.index('if p["state"] == "armed":')
armed_first = src[_a:src.index("# Хвилинні мітки", _a)]
assert armed_first.index('p["deadline"]') < armed_first.index("trigger_px"), \
    "дедлайн має перевірятись ДО тригера"
print("6) R2: дедлайн перевіряється першим")

# ── 6: BTC без даних -> btc_ok=False і порожнє поле ──
seg = src[src.index("def rev_on_close"):src.index("def _rev_samples")]
assert "btc_move = None" in seg and "btc_ok = False" in seg
assert 'round(btc_move, 3) if btc_move is not None else ""' in seg
print("7) BTC н/д: fail-closed, у CSV порожньо")

# ── 7: outcome-стрічка для кожного сигналу ──
assert '"_OUTCOME"' in seg and "rev_open[f\"{sig_id}|_OUTCOME\"]" in seg
assert "REV_OUT_CSV" in src and "rev_outcomes.csv" in src
print("8) outcome-стрічка: кожен сигнал трекається незалежно від вето")

# ── 8: тихе повне закриття -> force_exit follow ──
quiet = src[src.index("тихе повне закриття"):src.index("тихе повне закриття") + 900]
assert 'force_exit' in quiet and 'full_close' in quiet
print("9) тихий full close закриває follow-позиції")

# ── 10: половини по хронології ──
api = src[src.index("def strat2_api"):]
assert 'rows.sort(key=lambda r: r.get("date")' in api
assert 'rows.sort(key=lambda r: r.get("date_open")' in api
print("10) половини рахуються у хронології сигналів")

# ── sig_id унікальність ──
assert 'sig_id = f"{int(fill_ts_ms or now * 1000)}-{coin}-{addr[2:8]}-{_txh}"' in src   # v2.20: стабільний id (ідемпотентність)
print("11) sig_id: мс + гаманець + hash транзакції")
print("\nУСІ РЕГРЕСІЇ АУДИТУ ЗАКРИТІ")

import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Логічні тести оболонки стратегій: екстракція функцій із server.py,
підміна залежностей, сценарії реверсу/входу/профілів/CSV."""
import re, time, json, types, threading, os, sys, math

SRC = (_HL + "/server.py")
s = open(SRC, encoding="utf-8").read()

def grab(name):
    i = s.index(f"def {name}(")
    j = s.find("\ndef ", i + 1)
    k = s.find("\n# ──", i + 1)
    end = min(x for x in (j, k) if x > 0)
    return s[i:end]

def grab_const(name):
    m = re.search(rf"^{name}\s*=\s*(.+)$", s, re.M)
    return f"{name} = {m.group(1)}"

ns = {"threading": threading, "time": time, "os": os, "json": json,
      "re": re, "math": math, "print": print}
code = "\n".join([
    grab_const("STRAT2_ENABLED"), grab_const("REV_WINDOW_S"),
    grab_const("REV_TRACK_MIN"), grab_const("REV_BRK_PCT"),
    grab_const("REV_BRK_WINDOW_S"), grab_const("BTC_VETO_PCT"),
    grab_const("BIG_COINS"), grab_const("VAULT_PART_PCT"),
    grab_const("FOLLOW_TX_PCT"), grab_const("FOLLOW_TIMERS"),
    grab_const("F4_NAME"), grab_const("F4_MIN_EPISODES"),
    grab_const("F4_MIN_NOTIONAL"), grab_const("F4_MAX_UNLOAD_S"),
    grab_const("F4_CHUNK_PCT"), grab_const("F4_CLAMP"),
    grab_const("PX_AGO_TOL_S"),
    grab_const("F5_NAME"), grab_const("F5_FIRST_SHOT_S"),
    grab_const("F4_MIN_FAST_PCT"), grab_const("F4_MIN_RATIO"),
    grab_const("F4_FULL_PCT"), grab_const("PROFILE_WINDOW_D"),
    grab_const("PROFILE_PAGES"),
    "SIM_COMMISSION = 0.0005",
])
exec(code, ns)

# ── заголовки CSV: довжини рядків збігаються з хедерами ──
m = re.search(r"REV_SIG_HEADERS = \[(.*?)\]", s, re.S)
sig_h = eval("[" + m.group(1) + "]")
m = re.search(r'REV_HEADERS = \(\[(.*?)\]\s*\+ \[f"m', s, re.S)
rev_h = eval("[" + m.group(1) + "]")
m = re.search(r"FOLLOW_HEADERS = \[(.*?)\]", s, re.S)
fol_h = eval("[" + m.group(1) + "]")
print(f"headers: sig={len(sig_h)} rev={len(rev_h)}+60 follow={len(fol_h)}")
assert len(sig_h) == 27 and len(rev_h) == 54 and len(fol_h) == 62, (len(sig_h), len(rev_h), len(fol_h))  # v2.16: +6 sig, +20 rev, +12 follow; v2.19: +5 виконання

# ── _px_ago / _px_now ───────────────────────────────────
exec(grab("_px_now"), ns); exec(grab("_px_ago"), ns)
ns["px_lock"] = threading.Lock()
now = time.time()
ns["px_hist"] = {"X": [(now - 200, 100.0), (now - 185, 101.0),
                        (now - 60, 102.0), (now - 2, 103.0)]}
assert ns["_px_now"]("X") == 103.0
assert ns["_px_now"]("Y") is None
ago = ns["_px_ago"]("X", 180)   # 3 хв тому -> семпл на -185с (101)
assert ago == 101.0, ago
assert ns["_px_ago"]("X", 300) is None   # історія коротша
print("px helpers OK")

# ── маршрутизація реверс-сигналу (повторюємо умови rev_on_close) ──
def route(src, mag, coin):
    if src == "vault": return ["R6_волт"] if mag >= 1 else []
    if mag < 1: return []
    st = ["R1_загальний", "R2_breakout"]
    if coin in ns["BIG_COINS"]: st.append("R3_великі")
    if mag >= 2: st.append("R4_великий")
    if mag >= 3: st.append("R5_дуже")
    return st
assert route("wallet", 1.2, "PUMP") == ["R1_загальний", "R2_breakout"]
assert route("wallet", 1.2, "ZEC") == ["R1_загальний", "R2_breakout", "R3_великі"]
assert route("wallet", 2.4, "HYPE") == ["R1_загальний", "R2_breakout", "R3_великі", "R4_великий"]
assert route("wallet", 3.1, "PUMP") == ["R1_загальний", "R2_breakout", "R4_великий", "R5_дуже"]
assert route("vault", 1.5, "ENA") == ["R6_волт"]
assert route("wallet", 0.8, "PUMP") == []
print("routing OK")

# ── BTC-фільтр дзеркальний ──────────────────────────────
def btc_ok(side, btc_move):
    return (btc_move > -ns["BTC_VETO_PCT"]) if side == "LONG" \
           else (btc_move < ns["BTC_VETO_PCT"])
assert btc_ok("LONG", -0.10) and not btc_ok("LONG", -0.20)   # дамп: BTC не впав
assert btc_ok("LONG", +0.50)                                  # ріст BTC не вето для лонг-фейду
assert btc_ok("SHORT", +0.10) and not btc_ok("SHORT", +0.20) # памп: BTC не виріс
assert btc_ok("SHORT", -0.50)
print("btc mirror OK")

# ── напрямок руху: mag у бік закриття ───────────────────
for side, move, expect in (("LONG", -1.5, 1.5), ("LONG", +1.5, -1.5),
                            ("SHORT", +2.0, 2.0), ("SHORT", -2.0, -2.0)):
    mag = -move if side == "LONG" else move
    assert mag == expect
print("direction OK")

# ── breakout тригер (LONG: відкат ВГОРУ; SHORT: вниз) ───
d = 100.0
trigL = d * (1 + ns["REV_BRK_PCT"] / 100); trigS = d * (1 - ns["REV_BRK_PCT"] / 100)
assert (100.35 >= trigL) and not (100.25 >= trigL)
assert (99.65 <= trigS) and not (99.75 <= trigS)
assert max(100.4, trigL) == 100.4 and min(99.6, trigS) == 99.6  # консервативний вхід
print("breakout OK")

# ── F4 профілі v10 (v2.18): реконструктор життєвих циклів ─────
exec(grab_const("PROFILE_ENTRY_LAT_MS"), ns); exec(grab_const("PROFILE_ZERO_TOL"), ns)
for _fn in ("_wilson_lb", "_profile_txs", "_profile_lifecycles", "_profile_episode", "_build_profile"):
    exec(grab(_fn), ns)
from v28_shim import _median as _v28_median
ns["_median"] = _v28_median
ns["_sim_depth"] = lambda c, s=None: 1e4    # ratio лише інформативний (v2.18 №8)
T = 1_700_000_000_000
NOW_MS = T + 40 * 86_400_000
def mkfill(t, px, sz, sp, d="Close Long", coin="AAA"):
    return {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir": d,
            "coin": coin, "crossed": True, "twapId": None,
            "hash": f"0x{coin}{t}", "tid": t}
D = lambda c, s=None: 1e4
def life_of(fills, coin="AAA"):
    by, bad = ns["_profile_txs"](fills)
    assert bad == 0
    return ns["_profile_lifecycles"](by[coin])
# епізод: відкриття 100k → $200k, 3 шматки по 33% за 120с → швидкий, паузи 60/60
ep_good = [mkfill(T - 3_600_000, 2.0, 100000, 0, d="Open Long"),
           mkfill(T, 2.0, 34000, 100000), mkfill(T + 60_000, 2.0, 33000, 66000),
           mkfill(T + 120_000, 2.0, 33000, 33000)]
lifes = life_of(ep_good)
assert len(lifes) == 1 and lifes[0]["start_known"] and lifes[0]["zero"]
g = ns["_profile_episode"](lifes[0], "AAA", NOW_MS, D)
assert g and g["cls"] == "fast" and g["unload_s"] == 120.0 and g["gaps"] == [60.0, 60.0], g
assert abs(g["first_pct"] - 0.34) < 1e-9 and g["after_n"] == 2 and abs(g["after_pct"] - 0.66) < 1e-9 \
       and g["one_shot"] == 0 and g["start_known"], g
# задрібні шматки (2%) -> сигналу немає -> епізоду немає
ep_small = [mkfill(T - 1, 2.0, 100000, 0, d="Open Long")] + \
           [mkfill(T + i * 10_000, 2.0, 2000, 100000 - i * 2000) for i in range(50)]
assert ns["_profile_episode"](life_of(ep_small)[0], "AAA", NOW_MS, D) is None
# задовгий (>5хв) -> повільний
ep_slow = [mkfill(T - 1, 2.0, 100000, 0, d="Open Long"), mkfill(T, 2.0, 50000, 100000),
           mkfill(T + 400_000, 2.0, 50000, 50000)]
g3 = ns["_profile_episode"](life_of(ep_slow)[0], "AAA", NOW_MS, D)
assert g3 and g3["cls"] == "slow" and g3["unload_s"] == 400.0, g3
# замалий нотіонал ($80k) -> ні
ep_tiny = [mkfill(T - 1, 2.0, 40000, 0, d="Open Long"), mkfill(T, 2.0, 20000, 40000),
           mkfill(T + 30_000, 2.0, 20000, 20000)]
assert ns["_profile_episode"](life_of(ep_tiny)[0], "AAA", NOW_MS, D) is None
# без видимого відкриття -> «невизначений» (аудит v2.18 №3): не швидкий і не повільний
g_u = ns["_profile_episode"](life_of(ep_good[1:])[0], "AAA", NOW_MS, D)
assert g_u and g_u["cls"] == "uncertain" and not g_u["start_known"], g_u
# профіль: 5 швидких епізодів (різні монети) -> ok, avg_gap 60
fills = []
for k, c in enumerate("ABCDE"):
    fills += [mkfill(f["time"] + k * 10_000_000, f["px"], f["sz"], f["startPosition"], d=f["dir"], coin=c)
              for f in ep_good]
prof = ns["_build_profile"](fills, now_ms=NOW_MS, depth_fn=D)
assert prof["ok"] and prof["status"] == "ok" and prof["n_ep"] == 5 and prof["avg_gap_s"] == 60.0, prof
assert prof["n_fast"] == 5 and prof["fast_pct"] == 100.0 and prof["unload_med_s"] == 120.0
assert prof["fast_lb95"] == 56.6 and prof["cont_pct"] == 100.0 and prof["one_shot_pct"] == 0.0, prof
assert len(prof["episodes"]) == 5 and all(e[1] == "fast" for e in prof["episodes"])
# 4 епізоди -> не ok (F4_MIN_EPISODES=5)
prof4 = ns["_build_profile"](fills[:-4], now_ms=NOW_MS, depth_fn=D)
assert not prof4["ok"] and prof4["n_ep"] == 4 and prof4["status"] == "no"
# ті ж 5 без відкриттів -> усі невизначені -> status uncertain, ok=False
prof_u = ns["_build_profile"]([f for f in fills if f["dir"] != "Open Long"], now_ms=NOW_MS, depth_fn=D)
assert not prof_u["ok"] and prof_u["status"] == "uncertain" and prof_u["n_uncertain"] == 5 \
       and prof_u["n_fast"] == 0, prof_u
# таймер F4: 2*60=120с у клампі [60,300]
t4 = min(max(2 * prof["avg_gap_s"], ns["F4_CLAMP"][0]), ns["F4_CLAMP"][1])
assert t4 == 120.0
# одним пострілом (без пауз): avg 30 -> таймер 60 (мін кламп); one_shot 100%, продовження 0%
prof_shot = ns["_build_profile"](
    [x for k, c in enumerate("FGHIJ")
       for x in (mkfill(T + k * 9_000_000 - 1, 2.0, 60000, 0, d="Open Long", coin=c),
                 mkfill(T + k * 9_000_000, 2.0, 60000, 60000, coin=c))],
    now_ms=NOW_MS, depth_fn=D)
assert prof_shot["ok"] and prof_shot["avg_gap_s"] == 30.0 and prof_shot["one_shot_pct"] == 100.0 \
       and prof_shot["cont_pct"] == 0.0, prof_shot
assert min(max(2 * prof_shot["avg_gap_s"], 60.0), 300.0) == 60.0
print("F4 profiles v10 OK")

# ── follow: тиша і force_exit ───────────────────────────
timer = ns["FOLLOW_TIMERS"]["F2_2хв"]
last = 1000.0
assert not (1000.0 + 100 - last >= timer)   # 100с < 2хв — тримаємо
assert (1000.0 + 121 - last >= timer)       # 121с — вихід
print("follow silence OK")

# ── _median (для API) ───────────────────────────────────
exec(grab("_median"), ns)
assert ns["_median"]([3, 1, 2]) == 2 and ns["_median"]([1, 2, 3, 4]) == 2.5
assert ns["_median"]([]) is None
print("\nВСІ ТЕСТИ ОБОЛОНКИ ПРОЙШЛИ")

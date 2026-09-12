import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.8 (ТЗ 01.09): (1) WS форс-реконект при тиші на живому
з'єднанні; (2) профіль швидкості гаманця v7 — 90 днів, епізод від першої
агресивної tx ≥5% при ≥$100k і ratio ≥2, швидкий/повільний, статистика;
(3) F5 «перший постріл»; (4) пагінація/кап 10k; (5) заголовки CSV."""
import ast, threading, time, json, os, re, sys, math, textwrap, tempfile
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from v28_shim import with_opens_wrap as _v28_with_opens
from v28_shim import NEW, _median

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None):
    mod = ast.Module(body=[n for n in tree.body
                           if isinstance(n, ast.FunctionDef) and n.name in names],
                     type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "print": lambda *a, **k: None}
    ns.update(NEW)
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

def const(name):
    m = re.search(rf"^{name}\s*=\s*([^#\n]+)", src, re.M)
    return eval(m.group(1).strip())

# ═══ 1. WS: форс-реконект «протухлого» з'єднання ═══════════════════
WS_STALE_S, WS_STALE_MAX_S = const("WS_STALE_S"), const("WS_STALE_MAX_S")
assert WS_STALE_S == 180 and WS_STALE_MAX_S == 1800
assert WS_STALE_S > 120, "поріг має бути > 120с: інакше після форс-розриву backoff не скинеться до 5с"
i0 = src.index("            def _stale_limit(")
i1 = src.index("            def _stale_fire(", i0)
stale_src = textwrap.dedent(src[i0:i1])
stale_src = re.sub(r"def _stale_limit\([^)]*\):", "def _stale_limit(label):", stale_src)
stale_src = re.sub(r"def _stale\([^)]*\):", "def _stale(label, connected_at):", stale_src)
stale_src = stale_src.replace("limit = _stale_limit()", "limit = _stale_limit(label)")
class _Clock:
    def __init__(s): s.t = 1_000_000.0
    def time(s): return s.t
clk = _Clock()
nsw = {"stats": {}, "time": clk, "WS_STALE_S": 180, "WS_STALE_MAX_S": 1800}
exec(stale_src, nsw)
_stale = nsw["_stale"]
st = nsw["stats"]
c0 = clk.t
# (а) щойно підключились — не тиша, навіть без трейдів
st.clear(); st["ws_subs_A"] = 57
assert not _stale("A", c0)
# (б) 3 хв від конекту, підписки є, трейдів не було НІКОЛИ -> тиша
clk.t = c0 + 181
assert _stale("A", c0)
# (в) без підтверджених підписок — не наша справа (sub_checker)
st["ws_subs_A"] = 0
assert not _stale("A", c0)
st["ws_subs_A"] = 57
# (г) свіжий трейд — живе
st["ws_last_A"] = (clk.t - 10) * 1000
assert not _stale("A", c0)
# (д) останній трейд 200с тому при аптаймі 10 хв -> тиша
clk.t = c0 + 600
st["ws_last_A"] = (clk.t - 200) * 1000
assert _stale("A", c0)
# (е) той самий трейд 200с тому, але стрік=1 (поріг 360с) -> ще ні;
#     трейд скинув стрік -> поріг знову 180с -> тиша (рев'ю: динамічний ліміт)
st["ws_stale_streak_A"] = 1
assert not _stale("A", c0)
st["ws_stale_streak_A"] = 0
assert _stale("A", c0)
st["ws_stale_streak_A"] = 2
assert not _stale("A", c0)
st["ws_stale_streak_A"] = 0
# (є) інше з'єднання (B) з трейдами не маскує тишу A: ключ по label
st["ws_last_B"] = clk.t * 1000
assert _stale("A", c0)
# ескалація порога: 3→6→12→24→30 хв (стеля), скидання — при трейді
esc = [min(WS_STALE_S * (2 ** min(k, 4)), WS_STALE_MAX_S) for k in range(7)]
assert esc == [180, 360, 720, 1440, 1800, 1800, 1800], esc
assert "return min(WS_STALE_S * (2 ** min(sk, 4)), WS_STALE_MAX_S)" in src
j_trade = src.index('stats[f"ws_last_{label}"] = stats["ws_last_ms"]')
j_reset = src.index('stats[f"ws_stale_streak_{label}"] = 0')
assert j_reset > j_trade and j_reset - j_trade < 400, "скидання стріку має стояти у гілці трейда"
assert "stats[sk] = stats.get(sk, 0) + 1" in src   # стрік з ПОТОЧНОГО stats, не зі знімка
assert 'per_conn[f"ws_{lb}_stale_reconnects"]' in src   # видно у /status
# перевірка тиші — ЛИШЕ на службових фреймах (b"" контроль, pong, subscriptionResponse,
# error), і НІКОЛИ перед розбором даних: перший трейд після паузи має дійти до
# ws_trade_fastpath, а не стати приводом для розриву (рев'ю v2.8)
k_nf = src.index("                if not frame:\n")
k_chk = src.index("if _stale():", k_nf)
k_js = src.index("json.loads(frame.decode", k_nf)
assert k_nf < k_chk < k_js
k_trade = src.index('stats["ws_last_ms"] = time.time() * 1000')
assert src.count("if _stale():") == 4 and src.rfind("if _stale():") < k_trade
assert "ws_silent_since_" in src and 'stats.get(f"ws_silent_since_{lb}", 0)' in src
assert "_age = time.time() - max(connected_at, _last)" in src
print("1) WS: тиша на живому з'єднанні -> форс-реконект; ескалація 3..30 хв; скидання трейдом; /status")

# ═══ 2. Профіль швидкості v7 ═══════════════════════════════════════
C = dict(F4_MIN_EPISODES=5, F4_MIN_FAST_PCT=70.0, F4_MIN_NOTIONAL=100_000.0,
         F4_MIN_RATIO=2.0, F4_MAX_UNLOAD_S=300.0, F4_CHUNK_PCT=0.05,
         F4_FULL_PCT=0.95)
bp = load({"_build_profile", "_grade_episode"}, dict(C))
bp["_build_profile"] = _v28_with_opens(bp["_build_profile"])   # v2.18
build = bp["_build_profile"]
NOW = 1_760_000_000_000
D50K = lambda c, s=None: 50_000.0
calls = []
def D_REC(c, s=None):
    calls.append((c, s)); return 50_000.0

def mk(t, px, sz, sp, coin, h, d="Close Long", crossed=True, twap=None):
    return {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir": d,
            "coin": coin, "crossed": crossed, "twapId": twap, "hash": h,
            "tid": t * 7 + (hash(h) % 5)}

def episode(coin, t0, sp, px, chunks, d="Close Long", crossed=None, twap=None):
    """chunks: [(dt_s, frac_of_START)]; sp біжить як на біржі."""
    out, rem = [], float(sp)
    for i, (dt, frac) in enumerate(chunks):
        sz = sp * frac
        out.append(mk(t0 + int(dt * 1000), px, sz, rem, coin, f"0x{coin}{t0}{i}",
                      d=d, crossed=(True if crossed is None else crossed[i]),
                      twap=(None if twap is None else twap[i])))
        rem = max(0.0, rem - sz)
    return out

FAST = [(0, .06), (30, .40), (120, .50)]          # залишок 4% -> кінець на 120с
SLOW = [(0, .06), (600, .94)]                     # 10 хв -> повільний
T0 = NOW - 30 * 86400_000

# (а) 5 швидких на різних монетах -> ok, статистика
fills = sum((episode(c, T0 + k * 3_600_000, 1000, 200.0, FAST)
             for k, c in enumerate("ABCDE")), [])
p = build(fills, now_ms=NOW, depth_fn=D50K)
assert p["ok"] and p["n_fast"] == 5 and p["n_slow"] == 0 and p["n_big"] == 5, p
assert p["fast_pct"] == 100.0 and p["n_ep"] == 5
assert p["unload_med_s"] == 120.0 and p["unload_mean_s"] == 120.0, p
assert p["avg_gap_s"] == 60.0, p          # паузи 30 і 90 між агресивними tx
assert p["n_nodepth"] == 0 and p["n_inprog"] == 0
# (б) 5 швидких + 1 повільний -> 83% ok; + 3 повільних -> 62.5% не ok
slow1 = episode("F", T0 + 9 * 3_600_000, 1000, 200.0, SLOW)
p = build(fills + slow1, now_ms=NOW, depth_fn=D50K)
assert p["ok"] and p["n_slow"] == 1 and p["fast_pct"] == 83.3, p
assert p["unload_all_med_s"] == 120.0 and p["unload_med_s"] == 120.0
slow3 = slow1 + episode("G", T0 + 10 * 3_600_000, 1000, 200.0, SLOW) \
              + episode("H", T0 + 11 * 3_600_000, 1000, 200.0, SLOW)
p = build(fills + slow3, now_ms=NOW, depth_fn=D50K)
assert not p["ok"] and p["n_fast"] == 5 and p["n_slow"] == 3 and p["fast_pct"] == 62.5, p
# (в) 4 швидких — замало навіть при 100%
p = build(fills[:-3], now_ms=NOW, depth_fn=D50K)
assert not p["ok"] and p["n_fast"] == 4 and p["fast_pct"] == 100.0
print("2а) кваліфікація: ≥5 швидких І ≥70%; unload/gap статистика")

# (г) дрібні шматки ДО першої ≥5% не відкривають епізод; старт = перша ≥5%
pre = episode("P", T0, 1000, 200.0, [(0, .03), (40, .06), (100, .91)])
p = build(pre, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 60.0, p   # 100 - 40
# два ордери по 3% в одну мс — ДВІ транзакції, не «6%»
two = [mk(T0, 200.0, 30, 1000, "Q", "0xq1"), mk(T0, 200.0, 30, 970, "Q", "0xq2")]
p = build(two, now_ms=NOW, depth_fn=D50K)
assert p["n_big"] == 0, p
# один ордер, порізаний матчінгом на 2 філи по 3% (той самий hash) — 6%
one = [mk(T0, 200.0, 30, 1000, "Q", "0xq"), mk(T0, 200.0, 30, 1000, "Q", "0xq"),
       mk(T0 + 60_000, 200.0, 940, 940, "Q", "0xq3")]
one[1]["tid"] += 1
p = build(one, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 60.0, p
print("2б) старт епізоду — перша ОДИНИЧНА транзакція ≥5% (групування по hash)")

# (д) гейти: нотіонал <$100k; v2.18 (аудит пошуку швидких гаманців №8): ratio до ПОТОЧНОЇ
# глибини історію більше НЕ гейтить (поведінка не залежить від сьогоднішньої ліквідності;
# ratio-гейт — на момент сигналу у follow_on_txs); глибина лише інформативна
p = build(episode("N", T0, 100, 200.0, FAST), now_ms=NOW, depth_fn=D50K)   # $20k
assert p["n_big"] == 0 and p["n_nodepth"] == 0
p = build(episode("R", T0, 1000, 200.0, FAST), now_ms=NOW,
          depth_fn=lambda c, s=None: 200_000.0)                              # ratio 1 — все одно епізод
assert p["n_big"] == 1
p = build(episode("Z", T0, 1000, 200.0, FAST), now_ms=NOW, depth_fn=lambda c, s=None: 0)
assert p["n_big"] == 1 and p["n_nodepth"] == 0, p
p = build(episode("R2", T0, 1000, 200.0, FAST), now_ms=NOW, depth_fn=lambda c, s=None: 100_000.0)
assert p["n_big"] == 1
p = build(episode("R3", T0, 1000, 200.0, FAST), now_ms=NOW, depth_fn=lambda c, s=None: 100_500.0)
assert p["n_big"] == 1
# глибина береться по СТОРОНІ, що закривається
calls.clear()
build(episode("S", T0, 1000, 200.0, FAST, d="Close Short"), now_ms=NOW, depth_fn=D_REC)
assert calls and all(s == "SHORT" for _, s in calls), calls
calls.clear()
build(episode("S2", T0, 1000, 200.0, FAST, d="Long > Short"), now_ms=NOW, depth_fn=D_REC)
assert calls and all(s == "LONG" for _, s in calls), calls
print("2в) гейти $100k / ratio≥2 (за поточною глибиною сторони) / нема глибини")

# (е) незавершені: триває (<5 хв) — не рахуємо; старе незакрите — повільний
p = build(episode("I", NOW - 60_000, 1000, 200.0, [(0, .06)]), now_ms=NOW, depth_fn=D50K)
assert p["n_big"] == 0 and p["n_inprog"] == 1, p
p = build(episode("J", NOW - 600_000, 1000, 200.0, [(0, .06)]), now_ms=NOW, depth_fn=D50K)
assert p["n_slow"] == 1 and p["n_inprog"] == 0 and p["unload_all_med_s"] is None, p
# незакрите, потім позиція ВИРОСЛА (долив) і злита: v2.18 (аудит №2/№4) — долив не
# створює нової позиції, один життєвий цикл = один епізод (повільний: від сигналу
# на T0 до ≤5% через 2 год); нова позиція — лише через відкриття з нуля
reop = episode("K", T0, 1000, 200.0, [(0, .06), (30, .20)]) \
     + episode("K", T0 + 7_200_000, 1500, 200.0, FAST)
p = build(reop, now_ms=NOW, depth_fn=D50K)
assert p["n_slow"] == 1 and p["n_fast"] == 0 and p["n_lifecycles"] == 1, p
print("2г) триває / старе незакрите / реопен")

# (є) «закрив повністю» рахують і мейкер, і TWAP, і фліп; але епізод
#     відкриває лише агресивна tx
mk_maker = episode("M", T0, 1000, 200.0, [(0, .06), (60, .94)], crossed=[True, False])
p = build(mk_maker, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 60.0 and p["avg_gap_s"] == 30.0, p
tw = episode("W", T0, 1000, 200.0, [(0, .06), (60, .94)], twap=[None, 7])
p = build(tw, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 60.0, p
tw_only = episode("W2", T0, 1000, 200.0, FAST, twap=[1, 1, 1])
p = build(tw_only, now_ms=NOW, depth_fn=D50K)
assert p["n_big"] == 0, p
maker_only = episode("M2", T0, 1000, 200.0, FAST, crossed=[False, False, False])
p = build(maker_only, now_ms=NOW, depth_fn=D50K)
assert p["n_big"] == 0, p
flip = [mk(T0, 200.0, 1500, 1000, "FL", "0xfl", d="Long > Short")]
p = build(flip, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 0.0, p
print("2д) завершення: мейкер/TWAP/фліп рахуються; старт — лише агресивна")

# (ж) битий філ -> err (fail-closed), як і раніше
bad = fills + [mk(T0 + 99, "x", 10, 100, "B9", "0xb9")]
p = build(bad, now_ms=NOW, depth_fn=D50K)
assert p.get("err") == 1 and not p["ok"] and p["bad_rows"] == 1
# (з) дефолти: depth_fn/now_ms беруться з модуля (_sim_depth зі стабу шиму)
p = build(fills)
assert p["n_big"] == 5
print("2е) fail-closed на битих філах; дефолти depth/now")

# ═══ 3. follow_on_txs: F4 + F5 «перший постріл» ═══════════════════
BASE = dict(STRAT2_ENABLED=True, FOLLOW_TX_PCT=0.05,
            FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
            F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), REV_WINDOW_S=180,
            PROFILE_ERR_TTL_S=6 * 3600, PROFILE_TTL_S=24 * 3600, PROFILE_ALGO_V=7,
            PROFILE_HARD_TTL_S=48 * 3600, DATA_ALGO_V="2.8",
            MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0)
PROF_OK = {"ok": True, "v": 7, "fetched": int(time.time()), "n_fast": 6,
           "n_slow": 1, "fast_pct": 85.7, "unload_med_s": 95.0,
           "unload_mean_s": 110.5, "window_d": 61.3, "avg_gap_s": 40.0}
def mkns(prof, last_close=None):
    n = load({"follow_on_txs"}, dict(BASE,
        strat2_lock=threading.RLock(), follow_open={}, rev_open={},
        follow_last_close=({} if last_close is None else dict(last_close)),
        wallet_profiles={"0xabc": dict(prof)}, profiles_fetching=set(),
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: None))
    return n
OLD = {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5}
TX = [{"sz": 100.0, "px": 100.0, "ts": 1}]
def opened(n): return sorted(p["strategy"] for p in n["follow_open"].values())
# (а) перший контакт пари -> F1..F3 + F4 + F5, first_shot=1, пауза порожня
n = mkns(PROF_OK)
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert opened(n) == ["F10_розумний_60", "F1_1хв", "F2_2хв", "F3_3хв", "F4_розумний",
                     "F5_перший", "F6_1хв_перший", "F7_без_ратіо"], opened(n)   # v2.9: F6/F7 1-й постріл; v2.18: F10
f5 = [p for p in n["follow_open"].values() if p["strategy"] == "F5_перший"][0]
assert f5["first_shot"] == 1 and f5["pair_gap"] == "" and f5["timer"] == 60.0   # v2.18: F5 — фіксовані 60 с
f4 = [p for p in n["follow_open"].values() if p["strategy"] == "F4_розумний"][0]
f7 = [p for p in n["follow_open"].values() if p["strategy"] == "F7_без_ратіо"][0]
f10 = [p for p in n["follow_open"].values() if p["strategy"] == "F10_розумний_60"][0]
assert f4["timer"] == 80.0 and f7["timer"] == 80.0 and f10["timer"] == 60.0   # 2×40 / фіксовані 60
assert f4["prof_status"] == "ok"
assert f5["prof"]["n_fast"] == 6 and f5["prof"]["fast_pct"] == 85.7 \
       and f5["prof"]["unload_med_s"] == 95.0 and f5["prof"]["window_d"] == 61.3
assert all(p["first_shot"] == 1 for p in n["follow_open"].values())   # у F1..F4 теж
# (б) пара закривалась 30 хв тому -> F5 мовчить, F4 входить, first_shot=0
n = mkns(PROF_OK, {"0xabc:AAA": time.time() - 1800})
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F5_перший" not in opened(n) and "F4_розумний" in opened(n), opened(n)
f4 = [p for p in n["follow_open"].values() if p["strategy"] == "F4_розумний"][0]
assert f4["first_shot"] == 0 and 1795 <= f4["pair_gap"] <= 1805, f4["pair_gap"]
# (в) пауза ≥1 год -> знову перший постріл
n = mkns(PROF_OK, {"0xabc:AAA": time.time() - 4000})
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F5_перший" in opened(n)
# (г) пауза іншої МОНЕТИ того ж гаманця не рахується: ключ — пара
n = mkns(PROF_OK, {"0xabc:BBB": time.time() - 10})
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F5_перший" in opened(n)
# (д) мітка пари оновлюється навіть коли вхід не відбувся (дрібна tx):
#     наступна транзакція за 10 хв — НЕ перший постріл
n = mkns(PROF_OK)
n["follow_on_txs"]("0xabc", "AAA", OLD, [{"sz": 1.0, "px": 100.0, "ts": 1}], False)
assert not n["follow_open"] and "0xabc:AAA" in n["follow_last_close"]
n["follow_last_close"]["0xabc:AAA"] = time.time() - 600
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F5_перший" not in opened(n) and "F4_розумний" in opened(n)
# (е) профіль не ok (60% швидких) -> ні F4, ні F5; F1..F3 працюють
n = mkns(dict(PROF_OK, ok=False, fast_pct=60.0))
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший"], opened(n)   # v2.9: F6 без профілю
f1 = [p for p in n["follow_open"].values() if p["strategy"] == "F1_1хв"][0]
assert f1["prof"]["fast_pct"] == 60.0   # профіль у рядку і без входу F4
# (є) 10k-кап історії НЕ блокує (ТЗ: «до 10 тисяч філів»); truncated — блокує
n = mkns(dict(PROF_OK, hist_capped=1))
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F4_розумний" in opened(n) and "F5_перший" in opened(n)
n = mkns(dict(PROF_OK, truncated=1))
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F4_розумний" not in opened(n) and "F5_перший" not in opened(n)
# (ж) стара версія профілю (v6) -> не ok до перерахунку
n = mkns(dict(PROF_OK, v=6))
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, False)
assert "F4_розумний" not in opened(n) and not n["follow_open"][next(iter(n["follow_open"]))]["prof"]
# (з) повне закриття: мітка оновлюється, входу немає
n = mkns(PROF_OK)
n["follow_on_txs"]("0xabc", "AAA", OLD, TX, True)
assert not n["follow_open"] and "0xabc:AAA" in n["follow_last_close"]
print("3) F5: перший постріл пари / пауза ≥1 год; F4 без гейта паузи; профіль у кожному рядку")

# ═══ 4. _fetch_profile: 90 днів, 6 сторінок, кап 10k, семафор, проксі ═══
PW, PP = const("PROFILE_WINDOW_D"), const("PROFILE_PAGES")
assert PW == 90 and PP == 6
def page(n, t0):
    return [{"time": t0 + i, "coin": "X", "tid": t0 + i, "px": 1, "sz": 1,
             "startPosition": 1, "dir": "Close Long", "crossed": True,
             "hash": f"0x{t0+i}"} for i in range(n)]
def fetch_ns(pages, extra=None):
    got = {"builds": [], "posts": []}
    it = iter(pages)
    def post(body):
        got["posts"].append(body["startTime"])
        return next(it, [])
    tmpf = tempfile.mktemp(suffix=".json")
    n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key"}, dict(
        PROFILE_ALGO_V=7, PROFILES_FILE=tmpf, strat2_lock=threading.RLock(),
        _prof_save_lock=threading.Lock(), wallet_profiles={},
        profiles_fetching={"0xw"}, _profile_post=post,
        _build_profile=lambda fl, now_ms=None, depth_fn=None:
            got["builds"].append(len(fl)) or {"ok": True, "n_ep": 5, "n_fast": 5,
                                              "n_slow": 0, "fast_pct": 100.0,
                                              "avg_gap_s": 10.0, "unload_med_s": 90.0},
        **(extra or {})))
    n["_got"] = got
    return n
now_ms = int(time.time() * 1000)
# (а) 6 повних сторінок -> усі зчитано, hist_capped, вікно від найстарішого філа
pages = [page(2000, now_ms - 80 * 86400_000 + k * 3_000_000) for k in range(6)]
n = fetch_ns(pages)
t_sleep = time.sleep
time.sleep = lambda s: None
try:
    n["_fetch_profile"]("0xw")
finally:
    time.sleep = t_sleep
prof = n["wallet_profiles"]["0xw"]
assert n["_got"]["builds"] == [12000] and prof.get("hist_capped") == 1, prof
assert prof["n_fills"] == 12000 and 79.9 <= prof["window_d"] <= 80.1, prof["window_d"]
assert not prof.get("truncated") and prof["v"] == 7 and prof["ok"]
assert n["_got"]["posts"][0] <= now_ms - 89 * 86400_000   # старт вікна = 90 днів
assert "0xw" not in n["profiles_fetching"] and n["_profile_sem"]._value == 1
# (б) коротка історія (100 філів) — без капу; вікно = реальна глибина
n = fetch_ns([page(100, now_ms - 10 * 86400_000)])
n["_fetch_profile"]("0xw")
prof = n["wallet_profiles"]["0xw"]
assert not prof.get("hist_capped") and 9.9 <= prof["window_d"] <= 10.1, prof
assert n["_got"]["builds"] == [100]
# (в) >2000 філів в одну мс — truncated (діра), як і раніше
same = [dict(f, time=now_ms - 10 * 86400_000, tid=i) for i, f in enumerate(page(2000, 0))]
n = fetch_ns([same, same])
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"].get("truncated") == 1
# (г) не-list посеред пагінації -> err, ретрай після TTL
n = fetch_ns([page(2000, now_ms - 50 * 86400_000), {"error": "x"}])
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"].get("err") == 1
# (д) _profile_post: проксі є і впала -> сторінка напряму; без проксі — напряму
def fresh(): return dict(_profile_proxy={"dead_until": 0.0}, _prio_proxy_state={"streak": 0, "dead_since": 0.0})
pp = load({"_profile_post"}, dict(fresh(), REST_PROXY="1.2.3.4:1", hl_post=lambda b, retries=4: ["direct"],
                                   hl_post_prio=lambda b, retries=2, direct=False, **kw: (_ for _ in ()).throw(RuntimeError("dead"))))
assert pp["_profile_post"]({"type": "x"}) == ["direct"]
assert pp["_profile_proxy"]["dead_until"] > time.time() + 1000   # мертва на 30 хв
pp = load({"_profile_post"}, dict(fresh(), REST_PROXY="1.2.3.4:1", hl_post=lambda b, retries=4: ["direct"],
                                   hl_post_prio=lambda b, retries=2, direct=False, **kw: ["proxy"]))
assert pp["_profile_post"]({"type": "x"}) == ["proxy"]
pp = load({"_profile_post"}, dict(fresh(), REST_PROXY="", hl_post=lambda b, retries=4: ["direct"]))
assert pp["_profile_post"]({"type": "x"}) == ["direct"]
print("4) історія: 90 днів / 6 сторінок / кап 10k -> hist_capped (не блокує) / truncated / err / проксі-фолбек")

# ═══ 5. CSV/API/UI: заголовки, F5 у циклі API, UI ═══════════════════
m = re.search(r"FOLLOW_HEADERS = \[(.*?)\]", src, re.S)
fol_h = eval("[" + re.sub(r"#[^\n]*", "", m.group(1)) + "]")
assert fol_h[-1] == "eol" and len(fol_h) == 62, (len(fol_h), fol_h[-9:])   # v2.16: +12; v2.18: +6 (статус профілю, невизначені, Вілсон, продовження, one-shot, after)
assert fol_h[-11:-6] == ["exch", "entry_qty", "exit_fill_frac", "funding_pct", "exec_delay_s"]   # v2.19; v2.20: +5 (мітки часу, px_filled, trig_hash)
assert fol_h[-17:-11] == ["prof_status", "prof_n_uncertain", "prof_fast_lb95", "prof_cont_pct",
                        "prof_one_shot_pct", "prof_after_pct_med"], fol_h[-12:-6]
assert fol_h[-38:-29] == ["trade_id", "first_shot", "pair_gap_s", "prof_n_fast",
                      "prof_n_slow", "prof_fast_pct", "prof_unload_med_s",
                      "prof_unload_mean_s", "prof_window_d"], fol_h[-10:-1]
# фінальний рядок дописує рівно ці 8 полів після trade_id (fid)
seg = src[src.index('round(p["profile_gap"], 1), p.get("algo_v", ""), fid,'):]
seg = seg[:seg.index('fol_rows.append((fid, p["final_row"]))')]
assert all(f'pr.get("{k}"' in seg for k in ("n_fast", "n_slow", "fast_pct", "unload_med_s", "unload_mean_s", "window_d")) \
       and 'p.get("first_shot", "")' in seg and 'p.get("pair_gap", "")' in seg
# запис рядка: 33 значення + "^" вартовий
ca = load({"_strat_csv_append"}, dict(_csv_lock=threading.Lock()))
tmp = tempfile.mktemp(suffix=".csv")
assert ca["_strat_csv_append"](tmp, fol_h, list(range(len(fol_h) - 1)))
rows = open(tmp, encoding="utf-8").read().strip().splitlines()
assert rows[0] == ",".join(fol_h) and rows[1].endswith(",^") and rows[1].count(",") == len(fol_h) - 1
assert 'for st in list(FOLLOW_TIMERS) + [F6_NAME, F4_NAME, F5_NAME, F8_NAME,' in src   # v2.11: +F8/F9
assert '"F5_перший":    "За китом · швидкі гаманці · перший постріл · вихід 60 с"' in src   # v2.18
assert '"F10_розумний_60": "За китом · швидкі гаманці · вихід 60 с"' in src
assert "F4_MIN_EPISODES  = 5" in src and "F4_MIN_FAST_PCT  = 70.0" in src
assert "PROFILE_ALGO_V   = 11" in src and 'DATA_ALGO_V      = "2.20"' in src
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
assert "'F5_перший'" in ui and "S_PROF" in ui and "pf_unload" in ui and "first_shot" in ui
print("5) CSV 34 колонки з вартовим; F5 в API/UI; версії 7 / 2.8")

# ═══ 6. Фікси адверсарійного рев'ю v2.8 ═══════════════════════════
# (а) системні філи з нульовим hash (TWAP/ліквідації/ADL) НЕ склеюються в
#     одну «транзакцію» на 90 днів: два епізоди, завершені TWAP-філами
ZH = "0x" + "0" * 64
def twapf(t, px, sz, sp, coin, oid):
    f = mk(t, px, sz, sp, coin, ZH, twap=7); f["oid"] = oid; return f
ep1 = [mk(T0, 200.0, 60, 1000, "TW", "0xa1")] + \
      [twapf(T0 + 30_000 * (i + 1), 200.0, 235, 940 - 235 * i, "TW", 100 + i) for i in range(4)]
ep2 = [mk(T0 + 86_400_000, 200.0, 60, 1000, "TW", "0xa2")] + \
      [twapf(T0 + 86_400_000 + 30_000 * (i + 1), 200.0, 235, 940 - 235 * i, "TW", 200 + i) for i in range(4)]
p = build(ep1 + ep2, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 2 and p["n_slow"] == 0 and p["unload_med_s"] == 120.0, p
# (б) ліквідація (dir без «Close») закриває сегмент
liq = [mk(T0, 200.0, 60, 1000, "LQ", "0xl1"),
       dict(mk(T0 + 50_000, 190.0, 940, 940, "LQ", ZH), dir="Liquidated Cross Long", liquidation={"m": 1})]
p = build(liq, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["unload_med_s"] == 50.0, p
# ліквідація сама епізод не відкриває (не рішення гаманця)
p = build([dict(mk(T0, 200.0, 1000, 1000, "LQ2", ZH), dir="Liquidated Cross Long")], now_ms=NOW, depth_fn=D50K)
assert p["n_big"] == 0
# (в) долив >2% посеред зливу: один швидкий епізод, не slow + fast 0с
mid = [mk(T0, 200.0, 100, 1000, "RO", "0xr1"),                # -10% -> 900
       mk(T0 + 40_000, 200.0, 950, 950, "RO", "0xr2")]        # докупив 50, злив усе
p = build(mid, now_ms=NOW, depth_fn=D50K)
assert p["n_fast"] == 1 and p["n_slow"] == 0 and p["unload_med_s"] == 40.0, p
# (г) «триває» — лише для ВЕЛИКИХ епізодів (явний стан, не евристика)
p = build(episode("SM", NOW - 60_000, 100, 200.0, [(0, .06)]), now_ms=NOW, depth_fn=D50K)   # $20k
assert p["n_inprog"] == 0 and p["n_big"] == 0, p
# (д) 429 при підтягуванні: валідний профіль НЕ затирається, ретрай за 5 хв
def raising(exc):
    def post(body): raise exc
    return post
old_ok = {"ok": True, "v": 7, "fetched": int(time.time()) - 3600, "n_fast": 6, "n_slow": 0,
          "fast_pct": 100.0, "avg_gap_s": 20.0}
n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key"}, dict(
    PROFILE_ALGO_V=7, PROFILES_FILE=tempfile.mktemp(), strat2_lock=threading.RLock(),
    _prof_save_lock=threading.Lock(), wallet_profiles={"0xw": dict(old_ok)},
    profiles_fetching={"0xw"}, profile_retry_at={}, _profile_post=raising(NEW["RateLimited"]("429")),
    _build_profile=lambda fl, **kw: {"ok": False}))
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"]["ok"] and "err" not in n["wallet_profiles"]["0xw"]
assert n["profile_retry_at"]["0xw"] > time.time() + 200 and "0xw" not in n["profiles_fetching"]
# (е) мережевий збій рефрешу при валідному профілі: лишаємо, ретрай за 30 хв
n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key"}, dict(
    PROFILE_ALGO_V=7, PROFILES_FILE=tempfile.mktemp(), strat2_lock=threading.RLock(),
    _prof_save_lock=threading.Lock(), wallet_profiles={"0xw": dict(old_ok)},
    profiles_fetching={"0xw"}, profile_retry_at={}, _profile_post=raising(NEW["APIError"]("boom")),
    _build_profile=lambda fl, **kw: {"ok": False}))
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"]["ok"] and n["wallet_profiles"]["0xw"].get("refresh_failed_at")
assert n["profile_retry_at"]["0xw"] > time.time() + 1500
# без старого профілю збій = err, як і раніше
n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key"}, dict(
    PROFILE_ALGO_V=7, PROFILES_FILE=tempfile.mktemp(), strat2_lock=threading.RLock(),
    _prof_save_lock=threading.Lock(), wallet_profiles={}, profiles_fetching={"0xw"},
    profile_retry_at={}, _profile_post=raising(NEW["APIError"]("boom")),
    _build_profile=lambda fl, **kw: {"ok": False}))
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"].get("err") == 1
# (є) сторінка у зворотному порядку -> err (fail-closed), endTime у тілі
desc = list(reversed(page(2000, now_ms - 50 * 86400_000)))
bodies = []
def post_desc(body): bodies.append(body); return desc
n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key"}, dict(
    PROFILE_ALGO_V=7, PROFILES_FILE=tempfile.mktemp(), strat2_lock=threading.RLock(),
    _prof_save_lock=threading.Lock(), wallet_profiles={}, profiles_fetching={"0xw"},
    profile_retry_at={}, _profile_post=post_desc, _build_profile=lambda fl, **kw: {"ok": True}))
n["_fetch_profile"]("0xw")
assert n["wallet_profiles"]["0xw"].get("err") == 1 and "endTime" in bodies[0]
# (ж) _profile_post: 429 на проксі НЕ йде напряму; мертва проксі -> напряму без спроби
calls = {"prio": 0, "direct": 0}
def prio_429(b, retries=2, direct=False, **kw): calls["prio"] += 1; raise NEW["RateLimited"]("429")
def direct_ok(b, retries=4): calls["direct"] += 1; return []
pp = load({"_profile_post"}, dict(fresh(), REST_PROXY="1.2.3.4:1", hl_post=direct_ok, hl_post_prio=prio_429))
try:
    pp["_profile_post"]({"type": "x"}); raise AssertionError("429 мав пробитись")
except NEW["RateLimited"]:
    pass
assert calls == {"prio": 1, "direct": 0}, calls
def prio_dead(b, retries=2, direct=False, **kw): calls["prio"] += 1; raise NEW["APIError"]("timeout")
pp = load({"_profile_post"}, dict(fresh(), REST_PROXY="1.2.3.4:1", hl_post=direct_ok, hl_post_prio=prio_dead))
pp["_profile_post"]({"type": "x"}); pp["_profile_post"]({"type": "x"})
assert calls == {"prio": 2, "direct": 2}, calls   # друга сторінка: проксі не чіпали
# (з) бюджет ваги: вага сторінки = 20 + філи/20; при переповненні — сон до вікна
class _T:
    def __init__(s): s.t = 5000.0; s.slept = []
    def time(s): return s.t
    def sleep(s, x): s.slept.append(x); s.t += 60
tt = _T()
bw = load({"_profile_budget_wait", "_profile_budget_add"}, dict(
    time=tt, PROFILE_W_PER_MIN={"proxy": 600, "direct": 150},
    _profile_w={"proxy": NEW["deque"](), "direct": NEW["deque"]()}, _profile_w_lock=threading.Lock()))
bw["_profile_budget_add"]("proxy", page(2000, 0))
assert bw["_profile_w"]["proxy"][-1][1] == 120
for _ in range(4): bw["_profile_budget_add"]("proxy", page(2000, 0))   # 600 за хвилину
bw["_profile_budget_wait"]("proxy")
assert tt.slept, "мав заснути до звільнення вікна"
tt.slept.clear(); bw["_profile_budget_wait"]("proxy"); assert not tt.slept   # вікно минуло
# (и) prio-пари: тригерна монета отримує мітку пари (F5 не рахує 2-гу tx першою)
assert "marks.append(k)" in src and "follow_last_close[k] = time.time()" in src
assert "best_ratio, added, marks = 0.0, [], []" in src
# (і) TTL міток пари 7 днів; retry_at чиститься; лічильники у /status
assert "if ts < now_ - 7 * 86400]" in src and "profile_retry_at.pop(k, None)" in src
assert '"follow_busy_skips": stats.get("follow_busy_skips", 0)' in src and '"profiles_queued": len(profiles_fetching)' in src
assert 'stats["follow_busy_skips"] = stats.get("follow_busy_skips", 0) + 1' in src
# (ї) тіньова стрічка follow: first_shot/pair_gap_s перед m1; рядок = заголовок-1
m = re.search(r"FOLLOW_OUT_HEADERS = \(\[(.*?)\]\s*\+ \[f\"m", src, re.S)
fo_h = eval("[" + re.sub(r"#[^\n]*", "", m.group(1)) + "]")
assert fo_h[-7:-5] == ["first_shot", "pair_gap_s"], fo_h[-8:]   # v2.16: +5 колонок після
fr = load({"_fol_out_row", "_rev_samples"}, dict(_sim_slip=lambda d: 0.0002, SIM_COMMISSION=0.0005,
                                              _dt=lambda ts: "d", REV_TRACK_MIN=60))
row = fr["_fol_out_row"]({"sig_id": "fo", "detect_ts": 1, "coin": "X", "side": "LONG", "addr": "0xa",
                          "entry_px": 1.0, "peak": -999.0, "trough": 999.0, "samples": [], "first_shot": 1,
                          "pair_gap": ""})
assert len(row) == len(fo_h) + 60 and row[len(fo_h) - 7] == 1   # v2.16: +5 колонок після pair_gap_s
assert 'out["follow_tape_first"] = _tape(n_first, f1curve, f1complete)' in src
assert "follow_tape_first" in ui and "когорта F5" in ui
# (й) черга профілів: стеля і збій старту потоку
calls_q = []
class _Th:
    def __init__(self, target=None, args=(), daemon=None): self.a = (target, args)
    def start(self): raise RuntimeError("can't start new thread")
th = type(sys)("th"); th.Thread = _Th; th.Lock = threading.Lock; th.RLock = threading.RLock
nq = load({"_profile_request"}, dict(threading=th, strat2_lock=threading.RLock(), profiles_fetching=set(),
                                     wallet_profiles={}, profile_retry_at={}, PROFILE_ALGO_V=7,
                                     PROFILE_ERR_TTL_S=6 * 3600, PROFILE_TTL_S=24 * 3600,
                                     _fetch_profile=lambda a: None, stats={}))
nq["_profile_request"]("0xNEW")
assert "0xnew" not in nq["profiles_fetching"]   # збій start() -> discard, не вічний запис
nq["profile_retry_at"]["0xlater"] = time.time() + 100
nq["_profile_request"]("0xLATER")
assert "0xlater" not in nq["profiles_fetching"]  # retry_at шанується
nq["profiles_fetching"].update(f"0x{i}" for i in range(100))
nq["_profile_request"]("0xFULL")
assert "0xfull" not in nq["profiles_fetching"]   # стеля черги
print("6) фікси рев'ю: нульовий hash, ліквідації, долив посеред зливу, inprog, 429/збій без затирання, порядок сторінок, проксі/бюджет, prio-мітка, тіньова стрічка, черга")
print("\nALL v2.8 TESTS PASSED")

import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.18 — аудит пошуку швидких гаманців (10 пунктів, п.3 у м'якій
редакції) + пріоритети:
1 реконструктор: транзакції з відкриттями, життєві цикли (фактичний нуль ≠
  ціль 95%, долив не рве, реопен = нова позиція), ≤1 епізод на цикл,
  клас fast/slow/uncertain/inprog, дві тривалості, придатність до follow
2 контрольна таблиця аудиту (3×98% → 3 швидких; 10%+10 хв+90% → 1 повільний;
  лише останній продаж → невизначений; долив → 1; реопен у тій самій мс → 2;
  49 хв по 1% + 51% → швидкий від сигналу з dur_reduce; 142/203 → ні;
  one-shot; мейкер-фініш; Вілсон; avg_gap по всіх епізодах; inprog; err)
3 кваліфікація: три лічильники, ok / uncertain / no; глибина не гейтить історію
4 follow_on_txs: історія замовляється ДО ранніх відмов; F5/F10 фіксовані 60 с;
  F4/F7 таймер профілю; uncertain відкриває з міткою; F9 по точних; pending
5 _profile_status / _profile_refresh_pick / рефрешер
6 _fetch_profile_locked: prev_v/prev_status, сирі філи gz, err без сирих
7 API: статуси профілів і три групи міграції; unc поза заголовком; by_wallet/by_day
8 структурні: CSV-колонки, UI, версії, _sim_depth без придатної глибини = 0"""
import ast, threading, time, json, os, re, sys, math, tempfile, csv, glob, hashlib, shutil, gzip
import urllib.error, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HL)
from v28_shim import NEW, _median

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None, assigns=()):
    body = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in assigns
                        for t in n.targets))]
    mod = ast.Module(body=body, type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "hashlib": hashlib, "shutil": shutil, "glob": glob,
          "urllib": urllib, "print": lambda *a, **k: None}
    ns.update(NEW)
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

def const(name):
    m = re.search(rf"^{name}\s*=\s*([^#\n]+)", src, re.M)
    return eval(m.group(1).strip())

def _dt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else ""

F10 = "F10_розумний_60"
C = dict(PROFILE_ALGO_V=10, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.19", F4_MIN_FAST_PCT=70.0,
         F9_MIN_FAST_PCT=90.0, PROFILE_ENTRY_LAT_MS=5000, PROFILE_ZERO_TOL=1e-6,
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005, SIM_SPREAD=0.0002,
         SIM_POSITION_USD=1000.0, VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, MIN_DELTA_PCT=0.01, STRAT2_ENABLED=True,
         F6_NAME="F6_1хв_перший", F6_TIMER_S=60.0, F5_NAME="F5_перший",
         F5_FIRST_SHOT_S=3600.0, F7_NAME="F7_без_ратіо", RATIO_GRACE_S=1800,
         F8_NAME="F8_ratio35", F8_MIN_RATIO=3.5, F9_NAME="F9_без_ратіо_90", F10_NAME=F10,
         F_FIXED_TIMER_S=60.0,
         FC_ENABLED=True, FC_MAX_EPISODE_S=300, R7_NAME="R7_одним", R7_MIN_TX_USD=100_000.0,
         R8_NAME="R8_тп80", R8_TP_FRAC=0.8, REV_HOLD_MIN=30, REV_EP_MAX_S=300.0, REV_EP_MIN_MAG=1.0,
         FOLLOW_MAX_FILL_AGE_S=20.0, REV_MAX_FILL_AGE_S=60.0, BOOK_MAX_AGE_S=5.0, REV_REF_TOL_S=12.0,
         PROFILE_REFRESH_BATCH=3, PROFILE_QUEUE_MAX=100, PROFILE_429_RETRY_S=300,
         PROFILE_FAIL_RETRY_S=1800, PROFILE_WINDOW_D=90, PROFILE_PAGES=6)
assert const("DATA_ALGO_V") == "2.20" and const("PROFILE_ALGO_V") == 11
assert const("F10_NAME") == F10 and const("F_FIXED_TIMER_S") == 60.0
assert const("PROFILE_ENTRY_LAT_MS") == 5000 and const("PROFILE_ZERO_TOL") == 1e-6
assert const("PROFILE_REFRESH_EVERY_S") == 60 and const("PROFILE_REFRESH_BATCH") == 3
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
          assigns=("FOLLOW_HEADERS", "STRAT_SINCE", "STRAT2_TITLES", "STRAT2_DESC"))
fh = FH["FOLLOW_HEADERS"]
assert len(fh) == 62 and fh[-17:-11] == ["prof_status", "prof_n_uncertain", "prof_fast_lb95",
                                       "prof_cont_pct", "prof_one_shot_pct", "prof_after_pct_med"]
for k in ("F4_розумний", "F5_перший", "F7_без_ратіо", "F9_без_ратіо_90", F10):
    assert FH["STRAT_SINCE"][k] == "2.18", k     # популяція/таймер змінились — нова версія рядків
assert FH["STRAT_SINCE"]["F6_1хв_перший"] == "2.10" and FH["STRAT_SINCE"]["F8_ratio35"] == "2.12"
assert F10 in FH["STRAT2_TITLES"] and F10 in FH["STRAT2_DESC"] and "60 с" in FH["STRAT2_TITLES"]["F5_перший"]
A = "0x" + "a" * 40
now0 = time.time()
print("0) константи, заголовки (52), версії стратегій, титули F10/F5")

# ═══ 1. Реконструктор: транзакції, життєві цикли, епізод ═══════════════
P = load({"_wilson_lb", "_profile_txs", "_profile_lifecycles", "_profile_episode", "_build_profile"},
         dict(C, _median=_median, _sim_depth=lambda c, s=None: 1e5))
T = 1_750_000_000_000
NOW = T + 30 * 86_400_000
_seq = [0]
def fl(t, px, sz, sp, d="Close Long", coin="AAA", crossed=True, twap=None, hash_=None, liq=False):
    _seq[0] += 1
    f = {"time": t, "px": px, "sz": sz, "startPosition": sp, "dir": d, "coin": coin,
         "crossed": crossed, "twapId": twap, "hash": hash_ or f"0x{coin}{t}{_seq[0]}", "tid": _seq[0]}
    if liq: f["liquidation"] = {"liquidatedUser": "0x"}
    return f
def opn(t, px, sz, sp=0.0, coin="AAA", crossed=True): return fl(t, px, sz, sp, d="Open Long", coin=coin, crossed=crossed)
def build(fills, depth=lambda c, s=None: 1e5, now=NOW): return P["_build_profile"](fills, now_ms=now, depth_fn=depth)
def lifes(fills, coin="AAA"):
    by, bad = P["_profile_txs"](fills); assert bad == 0, bad
    return P["_profile_lifecycles"](by[coin])
def ep_of(fills, coin="AAA", i=0, depth=lambda c, s=None: 1e5, now=NOW):
    return P["_profile_episode"](lifes(fills, coin)[i], coin, now, depth)

# (а) транзакції: відкриття читаються; фліп = закриття старого боку + відкриття нового;
#     системні філи (twap/ліквідація/нульовий hash) — по oid/tid; групування по hash
by, bad = P["_profile_txs"]([opn(T, 2.0, 100), fl(T + 1000, 2.0, 60, 100, hash_="0xh1"),
                             fl(T + 1000, 2.1, 40, 40, hash_="0xh1"),
                             fl(T + 2000, 2.0, 30, 0, d="Short > Long", hash_="0xflip")])
assert bad == 0 and [x["kind"] for x in by["AAA"]] == ["open", "close", "open"], by
assert by["AAA"][1]["sz"] == 100 and abs(by["AAA"][1]["px"] - 2.04) < 1e-9 and by["AAA"][1]["sp"] == 100
by, bad = P["_profile_txs"]([fl(T, 2.0, 50, 100, d="Long > Short", hash_="0xf")])   # фліп із позиції 100: 50 закрити не вистачає → лише close
assert [x["kind"] for x in by["AAA"]] == ["close"] and by["AAA"][0]["sz"] == 50
by, bad = P["_profile_txs"]([fl(T, 2.0, 150, 100, d="Long > Short", hash_="0xf")])   # 150 через нуль: 100 close + 50 open
assert [(x["kind"], x["sz"]) for x in by["AAA"]] == [("close", 100), ("open", 50)]
# рев'ю №1: dir — ПО ФІЛУ; один ордер через нуль під одним hash: Close, Close, Long > Short, Open Short
by, bad = P["_profile_txs"]([fl(T, 2000.0, 40, 100, hash_="0xm"), fl(T, 2000.0, 40, 60, hash_="0xm"),
                             fl(T, 2000.0, 40, 20, d="Long > Short", hash_="0xm"),
                             fl(T, 2000.0, 50, -20, d="Open Short", hash_="0xm")])
assert [(x["kind"], x["sz"], x["side"]) for x in by["AAA"]] == [("close", 100, "LONG"), ("open", 70, "SHORT")], by["AAA"]
assert by["AAA"][0]["sp"] == 100 and by["AAA"][1]["sp"] == 0.0 and by["AAA"][1]["seq"] > by["AAA"][0]["seq"]
p_flip = build([opn(T - 9000, 2000.0, 100), fl(T, 2000.0, 40, 100, hash_="0xm"), fl(T, 2000.0, 40, 60, hash_="0xm"),
                fl(T, 2000.0, 40, 20, d="Long > Short", hash_="0xm"), fl(T, 2000.0, 50, -20, d="Open Short", hash_="0xm"),
                fl(T + 60_000, 2000.0, 70, 70, d="Close Short")])
assert [e[1] for e in p_flip["episodes"]] == ["fast", "fast"] and p_flip["n_lifecycles"] == 2 \
       and all(e[16] == 1 for e in p_flip["episodes"]), p_flip["episodes"]
# мультифіловий долив (усі Open) під одним hash — одне відкриття зі sp першого філа
by, bad = P["_profile_txs"]([opn(T, 2000.0, 5, 90), fl(T, 2000.0, 5, 95, d="Open Long", hash_=by["AAA"] and "0xa"), ])
assert [(x["kind"], x["sz"], x["sp"]) for x in by["AAA"]] == [("open", 5, 90), ("open", 5, 95)]
by, bad = P["_profile_txs"]([fl(T, 2000.0, 5, 90, d="Open Long", hash_="0xa2"), fl(T, 2000.0, 5, 95, d="Open Long", hash_="0xa2")])
assert [(x["kind"], x["sz"], x["sp"]) for x in by["AAA"]] == [("open", 10, 90)]
by, bad = P["_profile_txs"]([dict(fl(T, 2.0, 10, 100, hash_="0x0000"), oid=7), dict(fl(T, 2.0, 10, 90, hash_="0x0000"), oid=7),
                             dict(fl(T + 1, 2.0, 10, 80, hash_="0x0000"), oid=8)])
assert len(by["AAA"]) == 2 and by["AAA"][0]["sz"] == 20 and by["AAA"][1]["sz"] == 10   # нульовий hash: групування по oid
by, bad = P["_profile_txs"]([fl(T, 2.0, 10, 100, twap=5), fl(T + 1, 2.0, 10, 90, liq=True), fl(T + 2, 2.0, 10, 80, crossed=False)])
assert len(by["AAA"]) == 3 and not any(x["aggr"] for x in by["AAA"])
# биті: без hash / crossed не bool / inf / нульова ціна → bad_rows; «Buy» без напряму пропускається тихо
_b = [dict(fl(T, 2.0, 10, 100), hash=""), dict(fl(T, 2.0, 10, 100), crossed="yes"),
      fl(T, float("inf"), 10, 100), fl(T, 0.0, 10, 100), fl(T, 2.0, 10, 100, d="Buy")]
by, bad = P["_profile_txs"](_b)
assert bad == 4 and not by
# (б) життєві цикли: відкриття з нуля → start_known; закриття без відкриття → start_known=False;
#     фактичний нуль закриває цикл; долив (open посеред) і невидиме зростання — той самий цикл
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 100, 100)])
assert len(L) == 1 and L[0]["start_known"] and L[0]["zero"] and L[0]["n_add_in"] == 0
L = lifes([fl(T + 1000, 2000.0, 100, 100)])
assert len(L) == 1 and not L[0]["start_known"] and L[0]["zero"]
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 10, 100), opn(T + 2000, 2000.0, 5, 90), fl(T + 3000, 2000.0, 95, 95)])
assert len(L) == 1 and L[0]["n_add_in"] == 1 and L[0]["zero"]
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 10, 100), fl(T + 3000, 2000.0, 120, 120)])   # зросла без видимого open
assert len(L) == 1 and L[0]["n_add_in"] == 1 and L[0]["zero"]
# ціль 95% досягнута, лишок 2% → відкриття від пилу = НОВА позиція (from_dust), без відкриття — та сама
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 98, 100), opn(T + 5000, 2000.0, 100, 2), fl(T + 6000, 2000.0, 102, 102)])
assert len(L) == 2 and L[1]["from_dust"] == 1 and L[1]["start_known"] and L[1]["zero"]
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 98, 100), fl(T + 9000, 2000.0, 2, 2)])
assert len(L) == 1 and L[0]["zero"]
# ціль НЕ досягнута (лишок 30%) → відкриття = долив, не нова позиція
L = lifes([opn(T, 2000.0, 100), fl(T + 1000, 2000.0, 70, 100), opn(T + 5000, 2000.0, 50, 30), fl(T + 6000, 2000.0, 80, 80)])
assert len(L) == 1 and L[0]["n_add_in"] == 1
# (в) епізод: сигнал = перша агресивна tx ≥5% при ≥$100k; кінець = лишок ≤5%; мейкер-кінець рахується;
#     метрики придатності
e = ep_of([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 34000, 100000), fl(T + 60_000, 2.0, 33000, 66000),
           fl(T + 120_000, 2.0, 33000, 33000)])
assert e["cls"] == "fast" and e["unload_s"] == 120.0 and e["gaps"] == [60.0, 60.0] and e["ratio"] == 2.0
assert abs(e["first_pct"] - 0.34) < 1e-9 and e["after_n"] == 2 and abs(e["after_pct"] - 0.66) < 1e-9 \
       and e["after_dur_s"] == 120.0 and e["maker_finish"] == 0 and e["one_shot"] == 0 \
       and e["pre_signal_pct"] == 0.0 and e["dur_reduce_s"] == 120.0 and e["start_known"]
# агресивна tx 4 с після сигналу — НЕ доступна для входу (лаг 5 с); 6 с — доступна
e = ep_of([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 50000, 100000), fl(T + 4000, 2.0, 30000, 50000),
           fl(T + 6000, 2.0, 20000, 20000)])
assert e["after_n"] == 1 and abs(e["after_pct"] - 0.2) < 1e-9 and e["after_dur_s"] == 6.0
# мейкер-шматок не сигнал; сигнал — наступна агресивна ≥5%
e = ep_of([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 50000, 100000, crossed=False), fl(T + 30_000, 2.0, 50000, 50000)])
assert e["t0"] == T + 30_000 and e["sp0"] == 50000 and abs(e["pre_signal_pct"] - 0.5) < 1e-9 \
       and e["one_shot"] == 1 and e["dur_reduce_s"] == 30.0 and e["unload_s"] == 0.0
# без сигналу (лише дрібні/мейкер) → None
assert ep_of([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 100000, 100000, crossed=False)]) is None
assert ep_of([opn(T - 9000, 2.0, 100000)] + [fl(T + i * 1000, 2.0, 2000, 100000 - i * 2000) for i in range(50)]) is None
# 5% рахується від ПОТОЧНОЇ позиції: 4% від старту стає ≥5% від решти → сигнал (поки ≥$100k)
e = ep_of([opn(T - 9000, 2.0, 100000)] + [fl(T + i * 1000, 2.0, 4000, 100000 - i * 4000) for i in range(25)])
assert e is not None and e["sp0"] == 80000 and abs(e["first_pct"] - 0.05) < 1e-9 and abs(e["pre_signal_pct"] - 0.2) < 1e-9
# нотіонал <$100k → None; рівно $100k → сигнал
assert ep_of([opn(T - 9000, 1.0, 99999), fl(T, 1.0, 99999, 99999)]) is None
assert ep_of([opn(T - 9000, 1.0, 100000), fl(T, 1.0, 100000, 100000)])["cls"] == "fast"
# inprog: сигнал 2 хв тому, не закрито → inprog (не в n_big); через 6 хв — slow
fills_ip = [opn(T - 9000, 2.0, 100000), fl(T, 2.0, 20000, 100000)]
assert ep_of(fills_ip, now=T + 120_000)["cls"] == "inprog"
assert ep_of(fills_ip, now=T + 360_000)["cls"] == "slow" and ep_of(fills_ip, now=T + 360_000)["unload_s"] is None
# глибина: 0 / None / відсутня → ratio None, клас НЕ змінюється (№8)
assert ep_of(fills_ip[:1] + [fl(T, 2.0, 100000, 100000)], depth=lambda c, s=None: 0)["ratio"] is None
assert ep_of(fills_ip[:1] + [fl(T, 2.0, 100000, 100000)], depth=None)["cls"] == "fast"
print("1) реконструктор: tx з відкриттями/фліпами/системними, життєві цикли (нуль ≠ 95%, долив, from_dust), епізод і придатність")

# ═══ 2. Контрольна таблиця аудиту ════════════════════════════════════
def coins(n, mk):
    out = []
    for k in range(n):
        c = f"C{k}"
        out += mk(c, T + k * 3_600_000)
    return out
# 3×(98% одразу + лишок 2% пізніше) → 3 швидких (не 6): лишок після цілі — той самий цикл
def mk98(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 98000, 100000, coin=c), fl(t + 1_200_000, 2.0, 2000, 2000, coin=c)]
p = build(coins(3, mk98))
assert p["n_fast"] == 3 and p["n_slow"] == 0 and p["n_uncertain"] == 0 and p["n_lifecycles"] == 3, p
assert p["one_shot_pct"] == 100.0 and p["cont_pct"] == 0.0 and p["unload_med_s"] == 0.0
# 10% → 10 хв → 90% → 1 повільний (сигнал = 10%-шматок, кінець через 600 с)
p = build([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 10000, 100000), fl(T + 600_000, 2.0, 90000, 90000)])
assert p["n_slow"] == 1 and p["n_fast"] == 0 and p["episodes"][0][6] == 600.0 and p["unload_all_med_s"] == 600.0, p
# лише останній продаж (90%) видимий — початок поза історією → невизначений, НЕ швидкий
p = build([fl(T + 600_000, 2.0, 90000, 90000)])
assert p["n_uncertain"] == 1 and p["n_fast"] == 0 and p["n_slow"] == 0 and p["episodes"][0][16] == 0, p
# 10% + долив 5% + решта через 400 с → 1 повільний (долив не рве епізод), n_add_in=1
p = build([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 10000, 100000), opn(T + 60_000, 2.0, 5000, 90000),
           fl(T + 400_000, 2.0, 95000, 95000)])
assert p["n_slow"] == 1 and p["n_lifecycles"] == 1 and p["episodes"][0][15] == 1, p
# дві незалежні позиції: закриття і реопен у ТІЙ САМІЙ мс → 2 епізоди (ідентичність події, не мс)
p = build([opn(T - 9000, 2.0, 100000), fl(T, 2.0, 100000, 100000), opn(T, 2.0, 100000),
           fl(T + 1000, 2.0, 100000, 100000)])
assert p["n_fast"] == 2 and p["n_lifecycles"] == 2, p
# 49 хв по 1% (без сигналу), потім 51% одним → швидкий ВІД СИГНАЛУ (unload 0), але
# dur_reduce_s ≈ 49 хв і pre_signal_pct ≈ 0.49 — дві тривалості (№7)
f49 = [opn(T - 9000, 2000.0, 100)]
for i in range(1, 50):
    f49.append(fl(T + i * 60_000, 2000.0, 1, 100 - (i - 1)))
f49.append(fl(T + 49 * 60_000 + 1000, 2000.0, 51, 51))
p = build(f49)
e = p["episodes"][0]
assert p["n_fast"] == 1 and e[6] == 0.0 and abs(e[13] - 0.49) < 1e-9 and e[14] == 48 * 60 + 1.0 and e[12] == 1, e
assert p["dur_reduce_med_s"] == 2881.0 and abs(p["pre_signal_pct_med"] - 0.49) < 1e-9
# 142/203 = 69.95% → fast_pct показує 70.0, але ok=False (точні лічильники, №5); Вілсон LB ≈ 63.3
def mkfast(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 100000, 100000, coin=c)]
def mkslow(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 10000, 100000, coin=c), fl(t + 600_000, 2.0, 90000, 90000, coin=c)]
big = coins(142, mkfast) + [x for k in range(61) for x in mkslow(f"S{k}", T + (200 + k) * 3_600_000)]
p = build(big)
assert p["n_fast"] == 142 and p["n_slow"] == 61 and p["n_big"] == 203 and p["fast_pct"] == 70.0 and not p["ok"] \
       and p["status"] == "no" and p["fast_lb95"] == 63.3, (p["fast_pct"], p["ok"], p["fast_lb95"])
# 143/203 → ok
p = build(big + mkfast("X", T + 500 * 3_600_000))
assert p["ok"] and p["status"] == "ok" and p["n_big"] == 204
# 5 one-shot → ok, one_shot 100 / cont 0; avg_gap 30 (без пауз)
p = build(coins(5, mkfast))
assert p["ok"] and p["one_shot_pct"] == 100.0 and p["cont_pct"] == 0.0 and p["avg_gap_s"] == 30.0 and p["fast_lb95"] == 56.6
# агресивна 5% + мейкер 95% → епізод швидкий, maker_finish 100%, продовження після входу 0
def mkmaker(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 5000, 100000, coin=c),
                           fl(t + 30_000, 2.0, 95000, 95000, coin=c, crossed=False)]
p = build(coins(5, mkmaker))
assert p["ok"] and p["maker_finish_pct"] == 100.0 and p["cont_pct"] == 0.0 and p["after_pct_med"] == 0.0 \
       and p["unload_med_s"] == 30.0, p
# avg_gap_s — по ВСІХ завершених епізодах (fast медіана 60 + slow медіана 400 → 230), не лише швидких (№10)
def mkgap(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 34000, 100000, coin=c),
                         fl(t + 60_000, 2.0, 33000, 66000, coin=c), fl(t + 120_000, 2.0, 33000, 33000, coin=c)]
def mkgapslow(c, t): return [opn(t - 9000, 2.0, 100000, coin=c), fl(t, 2.0, 50000, 100000, coin=c),
                             fl(t + 400_000, 2.0, 50000, 50000, coin=c)]
p = build(mkgap("G1", T) + mkgapslow("G2", T + 3_600_000))
assert p["avg_gap_s"] == 230.0 and p["n_fast"] == 1 and p["n_slow"] == 1 and p["unload_med_s"] == 120.0 \
       and p["unload_all_med_s"] == 260.0, p
# inprog не входить у n_big; nosignal і lifecycles рахуються
p = build(coins(5, mkfast) + [opn(T + 900 * 3_600_000 - 9000, 2.0, 100000, coin="IP"),
                              fl(T + 900 * 3_600_000, 2.0, 20000, 100000, coin="IP")] +
          [opn(T - 9000, 2.0, 100000, coin="NS"), fl(T, 2.0, 100000, 100000, coin="NS", crossed=False)],
          now=T + 900 * 3_600_000 + 60_000)
assert p["n_big"] == 5 and p["n_inprog"] == 1 and p["n_nosignal"] == 1 and p["n_lifecycles"] == 7 and p["ok"]
# битий рядок → status err, ok False, bad_rows
p = build(coins(5, mkfast) + [dict(fl(T, 2.0, 1, 1), hash="")])
assert p["status"] == "err" and not p["ok"] and p["bad_rows"] == 1 and p["nr"]["status"] == "err"
# таблиця епізодів: 18 колонок, episode_id = монета:індекс циклу, версія/межі — у _fetch (п.6)
p = build(coins(2, mkfast))
assert len(p["episodes_cols"]) == 18 and all(len(e) == 18 for e in p["episodes"]) \
       and sorted(e[0] for e in p["episodes"]) == ["C0:0", "C1:0"]
assert "episodes" not in p["nr"] and p["nr"]["n_fast"] == 2
# Вілсон: 0/n → 0; монотонність по n при тій самій частці; None без n
W = P["_wilson_lb"]
assert W(0, 5) == 0.0 and W(5, 5) < W(50, 50) < W(500, 500) < 1.0 and W(0, 0) is None
assert abs(W(5, 5) - 0.5655) < 1e-3 and abs(W(142, 203) - 0.6332) < 1e-3
print("2) контрольна таблиця: 3×98%→3, 10%+90%→повільний, лише кінець→невизначений, долив→1, реопен у мс→2, 49 хв→дві тривалості, 142/203, one-shot, мейкер, avg_gap по всіх, inprog, err, Вілсон")

# ═══ 3. Кваліфікація: три лічильники ═════════════════════════════════
def mkunc(c, t): return [fl(t, 2.0, 100000, 100000, coin=c)]     # швидко, але початок поза історією
# 5 швидких + 1 повільний + 1 невизначений → 5/7 = 71.4% → ok (невизначений = повільний, і проходить)
p = build(coins(5, mkfast) + mkslow("S", T + 100 * 3_600_000) + mkunc("U", T + 101 * 3_600_000))
assert p["ok"] and p["status"] == "ok" and p["n_big"] == 7 and p["n_uncertain"] == 1 and p["fast_pct"] == 71.4, p
# 5 + 2 повільних + 1 невизначений → 62.5% ні; якби невизначений швидкий 75% → «кваліфікація невизначена»
p = build(coins(5, mkfast) + mkslow("S1", T + 100 * 3_600_000) + mkslow("S2", T + 102 * 3_600_000)
          + mkunc("U", T + 101 * 3_600_000))
assert not p["ok"] and p["status"] == "uncertain" and p["n_big"] == 8 and p["fast_pct"] == 62.5, p
# 4 швидких + 1 невизначений → підтверджених <5 → не ok; з невизначеним 5/5 → uncertain
p = build(coins(4, mkfast) + mkunc("U", T + 101 * 3_600_000))
assert not p["ok"] and p["status"] == "uncertain" and p["n_fast"] == 4
# 5 + 3 повільних + 1 невизначений → 55.6% / 66.7% → no
p = build(coins(5, mkfast) + [x for k in range(3) for x in mkslow(f"S{k}", T + (100 + k) * 3_600_000)]
          + mkunc("U", T + 120 * 3_600_000))
assert not p["ok"] and p["status"] == "no"
# лише невизначені (5) → uncertain, n_fast 0 — не викидається як «bad», а лишається на спостереженні
p = build([x for k in range(5) for x in mkunc(f"U{k}", T + k * 3_600_000)])
assert p["status"] == "uncertain" and p["n_fast"] == 0 and p["n_uncertain"] == 5 and p["fast_lb95"] == 0.0
# нема епізодів → no, fast_pct None
p = build([opn(T, 2.0, 100000)])
assert p["status"] == "no" and p["fast_pct"] is None and p["n_big"] == 0
# глибина СЬОГОДНІ не гейтить історію (№8): при глибині $1e9 (ratio 0.0002) ті самі 5 швидких → ok
p = build(coins(5, mkfast), depth=lambda c, s=None: 1e9)
assert p["ok"] and p["n_fast"] == 5
print("3) кваліфікація: ok / uncertain / no за трьома лічильниками; історія не залежить від поточної глибини")

# ═══ 4. follow_on_txs: рання історія, статуси, таймери, F9 ═════════════
DEPTH = {"AAA": {"bid": 1e5, "ask": 2e4, "max": 1e5}}
def mk_fol(prof=None, last_close=None, fetched=None):
    st = {}; req = []
    wp = {}
    if prof is not None:
        pr = dict(prof); pr.setdefault("v", 10); pr.setdefault("fetched", int(time.time()) - 100)
        if fetched is not None: pr["fetched"] = fetched
        wp[A] = pr
    n = load({"follow_on_txs", "_paper_px", "_px_mid_age", "_sim_costs", "_sim_slip", "depth_for_side",
              "_fol_row", "_ms", "_rnd", "_p_costs"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={}, rev_open={},
        follow_last_close=({} if last_close is None else dict(last_close)),
        wallet_profiles=wp, profiles_fetching=set(), stats=st,
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: req.append(a), _dt=_dt, _sig_seq=[0],
        hl_book_exec=(lambda coin, side, usd=None, qty=None, min_recv_ms=None: {"px": 99.5, "mid": 100.0, "bid": 99.9,
                                                    "ask": 100.1, "ts_ms": 0, "req_ms": 12, "partial": 0}),
        px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 2.0, 100.0)]},
        cache={"depth": DEPTH, "depth_prev": {}}, cache_lock=threading.Lock()))
    return n, st, req
OLD_F = {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5}
fresh = lambda: int((time.time() - 2) * 1000)
def opened(n): return sorted(p["strategy"] for p in n["follow_open"].values())
def by_st(n, s): return [p for p in n["follow_open"].values() if p["strategy"] == s][0]
PROF_OK = {"ok": True, "status": "ok", "n_big": 7, "n_fast": 6, "n_slow": 1, "n_uncertain": 0,
           "fast_pct": 85.7, "fast_lb95": 48.7, "unload_med_s": 95.0, "unload_mean_s": 110.5,
           "window_d": 61.3, "avg_gap_s": 40.0, "cont_pct": 66.7, "one_shot_pct": 16.7, "after_pct_med": 0.4}
# (а) історія замовляється на КОЖНОМУ закритті — і при повному закритті, і при дрібній tx, і при старому філі
for full, tx in ((True, [{"sz": 1000.0, "px": 100.0, "ts": fresh(), "hash": "0x1"}]),
                 (False, [{"sz": 1.0, "px": 100.0, "ts": fresh(), "hash": "0x2"}]),
                 (False, [{"sz": 100.0, "px": 100.0, "ts": fresh() - 60_000, "hash": "0x3"}])):
    n, st, req = mk_fol()
    n["follow_on_txs"](A, "AAA", OLD_F, tx, full)
    assert req == [A] and not n["follow_open"], (full, req)
_fo = src[src.index("def follow_on_txs("):src.index("# ── F4: профілі гаманців з історії філів")]
assert _fo.index("_profile_request(addr)") < _fo.index("if full_close") < _fo.index("follow_stale_skips")
assert _fo.count("_profile_request(addr)") == 1
# (б) профіль ok: F4/F7 таймер 2×пауза (80 с), F5/F10 — фіксовані 60 с; prof_status ok у КОЖНОМУ рядку
n, st, req = mk_fol(PROF_OK)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert opened(n) == [F10, "F1_1хв", "F2_2хв", "F3_3хв", "F4_розумний", "F5_перший", "F6_1хв_перший", "F7_без_ратіо"], opened(n)
assert by_st(n, "F4_розумний")["timer"] == 80.0 and by_st(n, "F7_без_ратіо")["timer"] == 80.0
assert by_st(n, "F5_перший")["timer"] == 60.0 and by_st(n, F10)["timer"] == 60.0
assert all(p["prof_status"] == "ok" for p in n["follow_open"].values())
pf = by_st(n, "F1_1хв")["prof"]
assert pf["n_uncertain"] == 0 and pf["fast_lb95"] == 48.7 and pf["cont_pct"] == 66.7 and pf["one_shot_pct"] == 16.7
# кламп таймера профілю 60–300
n, _, _ = mk_fol(dict(PROF_OK, avg_gap_s=10.0))
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert by_st(n, "F4_розумний")["timer"] == 60.0 and by_st(n, F10)["timer"] == 60.0
n, _, _ = mk_fol(dict(PROF_OK, avg_gap_s=500.0))
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert by_st(n, "F4_розумний")["timer"] == 300.0 and by_st(n, F10)["timer"] == 60.0 and by_st(n, "F5_перший")["timer"] == 60.0
# (в) НЕ перший постріл: F4/F10 входять, F5/F7 — ні
n, _, _ = mk_fol(PROF_OK, {A + ":AAA": time.time() - 1800})
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert opened(n) == [F10, "F1_1хв", "F2_2хв", "F3_3хв", "F4_розумний"], opened(n)
# (г) uncertain: ті самі стратегії відкриваються, рядки несуть prof_status=uncertain; F9 — ні
PROF_U = dict(PROF_OK, ok=False, status="uncertain", n_big=8, n_fast=5, n_slow=1, n_uncertain=2, fast_pct=62.5)
n, _, _ = mk_fol(PROF_U)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert opened(n) == [F10, "F1_1хв", "F2_2хв", "F3_3хв", "F4_розумний", "F5_перший", "F6_1хв_перший", "F7_без_ратіо"], opened(n)
assert all(p["prof_status"] == "uncertain" for p in n["follow_open"].values())
assert by_st(n, "F1_1хв")["prof"]["n_uncertain"] == 2
# (д) no: жодної профільної; prof_status=no у F1..F3/F6; старий профіль без status (ok=False) → no
for pr in (dict(PROF_OK, ok=False, status="no", n_fast=3),
           dict({k: v for k, v in PROF_OK.items() if k != "status"}, ok=False)):   # без status (старий формат)
    n, _, _ = mk_fol(pr)
    n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
    assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший"], opened(n)
    assert all(p["prof_status"] == "no" for p in n["follow_open"].values())
# (е) профілю нема / стара версія / старший 48 год / truncated / err → pending, профільні закриті
for pr, fe, want in ((None, None, "pending"), (dict(PROF_OK, v=9), None, "pending"),
                     (PROF_OK, int(time.time()) - 49 * 3600, "pending"), (dict(PROF_OK, truncated=1), None, "pending"),
                     ({"ok": False, "err": 1, "n_fast": 0, "n_big": 0}, None, "err")):
    n, _, _ = mk_fol(pr, fetched=fe)
    n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
    assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший"], (pr, opened(n))
    assert {p["prof_status"] for p in n["follow_open"].values()} == {want}, (pr, want)
# помилковий профіль: статус err (не pending — історія не отрималась, TTL 6 год)
n, _, _ = mk_fol({"ok": False, "err": 1, "n_fast": 0, "n_big": 0})
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert {p["prof_status"] for p in n["follow_open"].values()} == {"err"}
# 47 год — ще відкриває (stale-while-revalidate), 49 — ні
n, _, _ = mk_fol(PROF_OK, fetched=int(time.time()) - 47 * 3600)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert "F4_розумний" in opened(n) and F10 in opened(n)
# (є) F9: ≥90% ПІДТВЕРДЖЕНО швидких по точних лічильниках; невизначені — як повільні
for nf, ns_, nu, want in ((9, 1, 0, True), (9, 0, 1, True), (8, 0, 2, False), (4, 0, 0, False), (899, 101, 0, False)):
    pr = dict(PROF_OK, n_fast=nf, n_slow=ns_, n_uncertain=nu, n_big=nf + ns_ + nu, fast_pct=90.0)
    n, _, _ = mk_fol(pr)
    n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
    assert ("F9_без_ратіо_90" in opened(n)) == want, (nf, ns_, nu, opened(n))
# F9 — лише перший постріл
n, _, _ = mk_fol(dict(PROF_OK, n_fast=9, n_slow=1, n_big=10), {A + ":AAA": time.time() - 1800})
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
assert "F9_без_ратіо_90" not in opened(n) and F10 in opened(n)
# (ж) _fol_row: 6 нових полів наприкінці; без профілю — порожні
n, _, _ = mk_fol(PROF_U)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
p = by_st(n, "F4_розумний")
row = n["_fol_row"](p, "t-1", p["open_ts"] + 61, 98.8, "silence", 0.4)
assert len(row) == len(fh) - 1
g = lambda k: row[fh.index(k)]
assert g("prof_status") == "uncertain" and g("prof_n_uncertain") == 2 and g("prof_fast_lb95") == 48.7 \
       and g("prof_cont_pct") == 66.7 and g("prof_one_shot_pct") == 16.7 and g("prof_after_pct_med") == 0.4
assert g("prof_n_fast") == 5 and g("prof_n_slow") == 1 and g("prof_fast_pct") == 62.5
n, _, _ = mk_fol(None)
n["follow_on_txs"](A, "AAA", OLD_F, [{"sz": 100.0, "px": 100.0, "ts": fresh(), "hash": "0x4"}], False)
p = by_st(n, "F1_1хв")
row = n["_fol_row"](p, "t-2", p["open_ts"] + 61, 98.8, "silence", 0.4)
assert g("prof_status") == "pending" and g("prof_n_uncertain") == "" and g("prof_fast_lb95") == "" and g("prof_n_fast") == ""
print("4) follow: історія до відмов; F4/F7 2×пауза, F5/F10 60 с; ok/uncertain відкривають з міткою, no/pending/err — ні; F9 точний; CSV")

# ═══ 5. _profile_status / _profile_refresh_pick / рефрешер ═════════════
def mk_ps(profiles, fetching=(), retry=None, wl=(), batch=3):
    return load({"_profile_status", "_profile_refresh_pick", "_profile_request"}, dict(C,
        PROFILE_REFRESH_BATCH=batch, strat2_lock=threading.RLock(), watchlist_lock=threading.Lock(),
        watchlist={a: {} for a in wl}, wallet_profiles=dict(profiles), profiles_fetching=set(fetching),
        profile_retry_at=dict(retry or {}), stats={}))
t_now = int(time.time())
PS = {"a1": None, "a2": dict(PROF_OK, v=9, fetched=t_now - 100), "a3": {"ok": False, "err": 1, "v": 10, "fetched": t_now - 100},
      "a4": dict(PROF_OK, v=10, fetched=t_now - 100, truncated=1), "a5": dict(PROF_OK, v=10, fetched=t_now - 49 * 3600),
      "a6": dict(PROF_OK, v=10, fetched=t_now - 100, status="uncertain", ok=False),
      "a7": dict(PROF_OK, v=10, fetched=t_now - 100), "a8": dict(PROF_OK, v=10, fetched=t_now - 100, status="no", ok=False),
      "a9": dict(PROF_OK, v=10, fetched=t_now - 100), "a10": {k: v for k, v in dict(PROF_OK, v=10, fetched=t_now - 100).items() if k != "status"}}
n = mk_ps({k: v for k, v in PS.items() if v is not None}, fetching={"a9"})
want = {"a1": "unknown", "a2": "pending", "a3": "err", "a4": "pending", "a5": "pending", "a6": "uncertain",
        "a7": "ok", "a8": "no", "a9": "pending", "a10": "ok", "0xNOPE": "unknown"}
for a, w in want.items():
    assert n["_profile_status"](a) == w, (a, n["_profile_status"](a), w)
assert n["_profile_status"]("A7") == "ok"   # регістр
# черга: watchlist без профілю → 0; стара версія → 1; застарілий (>24 год) → 2 (найстаріший перший);
# свіжий — ні; fetching / retry — ні; відомі швидкі/невизначені ПОЗА watchlist → після watchlist; «no» поза — ні
profs = {"w2": dict(PROF_OK, v=9, fetched=t_now - 100), "w3": dict(PROF_OK, v=10, fetched=t_now - 100),
         "w4": dict(PROF_OK, v=10, fetched=t_now - 25 * 3600), "w5": dict(PROF_OK, v=10, fetched=t_now - 30 * 3600),
         "w6": dict(PROF_OK, v=10, fetched=t_now - 40 * 3600), "w7": dict(PROF_OK, v=10, fetched=t_now - 40 * 3600),
         "o1": dict(PROF_OK, v=10, fetched=t_now - 30 * 3600), "o2": dict(PROF_OK, v=10, fetched=t_now - 30 * 3600, status="uncertain", ok=False),
         "o3": dict(PROF_OK, v=10, fetched=t_now - 30 * 3600, status="no", ok=False), "o4": dict(PROF_OK, v=9, fetched=t_now - 100),
         "o5": {"ok": False, "err": 1, "v": 10, "fetched": t_now - 7 * 3600}, "o6": dict(PROF_OK, v=10, fetched=t_now - 100),
         "o7": dict(PROF_OK, v=9, ok=False, status="no", fetched=t_now - 100),      # рев'ю №3: стара версія, не ok → останнім
         "o8": {"ok": False, "err": 1, "v": 9, "fetched": t_now - 100},
         "o9": dict(PROF_OK, v=10, ok=False, status="no", fetched=t_now - 30 * 3600)}   # поточна версія, не ok → ні
wl = ["W1", "w2", "w3", "w4", "w5", "w6", "w7"]
n = mk_ps(profs, fetching={"w6"}, retry={"w7": time.time() + 600}, wl=wl, batch=20)
# v2.20: o5 (err поза watchlist, старший за PROFILE_ERR_TTL_S) — теж у черзі (найнижчий пріоритет)
assert n["_profile_refresh_pick"]() == ["w1", "w2", "w5", "w4", "o4", "o1", "o2", "o7", "o8", "o5"], n["_profile_refresh_pick"]()
n = mk_ps(profs, fetching={"w6"}, retry={"w7": time.time() + 600}, wl=wl, batch=3)
assert n["_profile_refresh_pick"]() == ["w1", "w2", "w5"]
# err-профіль (6 год TTL) поза watchlist не тягнеться (не швидкий); у watchlist — через 6 год так
n = mk_ps({"e1": {"ok": False, "err": 1, "v": 10, "fetched": t_now - 7 * 3600},
           "e2": {"ok": False, "err": 1, "v": 10, "fetched": t_now - 5 * 3600}}, wl=["e1", "e2"], batch=5)
assert n["_profile_refresh_pick"]() == ["e1"]
# порожньо → []
assert mk_ps({}, batch=3)["_profile_refresh_pick"]() == []
# рефрешер: потік стартує у main після settle-воркера; крок 60 с; кожен пік → _profile_request
_main = src[src.index("threading.Thread(target=run_settle_worker"):]
assert "threading.Thread(target=run_profile_refresher, daemon=True).start()" in _main[:300]
_rf = src[src.index("def run_profile_refresher("):src.index("def _profile_request(")]
assert "_profile_refresh_pick()" in _rf and "_profile_request(a)" in _rf and "time.sleep(PROFILE_REFRESH_EVERY_S)" in _rf \
       and "profile_refresh_err" in _rf
# _profile_request: свіжий ok — не запитує; стара версія — запитує (потік); retry_at блокує
started = []
n = mk_ps({"r1": dict(PROF_OK, v=10, fetched=t_now - 100), "r2": dict(PROF_OK, v=9, fetched=t_now - 100)},
          retry={"r3": time.time() + 100})
class _Th:
    def __init__(self, target=None, args=(), daemon=None): started.append(args[0])
    def start(self): pass
n["threading"] = type("T", (), {"Thread": _Th, "RLock": threading.RLock, "Lock": threading.Lock})
n["_fetch_profile"] = lambda a: None
for a in ("r1", "r2", "r3", "r4"):
    n["_profile_request"](a)
assert started == ["r2", "r4"] and n["profiles_fetching"] == {"r2", "r4"}
print("5) статуси профілю (unknown/pending/err/ok/uncertain/no), черга рефрешу (порядок і стеля), рефрешер, _profile_request")

# ═══ 6. _fetch_profile_locked: prev_status, сирі філи, err ══════════════
d6 = tempfile.mkdtemp()
def mk_fetch(pages, old=None, algo_v=10):
    posts = []
    it = iter(pages)
    def post(body):
        posts.append(dict(body))
        r = next(it, [])
        if isinstance(r, Exception): raise r
        return r
    n = load({"_fetch_profile", "_fetch_profile_locked", "_fill_key", "_build_profile", "_wilson_lb",
              "_profile_txs", "_profile_lifecycles", "_profile_episode"}, dict(C,
        PROFILE_ALGO_V=algo_v, PROFILES_FILE=os.path.join(d6, "wp.json"), DATA_DIR=d6,
        strat2_lock=threading.RLock(), _prof_save_lock=threading.Lock(),
        wallet_profiles=({} if old is None else {A: dict(old)}), profiles_fetching={A}, profile_retry_at={},
        _profile_post=post, _median=_median, _sim_depth=lambda c, s=None: 1e5, stats={}, gzip=gzip))
    n["_profile_sem"] = threading.Semaphore(1)
    return n, posts
five = coins(5, mkfast)
# (а) перший розрахунок: prev_v/prev_status None; сирі філи у profiles_raw/<addr>.json.gz з версією/межами
n, posts = mk_fetch([five])
n["_fetch_profile_locked"](A)
pr = n["wallet_profiles"][A]
assert pr["v"] == 10 and pr["status"] == "ok" and pr["prev_v"] is None and pr["prev_status"] is None \
       and pr["n_fills"] == 10 and pr["window_d"] > 0 and len(pr["episodes"]) == 5, pr.get("status")
raw = json.load(gzip.open(os.path.join(d6, "profiles_raw", f"{A}.json.gz"), "rt"))
assert raw["addr"] == A and raw["v"] == 10 and raw["n"] == 10 and len(raw["fills"]) == 10 \
       and raw["window_start_ms"] < raw["end_ms"] and raw["fetched"] == pr["fetched"]
assert posts[0]["startTime"] == raw["window_start_ms"] and posts[0]["endTime"] == raw["end_ms"]
assert not glob.glob(os.path.join(d6, "profiles_raw", "*.tmp"))
# (б) перерахунок зі старої версії: prev_v=9, prev_status за старим ok → «ok»; новий — uncertain (без відкриттів)
n, _ = mk_fetch([[f for f in five if f["dir"] != "Open Long"]], old={"v": 9, "ok": True, "fetched": t_now - 100, "n_fast": 6})
n["_fetch_profile_locked"](A)
pr = n["wallet_profiles"][A]
assert pr["prev_v"] == 9 and pr["prev_status"] == "ok" and pr["status"] == "uncertain" and not pr["ok"]
# (в) рефреш тієї ж версії зберігає prev_*; старий без ok → "no"; старий err → "err"
n, _ = mk_fetch([five], old=dict(pr))
n["_fetch_profile_locked"](A)
assert n["wallet_profiles"][A]["prev_status"] == "ok" and n["wallet_profiles"][A]["prev_v"] == 9 \
       and n["wallet_profiles"][A]["status"] == "ok"
n, _ = mk_fetch([five], old={"v": 9, "ok": False, "fetched": t_now - 100})
n["_fetch_profile_locked"](A)
assert n["wallet_profiles"][A]["prev_status"] == "no"
n, _ = mk_fetch([five], old={"v": 9, "ok": False, "err": 1, "fetched": t_now - 100})
n["_fetch_profile_locked"](A)
assert n["wallet_profiles"][A]["prev_status"] == "err"
# (г) збій історії → err-профіль, сирі філи НЕ перезаписуються (лишається попередній gz)
_before = os.path.getmtime(os.path.join(d6, "profiles_raw", f"{A}.json.gz"))
n, _ = mk_fetch([{"error": "boom"}])
n["_fetch_profile_locked"](A)
assert n["wallet_profiles"][A].get("err") == 1 and n["wallet_profiles"][A].get("status") is None
assert os.path.getmtime(os.path.join(d6, "profiles_raw", f"{A}.json.gz")) == _before
# (д) 429 → профіль не пишеться, retry 5 хв
n, _ = mk_fetch([NEW["RateLimited"]("429")], old=None)
n["_fetch_profile_locked"](A)
assert A not in n["wallet_profiles"] and n["profile_retry_at"][A] > time.time() + 200 and A not in n["profiles_fetching"]
# (е) DATA_DIR без прав на запис сирих → профіль все одно записаний, лічильник помилки
n, _ = mk_fetch([five])
n["DATA_DIR"] = os.path.join(d6, "wp.json")   # файл замість каталогу → makedirs впаде
n["_fetch_profile_locked"](A)
assert n["wallet_profiles"][A]["status"] == "ok" and n["stats"]["profile_raw_err"] == 1
print("6) _fetch_profile_locked: prev_v/prev_status для трьох груп, сирі філи gz (версія/межі), err/429 без затирання")

# ═══ 7. API: статуси профілів, міграція, unc поза заголовком, by_wallet/by_day ═══
import settle
d7 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d7, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def mkrow(headers, **kv):
    r = {k: "" for k in headers}; r["eol"] = "^"; r.update(kv)
    return [r[k] for k in headers]
today = _dt(now0); ago1 = _dt(now0 - 86400); ago2 = _dt(now0 - 2 * 86400)
B = "0x" + "b" * 40; Cw = "0x" + "c" * 40
def folr(tid, date, addr=A, st="F4_розумний", **kv):
    base = dict(date_open=date, date_close=date, strategy=st, coin="AAA", our_side="SHORT", whale_addr=addr,
                entry_px="100", exit_px="99.5", exit_reason="silence", hold_s="70", gross_pct="0.5",
                costs_pct="0.15", net_pct="0.35", ratio="3", pos_usd="5e5", vault="0", hour="10",
                profile_gap_s="80", algo_v="2.19", trade_id=tid, grace="0", prof_status="ok",
                prof_n_fast="6", prof_n_slow="1", prof_n_uncertain="0", prof_fast_pct="85.7",
                prof_fast_lb95="48.7", prof_cont_pct="66.7", prof_one_shot_pct="16.7", prof_after_pct_med="0.4")
    base.update(kv); return mkrow(fh, **base)
wcsv("follow.csv", fh, [
    folr("t1", today, net_pct="1.0"),
    folr("t2", today, net_pct="0.5", addr=B),
    folr("t3", ago1, net_pct="-0.2", addr=B),
    folr("t4", ago2, net_pct="3.0", addr=Cw, prof_status="uncertain", prof_n_fast="5", prof_n_slow="1",
         prof_n_uncertain="2", prof_fast_pct="62.5", prof_fast_lb95="30.6"),
    folr("t5", ago2, net_pct="-2.0", addr=Cw, prof_status="uncertain", prof_n_uncertain="2"),
    folr("t6", today, st=F10, net_pct="0.8"),
    folr("t7", today, st=F10, net_pct="-0.4", prof_status="uncertain", prof_n_uncertain="1"),
    folr("t8", today, st="F1_1хв", net_pct="0.9", prof_status="uncertain", prof_n_uncertain="2"),   # рев'ю №2
    folr("t9", today, st="F1_1хв", net_pct="0.1", prof_status="pending", prof_n_fast="", prof_n_slow="", prof_fast_pct=""),
])
for nm, hdr in (("rev.csv", None), ("out.csv", None), ("tw.csv", None), ("tc.csv", None), ("sig.csv", None), ("tws.csv", None), ("fo.csv", None)):
    pass
FH2 = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
           assigns=("REV_HEADERS", "TWAP_HEADERS", "REV_OUT_HEADERS", "FOLLOW_OUT_HEADERS", "TWAP_CURVE_HEADERS",
                    "REV_SIG_HEADERS", "STRAT_SINCE", "TAPE_SINCE", "SYMBOL_MAP"))
wcsv("rev.csv", FH2["REV_HEADERS"], []); wcsv("out.csv", FH2["REV_OUT_HEADERS"], [])
wcsv("tw.csv", FH2["TWAP_HEADERS"], []); wcsv("tc.csv", FH2["TWAP_CURVE_HEADERS"], [])
wcsv("sig.csv", FH2["REV_SIG_HEADERS"], []); wcsv("tws.csv", FH2["REV_SIG_HEADERS"], [])
wcsv("fo.csv", FH2["FOLLOW_OUT_HEADERS"], [])
wcsv("settlements.csv", settle.HEADERS, [])
WP = {A: dict(PROF_OK, v=10, fetched=t_now - 100, prev_v=9, prev_status="ok"),
      B: dict(PROF_OK, v=10, fetched=t_now - 100, prev_v=9, prev_status="no"),
      Cw: dict(PROF_OK, v=10, fetched=t_now - 100, status="uncertain", ok=False, prev_v=9, prev_status="ok"),
      "0xd": dict(PROF_OK, v=10, fetched=t_now - 100, status="no", ok=False, prev_v=9, prev_status="ok"),
      "0xe": dict(PROF_OK, v=9, fetched=t_now - 100),
      "0xf": {"ok": False, "err": 1, "v": 10, "fetched": t_now - 100},
      "0xg": dict(PROF_OK, v=10, fetched=t_now - 49 * 3600),
      "0xh": dict(PROF_OK, v=10, fetched=t_now - 100, truncated=1),
      "0xi": dict(PROF_OK, v=10, fetched=t_now - 100, prev_v=None, prev_status=None),
      "0xj": dict(PROF_OK, v=10, fetched=t_now - 100, status="no", ok=False, prev_v=9, prev_status="no")}
def mk_api(d=d7):
    return load({"strat2_api", "strat2_slice", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
                 "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key", "_twap_exit",
                 "_twap_curve_row", "_stat_small", "_period_stats", "_ts_local", "_load_settlements",
                 "_official_net", "_parse_curve", "_curves_of", "_agg_block", "_pub", "_leg_costs"}, dict(C,
        _strat2_full={}, strat2_lock=threading.RLock(), rev_open={}, follow_open={}, wallet_profiles=dict(WP),
        profiles_fetching={"0xz"}, STRAT2_DESC={}, STRAT2_TITLES={}, STRAT_SINCE=FH["STRAT_SINCE"],
        TAPE_SINCE=FH2["TAPE_SINCE"], _strat2_cache={"ts": 0.0, "data": None}, _legacy_csv_cache={},
        _settle_cache={"key": None, "data": {}}, DATA_DIR=d, stats={},
        FOLLOW_OUT_CSV=os.path.join(d, "fo.csv"), REV_CSV=os.path.join(d, "rev.csv"),
        REV_SIG_CSV=os.path.join(d, "sig.csv"), FOLLOW_CSV=os.path.join(d, "follow.csv"),
        REV_OUT_CSV=os.path.join(d, "out.csv"), TWAP_CSV=os.path.join(d, "tw.csv"),
        TWAP_CURVE_CSV=os.path.join(d, "tc.csv"), TWAP_SIG_CSV=os.path.join(d, "tws.csv"),
        TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_COHORTS=(1.0, 1.5, 2.0),
        T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення", strat_activated={}, _dt=_dt,
        _sim_slip=lambda d_: 0.0005, RESEARCH_SINCE="2.17"), assigns=("TWAP_HEADERS",))
o = mk_api()["strat2_api"]()
pf = o["profiles"]
assert pf["total"] == 10 and pf["ok"] == 3 and pf["ok_nr"] == 3 and pf["uncertain"] == 1 and pf["no"] == 2 \
       and pf["pending"] == 3 and pf["err"] == 1 and pf["fetching"] == 1 and pf["algo_v"] == 10, pf
assert pf["migration"] == {"kept": 1, "downgraded": 1, "lost": 1, "gained": 1}, pf["migration"]
f4 = o["strategies"]["F4_розумний"]
# заголовок: лише ok-профілі (t1, t2, t3): медіана 0.5; невизначені (t4, t5) — окремо, з власною статистикою
assert f4["n"] == 3 and f4["median"] == 0.5 and f4["n_prof_unc"] == 2 and f4["prof_unc"]["n"] == 2 \
       and f4["prof_unc"]["median"] == 0.5 and f4["n_trades_total"] == 5, (f4["n"], f4["median"], f4["n_prof_unc"], f4["prof_unc"])
tr = {t["net30"]: t for t in f4["trades"]}
assert tr[3.0]["prof_status"] == "uncertain" and tr[3.0]["unc"] == 1 and tr[3.0]["pf_lb95"] == 30.6 \
       and tr[1.0]["prof_status"] == "ok" and tr[1.0]["unc"] == 0 and tr[1.0]["pf_cont"] == 66.7 and tr[1.0]["pf_oneshot"] == 16.7
# by_wallet: A(1.0) B(0.5,-0.2) → 2 групи, медіана медіан (1.0+0.15)/2, найбільша частка 66.7; by_day: today 2, ago1 1
assert {k: v for k, v in f4["by_wallet"].items() if k != "top_key"} == {"n_groups": 2, "median_of_medians": 0.575, "top_share": 66.7}, f4["by_wallet"]
assert f4["by_day"]["n_groups"] == 2 and f4["by_day"]["top_share"] == 66.7 \
       and abs(f4["by_day"]["median_of_medians"] - (0.75 - 0.2) / 2) < 1e-9, f4["by_day"]   # сьогодні 0.75 (1.0, 0.5), вчора -0.2
# профіль стратегії: один запис на гаманець, n_uncertain/lb95/cont/one_shot медіани
assert f4["prof"]["wallets"] == 3 and f4["prof"]["n_uncertain"] == 2 and f4["prof"]["lb95_med"] == 48.7 \
       and f4["prof"]["cont_pct_med"] == 66.7 and f4["prof"]["one_shot_pct_med"] == 16.7, f4["prof"]
# F10 у циклі API; unc і там
f10 = o["strategies"][F10]
assert f10["n"] == 1 and f10["median"] == 0.8 and f10["n_prof_unc"] == 1 and f10["since"] == "2.18"
# рев'ю №2: F1 профіль не відбирає — рядок із uncertain-профілем У заголовку, n_prof_unc 0
f1 = o["strategies"]["F1_1хв"]
assert f1["n"] == 2 and f1["n_prof_unc"] == 0 and f1["prof_unc"]["n"] == 0 and f1["median"] == 0.5 \
       and {t["unc"] for t in f1["trades"]} == {0} and {t["prof_status"] for t in f1["trades"]} == {"uncertain", "pending"}, f1["n"]
# зріз: unc-рядки видно у списку (для окремої перевірки), але не в n
sl = mk_api()["strat2_slice"]("F4_розумний") if "strat2_slice" in mk_api() else None
print("7) API: статуси профілів (ok/uncertain/no/pending/err), три групи міграції, unc поза заголовком, by_wallet/by_day, F10")

# ═══ 8. Структурні: UI, _sim_depth, версії ══════════════════════════════
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for k in (f"'{F10}'", "pr.uncertain", "mg.kept", "mg.downgraded", "mg.lost", "prof_status", "Вілсон",
          "невизначений профіль", "n_prof_unc", "pf_lb95", "pf_cont", "by_wallet", "by_day", "lb95_med"):
    assert k in ui, k
assert ui.count(f"'{F10}'") >= 2   # S_ORDER і S_PROF
_js = "\n".join(re.findall(r"<script>(.*?)</script>", ui, re.S))
open(os.path.join(tempfile.gettempdir(), "_v218_inline.js"), "w", encoding="utf-8").write(_js)
assert os.system(f"node --check {os.path.join(tempfile.gettempdir(), '_v218_inline.js')}") == 0
# _sim_depth: стара (>2 цикли) або обрізана глибина → 0 (не база для ratio, №8)
SD = load({"_sim_depth", "_depth_ok", "depth_for_side"}, dict(C, REFRESH_S=1800, stats={}, cache_lock=threading.Lock(),
          cache={"depth": {"AAA": {"bid": 1e5, "ask": 2e4, "max": 1e5, "ts": time.time() - 4000},
                           "BBB": {"bid": 1e5, "ask": 2e4, "max": 1e5, "ts": time.time() - 10, "trunc": 1},
                           "CCC": {"bid": 1e5, "ask": 2e4, "max": 1e5, "ts": time.time() - 10}}, "depth_prev": {}}))
assert SD["_sim_depth"]("AAA") == 0 and SD["_sim_depth"]("BBB", "LONG") == 0 and SD["_sim_depth"]("CCC") == 1e5 \
       and SD["_sim_depth"]("CCC", "LONG") == 1e5 and SD["_sim_depth"]("ZZZ") == 0
assert SD["stats"]["ratio_stale_depth"] == 1 and SD["stats"]["ratio_trunc_depth"] == 1
# рев'ю №4: count=False — без лічильників; профіль за дефолтною глибиною — лише count=False, 1 запит на (монета, бік)
assert SD["_sim_depth"]("AAA", "LONG", count=False) == 0 and SD["_sim_depth"]("BBB", count=False) == 0
assert SD["stats"]["ratio_stale_depth"] == 1 and SD["stats"]["ratio_trunc_depth"] == 1
_dcalls = []
P["_sim_depth"] = lambda c, s=None, count=True: (_ for _ in ()).throw(AssertionError("лічильний _sim_depth у профілі"))
P["_sim_depth_quiet"] = lambda c, s=None: _dcalls.append((c, s)) or 1e5
pd_ = P["_build_profile"](coins(5, mkfast), now_ms=NOW)
assert pd_["n_fast"] == 5 and len(_dcalls) == 5 and len(set(_dcalls)) == 5, _dcalls   # 1 запит на (монета, бік)
pd_ = P["_build_profile"](coins(2, mkfast) + coins(2, mkslow), now_ms=NOW)
assert len(_dcalls) == 5 + 2   # C0/C1 з обох наборів — та сама пара (монета, бік) → без повтору
assert "def _sim_depth_quiet" in src and "_sim_depth(coin, side, count=False)" in src
# _build_profile у проді бере _sim_depth лише як інформативний ratio: без глибини профіль той самий
assert 'depth_fn = globals().get("_sim_depth_quiet") or globals().get("_sim_depth")' in src and "n_nodepth\": 0" in src
# CLAUDE.md: розділ v2.18
cm = open((_HL + "/CLAUDE.md"), encoding="utf-8").read()
assert "v2.18" in cm and "невизначен" in cm and F10 in cm
print("8) UI-маркери і синтаксис, _sim_depth без придатної глибини = 0, CLAUDE.md v2.18")

print("\nУСІ РЕГРЕСІЇ v2.18 ЗЕЛЕНІ")

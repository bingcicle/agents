import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.10 — фікси зовнішнього аудиту 04.09 (9-й аудит):
№1 гейт $50k по позиції ЕПІЗОДУ при full_close; №2 R7 по епізоду
(start_size/max_tx, частки в токенах); №3 ratio-гейт у rev/follow;
№4 свіжий ratio після часткових закриттів; №5 sys tx-id для нульових
hash + ADL + глобальний дедуп; №6 перший постріл — властивість tx, не
батча; №7 база відсотка = startPosition транзакції; №8 бік Short > Long
у профілі; + fc_episodes у state, fc_lock у save, width-гард CSV,
робастний load_leaderboard."""
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

def _dt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else ""

C = dict(PROFILE_ALGO_V=9, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.10",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         MIN_CLOSE_PCT=0.05, STRAT2_ENABLED=True)

assert const("DATA_ALGO_V") == "2.20" and const("PROFILE_ALGO_V") == 11

def mk_rev(px_ago=102.0, eps=None):
    ns = load({"rev_on_close"}, dict(C,
        strat2_lock=threading.RLock(), rev_open={},
        fc_lock=threading.Lock(), fc_episodes=dict(eps or {}),
        is_vault=lambda a: False,
        _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (px_ago if c != "BTC" else 50000.0),
        _sim_depth=lambda c, s: 1e5,
        _dt=_dt, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _sig_seq=[0], REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
        _strat_csv_append=lambda p, h, r: True))
    return ns

def strats_of(ns):
    return {p["strategy"] for p in ns["rev_open"].values()}

def mk_ep(val, start_size, max_sz, max_usd, max_liq=0):
    return {"first_ts": 0, "last_ts": 60_000, "sum_usd": 0.0, "seen": set(),
            "side": "LONG", "ratio": 3, "val": val, "first_px": 100.0,
            "start_size": start_size, "max_sz": max_sz, "max_usd": max_usd,
            "max_liq": max_liq}

# ═══ 1. Гейт $50k при full_close — по позиції ЕПІЗОДУ ═════════════
# Кит $200k злив $195k раніше; фінальний батч бачить залишок $5k.
# СТАРА логіка: return (жодного R, жодного _OUTCOME). НОВА: епізод
# пам'ятає $200k -> сигнал живе (R1 відкривається)
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 950.0, 95_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 50.0, "side": "LONG", "val": 5_000, "ratio": 3},
                   [{"sz": 50.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
st = strats_of(ns)
assert "R1_загальний" in st and "_OUTCOME" in {p["strategy"] for p in ns["rev_open"].values()}, st
# ...але СПРАВДІ мала позиція ($30k епізод) — як і раніше, тиша
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(30_000.0, 300.0, 300.0, 30_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 300.0, "side": "LONG", "val": 30_000, "ratio": 3},
                   [{"sz": 300.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
assert ns["rev_open"] == {}
# частковий сигнал без full_close гейтиться по old["val"], як раніше
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 100.0, 10_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 400.0, "side": "LONG", "val": 40_000, "ratio": 3},
                   [{"sz": 150.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long"}], False)
assert ns["rev_open"] == {}
print("1) full_close гейтиться позицією епізоду: $5k-хвіст $200k-зливу = сигнал")

# ═══ 2. R7 по епізоду: велика tx у ПОПЕРЕДНЬОМУ батчі ═════════════
# (а) епізод $1M: перша tx $950k (95% токенів), фінальний батч — пил
# $50k. R7 ВІДКРИВАЄТЬСЯ (кейс аудиту: раніше велика tx губилась)
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(1_000_000.0, 10_000.0, 9_500.0,
                                       950_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 500.0, "side": "LONG", "val": 50_000, "ratio": 3},
                   [{"sz": 500.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
assert "R7_одним" in strats_of(ns)
# (б) 50%+50% двома батчами: max_sz = половина -> R7 ні, R1 так
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 1000.0,
                                       100_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 1000.0, "side": "LONG", "val": 100_000, "ratio": 3},
                   [{"sz": 1000.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
st = strats_of(ns)
assert "R1_загальний" in st and "R7_одним" not in st, st
# (в) частка в ТОКЕНАХ, не доларах: 90% токенів по вищій ціні
# (нотіонал 95.4% від стартової вартості) — НЕ один постріл
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 1800.0,
                                       190_800.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 200.0, "side": "LONG", "val": 20_000, "ratio": 3},
                   [{"sz": 200.0, "px": 106.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
assert "R7_одним" not in strats_of(ns)
# (г) і навпаки: всі 100% токенів по нижчій ціні ($180k < 95% від
# $200k) — ЦЕ один постріл, доларова формула його губила
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 2000.0,
                                       180_000.0)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 2000.0, "side": "LONG", "val": 180_000, "ratio": 3},
                   [{"sz": 2000.0, "px": 90.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
assert "R7_одним" in strats_of(ns)
# (д) ліквідація як найбільша tx епізоду -> R7 ні
ns = mk_rev(eps={("0xa", "AAA"): mk_ep(200_000.0, 2000.0, 2000.0,
                                       200_000.0, max_liq=1)})
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 2000.0, "side": "LONG", "val": 200_000, "ratio": 3},
                   [{"sz": 2000.0, "px": 100.0, "hash": "0x9",
                     "dir": "Close Long", "liq": True}], True)
assert "R7_одним" not in strats_of(ns)
# (е) fc_on_txs веде start_size/max_tx епізоду
fct = load({"fc_on_txs"}, dict(C, FC_ENABLED=True, FC_MAX_EPISODE_S=300,
                               fc_lock=threading.Lock(), fc_episodes={},
                               fc_positions={}))
fct["fc_on_txs"]("0xa", "AAA",
                 {"side": "LONG", "ratio": 3, "val": 200_000.0, "size": 2000.0},
                 [{"sz": 300.0, "px": 100.0, "ts": 1000, "hash": "0x1"},
                  {"sz": 1500.0, "px": 101.0, "ts": 2000, "hash": "0x2"}])
ep = fct["fc_episodes"][("0xa", "AAA")]
assert ep["start_size"] == 2000.0 and ep["max_sz"] == 1500.0
assert ep["max_usd"] == 1500.0 * 101.0 and ep["max_liq"] == 0
print("2) R7: епізодні start_size/max_tx, частки в токенах, ліквідація — ні")

# ═══ 3. Ratio-гейт у rev і follow хуках ═══════════════════════════
ns = mk_rev()
ns["rev_on_close"]("0xa", "AAA",
                   {"size": 1000.0, "side": "LONG", "val": 150_000,
                    "ratio": 1.4},
                   [{"sz": 1000.0, "px": 150.0, "hash": "0x9",
                     "dir": "Close Long"}], True)
assert ns["rev_open"] == {}   # ratio < 2: ні стратегій, ні _OUTCOME
def mk_fol(prof=None, last_close=None):
    return load({"follow_on_txs"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={}, rev_open={},
        follow_last_close=dict(last_close or {}),
        wallet_profiles=({} if prof is None else {"0xabc": dict(prof)}),
        profiles_fetching=set(),
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: None, stats={}))
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 1000.0, "side": "LONG", "ratio": 1.0, "val": 5e5},
                   [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
assert n["follow_open"] == {}   # відтворений кейс аудиту: було F1-F3+F6
assert "0xabc:AAA" in n["follow_last_close"]   # але мітка пари оновлена
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 1000.0, "side": "LONG", "ratio": 2.5, "val": 5e5},
                   [{"sz": 100.0, "px": 100.0, "ts": 1}], False)
assert len(n["follow_open"]) == 4   # ratio >= 2 — працює як раніше
print("3) ratio<2: rev і follow мовчать (як алерти); мітка пари живе")

# ═══ 4. Свіжий ratio після часткового закриття ════════════════════
i_fr = src.index("def _fresh_ratio")
assert "cache_lock" in src[i_fr:i_fr + 600]
# усі три місця оновлення size/val тепер оновлюють і ratio
n_sites = src.count('watchlist[addr][coin]["upd"]  = time.time()')
n_ratio = src.count('_mark_ratio(watchlist[addr][coin], _r9)')   # v2.11: через _mark_ratio
assert n_sites == 3 and n_ratio == 3, (n_sites, n_ratio)
fr = load({"_fresh_ratio"}, {
    "cache_lock": threading.Lock(),
    "cache": {"depth": {"AAA": {"bid": 1e5}}, "depth_prev": {}},
    "depth_for_side": lambda d, s: (d or {}).get("bid", 0)})["_fresh_ratio"]
assert fr("AAA", "LONG", 150_000.0) == 1.5
assert fr("ZZZ", "LONG", 150_000.0) is None   # немає глибини — не чіпаємо
print("4) часткові закриття оновлюють ratio за свіжою глибиною (3/3 місця)")

# ═══ 5. Системні tx-id: нульовий hash / ADL / дедуп ═══════════════
T = 1_700_000_000_000
def base_fill(i, **kw):
    f = {"time": T + i, "px": "100.0", "sz": "1000.0", "dir": "Close Long",
         "hash": f"0xaaa{i}", "crossed": True, "coin": "X", "oid": 100 + i,
         "twapId": None, "tid": 500 + i, "startPosition": "3000.0"}
    f.update(kw); return f
def gmf_with(fills):
    return load({"get_recent_market_fills"}, dict(C,
        hl_post=lambda b, retries=4: fills))["get_recent_market_fills"]
ZERO_H = "0x" + "0" * 64
# (а) дві РІЗНІ ліквідації з нульовим hash (різні oid) — 2 транзакції,
# а не склеєна одна 2×3%≈6% (кейс аудиту)
liq = {"liquidation": {"method": "market"}}
txs = gmf_with([base_fill(0, hash=ZERO_H, oid=1, **liq),
                base_fill(1, hash=ZERO_H, oid=2, **liq)])(
    "0xwallet00ff", "X", T - 10, "LONG")
assert len(txs) == 2, [t["hash"] for t in txs]
ids = {t["hash"] for t in txs}
assert len(ids) == 2 and all(i.startswith("sys:0xwallet00") for i in ids)
# id стабільний і НЕ колізує глобально: інший гаманець -> інший id
txs2 = gmf_with([base_fill(0, hash=ZERO_H, oid=1, **liq)])(
    "0xother0000", "X", T - 10, "LONG")
assert txs2[0]["hash"] not in ids
# (б) ADL класифікується як закриття у live (раніше пропускався)
txs = gmf_with([base_fill(0, hash=ZERO_H, dir="Auto-Deleveraging",
                          crossed=False)])("0xwallet00ff", "X", T - 10, "LONG")
assert len(txs) == 1 and txs[0]["liq"]
# (в) ліквідація зі СПРАВЖНІМ hash теж отримує sys-id (двом різним
# ліквідаціям біржа може дати спільний блок)
txs = gmf_with([base_fill(0, **liq)])("0xwallet00ff", "X", T - 10, "LONG")
assert txs[0]["hash"].startswith("sys:")
print("5) нульовий hash/ADL: окремі sys-id по oid+гаманцю, глобальний дедуп цілий")

# ═══ 6. Перший постріл — властивість транзакції, не батча ═════════
# батч: старша дрібна tx (1%) + новіша велика (10%): велика ВЖЕ не перша
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5},
                   [{"sz": 10.0, "px": 100.0, "ts": 1000},
                    {"sz": 100.0, "px": 100.0, "ts": 2000}], False)
f1 = [p for p in n["follow_open"].values() if p["strategy"] == "F1_1хв"][0]
assert f1["first_shot"] == 0, f1["first_shot"]
assert not any(p["strategy"] == "F6_1хв_перший"
               for p in n["follow_open"].values())
# велика tx НАЙСТАРША в батчі -> перший постріл її
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5},
                   [{"sz": 100.0, "px": 100.0, "ts": 1000},
                    {"sz": 10.0, "px": 100.0, "ts": 2000}], False)
f1 = [p for p in n["follow_open"].values() if p["strategy"] == "F1_1хв"][0]
assert f1["first_shot"] == 1
print("6) перший постріл: старша tx у батчі з'їдає його ще ДО великої")

# ═══ 7. База відсотка = startPosition транзакції ══════════════════
# 100 токенів: tx 4.0 (4%), потім 4.9 при sp=96 -> 5.1% — СИГНАЛ
big_src = src[src.index("def get_recent_market_fills"):]
assert '"sp":      t.get("sp", 0.0)' in big_src
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 100.0, "side": "LONG", "ratio": 3, "val": 5e5},
                   [{"sz": 4.9, "px": 20000.0, "ts": 1000, "sp": 96.0}],
                   False)
assert len(n["follow_open"]) == 4   # 4.9/96 = 5.1% >= 5%
f1 = [p for p in n["follow_open"].values() if p["strategy"] == "F1_1хв"][0]
assert abs(f1["tx_pct"] - 4.9 / 96.0 * 100) < 1e-9
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA",
                   {"size": 100.0, "side": "LONG", "ratio": 3, "val": 5e5},
                   [{"sz": 4.9, "px": 20000.0, "ts": 1000, "sp": 100.0}],
                   False)
assert n["follow_open"] == {}       # 4.9/100 = 4.9% < 5%
# групування переносить sp (максимальний по групі)
txs = gmf_with([base_fill(0, hash="0xh1", startPosition="2000.0"),
                base_fill(1, hash="0xh1", startPosition="1000.0")])(
    "0xwallet00ff", "X", T - 10, "LONG")
assert txs[0]["sp"] == 2000.0
# у алерт-шляхах база — sp (структурно, обидва шляхи)
assert src.count('f["sz"] >= (f.get("sp") or _base) * MIN_CLOSE_PCT') == 2
print("7) поріг 5% і tx_pct рахуються від startPosition транзакції")

# ═══ 8. Бік Short > Long у профілі ════════════════════════════════
calls = []
def depth_spy(coin, side):
    calls.append(side)
    return 1e4
bp = _v28_with_opens(load({"_grade_episode", "_build_profile"},
          dict(C, _sim_depth=depth_spy))["_build_profile"])   # v2.18
fills8 = [{"coin": "AAA", "hash": "0x1", "time": T, "px": "100.0",
           "sz": "2000.0", "startPosition": "-2000.0",
           "dir": "Short > Long", "crossed": True}]
prof8 = bp(fills8, now_ms=T + 10_000_000)
assert "SHORT" in calls and "LONG" not in calls, calls
assert prof8["n_big"] == 1
print("8) Short > Long закриває ШОРТ: глибина з ask-сторони")

# ═══ 9. Стан/CSV/лідерборд ════════════════════════════════════════
i_sv = src.index("def _save_state_locked")
seg = src[i_sv:src.index("def load_state")]
assert "with fc_lock:" in seg and '"fc_episodes"' in seg
i_ld = src.index("def load_state")
assert 'snap.get("fc_episodes", [])' in src[i_ld:i_ld + 4500]   # v2.14: load_state довший (.bak/структура)
# width-гард: рядок чужої ширини — відмова, файл не забруднений
ap = load({"_strat_csv_append"},
          {"_csv_lock": threading.Lock()})["_strat_csv_append"]
d9 = tempfile.mkdtemp(); p9 = os.path.join(d9, "t.csv")
assert ap(p9, ["a", "b", "c"], ["1", "2", "3"])
assert not ap(p9, ["a", "b", "c"], ["1"])          # 1 колонка з 3
p9b = os.path.join(d9, "t2.csv")                    # окремий файл: інші
assert not ap(p9b, ["a", "b", "eol"], ["1", "2", "3", "4"])  # задовгий
assert not os.path.exists(p9b)                      # відмова ДО створення
assert len(open(p9).read().strip().splitlines()) == 2   # заголовок + 1
# лідерборд: list-відповідь і порожня відповідь не валять функцію тихо
llb = load({"load_leaderboard"}, dict(C,
    hl_get=lambda u: [], cache_lock=threading.Lock(),
    cache={"lb_total": 0}, SCAN_TOP=60000))
try:
    llb["load_leaderboard"](); raise SystemExit("FAIL: порожній лідерборд")
except NEW["APIError"]:
    pass
llb = load({"load_leaderboard"}, dict(C,
    hl_get=lambda u: [{"ethAddress": "0xw", "accountValue": "5",
                       "windowPerformances": []}],
    cache_lock=threading.Lock(), cache={"lb_total": 0}, SCAN_TOP=60000))
assert llb["load_leaderboard"]()[0]["addr"] == "0xw"   # list-формат ок
print("9) fc_episodes у state (під fc_lock), width-гард CSV, лідерборд робастний")

print("\nALL v2.10 TESTS PASSED")

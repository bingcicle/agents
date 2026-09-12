import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.9 (ТЗ 04.09): (1) FC-трек 60 хв; (2) R7 «одним пострілом»;
(3) F6 «тиша 1 хв · перший постріл»; (4) F7 «без ratio» + nr-гілка
профілю; (5) фікси аудиту покриття — depth-гард update_watchlist, sweep
підхоплює нові монети кита, /positions поза cache_lock, VIP-скан."""
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

C = dict(PROFILE_ALGO_V=8, PROFILE_ERR_TTL_S=6*3600, PROFILE_TTL_S=24*3600,
         PROFILE_HARD_TTL_S=48*3600, MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         F4_MIN_EPISODES=5, F4_MIN_NOTIONAL=100_000.0, F4_MAX_UNLOAD_S=300.0,
         F4_CHUNK_PCT=0.05, PX_AGO_TOL_S=90.0, DATA_ALGO_V="2.9",
         WFAIL_CAP=200, REV_TRACK_MIN=60, SIM_COMMISSION=0.0005,
         VAULT_PART_PCT=0.05, PART_OUT_PCT=0.30, REV_OUT_MIN_MAG=0.5,
         REV_WINDOW_S=180, BTC_VETO_PCT=0.15, BIG_COINS=("ZEC", "HYPE"),
         REV_BRK_PCT=0.3, REV_BRK_WINDOW_S=600, FOLLOW_TX_PCT=0.05,
         FOLLOW_TIMERS={"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180},
         F4_NAME="F4_розумний", F4_CLAMP=(60.0, 300.0), F4_FULL_PCT=0.95,
         STRAT2_ENABLED=True)

# ═══ 1. FC: трек 60 хв, нові заголовки, ротація старого файла ══════
assert const("FC_TRACK_MIN") == 60
FC_HEADERS = load([], {})  # заголовки рахуємо з констант
fc_h = (["date", "coin", "our_side", "whale_addr", "sum_usd",
         "duration_s", "ratio", "ratio_per_min", "move_pct",
         "entry_px", "pnl_end_pct", "peak_pct"]
        + [f"m{i}" for i in range(1, 61)])
i_fc = src.index("FC_HEADERS = ")
assert '"pnl_end_pct"' in src[i_fc:i_fc + 300]
assert "FC_TRACK_MIN + 1" in src[i_fc:i_fc + 300]
# _fc_finish пише через _strat_csv_append (ротація формату), не сирим csv
i_ff = src.index("def _fc_finish")
seg = src[i_ff:src.index("def run_fc_loop")]
assert "_strat_csv_append(FC_CSV, FC_HEADERS, row)" in seg
assert "csv.writer" not in seg
# ротація: файл зі СТАРИМ заголовком (m1..m30) їде у .legacy
d1 = tempfile.mkdtemp()
p1 = os.path.join(d1, "fc_trades.csv")
open(p1, "w").write(",".join(fc_h[:12] + [f"m{i}" for i in range(1, 31)]) + "\n1,2\n")
ap = load({"_strat_csv_append"},
          {"_csv_lock": threading.Lock()})["_strat_csv_append"]
assert ap(p1, fc_h, ["x"] * len(fc_h))
assert any(f.startswith("fc_trades.csv.legacy-") for f in os.listdir(d1))
head = open(p1).readline().strip()
assert head == ",".join(fc_h)
print("1) FC: 60 хв, pnl_end_pct, старий файл ротується у .legacy")

# ═══ 2. R7 «одним пострілом» ══════════════════════════════════════
assert const("R7_MIN_TX_USD") == 100_000.0
def mk_rev(px_ago=102.0):
    calls = []
    ns = load({"rev_on_close"}, dict(C,
        strat2_lock=threading.RLock(), rev_open={},
        fc_lock=threading.Lock(), fc_episodes={},
        is_vault=lambda a: False,
        _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
        _px_ago=lambda c, s: (px_ago if c != "BTC" else 50000.0),
        _sim_depth=lambda c, s: 1e5,
        _dt=_dt, _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
        _sig_seq=[0], REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
        _strat_csv_append=lambda p, h, r: calls.append(r) or True))
    return ns, calls
OLD_POS = {"size": 1000.0, "side": "LONG", "val": 150_000, "ratio": 3}
def strats_of(ns):
    return {p["strategy"] for p in ns["rev_open"].values()}
# (а) повне закриття ОДНІЄЮ tx $150k (100% позиції), рух 2% -> R7 (+R1/R2/R4)
r, _ = mk_rev()
r["rev_on_close"]("0xa", "AAA", OLD_POS,
                  [{"sz": 1000.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"}],
                  True)
st = strats_of(r)
assert "R7_одним" in st and "R1_загальний" in st, st
r7 = [p for p in r["rev_open"].values() if p["strategy"] == "R7_одним"][0]
assert r7["state"] == "open" and r7.get("entry_px") == 100.0   # вхід одразу
# (б) повне закриття ДВОМА tx (60%+40%) -> R1 так, R7 ні
r, _ = mk_rev()
r["rev_on_close"]("0xa", "AAA", OLD_POS,
                  [{"sz": 600.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"},
                   {"sz": 400.0, "px": 150.0, "hash": "0x2", "dir": "Close Long"}],
                  True)
st = strats_of(r)
assert "R1_загальний" in st and "R7_одним" not in st, st
# (в) одна tx, але < $100k -> ні (позиція $60k >= MIN_POS_USD)
r, _ = mk_rev()
r["rev_on_close"]("0xa", "AAA",
                  {"size": 1000.0, "side": "LONG", "val": 60_000, "ratio": 3},
                  [{"sz": 1000.0, "px": 60.0, "hash": "0x1", "dir": "Close Long"}],
                  True)
st = strats_of(r)
assert "R1_загальний" in st and "R7_одним" not in st, st
# (г) одна велика tx, але НЕ повне закриття (партіал 40%) -> ні
r, _ = mk_rev()
r["rev_on_close"]("0xa", "AAA",
                  {"size": 1000.0, "side": "LONG", "val": 400_000, "ratio": 3},
                  [{"sz": 400.0, "px": 400.0, "hash": "0x1", "dir": "Close Long"}],
                  False)
assert "R7_одним" not in strats_of(r)
# (д) волт -> лише R6, без R7 (свідомо: волти окремо)
r, _ = mk_rev()
r["is_vault"] = lambda a: True
ns2 = load({"rev_on_close"}, dict(C,
    strat2_lock=threading.RLock(), rev_open={},
    fc_lock=threading.Lock(), fc_episodes={},
    is_vault=lambda a: True,
    _px_now=lambda c: (100.0 if c != "BTC" else 50000.0),
    _px_ago=lambda c, s: (102.0 if c != "BTC" else 50000.0),
    _sim_depth=lambda c, s: 1e5, _dt=_dt,
    _sig_retry_lock=threading.Lock(), _sig_retry_q=[], _sig_seq=[0],
    REV_SIG_CSV="sig.csv", REV_SIG_HEADERS=[],
    _strat_csv_append=lambda p, h, r_: True))
ns2["rev_on_close"]("0xa", "AAA", OLD_POS,
                    [{"sz": 1000.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"}],
                    True)
st = strats_of(ns2)
assert "R6_волт" in st and "R7_одним" not in st, st
# (е-1, рев'ю S1) мультибатч при живому WS: епізод стартував із $200k,
# перед батчем лишилось 50% — остання tx закриває 100% ЗАЛИШКУ, але це
# НЕ «одним пострілом» (нотіонал $100k < 95% від val епізоду $200k)
r, _ = mk_rev()
r["fc_episodes"][("0xa", "AAA")] = {"first_ts": 0, "last_ts": 60_000,
    "sum_usd": 100_000.0, "seen": set(), "side": "LONG",
    "ratio": 3, "val": 200_000.0, "first_px": 100.0,
    "start_size": 1000.0, "max_sz": 500.0, "max_usd": 100_000.0,
    "max_liq": 0}
r["rev_on_close"]("0xa", "AAA",
                  {"size": 500.0, "side": "LONG", "val": 100_000, "ratio": 3},
                  [{"sz": 500.0, "px": 200.0, "hash": "0x2", "dir": "Close Long"}],
                  True)
st = strats_of(r)
assert "R1_загальний" in st and "R7_одним" not in st, st
# (е-2) а СПРАВЖНІЙ one-shot із живим епізодом тієї ж ширини — проходить
r, _ = mk_rev()
r["fc_episodes"][("0xa", "AAA")] = {"first_ts": 0, "last_ts": 1_000,
    "sum_usd": 0.0, "seen": set(), "side": "LONG",
    "ratio": 3, "val": 200_000.0, "first_px": 100.0,
    "start_size": 1000.0, "max_sz": 1000.0, "max_usd": 200_000.0,
    "max_liq": 0}
r["rev_on_close"]("0xa", "AAA",
                  {"size": 1000.0, "side": "LONG", "val": 200_000, "ratio": 3},
                  [{"sz": 1000.0, "px": 200.0, "hash": "0x2", "dir": "Close Long"}],
                  True)
assert "R7_одним" in strats_of(r)
# (е-3, рев'ю S2) ліквідація не відкриває R7 (не рішення кита)
r, _ = mk_rev()
r["rev_on_close"]("0xa", "AAA", OLD_POS,
                  [{"sz": 1000.0, "px": 150.0, "hash": "0x1",
                    "dir": "Close Long", "liq": True}],
                  True)
st = strats_of(r)
assert "R1_загальний" in st and "R7_одним" not in st, st
# (е) рух < 1% -> жодних стратегій (0.7% лише outcome)
r, _ = mk_rev(px_ago=100.7)
r["rev_on_close"]("0xa", "AAA", OLD_POS,
                  [{"sz": 1000.0, "px": 150.0, "hash": "0x1", "dir": "Close Long"}],
                  True)
assert strats_of(r) == {"_OUTCOME"}
# API рахує R7 у списку реверсів
assert '"R5_дуже", "R6_волт", R7_NAME, R8_NAME)' in src
print("2) R7: одна tx >=95% і >=$100k; батч/малий/партіал/волт/слабкий рух — ні")

# ═══ 3. F6 «тиша 1 хв · перший постріл» ═══════════════════════════
PROF_OK = {"ok": True, "v": 8, "fetched": time.time() - 100,
           "n_big": 7, "n_fast": 6, "n_slow": 1, "fast_pct": 85.7,
           "unload_med_s": 95.0, "unload_mean_s": 110.5,
           "window_d": 61.3, "avg_gap_s": 40.0}
def mk_fol(prof=None, last_close=None):
    n = load({"follow_on_txs"}, dict(C,
        strat2_lock=threading.RLock(), follow_open={}, rev_open={},
        follow_last_close=({} if last_close is None else dict(last_close)),
        wallet_profiles=({} if prof is None else {"0xabc": dict(prof)}),
        profiles_fetching=set(),
        is_vault=lambda a: False, _px_now=lambda c, max_age=20: 100.0,
        _px_ago=lambda c, s: 100.0, _sim_depth=lambda c, s=None: 1e5,
        _profile_request=lambda a: None, stats={}))
    return n
OLD_F = {"size": 1000.0, "side": "LONG", "ratio": 3, "val": 5e5}
TX = [{"sz": 100.0, "px": 100.0, "ts": 1}]
def opened(n): return sorted(p["strategy"] for p in n["follow_open"].values())
# (а) перший постріл без профілю: F1-F3 + F6, таймер F6 = 60с
n = mk_fol()
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв", "F6_1хв_перший"], opened(n)
f6 = [p for p in n["follow_open"].values() if p["strategy"] == "F6_1хв_перший"][0]
assert f6["timer"] == 60.0 and f6["first_shot"] == 1
# (б) повтор пари за 10 хв: F6 мовчить, F1-F3 працюють
n = mk_fol(last_close={"0xabc:AAA": time.time() - 600})
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert opened(n) == ["F1_1хв", "F2_2хв", "F3_3хв"], opened(n)
# (в) пауза >= 1 год: F6 знову відкривається
n = mk_fol(last_close={"0xabc:AAA": time.time() - 3700})
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert "F6_1хв_перший" in opened(n)
print("3) F6: перший постріл/пауза >=1 год, таймер 60с, без профілю")

# ═══ 4. F7 «без ratio» + nr-гілка профілю ═════════════════════════
# (а) _build_profile: глибока монета (ratio < 2) — основна гілка
# порожня, nr бачить епізоди (лише гейт $100k)
bp = _v28_with_opens(load({"_grade_episode", "_build_profile"},
          dict(C, _sim_depth=lambda c, s=None: 1e6))["_build_profile"])   # v2.18
def mk_fills(n_ep, t0=1_000_000_000_000):
    fills, t = [], t0
    for i in range(n_ep):
        # позиція $200k, злив двома tx по 50% за 100с
        for j, frac in enumerate((0.5, 0.5)):
            fills.append({"coin": "AAA", "hash": f"0x{i}_{j}",
                          "time": t + j * 100_000, "px": "100.0",
                          "sz": str(1000.0 * frac),
                          "startPosition": str(2000.0 - 1000.0 * 2 * (0.5 * j)),
                          "dir": "Close Long", "crossed": True})
        t += 3_600_000   # нова позиція за годину (реопен ділить сегменти)
        fills.append({"coin": "AAA", "hash": f"0xr{i}", "time": t - 1,
                      "px": "100.0", "sz": "1.0", "startPosition": "2000.0",
                      "dir": "Close Long", "crossed": True})
    return fills
# простіша фікстура: 6 позицій $200k, кожна злита в нуль одним пострілом
fills = []
t = 1_000_000_000_000
for i in range(6):
    fills.append({"coin": "AAA", "hash": f"0x{i}", "time": t,
                  "px": "100.0", "sz": "2000.0", "startPosition": "2000.0",
                  "dir": "Close Long", "crossed": True})
    t += 3_600_000
prof = bp(fills, now_ms=t + 10_000_000)
assert prof["n_big"] == 6 and prof["ok"]              # v2.18: ratio до поточної глибини історію не гейтить
nr = prof["nr"]
assert nr["n_big"] == 6 and nr["n_fast"] == 6 and nr["ok"], nr
assert nr["fast_pct"] == 100.0 and nr["unload_med_s"] == 0.0
# (б) мілка монета (depth $10k): основна гілка теж бачить (ratio 20)
bp2 = _v28_with_opens(load({"_grade_episode", "_build_profile"},
           dict(C, _sim_depth=lambda c, s=None: 1e4))["_build_profile"])   # v2.18
prof2 = bp2(fills, now_ms=t + 10_000_000)
assert prof2["ok"] and prof2["nr"]["ok"]
assert prof2["n_fast"] == 6 == prof2["nr"]["n_fast"]  # nr — надмножина
# (в) битий філ: err на весь профіль, nr.ok теж False
bad_fills = fills + [{"coin": "AAA", "hash": "0xbad", "time": t,
                      "px": "100.0", "sz": "10.0",
                      "dir": "Close Long", "crossed": True}]  # без startPosition
prof3 = bp2(bad_fills, now_ms=t + 10_000_000)
assert prof3.get("err") and not prof3["ok"] and not prof3["nr"]["ok"]
# (г) v2.18: профіль ЄДИНИЙ (без ratio-гейта в історії; nr = той самий), статус ok →
# F4 і F10 на кожній достатній tx, F5 (вихід 60 с)/F7 (таймер профілю) — лише 1-й постріл;
# F9 — лише ≥90% підтверджено швидких (7/8 = 87.5% — ні)
PROF_NR = {"ok": True, "status": "ok", "v": 8, "fetched": time.time() - 100,
           "n_big": 8, "n_fast": 7, "n_slow": 1, "n_uncertain": 0, "fast_pct": 87.5,
           "unload_med_s": 120.0, "unload_mean_s": 130.0,
           "window_d": 61.3, "avg_gap_s": 50.0}
n = mk_fol(PROF_NR)
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
st = set(opened(n))
assert {"F4_розумний", "F10_розумний_60", "F5_перший", "F7_без_ратіо"} <= st and "F9_без_ратіо_90" not in st, st
f7 = [p for p in n["follow_open"].values() if p["strategy"] == "F7_без_ратіо"][0]
f5 = [p for p in n["follow_open"].values() if p["strategy"] == "F5_перший"][0]
f10 = [p for p in n["follow_open"].values() if p["strategy"] == "F10_розумний_60"][0]
assert f7["timer"] == 100.0 and f7["profile_gap"] == 100.0   # 2×50
assert f5["timer"] == 60.0 and f10["timer"] == 60.0            # фіксований вихід
assert f7["prof"]["n_fast"] == 7 and f7["prof"]["fast_pct"] == 87.5 \
       and f7["prof"]["unload_med_s"] == 120.0 and f7["prof"]["window_d"] == 61.3 and f7["prof_status"] == "ok"
# (д) повтор пари за 10 хв: F5/F7 мовчать (перший постріл обов'язковий), F4/F10 — ні
n = mk_fol(PROF_NR, last_close={"0xabc:AAA": time.time() - 600})
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
st = set(opened(n))
assert "F7_без_ратіо" not in st and "F5_перший" not in st and "F4_розумний" in st and "F10_розумний_60" in st
# (е) ≥90% підтверджено швидких → F9 (по точних лічильниках, невизначені = повільні)
n = mk_fol(dict(PROF_NR, n_big=10, n_fast=9, n_slow=1, fast_pct=90.0))
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert "F9_без_ратіо_90" in opened(n)
n = mk_fol(dict(PROF_NR, n_big=10, n_fast=8, n_slow=1, n_uncertain=1, fast_pct=80.0))
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert "F9_без_ратіо_90" not in opened(n)
# (є) жорстка стеля віку діє на всі профільні стратегії
n = mk_fol(dict(PROF_NR, fetched=time.time() - 49 * 3600))
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert not ({"F4_розумний", "F5_перший", "F7_без_ратіо", "F10_розумний_60"} & set(opened(n)))
# (ж) truncated блокує всі профільні стратегії
n = mk_fol(dict(PROF_NR, truncated=1))
n["follow_on_txs"]("0xabc", "AAA", OLD_F, TX, False)
assert not ({"F4_розумний", "F5_перший", "F7_без_ратіо"} & set(opened(n)))
# API: слідує за FOLLOW_TIMERS + F6/F4/F5/F7; PROFILE_ALGO_V = 8
assert "for st in list(FOLLOW_TIMERS) + [F6_NAME, F4_NAME, F5_NAME, F8_NAME," in src   # v2.11: +F8/F9
assert const("PROFILE_ALGO_V") == 11 and const("DATA_ALGO_V") == "2.20"
assert '"ok_nr"' in src   # лічильник придатних для F7 у /strat2
print("4) F7/nr: кваліфікація без ratio, свій таймер і prof-колонки, гейти віку")

# ═══ 5. Фікси аудиту покриття ═════════════════════════════════════
# (а) update_watchlist: порожній depth-знімок НЕ стирає watchlist
wl = {"0xw": {"AAA": {"size": 1.0, "val": 5e5, "side": "LONG",
                      "ratio": 4.0, "entry": 1.0, "liq": 0}}}
ns = load({"update_watchlist"}, dict(
    COIN_BLACKLIST=set(), watchlist_lock=threading.Lock(),
    watchlist=dict(wl), scan_tombstones={}, sent_alerts=set(),
    close_episodes={}, delta_seen={}, fill_cursor={},
    depth_for_side=lambda d, s: (d or {}).get("bid", 0)))
ns["update_watchlist"]({"AAA": []}, {"AAA": {"bid": 1e5}}, time.time())
assert ns["watchlist"] == wl   # 1 монета глибини < 10 -> гард, без змін
# з нормальним depth-знімком (>=10 монет) перебудова працює
depth10 = {f"C{i}": {"bid": 1e5} for i in range(10)}
ns["update_watchlist"]({"AAA": []}, depth10, time.time(), None,
                       {"0xw": time.time()})   # гаманець реально зчитаний
# v2.20 (аудит v2.19 №8): скан каже «позицій нема», а realtime ще не підтвердив (без tombstone) —
# пара НЕ зникає: лишається з міткою _gone_ts, щоб sweep дістав філи й доставив повне закриття
assert set(ns["watchlist"]) == {"0xw"} and ns["watchlist"]["0xw"]["AAA"].get("_gone_ts"), ns["watchlist"]
ns["scan_tombstones"]["0xw:AAA"] = time.time() + 1      # realtime підтвердив закриття після старту скану
ns["update_watchlist"]({"AAA": []}, depth10, time.time(), None, {"0xw": time.time()})
assert ns["watchlist"] == {}   # тепер перебудовано
i_uw = src.index("def update_watchlist")
assert "len(depth_snap) < 10" in src[i_uw:i_uw + 1500]
print("5а) update_watchlist: гард глибини (<10 монет -> без перезапису)")

# (б) sweep: нова монета відомого кита зі знімка -> у watchlist
i0 = src.index("            # НОВІ монети відомого кита")
i1 = src.index("    def _safe_worker", i0)
sweep_src = textwrap.dedent(src[i0:i1])
class _CacheLock:
    def __enter__(self): return self
    def __exit__(self, *a): return False
nsw = {"current": {"AAA": {"size": 1.0, "val": 5e5, "side": "LONG",
                           "entry": 2.0, "liq": 1.0},
                   "BBB": {"size": 1.0, "val": 1e4, "side": "LONG",
                           "entry": 2.0, "liq": 1.0},          # ratio 0.1
                   "BTC": {"size": 1.0, "val": 9e9, "side": "LONG",
                           "entry": 2.0, "liq": 1.0}},          # блекліст
       "coins": {"CCC": {}},   # CCC вже під наглядом — цикл вище
       "addr": "0xw", "snap_ms": 123456,
       "cache": {"depth": {"AAA": {"bid": 1e5}, "BBB": {"bid": 1e5}},
                 "depth_prev": {}},
       "cache_lock": _CacheLock(), "watchlist_lock": _CacheLock(),
       "watchlist": {}, "sent_alerts": set(), "close_episodes": {},
       "fill_cursor": {}, "COIN_BLACKLIST": {"BTC"},
       "scan_tombstones": {}, "delta_seen": {},
       "depth_for_side": lambda d, s: (d or {}).get("bid", 0),
       "time": time, "print": lambda *a, **k: None}
exec(sweep_src, nsw)
assert "AAA" in nsw["watchlist"].get("0xw", {}), nsw["watchlist"]
assert nsw["watchlist"]["0xw"]["AAA"]["ratio"] == 5.0
assert "BBB" not in nsw["watchlist"].get("0xw", {})   # ratio < 2
assert "BTC" not in nsw["watchlist"].get("0xw", {})   # блекліст
assert nsw["fill_cursor"]["0xw:AAA"] == 123456        # курсор = знімок
# (рев'ю C1) tombstone СВІЖІШИЙ за знімок -> пара не воскрешається
nsw_t = dict(nsw, watchlist={}, fill_cursor={}, sent_alerts=set(),
             close_episodes={}, delta_seen={},
             scan_tombstones={"0xw:AAA": nsw["snap_ms"] / 1000.0 + 5})
exec(sweep_src, nsw_t)
assert "AAA" not in nsw_t["watchlist"].get("0xw", {})
# tombstone СТАРІШИЙ за знімок (кит перевідкрився) -> адопція дозволена
nsw_t2 = dict(nsw, watchlist={}, fill_cursor={}, sent_alerts=set(),
              close_episodes={}, delta_seen={"0xw:AAA": 1.0},
              scan_tombstones={"0xw:AAA": nsw["snap_ms"] / 1000.0 - 5})
exec(sweep_src, nsw_t2)
assert "AAA" in nsw_t2["watchlist"].get("0xw", {})
assert "0xw:AAA" not in nsw_t2["delta_seen"]   # (рев'ю C2) мітка знята
# пара, яку realtime встиг додати, не перетирається
nsw2 = dict(nsw, watchlist={"0xw": {"AAA": {"marker": 1}}},
            fill_cursor={}, sent_alerts=set(), close_episodes={})
exec(sweep_src, nsw2)
assert nsw2["watchlist"]["0xw"]["AAA"] == {"marker": 1}
assert "0xw:AAA" not in nsw2["fill_cursor"]
print("5б) sweep: нова монета кита ratio>=2 -> watchlist, курсор на знімок")

# (в) /positions: серіалізація поза cache_lock (структурно)
i_p = src.index('if self.path == "/positions":')
seg = src[i_p:src.index('elif self.path == "/watchlist"', i_p)]
assert seg.count("self.send_json(snap)") == 2   # /positions і /wallets
for line in seg.splitlines():
    if "self.send_json" in line:
        # send_json на 12 пробілах = ПОЗА with cache_lock (16+)
        assert line.startswith("            self.send_json"), repr(line)
print("5в) /positions,/wallets: json.dumps і запис у сокет поза cache_lock")

# (г) VIP-скан: топ-N за accountValue обходить smart-skip
assert const("VIP_TOP_N") == 2000
i_v = src.index('vip = {w["addr"].lower() for w in lb_wallets[:VIP_TOP_N]}')
i_v = src.rindex("\n", 0, i_v) + 1   # від початку рядка, щоб dedent спрацював
split_src = textwrap.dedent(src[i_v:src.index("total = len(to_scan)", i_v)])
skipset = {"0xpoor", "0xrich"}
nsv = {"lb_wallets": [{"addr": "0xRICH"}, {"addr": "0xPOOR"}],
       "VIP_TOP_N": 1, "all_wallets": [{"addr": "0xRICH"}, {"addr": "0xPOOR"},
                                       {"addr": "0xNEW"}],
       "should_skip": lambda k, sn: k in skipset, "sn": 1}
exec(split_src, nsv)
got = [w["addr"] for w in nsv["to_scan"]]
assert got == ["0xRICH", "0xNEW"], got        # rich урятований, poor скіпнутий
assert [w["addr"] for w in nsv["skipped"]] == ["0xPOOR"]
assert nsv["vip_kept"] == 1
print("5г) VIP: топ-N за екваті сканується завжди, решта — smart-skip")

# ═══ 6. Титули/описи/UI для R7, F6, F7 ════════════════════════════
i_t = src.index("STRAT2_TITLES = {")
seg_t = src[i_t:src.index("}", i_t)]
i_d = src.index("STRAT2_DESC = {")
seg_d = src[i_d:src.index("REV_SIG_HEADERS", i_d)]
for k in ("R7_одним", "F6_1хв_перший", "F7_без_ратіо"):
    assert f'"{k}"' in seg_t, ("titles", k)
    assert f'"{k}"' in seg_d, ("desc", k)
ui = open((_HL + "/hyperliquid-terminal.html"),
          encoding="utf-8").read()
for k in ("R7_одним", "F6_1хв_перший", "F7_без_ратіо", "S_FIRST", "F10_розумний_60",
          "pr.uncertain", "Вілсон"):   # v2.18: ok_nr у UI замінено на статуси ok/uncertain/no
    assert k in ui, k
print("6) титули, описи і UI (S_ORDER/S_PROF/S_FIRST) для R7/F6/F7")

print("\nALL v2.9 TESTS PASSED")

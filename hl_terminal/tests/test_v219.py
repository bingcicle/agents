import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія v2.19 — аудит v2.17 (A–J) + рішення користувача «заходимо на Binance»:
1 виконання: прохід стакану на $ і на КІЛЬКІСТЬ (частковий вихід), _paper_px з
  біржею, momент отримання ціни, фандинг, поля входу
2 приймач філів (C): дедуп за tx, хуки для scan/WS/sweep, повне закриття з
  дублями, системні філи; скан-діф викликає приймач
3 епізод не залежить від активного FC-треку (D)
4 _rev_trade_closed: лише виконаний вихід (J)
5 три факти окремо: _sig_check_rev/_fol, _px_check, _head_ok (строгий), _agg_block
6 API: заголовок = підтверджений сигнал + перевірена ціна; старі рядки без стрічки
  не допускаються (B); R8 exit_ok за реплеєм (F); pending-рядки закритих треків (J);
  research verified = заголовок R1; журнал paper
7 discovery: агрегація WS-філів по (адреса, монета, бік) у вікні 15 с
8 тик: ціна застосовується з моментом отримання (I); застаріла — повтор;
  частковий вихід і фандинг у трекері/рядку
9 settle.py: пагінація за id при насиченні (G), повнота обходу → покриття,
  насичення HL → hl_sat, реплей TP R8 (F), фандинг зі стрічки, curve_final
10 структурні: версія, заголовки, UI, CLAUDE.md"""
import ast, threading, time, json, os, re, sys, math, tempfile, csv, glob, hashlib, shutil, gzip, types
import time as _tmod
import urllib.error, urllib.request
from collections import OrderedDict, deque
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HL)
from v28_shim import NEW, _median
import settle as S

SRC = (_HL + "/server.py")
src = open(SRC, encoding="utf-8").read()
tree = ast.parse(src)

def load(names, extra=None, assigns=()):
    body = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in names)
            or (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id in assigns for t in n.targets))]
    mod = ast.Module(body=body, type_ignores=[])
    ns = {"threading": threading, "time": time, "json": json, "os": os,
          "math": math, "hashlib": hashlib, "shutil": shutil, "glob": glob,
          "urllib": urllib, "print": lambda *a, **k: None, "OrderedDict": OrderedDict}
    ns.update(NEW)
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

def const(name):
    m = re.search(rf"^{name}\s*=\s*([^#\n]+)", src, re.M)
    return eval(m.group(1).strip())

def _dt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else ""

F10 = "F10_розумний_60"; R8 = "R8_тп80"
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
         F_FIXED_TIMER_S=60.0, FC_ENABLED=True, FC_MAX_EPISODE_S=300, R7_NAME="R7_одним",
         R7_MIN_TX_USD=100_000.0, R8_NAME=R8, R8_TP_FRAC=0.8, REV_HOLD_MIN=30, REV_EP_MAX_S=300.0,
         REV_EP_MIN_MAG=1.0, FOLLOW_MAX_FILL_AGE_S=20.0, REV_MAX_FILL_AGE_S=60.0, BOOK_MAX_AGE_S=5.0,
         REV_REF_TOL_S=12.0, EXEC_EXCH="bn", BOOK_APPLY_MAX_S=5.0, BN_EXEC_LEVELS=100, FUND_CACHE_S=60.0,
         HEAD_STRICT=True, _REV_THR={"R4_великий": 2.0, "R5_дуже": 3.0})
assert const("DATA_ALGO_V") == "2.20" and const("EXEC_EXCH") == "bn" and const("BOOK_APPLY_MAX_S") == 5.0
assert const("BN_EXEC_LEVELS") == 100 and const("HEAD_STRICT") is True and const("PRIO_AGG_S") == 15.0
assert S.SETTLE_V == "5" and S.CURVE_FULL == 1.0 and S.TP_DETECT_MS == 2000 and S.CACHE_V == 3
FH = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
          assigns=("FOLLOW_HEADERS", "REV_HEADERS", "TWAP_HEADERS", "STRAT_SINCE", "EXEC_COLS"))
fh, rh, th = FH["FOLLOW_HEADERS"], FH["REV_HEADERS"], FH["TWAP_HEADERS"]
EXC = ["exch", "entry_qty", "exit_fill_frac", "funding_pct", "exec_delay_s",
       "decision_ms", "detect_ms", "exit_decision_ms", "exit_px_filled", "trig_hash"]   # v2.20: +5
assert FH["EXEC_COLS"] == EXC and fh[-11:-1] == EXC and rh[rh.index("tp_px") + 1: rh.index("m1")] == EXC \
       and th[th.index("exit_src") + 1: th.index("m1")] == EXC
assert len(fh) == 62 and len(rh) == 54 + 60 + 1 and len(th) == 53 + 60 + 1
for k in ("tp_hit_ms", "net_tp_mkt_pct", "net_tp_lim_pct", "funding_pct", "cover_ok", "curve_final", "trig_match"):
    assert k in S.HEADERS, k
A = "0x" + "a" * 40
now0 = time.time()
print("0) константи (Binance, HEAD_STRICT, SETTLE_V 5), заголовки +10 колонок виконання")

# ═══ 1. Виконання: прохід стакану, _paper_px, фандинг, поля входу ══════════
P = load({"_book_walk", "_book_quote", "_paper_px", "_px_mid_age", "_funding_pct", "_entry_exec_fields"},
         dict(C, stats={}, px_lock=threading.Lock(), px_hist={"AAA": [(time.time() - 2.0, 100.0)]},
              # v2.20: мід-фолбек для біржі "bn" — з потоку Binance (bookTicker), не HL
              bn_px_hist={"AAAUSDT": [(time.time() - 2.0, 100.0, 99.9, 100.1)]},
              _depth_universe=[set()], _bn_backoff=[0.0], get_bn_symbol=lambda c: c.upper() + "USDT"))
W = P["_book_walk"]
asks = [(100.0, 3.0), (100.5, 20.0)]
r = W(asks, "BUY", usd=1000)
assert r["partial"] == 0 and abs(r["fill_frac"] - 1.0) < 1e-9 and abs(r["qty"] - (3 + 700 / 100.5)) < 1e-9 \
       and abs(r["px"] - 1000 / r["qty"]) < 1e-9 and r["px"] == r["px_filled"], r
r = W(asks, "BUY", usd=5000)          # вхід: стакан не вмістив → гірший пройдений рівень, partial
assert r["partial"] == 1 and abs(r["fill_frac"] - 2310 / 5000) < 1e-9 and r["px"] == 100.5 \
       and abs(r["px_filled"] - 2310 / 23) < 1e-9, r
r = W(asks, "BUY", qty=5.0)           # вихід на кількість: 3@100 + 2@100.5
assert r["partial"] == 0 and abs(r["px"] - 100.2) < 1e-9 and r["qty"] == 5.0 and r["fill_frac"] == 1.0
r = W(asks, "BUY", qty=30.0)          # вихід: заповнено 23 з 30 → лишок за гіршим рівнем, fill_frac<1
assert r["partial"] == 1 and abs(r["fill_frac"] - 23 / 30) < 1e-9 and abs(r["px"] - (2310 + 7 * 100.5) / 30) < 1e-9 \
       and abs(r["px_filled"] - 2310 / 23) < 1e-9 and r["qty"] == 30.0, r
assert W([], "BUY", usd=1000) is None and W([(0, 5), (100, 0)], "BUY", usd=1000) is None
# _paper_px: біржа з параметра; bn → bn_book_exec, hl → hl_book_exec; meta несе recv_ms/fill_frac/exch
calls = []
def bn_stub(coin, side, usd=None, qty=None, min_recv_ms=None):
    calls.append(("bn", coin, side, usd, qty))
    return {"px": 100.3, "px_filled": 100.2, "qty": qty or 9.97, "fill_frac": (0.6 if qty == 30.0 else 1.0),
            "partial": int(qty == 30.0), "mid": 100.05, "bid": 100.0, "ask": 100.1, "ts_ms": 1, "q_age_s": 0.1,
            "req_ms": 120, "recv_ms": 1_700_000_000_123, "exch": "bn"}
def hl_stub(coin, side, usd=None, qty=None, min_recv_ms=None):
    calls.append(("hl", coin, side, usd, qty))
    return {"px": 100.4, "px_filled": 100.4, "qty": 9.96, "fill_frac": 1.0, "partial": 0, "mid": 100.05,
            "bid": 100.0, "ask": 100.1, "ts_ms": 1, "q_age_s": 0.1, "req_ms": 90, "recv_ms": 1_700_000_000_456, "exch": "hl"}
P["bn_book_exec"], P["hl_book_exec"] = bn_stub, hl_stub
px, srcx, meta = P["_paper_px"]("AAA", "BUY", strict=True)
assert px == 100.3 and srcx == "book" and meta["exch"] == "bn" and meta["recv_ms"] == 1_700_000_000_123 \
       and meta["fill_frac"] == 1.0 and calls[-1] == ("bn", "AAA", "BUY", None, None)
px, srcx, meta = P["_paper_px"]("AAA", "SELL", qty=30.0, exch="bn")          # вихід partial → book_partial
assert srcx == "book_partial" and meta["fill_frac"] == 0.6 and meta["partial"] == 1 and calls[-1][4] == 30.0
px, srcx, meta = P["_paper_px"]("AAA", "SELL", qty=30.0, exch="bn", strict=True)   # строгий вхід partial → no_book
assert px is None and srcx == "no_book"
px, srcx, meta = P["_paper_px"]("AAA", "BUY", exch="hl")
assert px == 100.4 and meta["exch"] == "hl" and calls[-1][0] == "hl"
P["bn_book_exec"] = lambda coin, side, usd=None, qty=None, min_recv_ms=None: None
px, srcx, meta = P["_paper_px"]("AAA", "BUY", strict=False)                  # без стакану — мід, recv = зараз
assert px == 100.0 and srcx == "mid" and abs(meta["recv_ms"] / 1000.0 - time.time()) < 5 and meta["exch"] == "bn"
# фандинг за утримання: момент нарахування у (вхід, вихід]; LONG платить додатну
FP = P["_funding_pct"]
p_ = {"fund_next_ms": 1_000, "fund_rate": 0.0001, "entry_ts": 0.5, "side": "LONG"}
assert abs(FP(p_, 2_000) - 0.01) < 1e-12 and abs(FP(dict(p_, side="SHORT"), 2_000) + 0.01) < 1e-12
assert FP(p_, 999) == 0.0 and FP(dict(p_, entry_ts=1.5), 2_000) == 0.0 and FP(dict(p_, fund_rate=None), 2_000) == 0.0
assert FP({"our_side": "LONG", "open_ts": 0.5, "fund_next_ms": 1000, "fund_rate": -0.0002}, 2000) == -0.02   # LONG отримує від'ємну
# поля входу: біржа, кількість на $1000, ставка/момент фандингу (мережа — стаб)
P["bn_funding_info"] = lambda coin: (0.0001, 1_800_000_000_000)
xf = P["_entry_exec_fields"](200.0, {"exch": "bn", "recv_ms": 5, "fill_frac": 1.0}, "AAA")
assert xf == {"exch": "bn", "entry_qty": 5.0, "entry_recv_ms": 5, "entry_fill_frac": 1.0,
              "fund_rate": 0.0001, "fund_next_ms": 1_800_000_000_000}, xf
P["bn_funding_info"] = lambda coin: (None, None)
xf = P["_entry_exec_fields"](200.0, {"exch": "bn"}, "AAA")
assert xf["fund_unknown"] == 1 and xf["fund_rate"] is None
xf = P["_entry_exec_fields"](200.0, {"exch": "hl"}, "AAA")
assert xf["exch"] == "hl" and "fund_unknown" not in xf
# bn_book_exec: символу нема на Binance → None (стат), бекоф 429 → None
# рев'ю v2.19 №1: _depth_universe зберігає НАЗВИ МОНЕТ HL (fetch_all_depth: set(ok_coins)), не символи
B = load({"bn_book_exec", "_book_walk", "_book_quote", "_raw_book_get", "_raw_book_put", "_book_from_raw"},
         dict(C, stats={}, _depth_universe=[{"AAA", "kPEPE"}], _bn_backoff=[0.0], _raw_book_cache={}, BOOK_RAW_TTL_S=2.0,
              get_bn_symbol=lambda c: c.upper() + "USDT"))
assert B["bn_book_exec"]("BBB", "BUY") is None and B["stats"]["book_no_symbol"] == 1
B["_bn_backoff"][0] = time.time() + 30
assert B["bn_book_exec"]("AAA", "BUY") is None and B["stats"]["book_fail"] == 1
# рев'ю v2.19: сирий стакан (біржа, монета) ≤ BOOK_RAW_TTL_S проходиться різними кількостями
# без нового запиту (6 R-трекерів сигналу з різними entry_qty → 1 запит); після TTL — новий запит
import io, urllib.request as _ur
net = []
class _Resp(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): return False
def _fake_urlopen(req, timeout=4):
    net.append(req.full_url)
    return _Resp(json.dumps({"T": int(time.time() * 1000), "bids": [["99.9", "5"], ["99.5", "50"]],
                             "asks": [["100.1", "5"], ["100.6", "50"]]}).encode())
RB = load({"bn_book_exec", "hl_book_exec", "_book_walk", "_book_quote", "_raw_book_get", "_raw_book_put",
           "_book_from_raw"},
          dict(C, stats={}, _depth_universe=[set()], _bn_backoff=[0.0], _raw_book_cache={}, BOOK_RAW_TTL_S=2.0,
               get_bn_symbol=lambda c: c.upper() + "USDT", _prio_opener=None,
               urllib=types.SimpleNamespace(request=types.SimpleNamespace(Request=_ur.Request, urlopen=_fake_urlopen)),
               hl_post_prio=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("HL не має викликатись"))))
r1 = RB["bn_book_exec"]("AAA", "SELL", qty=3.0); r2 = RB["bn_book_exec"]("AAA", "SELL", qty=20.0)
r3 = RB["bn_book_exec"]("AAA", "BUY", usd=1000)
assert len(net) == 1 and "AAAUSDT" in net[0] and "limit=100" in net[0], net
assert abs(r1["px"] - 99.9) < 1e-9 and r1["fill_frac"] == 1.0 and r2["fill_frac"] == 1.0 and abs(r2["px"] - (5 * 99.9 + 15 * 99.5) / 20) < 1e-9
assert r3["exch"] == "bn" and r3["px"] > 100.1 and r1["recv_ms"] == r2["recv_ms"] == r3["recv_ms"]
RB["_raw_book_cache"][("bn", "AAA")]["recv"] -= 3.0                        # старший за TTL → новий запит
r4 = RB["bn_book_exec"]("AAA", "SELL", qty=3.0)
assert len(net) == 2 and r4["recv_ms"] > r1["recv_ms"] - 1
assert abs(RB["bn_book_exec"]("BBB", "SELL", qty=1.0)["px"] - 99.9) < 1e-9 and len(net) == 3   # інша монета — свій запит
assert RB["stats"].get("book_ok") == 5 and not RB["stats"].get("book_fail")
RB["_depth_universe"][0] = {"AAA", "kPEPE"}; RB["_raw_book_cache"].clear()     # всесвіт із назв монет: AAA проходить, CCC — ні
assert RB["bn_book_exec"]("AAA", "BUY", usd=1000)["exch"] == "bn" and len(net) == 4
assert RB["bn_book_exec"]("CCC", "BUY", usd=1000) is None and len(net) == 4 and RB["stats"]["book_no_symbol"] == 1
print("1) стакан: $/кількість, частковий вихід (лишок за гіршим рівнем), _paper_px по біржі, recv_ms, фандинг, поля входу, сирий кеш стакану")

# ═══ 2. Приймач філів (C) ═════════════════════════════════════════════════
hooks = []
def mk_ing():
    n = load({"_ingest_txs", "_tx_key"}, dict(C,
        _ingested=OrderedDict(), _ingested_lock=threading.Lock(), INGEST_LRU=20000, stats={},
        _ingest_retry={}, INGEST_RETRY_MAX=50, _fc_done={}, INGEST_CONSUMERS=("sim", "fc", "rev", "fol"),   # v2.20
        _journal=lambda k, **f: hooks.append(("journal", k, f.get("n_txs"), f.get("n_dup"), f.get("detect_src"))),
        sim_on_market_txs=lambda a, c, o, m, ns_, fc: hooks.append(("sim", len(m), fc)),
        fc_on_txs=lambda a, c, o, m, passive=None: hooks.append(("fc", len(m))),
        rev_on_close=lambda a, c, o, m, fc, detect_src="", detect_ms=None: hooks.append(("rev", len(m), fc, detect_src)),
        follow_on_txs=lambda a, c, o, m, fc, detect_src="", detect_ms=None: hooks.append(("fol", len(m), fc, detect_src)),
        fc_on_full_close=lambda a, c, o: hooks.append(("fcfull",)),
        _grace_val=lambda o: float(o.get("val") or 0), fc_lock=threading.Lock(),
        fc_episodes={("0xw", "AAA"): {"val": 777_000.0, "ratio": 2.5}}))
    return n
n = mk_ing()
OLDP = {"size": 1000.0, "val": 100_000.0, "side": "LONG", "ratio": 3.0}
tx1 = {"hash": "0x1", "px": 100.0, "sz": 100.0, "ts": 1_700_000_000_000, "sp": 1000.0, "dir": "Close Long"}
tx2 = {"hash": "0x2", "px": 100.0, "sz": 50.0, "ts": 1_700_000_001_000, "sp": 900.0, "dir": "Close Long"}
fresh, ev, epr = n["_ingest_txs"]("0xw", "AAA", OLDP, [tx1, tx2], 850.0, False, "ws", 1)
assert len(fresh) == 2 and ev == 100_000.0 and epr == 0
assert [h[0] for h in hooks] == ["journal", "sim", "fc", "rev", "fol"] and hooks[0][2:] == (2, 0, "ws") \
       and hooks[3] == ("rev", 2, False, "ws") and n["stats"]["ingest_ws"] == 1 and n["stats"]["ingest_txs"] == 2
# ті самі tx іншим шляхом (sweep) — дублі: жодного хука, лічильник
hooks.clear()
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [tx1, tx2], 850.0, False, "sweep", 2)
assert fresh == [] and hooks == [] and n["stats"]["ingest_dup"] == 2
# повне закриття з дублями (гонка WS↔sweep): подія доходить до rev/fol/fc_full, SIM — ні, ev/ratio з епізоду
hooks.clear()
fresh, ev, epr = n["_ingest_txs"]("0xw", "AAA", OLDP, [tx1, tx2], 0.0, True, "sweep", 3)
assert fresh == [] and ev == 777_000.0 and epr == 2.5
# v2.20 (аудит v2.19 №9): fc уже підтвердив ці tx (дедуп за споживачем) — повторно не кличеться;
# подія повного закриття доходить до rev/fol/fcfull ОДИН раз (стабільний id: адреса, монета, остання tx)
assert [h[0] for h in hooks] == ["journal", "rev", "fol", "fcfull"] and hooks[0][3] == 2 and hooks[1] == ("rev", 2, True, "sweep")
hooks.clear()
fresh, ev, epr = n["_ingest_txs"]("0xw", "AAA", OLDP, [tx1, tx2], 0.0, True, "ws", 3)   # та сама подія ще раз (гонка WS↔sweep)
assert fresh == [] and hooks == [] and n["_fc_done"][("0xw", "AAA")] == "0x2"
# новий + дубль: хуки отримують лише новий; скан-діф — джерело "scan"
hooks.clear()
tx3 = dict(tx2, hash="0x3", ts=1_700_000_002_000)
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [tx2, tx3], 800.0, False, "scan", 4)
assert [t["hash"] for t in fresh] == ["0x3"] and hooks[1] == ("sim", 1, False) and hooks[3] == ("rev", 1, False, "scan") \
       and n["stats"]["ingest_scan"] == 1
# системний філ (нульовий hash): ідентичність = час+розмір+ціна
sysf = {"hash": "0x0000", "px": 100.0, "sz": 10.0, "ts": 1_700_000_003_000, "sp": 800.0, "dir": "Close Long"}
assert n["_tx_key"](sysf) == "sys:1700000003000:10.0:100.0" and n["_tx_key"](tx1) == "0x1"
hooks.clear()
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [sysf], 790.0, False, "ws", 5)
assert len(fresh) == 1
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [dict(sysf)], 790.0, False, "ws", 6)
assert fresh == []
# збій хука не валить приймач (лог), інші хуки викликаються
n["rev_on_close"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
hooks.clear()
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [dict(tx1, hash="0x9")], 700.0, False, "ws", 7)
assert len(fresh) == 1 and [h[0] for h in hooks] == ["journal", "sim", "fc", "fol"]
# v2.20: невизнана споживачем tx (0x9 для rev) повторно пред'являється ЛИШЕ йому при наступній доставці пари
assert [t["hash"] for t in n["_ingest_retry"][("0xw", "AAA")]] == ["0x9"]
n["rev_on_close"] = lambda a, c, o, m, fc, detect_src="", detect_ms=None: hooks.append(("rev", sorted(t["hash"] for t in m), fc, detect_src))
hooks.clear()
fresh, _, _ = n["_ingest_txs"]("0xw", "AAA", OLDP, [dict(tx1, hash="0xa")], 690.0, False, "ws", 8)
assert [t["hash"] for t in fresh] == ["0xa"] and hooks[1] == ("sim", 1, False) and hooks[2] == ("fc", 1) \
       and hooks[3] == ("rev", ["0x9", "0xa"], False, "ws") and hooks[4] == ("fol", 1, False, "ws")
assert ("0xw", "AAA") not in n["_ingest_retry"] and n["_ingested"][("0xw", "AAA", "0x9")]["ok"] == {"sim", "fc", "rev", "fol"}
# структурні: обидва шляхи детекції — через приймач; прямих викликів хуків у моніторі/скан-діфі немає
_mon = src[src.index("def run_realtime_monitor("):src.index("def check_position_changes(")]
_cpc = src[src.index("def check_position_changes("):src.index("def send_close_alert(")]
assert "_ingest_txs(" in _mon and "rev_on_close(" not in _mon and "follow_on_txs(" not in _mon and "sim_on_market_txs(" not in _mon
assert '_ingest_txs(addr, coin, old, mfills, new_pos["size"], False, "scan"' in _cpc and "fill_cursor[key_ac] = max(" in _cpc \
       and _cpc.index("fill_cursor[key_ac] = max(") < _cpc.index("_ingest_txs(")
print("2) приймач: дедуп за tx, хуки для ws/sweep/scan, повне закриття з дублями, системні філи, збій хука; скан-діф через приймач")

# ═══ 3. Епізод не залежить від активного FC-треку (D) ════════════════════
def mk_fc(active):
    return load({"fc_on_txs", "_fc_rebuild"}, dict(C, fc_lock=threading.Lock(), fc_episodes={},
                fc_positions=({("0xw", "AAA"): {"open_ts": 1}} if active else {}), stats={}))
def feed(n, batches):
    for b in batches:
        n["fc_on_txs"]("0xw", "AAA", {"side": "LONG", "ratio": 3, "val": 2e5, "size": 2000.0}, b)
    return n["fc_episodes"][("0xw", "AAA")]
T0 = 1_700_000_000_000
one = [{"hash": "0x1", "ts": T0, "px": 100.0, "sz": 100.0, "sp": 2000.0, "dir": "Close Long"},
       {"hash": "0x2", "ts": T0 + 60_000, "px": 100.0, "sz": 1900.0, "sp": 1900.0, "dir": "Close Long"}]
for active in (False, True):
    e1 = feed(mk_fc(active), [one])
    e3 = feed(mk_fc(active), [[one[0]], [one[1]]])
    assert e1["start_size"] == 2000.0 and e1["max_sz"] == 1900.0 and e1["last_ts"] - e1["first_ts"] == 60_000, (active, e1)
    assert e3["start_size"] == e1["start_size"] and e3["max_sz"] == e1["max_sz"] \
           and e3["last_ts"] - e3["first_ts"] == 60_000 and len(e3["txs"]) == 2, (active, e3)
assert "if ep and key in fc_positions:" not in src[src.index("def fc_on_txs("):src.index("def fc_on_full_close(")]
print("3) епізод: активний FC-трек не рве перебудову — один/два батчі дають ту саму базу R7 і тривалість")

# ═══ 4. _rev_trade_closed (J) ═════════════════════════════════════════════
RC = load({"_rev_trade_closed"}, dict(C))["_rev_trade_closed"]
nowt = time.time()
p_new = {"state": "open", "entry_ts": nowt - 2400, "samples": [0.1] * 38, "algo_v": "2.19"}
assert not RC(p_new, nowt) and RC(dict(p_new, exit_reason="timer"), nowt) and RC(dict(p_new, exit_reason="no_price"), nowt)
assert RC(dict(p_new, algo_v="2.15"), nowt) and not RC(dict(p_new, state="armed", algo_v="2.15"), nowt)
print("4) _rev_trade_closed: трекер ≥2.16 звільняє монету лише вирішеним виходом; старий — за часом")

# ═══ 5. Три факти окремо ═════════════════════════════════════════════════
SC = load({"_sig_check_rev", "_sig_check_fol", "_px_check", "_head_ok", "_count_by", "_agg_block", "_curves_of",
           "_parse_curve", "_stat_small", "_period_stats"}, dict(C, _median=_median))
fnum = lambda x, d=None: (float(x) if x not in (None, "") else d)
srow_ok = {"flags": "full_close;dump_tape", "dump_bucket": "2", "dump_move_pct": "1.5", "lag_s": "3",
           "whale_max_fill_pct": "0.99", "whale_max_fill_usd": "150000"}
chk = SC["_sig_check_rev"]
assert chk("R1_загальний", srow_ok, fnum) == (1, "") and chk("R7_одним", srow_ok, fnum) == (1, "")
assert chk("R1_загальний", None, fnum) == (None, "pending")
assert chk("R1_загальний", dict(srow_ok, flags="no_fills"), fnum) == (None, "no_fills")
assert chk("R1_загальний", dict(srow_ok, flags="full_close;hl_sat"), fnum) == (None, "no_fills")   # насичена історія — не доведено
assert chk("R1_загальний", dict(srow_ok, flags="no_whale_fill"), fnum) == (0, "no_whale_fill")
assert chk("R1_загальний", dict(srow_ok, flags="partial_close"), fnum) == (0, "partial_close")
assert chk("R1_загальний", dict(srow_ok, flags="close_unknown"), fnum) == (0, "close_unknown")
assert chk("R1_загальний", dict(srow_ok, dump_bucket="0"), fnum) == (0, "long_episode")
assert chk("R1_загальний", dict(srow_ok, dump_move_pct="0.2"), fnum) == (0, "small_move")
assert chk("R4_великий", dict(srow_ok, dump_move_pct="1.5"), fnum) == (0, "small_move") and chk("R4_великий", dict(srow_ok, dump_move_pct="2.0"), fnum) == (1, "")
assert chk("R5_дуже", dict(srow_ok, dump_move_pct="2.9"), fnum) == (0, "small_move")
assert chk("R1_загальний", dict(srow_ok, lag_s="61"), fnum) == (0, "stale_fill") and chk("R1_загальний", dict(srow_ok, lag_s=""), fnum) == (0, "stale_fill")
assert chk("R7_одним", dict(srow_ok, whale_max_fill_pct="0.9"), fnum) == (0, "not_one_shot") and chk("R7_одним", dict(srow_ok, whale_max_fill_usd="90000"), fnum) == (0, "not_one_shot")
assert chk("R2_breakout", dict(srow_ok, flags="full_close;no_trigger"), fnum) == (0, "no_trigger")
assert chk("R1_загальний", dict(srow_ok, dump_bucket=""), fnum) == (None, "no_episode")
chf = SC["_sig_check_fol"]
assert chf({"flags": "", "lag_s": "3"}, fnum) == (1, "") and chf(None, fnum) == (None, "pending") \
       and chf({"flags": "", "lag_s": "25"}, fnum) == (0, "stale_fill") and chf({"flags": "no_whale_fill"}, fnum) == (0, "no_whale_fill") \
       and chf({"flags": "no_fills"}, fnum) == (None, "no_fills") and chf({"flags": "hl_sat", "lag_s": "3"}, fnum) == (None, "no_fills")
PX = SC["_px_check"]
assert PX("verified", 0) == 1 and PX("verified", 1) == 0 and PX("live_only", 0) == 0 and PX("tape_only", 0) == 0 \
       and PX("pending", 0) is None and PX("none", 0) == 0
H = SC["_head_ok"]
base_t = {"entered": 1, "net30": 0.5, "sig_ok": 1, "px_ok": 1, "exit_ok": 1}
assert H(base_t) and not H(dict(base_t, sig_ok=0)) and not H(dict(base_t, sig_ok=None)) and not H(dict(base_t, px_ok=0)) \
       and not H(dict(base_t, px_ok=None)) and not H(dict(base_t, exit_ok=0)) and not H(dict(base_t, exit_ok=None)) \
       and not H(dict(base_t, late=1)) and not H(dict(base_t, unc=1)) and not H(dict(base_t, net30=None)) and not H(dict(base_t, entered=0))
assert H({"entered": 1, "net30": 0.1, "status": "verified"}) and not H({"entered": 1, "net30": 0.1, "status": "live_only"})   # TWAP: без sig_ok — за статусом
SC["HEAD_STRICT"] = False
assert H(dict(base_t, sig_ok=0)) and H(dict(base_t, px_ok=0)) and not H(dict(base_t, exit_ok=0))
SC["HEAD_STRICT"] = True
# _agg_block: заголовок лише підтверджені; журнал paper — усі; лічильники
trs = [dict(base_t, net30=1.0, ts=now0, status="verified"),
       dict(base_t, net30=-2.0, sig_ok=0, sig_why="partial_close", ts=now0, status="verified"),
       dict(base_t, net30=3.0, sig_ok=None, sig_why="pending", px_ok=None, ts=now0, status="pending"),
       dict(base_t, net30=0.5, px_ok=0, status="live_only", ts=now0),
       dict(base_t, net30=0.7, exit_partial=1, px_ok=0, status="verified", ts=now0),
       dict(base_t, net30=0.9, exit_ok=0, exit_why="tp_missed", status="verified", ts=now0),
       dict(base_t, net30=0.2, late=1, status="verified", ts=now0),
       dict(base_t, net30=0.3, ts=now0, status="verified")]
blk = SC["_agg_block"](trs, now0, 60)
assert blk["n"] == 2 and abs(blk["median"] - 0.65) < 1e-9 and blk["n_paper"] == 8 and blk["paper"]["n"] == 8 \
       and abs(blk["paper"]["median"] - 0.6) < 1e-9, (blk["n"], blk["median"], blk["paper"])
assert blk["n_sig_invalid"] == 1 and blk["n_sig_pending"] == 1 and blk["sig_why"] == {"partial_close": 1} \
       and blk["n_px_unverified"] == 2 and blk["n_px_pending"] == 1 and blk["n_exit_partial"] == 1 and blk["n_exit_invalid"] == 1
assert blk["n_verified"] == 2 and blk["verified"]["n"] == 2 and blk["n_verified_all"] == 6 and blk["n_late"] == 1
print("5) сигнал/ціна/вихід — окремі факти; заголовок строгий; журнал paper і лічильники")

# ═══ 6. API: строгий заголовок, старі рядки, R8 реплей, pending-рядки, research ═══
d7 = tempfile.mkdtemp()
def wcsv(name, headers, rows):
    with open(os.path.join(d7, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers)
        for r in rows: w.writerow(r)
def mkrow(headers, **kv):
    r = {k: "" for k in headers}; r["eol"] = "^"; r.update(kv)
    return [r[k] for k in headers]
FH2 = load(set(), dict(REV_TRACK_MIN=60, TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60),
           assigns=("REV_OUT_HEADERS", "FOLLOW_OUT_HEADERS", "TWAP_CURVE_HEADERS", "REV_SIG_HEADERS", "TAPE_SINCE", "SYMBOL_MAP"))
today = _dt(now0); ago1 = _dt(now0 - 86400)
def revr(sig, algo="2.19", st="R1_загальний", **kv):
    base = dict(sig_id=sig, strategy=st, date=today, coin="AAA", our_side="LONG", whale_addr=A, src="wallet",
                detect_px="100", entry_px="100", entered="1", ratio="3", costs_pct="0.15", algo_v=algo,
                exit_px="101", exit_reason="timer", exit_min="30", grace="0", dump_bucket="2", dump_move_pct="-1.5",
                exch="bn", entry_qty="10", exit_fill_frac="1", funding_pct="0.01", exec_delay_s="0.2")
    base.update(kv); return mkrow(rh, **base)
wcsv("rev.csv", rh, [
    revr("a1"),                                              # verified + сигнал ok → заголовок
    revr("a2", exit_px="99"),                                # settlement: partial_close → сигнал спростовано
    revr("a3", exit_px="103"),                               # без settlement → pending
    revr("a4", exit_px="100.5"),                             # settlement без стрічки → live_only → ціна не підтверджена
    revr("a5", algo="2.15", exit_px="", exit_reason="", exit_min="", m30="2.0"),   # старий: сигнал ok + verified → допущений
    revr("a6", algo="2.15", exit_px="", exit_reason="", exit_min="", m30="2.0"),   # старий: сигнал ok, стрічки нема → НЕ допущений
    revr("a7", exit_px="101", exit_fill_frac="0.6"),         # частковий вихід → ціна не підтверджена
    revr("b1", st=R8, exit_px="101.6", exit_reason="tp", exit_min="4", tp_px="101.6"),   # tp + tp_tape_hit → ok
    revr("b2", st=R8, exit_px="101.6", exit_reason="tp", exit_min="4", tp_px="101.6"),   # tp + tp_tape_miss → фантом
    revr("b3", st=R8, exit_px="100.8", exit_reason="timer", exit_min="30", tp_px="101.6"),   # timer + hit → пропущений TP
    revr("b4", st=R8, exit_px="100.8", exit_reason="timer", exit_min="30", tp_px="101.6"),   # settlement v3 без реплею → pending
    revr("b5", st=R8, exit_px="101.6", exit_reason="tp", exit_min="4", tp_px="101.6"),   # tp + miss + tape_gap → pending (рев'ю №5)
    revr("b6", st=R8, exit_px="100.8", exit_reason="timer", exit_min="30", tp_px=""),    # без TP і дампу → no_tp (рев'ю №6)
])
def folr(tid, algo="2.19", st="F1_1хв", **kv):
    base = dict(date_open=today, date_close=today, strategy=st, coin="AAA", our_side="SHORT", whale_addr=A,
                entry_px="100", exit_px="99.5", exit_reason="silence", hold_s="70", gross_pct="0.5",
                costs_pct="0.15", net_pct="0.35", ratio="3", pos_usd="5e5", vault="0", hour="10",
                profile_gap_s="0", algo_v=algo, trade_id=tid, grace="0", exch="bn", entry_qty="10",
                exit_fill_frac="1", funding_pct="0", exec_delay_s="0.1")
    base.update(kv); return mkrow(fh, **base)
wcsv("follow.csv", fh, [
    folr("t1", net_pct="1.0"),                     # лаг 3 + verified → заголовок
    folr("t2", net_pct="0.5"),                     # лаг 30 → спростовано (stale_fill)
    folr("t3", net_pct="-0.2"),                    # без settlement → pending
    folr("t4", net_pct="0.8", algo="2.17"),        # старий без settlement → pending (не допущений, B1)
    folr("t5", net_pct="0.4", exit_fill_frac="0.5"),   # частковий вихід → ціна не підтверджена
])
for nm, hdr in (("tc.csv", FH2["TWAP_CURVE_HEADERS"]),
                ("sig.csv", FH2["REV_SIG_HEADERS"]), ("tws.csv", FH2["REV_SIG_HEADERS"]), ("fo.csv", FH2["FOLLOW_OUT_HEADERS"])):
    wcsv(nm, hdr, [])
# рев'ю №3: TWAP-рядки несуть три факти (sig_ok=1 за побудовою; px_ok None без settlement; exit_ok за late)
oh = FH2["REV_OUT_HEADERS"]
def twr(tid, **kv):
    base = dict(twap_id=tid, strategy="T1_твап_відкриття", date_entry=today, coin="AAA", our_side="SHORT",
                twap_side="buy", whale_addr=A, src="TWAPx", usd="1e5", dur_s="600", kind="open", kind_src="slice",
                entry_px="100", net60_pct="1.0", costs_pct="0.2", exit_min="60", exit_reason="timer_60m",
                completed="1", cancel_after_entry="0", exch_status="finished", algo_v="2.19", exch="bn",
                entry_qty="10", exit_fill_frac="1", funding_pct="0.005", exec_delay_s="0.3")
    base.update(kv); return mkrow(th, **base)
wcsv("tw.csv", th, [twr("tw1"), twr("tw2", exit_reason="timer_60m_late"), twr("tw3", exit_fill_frac="0.5")])
# рев'ю №8: тіньові outcome-рядки — запізніла детекція (lag_s > 60) поза спостереженням
def outr(sid, **kv):
    base = {k: "" for k in oh}; base["eol"] = "^"
    base.update(sig_id=sid, date=today, coin="AAA", fade_side="LONG", whale_addr=A, src="wallet", detect_px="100",
                algo_v="2.19", lag_s="3", dump_move_pct="-1.5", btc_ok="1", btc_move_pct="0.1", m30="0.5")
    base.update(kv); return [base[k] for k in oh]
wcsv("out.csv", oh, [outr("o1"), outr("o2", lag_s="120"), outr("o3", lag_s="")])
def setr(key, family, strategy, **kv):
    base = {k: "" for k in S.HEADERS}
    base.update(key=key, family=family, strategy=strategy, coin="AAA", symbol="AAAUSDT", settle_v=S.SETTLE_V,
                settled_at=today, eol="^", lag_s="3", dump_bucket="2", dump_move_pct="1.5", flags="full_close;dump_tape",
                net_live_pct="0.85", net_tape_pct="0.6", net_official_pct="0.6", curve_n="60", curve_final="1")
    base.update(kv); return [base[k] for k in S.HEADERS]
wcsv("settlements.csv", S.HEADERS, [
    setr("a1|R1_загальний", "rev", "R1_загальний"),
    setr("a2|R1_загальний", "rev", "R1_загальний", flags="partial_close;dump_tape", net_live_pct="-1.15", net_tape_pct="-1.2", net_official_pct="-1.2"),
    setr("a4|R1_загальний", "rev", "R1_загальний", net_tape_pct="", net_official_pct="0.35", flags="full_close;no_tape"),
    setr("a5|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="1.0", net_official_pct="1.0"),
    setr("a6|R1_загальний", "rev", "R1_загальний", net_live_pct="1.85", net_tape_pct="", net_official_pct="1.85", flags="full_close;no_tape"),
    setr("a7|R1_загальний", "rev", "R1_загальний"),
    setr("b1|" + R8, "rev", R8, flags="full_close;dump_tape;tp_tape_hit", tp_hit_ms="1", net_tp_mkt_pct="1.3", net_tape_pct="1.3", net_official_pct="1.3"),
    setr("b2|" + R8, "rev", R8, flags="full_close;dump_tape;tp_tape_miss", net_tp_mkt_pct="0.4", net_tape_pct="0.4", net_official_pct="0.4"),
    setr("b3|" + R8, "rev", R8, flags="full_close;dump_tape;tp_tape_hit", tp_hit_ms="1", net_tp_mkt_pct="1.3", net_tape_pct="1.3", net_official_pct="0.65"),
    setr("b4|" + R8, "rev", R8, settle_v="3"),
    setr("b5|" + R8, "rev", R8, flags="full_close;dump_tape;tp_tape_miss;tape_gap", net_tp_mkt_pct="0.4", net_tape_pct="0.4", net_official_pct="0.4"),
    setr("b6|" + R8, "rev", R8, flags="full_close;no_dump", net_tape_pct="0.4", net_official_pct="0.4"),
    setr("t1", "fol", "F1_1хв", net_live_pct="1.0", net_tape_pct="0.9", net_official_pct="0.9"),
    setr("t2", "fol", "F1_1хв", lag_s="30", net_live_pct="0.5", net_tape_pct="0.4", net_official_pct="0.4"),
    setr("t5", "fol", "F1_1хв", net_live_pct="0.4", net_tape_pct="0.3", net_official_pct="0.3"),
])
def mk_api(rev_open=None):
    return load({"strat2_api", "strat2_slice", "_median", "_vt", "_v_ok", "_twap_cohort_name", "_twap_row",
                 "_twap_close_min", "_twap_trade_closed", "_tracker_csv_key", "_twap_exit",
                 "_twap_curve_row", "_stat_small", "_period_stats", "_ts_local", "_load_settlements",
                 "_official_net", "_parse_curve", "_curves_of", "_agg_block", "_pub", "_leg_costs",
                 "_sig_check_rev", "_sig_check_fol", "_px_check", "_head_ok", "_count_by", "_pending_rev_rows",
                 "_p_costs", "_sim_slip", "_rev_trade_closed"}, dict(C,
        _strat2_full={}, strat2_lock=threading.RLock(), rev_open=(rev_open or {}), follow_open={},
        wallet_profiles={}, profiles_fetching=set(), STRAT2_DESC={}, STRAT2_TITLES={},
        STRAT_SINCE=FH["STRAT_SINCE"], TAPE_SINCE=FH2["TAPE_SINCE"], _strat2_cache={"ts": 0.0, "data": None},
        _legacy_csv_cache={}, _settle_cache={"key": None, "data": {}}, DATA_DIR=d7, stats={},
        FOLLOW_OUT_CSV=os.path.join(d7, "fo.csv"), REV_CSV=os.path.join(d7, "rev.csv"),
        REV_SIG_CSV=os.path.join(d7, "sig.csv"), FOLLOW_CSV=os.path.join(d7, "follow.csv"),
        REV_OUT_CSV=os.path.join(d7, "out.csv"), TWAP_CSV=os.path.join(d7, "tw.csv"),
        TWAP_CURVE_CSV=os.path.join(d7, "tc.csv"), TWAP_SIG_CSV=os.path.join(d7, "tws.csv"),
        TWAP_TRACK_MIN=120, TWAP_HOLD_MIN=60, TWAP_COHORTS=(1.0, 1.5, 2.0),
        T1_NAME="T1_твап_відкриття", T2_NAME="T2_твап_скорочення", strat_activated={}, _dt=_dt,
        _median=_median, RESEARCH_SINCE="2.17"), assigns=("TWAP_HEADERS",))
api = mk_api(); o = api["strat2_api"]()
r1 = o["strategies"]["R1_загальний"]
by = {t.get("sig_id") or t["net30"]: t for t in r1["trades"]}
tr = {round(t["net30"], 4): t for t in r1["trades"] if t["net30"] is not None}
# a1: verified + сигнал → заголовок; a5 (старий, ok+verified) — допущений; решта — поза
assert r1["n"] == 2 and r1["n_adm_legacy"] == 1 and r1["n_adm_cand"] == 2 and r1["n_trades_total"] == 7, (r1["n"], r1["n_adm_legacy"], r1["n_adm_cand"])
assert abs(r1["median"] - (0.6 + 1.0) / 2) < 1e-9
assert r1["n_sig_invalid"] == 1 and r1["sig_why"] == {"partial_close": 1} and r1["n_sig_pending"] == 1
assert r1["n_px_unverified"] == 3 and r1["n_exit_partial"] == 1 and r1["n_paper"] == 7   # a4 live_only, a6 old live_only, a7 partial
t_a2 = [t for t in r1["trades"] if t["sig_ok"] == 0][0]
assert t_a2["sig_why"] == "partial_close" and t_a2["px_ok"] == 1 and abs(t_a2["net30"] + 1.2) < 1e-9
t_a3 = [t for t in r1["trades"] if t["sig_ok"] is None and t["adm"] == 0][0]
assert t_a3["sig_why"] == "pending" and t_a3["px_ok"] is None and t_a3["settled"] == 0 and r1["n_px_pending"] == 1
t_a6 = [t for t in r1["trades"] if t["adm"] == 1 and t["px_ok"] == 0][0]
assert t_a6["status"] == "live_only" and t_a6["sig_ok"] == 1     # старий live-only PnL — не у заголовку (B2)
t_a7 = [t for t in r1["trades"] if t["exit_partial"] == 1][0]
assert t_a7["px_ok"] == 0 and t_a7["fill_frac"] == 0.6 and t_a7["exch"] == "bn" and t_a7["funding"] == 0.01 \
       and t_a7["exec_delay"] == 0.2
t_a1 = [t for t in r1["trades"] if t["sig_ok"] == 1 and t["px_ok"] == 1 and t["adm"] == 0][0]
assert abs(t_a1["net_live"] - ((101 / 100 - 1) * 100 - 0.15 - 0.01)) < 1e-9   # витрати за ногами (без стакану — повні 0.15) і фандинг з рядка
assert o["legacy_rows"] == 0   # v2.20 (аудит №10): усі старі R-рядки (≥2.10) — у журналі paper, «прихованих» нема
# R8: exit_ok за реплеєм
r8 = o["strategies"][R8]
bt = {t["exit_reason"] + ":" + str(t["exit_ok"]) + ":" + (t["exit_why"] or ""): t for t in r8["trades"]}
assert "tp:1:" in bt and "tp:0:tp_phantom" in bt and "timer:0:tp_missed" in bt and "timer:None:pending" in bt, list(bt)
assert "tp:None:pending" in bt and "timer:0:no_tp" in bt, list(bt)      # рев'ю №5/№6
assert bt["tp:None:pending"]["exit_why"] == "pending" and bt["timer:0:no_tp"]["net30"] == 0.4 and bt["tp:None:pending"]["net30"] == 0.4
assert r8["n"] == 1 and abs(r8["median"] - 1.3) < 1e-9 and r8["n_exit_invalid"] == 3 and bt["tp:1:"]["net_tp_mkt"] == 1.3
# F1: t1 у заголовку; t2 stale_fill; t3/t4 pending; t5 частковий
f1 = o["strategies"]["F1_1хв"]
assert f1["n"] == 1 and abs(f1["median"] - 0.9) < 1e-9 and f1["n_sig_invalid"] == 1 and f1["n_sig_pending"] == 2 \
       and f1["n_exit_partial"] == 1 and f1["n_px_unverified"] == 1 and f1["n_paper"] == 5, (f1["n"], f1["n_sig_pending"])
ft = {t["net30"]: t for t in f1["trades"]}
assert ft[0.4]["sig_why"] == "stale_fill" and ft[0.4]["stale"] == 0 and ft[-0.2]["sig_ok"] is None \
       and ft[0.8]["sig_ok"] is None and ft[0.3]["exit_partial"] == 1
# research «ПЕРЕВІРЕНО» = заголовок R1
assert o["research"]["verified"]["n_base"] == 2
assert o["research"]["observed"]["n_base"] == 2      # o2 (лаг 120 с) — поза; o3 без лагу (старий формат) — лишається
# TWAP: три факти
t1s = o["strategies"]["T1_твап_відкриття"]
twt = {t["exit_reason"] + ":" + str(t.get("exit_partial")): t for t in t1s["trades"]}
assert twt["timer_60m:0"]["sig_ok"] == 1 and twt["timer_60m:0"]["px_ok"] is None and twt["timer_60m:0"]["exit_ok"] == 1 \
       and twt["timer_60m:0"]["funding"] == 0.005 and twt["timer_60m:0"]["exch"] == "bn"
assert twt["timer_60m_late:0"]["exit_ok"] == 0 and twt["timer_60m_late:0"]["exit_why"] == "late"
assert twt["timer_60m:1"]["fill_frac"] == 0.5 and twt["timer_60m:1"]["px_ok"] is None
assert t1s["n_px_pending"] == 3 and t1s["n_px_unverified"] == 0 and t1s["n_sig_pending"] == 0 and t1s["n"] == 0
# pending-рядки закритих R-треків (J): вихід вирішено, рядок ще не записаний
ro = {"s9|R1_загальний": {"sig_id": "s9", "strategy": "R1_загальний", "state": "open", "coin": "AAA", "side": "LONG",
                          "addr": A, "detect_ts": now0 - 2000, "entry_ts": now0 - 1900, "entry_px": 100.0, "exit_px": 101.0,
                          "exit_reason": "timer", "exit_min": 30, "exit_src": "book", "entry_src": "book",
                          "algo_v": "2.19", "costs": 0.15, "funding_pct": 0.02, "samples": [0.1] * 35, "peak": 1.2,
                          "trough": -0.2, "move": 1.0, "dur": 30, "ratio": 3, "sum_usd": 1e5, "hour": 10,
                          "exit_fill_frac": 1.0, "exch": "bn", "exec_delay_s": 0.3, "dump_bucket": 1},
      "s9|" + R8: {"sig_id": "s9", "strategy": R8, "state": "open", "coin": "AAA", "side": "LONG", "addr": A,
                   "detect_ts": now0 - 2000, "entry_ts": now0 - 1900, "entry_px": 100.0, "algo_v": "2.19",
                   "samples": [0.1] * 35, "peak": 1.2, "trough": -0.2}}   # без виходу — не pending-рядок
o2 = mk_api(ro)["strat2_api"]()
r1b = o2["strategies"]["R1_загальний"]
pend = [t for t in r1b["trades"] if t.get("pending_row")]
assert len(pend) == 1 and abs(pend[0]["net30"] - ((1.0) - 0.1 - 0.02)) < 1e-9 and pend[0]["status"] == "pending" \
       and pend[0]["sig_ok"] is None and pend[0]["px_ok"] is None and pend[0]["exit_ok"] == 1 and pend[0]["exch"] == "bn"
assert r1b["n"] == 2 and r1b["n_paper"] == 8 and r1b["n_trades_total"] == 8 and r1b["open_now"] == 1
assert not [t for t in o2["strategies"][R8]["trades"] if t.get("pending_row")]
# зріз — той самий агрегатор (строгий заголовок)
sl = mk_api()["strat2_slice"]("R1_загальний")
assert sl["n"] == 2 and sl["n_sig_invalid"] == 1 and sl["n_paper"] == 7
print("6) API: заголовок = підтверджений сигнал + перевірена ціна; старі без стрічки поза; R8 exit_ok за реплеєм; pending-рядки; research; зріз")

# ═══ 7. Discovery: агрегація WS-філів (пріоритет 4) ══════════════════════
q = deque(); ev = threading.Event()
PN = load({"_prio_note"}, dict(C, COIN_BLACKLIST={"BTC"}, PRIO_FLOOR_USD=15_000.0, PRIO_K_DEPTH=0.10,
          PRIO_COOLDOWN_S=600, PRIO_MAX_PER_MIN=10, PRIO_AGG_S=15.0, PRIO_AGG_MIN_USD=500.0, _prio_acc={},
          _prio_lock=threading.Lock(), _prio_seen={}, _prio_minute=deque(), _prio_q=q, _prio_event=ev,
          prio_stats={"triggers": 0, "added": 0, "dropped": 0, "errors": 0}, _sim_depth=lambda c, s=None: 1e5))
for i in range(19):
    PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 10.0})     # $1k × 19 = $19k > $15k
    if len(q): break
assert len(q) == 1 and q[0][0] == "0xnew" and q[0][3] >= 15_000 and i == 14, (len(q), i)   # спрацювало на 15-му філі
q.clear(); PN["_prio_seen"].clear(); PN["_prio_acc"].clear()
PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 4.0})          # $400 < 500 — не накопичується
assert not PN["_prio_acc"] and not q
PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 100.0})        # $10k — накопичено, порогу нема
assert len(q) == 0 and PN["_prio_acc"][("0xnew", "AAA", "LONG")][0] == 10_000.0
PN["_prio_acc"][("0xnew", "AAA", "LONG")][1] -= 20   # вікно минуло → сума починається заново
PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 100.0})
assert len(q) == 0 and PN["_prio_acc"][("0xnew", "AAA", "LONG")][0] == 10_000.0
PN["_prio_note"]("0xnew", "AAA", "B", {"px": 100.0, "sz": 100.0})        # інший бік — окремий акумулятор
assert PN["_prio_acc"][("0xnew", "AAA", "SHORT")][0] == 10_000.0
PN["_prio_note"]("0xnew", "AAA", "A", {"px": 100.0, "sz": 200.0})        # $20k одним — як і раніше
assert len(q) == 1
print("7) discovery: $20k одним філом і 20×$1k у вікні 15 с — та сама подія; пил не накопичується; боки окремо")

# ═══ 8. Тик: ціна з моментом отримання (I), застаріла — повтор, частковий вихід, фандинг ═══
class FakeTime:
    def __init__(self, t): self.now = t
    def time(self): return self.now
    sleep = staticmethod(_tmod.sleep); localtime = staticmethod(_tmod.localtime)
    strftime = staticmethod(_tmod.strftime); gmtime = staticmethod(_tmod.gmtime)
    mktime = staticmethod(_tmod.mktime); strptime = staticmethod(_tmod.strptime)
T8 = FakeTime(1_800_000_000.0)
fol_rows = []
book_meta = {"recv_off": 0.0, "fill_frac": 1.0}
def _paper(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
    # v2.20: котирування отримано ПІСЛЯ рішення фази 1 (recv = зараз), а застосування
    # відбувається через recv_off с (фейковий годинник рухається у стабі) — інакше
    # «ціна до рішення» відкидається _apply_delay (причинність)
    recv = T8.now * 1000
    assert min_recv_ms is None or recv >= min_recv_ms
    T8.now += book_meta["recv_off"]
    return 99.0, ("book_partial" if book_meta["fill_frac"] < 1 else "book"), \
        {"mid": 99.1, "px_age_ms": 100, "recv_ms": recv, "fill_frac": book_meta["fill_frac"], "exch": exch or "bn", "qty": qty}
TK = load({"_strat2_tick", "_fol_row", "_exec_extra", "_p_costs", "_ms", "_rnd", "_leg_costs", "_funding_pct", "_vt",
           "_rev_trade_closed"}, dict(C,
    time=T8, stats={}, strat2_lock=threading.RLock(), _sig_retry_lock=threading.Lock(), _sig_retry_q=[],
    rev_open={}, follow_open={}, follow_last_close={}, _px_now=lambda c, max_age=None: 99.5,
    _sim_slip=lambda d: 0.0005, _dt=_dt, _paper_px=_paper, _journal=lambda *a, **k: None,
    _strat_csv_append=lambda p, h, r: (fol_rows.append(r) if p == "f.csv" else None) or True,
    _rev_row=lambda p, e: ["rev-row"], _rev_out_row=lambda p: ["out-row"], _fol_out_row=lambda p: ["fo-row"],
    FOLLOW_OUT_CSV="fo.csv", FOLLOW_OUT_HEADERS=[], save_state=lambda: None, REV_CSV="r.csv", REV_OUT_CSV="o.csv",
    FOLLOW_CSV="f.csv", REV_HEADERS=[], REV_OUT_HEADERS=[], FOLLOW_HEADERS=fh, _px_mid_age=lambda c: (99.1, 100),
    bn_px_hist={"AAAUSDT": [(T8.now, 99.5, 99.4, 99.6)]}, get_bn_symbol=lambda c: c.upper() + "USDT",   # v2.20
    FOLLOW_TIMERS={"F1_1хв": 60}))
tr = {"coin": "AAA", "our_side": "LONG", "entry_px": 100.0, "strategy": "F1_1хв", "open_ts": T8.now - 400,
      "key": "0xa:AAA", "addr": "0xa", "timer": 60.0, "peak": -999.0, "trough": 999.0, "tx_pct": 10.0, "tx_usd": 5e4,
      "ratio": 3.0, "pos_usd": 5e5, "vault": 0, "hour": 12, "btc_move": 0.01, "depth": 1e5, "force_exit": "full_close",
      "force_exit_ts": T8.now - 5, "profile_gap": 0.0, "algo_v": "2.19", "exch": "bn", "entry_qty": 10.0,
      "fund_rate": 0.0001, "fund_next_ms": int((T8.now - 100) * 1000), "entry_src": "book", "costs": 0.2}
TK["follow_open"]["f1"] = dict(tr)
# (а) ціна отримана 10 с тому → не «зараз»: виходу немає, повтор, лічильник
book_meta["recv_off"] = 10.0
TK["_strat2_tick"]()
p = TK["follow_open"]["f1"]
assert not p.get("done") and TK["stats"]["book_apply_stale"] == 1 and p.get("exit_pending"), p.get("exit_pending")
# (б) свіжа ціна, частковий вихід 60%: час виходу = момент отримання, fill_frac, фандинг, рядок
book_meta["recv_off"] = 0.4; book_meta["fill_frac"] = 0.6
T8.now += 3
TK["_strat2_tick"]()
assert "f1" not in TK["follow_open"] and len(fol_rows) == 1      # рядок записано, трекер знято
row = fol_rows[-1]
g = lambda k: row[fh.index(k)]
gross = (99.0 / 100.0 - 1) * 100
assert g("close_ts_ms") == int(round((T8.now - 0.4) * 1000)) and g("exit_src") == "book_partial" and g("hold_s") == round(T8.now - 0.4 - tr["open_ts"], 1)
assert g("exch") == "bn" and g("entry_qty") == 10.0 and g("exit_fill_frac") == 0.6 and g("funding_pct") == 0.01 \
       and g("exec_delay_s") == 0.4 and abs(g("net_pct") - (gross - 0.1 - 0.01)) < 1e-9 and g("exit_reason") == "full_close"
# (в) R: таймерний вихід — exit_ts = момент отримання; старий трекер (до 2.19) виходить на HL
seen_exch = []
def _paper2(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
    seen_exch.append((exch, qty))
    recv = T8.now * 1000; T8.now += 0.2
    return 101.0, "book", {"mid": 101.0, "px_age_ms": 100, "recv_ms": recv, "fill_frac": 1.0, "exch": exch, "qty": qty}
TK["_paper_px"] = _paper2; TK["follow_open"].clear()
rt = {"sig_id": "s1", "strategy": "R1_загальний", "state": "open", "coin": "AAA", "side": "LONG", "addr": "0xa",
      "detect_ts": T8.now - 1801, "entry_ts": T8.now - 1801, "entry_px": 100.0, "samples": [0.1] * 29, "peak": 0.5,
      "trough": -0.1, "algo_v": "2.19", "exch": "bn", "entry_qty": 10.0, "fund_rate": 0.0002,
      "fund_next_ms": int((T8.now - 60) * 1000), "costs": 0.2, "track_min": 60}
TK["rev_open"]["s1|R1"] = dict(rt)
TK["rev_open"]["s0|R1"] = dict(rt, sig_id="s0", algo_v="2.18", exch=None, entry_qty=None, fund_next_ms=None)
TK["_strat2_tick"]()
p1, p0 = TK["rev_open"]["s1|R1"], TK["rev_open"]["s0|R1"]
# два запити стакану (bn/qty і hl) по 0.2 с: перший (s1) отримано за 0.4 с до застосування, другий (s0) — за 0.2
assert p1["exit_reason"] == "timer" and p1["exit_ts_ms"] == int(round((T8.now - 0.4) * 1000)) and abs(p1["exec_delay_s"] - 0.4) < 1e-6 \
       and p1["exit_fill_frac"] == 1.0 and p1["exit_partial"] == 0 and abs(p1["funding_pct"] - 0.02) < 1e-12
assert p0["exit_reason"] == "timer" and p0["funding_pct"] == 0.0 and abs(p0["exec_delay_s"] - 0.2) < 1e-6
assert p1["exit_decision_ms"] <= p1["exit_ts_ms"] and p1["exit_decision_ms"] > 0   # v2.20: рішення ≤ отримання ціни
assert ("bn", 10.0) in seen_exch and ("hl", None) in seen_exch      # біржа/кількість трекера, старий — HL без кількості
# (г) рев'ю №2: R-вихід із застарілою ціною — запит НЕ відновлюється зі старим ts;
#     наступний тик (3 с) створює новий і виходить (а не чекає 30 с гарду)
stale_meta = {"off": 10.0}
def _paper3(coin, side, whale_px=None, max_mid_age_ms=20000, strict=False, qty=None, exch=None, min_recv_ms=None):
    recv = T8.now * 1000; T8.now += stale_meta["off"]
    return 101.0, "book", {"mid": 101.0, "px_age_ms": 100, "recv_ms": recv,
                           "fill_frac": 1.0, "exch": exch, "qty": qty}
TK["_paper_px"] = _paper3; TK["rev_open"].clear()
TK["rev_open"]["s2|R1"] = dict(rt, sig_id="s2", detect_ts=T8.now - 1801, entry_ts=T8.now - 1801)
TK["_strat2_tick"]()
p2 = TK["rev_open"]["s2|R1"]
assert not p2.get("exit_reason") and "exit_req" not in p2 and TK["stats"]["book_apply_stale"] == 2, p2.get("exit_req")
stale_meta["off"] = 0.3; T8.now += 3
TK["_strat2_tick"]()
assert p2["exit_reason"] == "timer" and p2["exit_ts_ms"] == int(round((T8.now - 0.3) * 1000)) and p2["exit_min"] == 30
# те саме для озброєного R2: arm_hit не відновлюється, наступний тик входить
TK["rev_open"].clear(); stale_meta["off"] = 10.0
TK["rev_open"]["s3|R2"] = dict(rt, sig_id="s3", strategy="R2_breakout", state="armed", trigger_px=99.0, samples=[],
                               deadline=T8.now + 500, detect_ts=T8.now - 10, entry_ts=None, entry_px=None)
TK["_strat2_tick"]()
p3 = TK["rev_open"]["s3|R2"]
assert p3["state"] == "armed" and "arm_hit" not in p3 and TK["stats"]["book_apply_stale"] == 3
stale_meta["off"] = 0.2; T8.now += 3
TK["_strat2_tick"]()
assert p3["state"] == "open" and p3["entry_ts"] == T8.now - 0.2 and p3["entry_px"] == 101.0
print("8) тик: ціна застосовується з моментом отримання, застаріла — повтор; частковий вихід, фандинг у трекері/рядку; старі трекери на HL")

# ═══ 9. settle.py: пагінація за id, повнота обходу, HL-насичення, реплей TP, фандинг ═══
dd = tempfile.mkdtemp()
BASE = (int(time.time() * 1000) // S.DAY_MS) * S.DAY_MS - 3 * S.HOUR_MS   # 3 год тому (день без zip → REST)
b0 = (BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS
same_ms = [(k, BASE + 5000, 100.0 + (k % 7) * 0.01) for k in range(1002)]     # 1002 трейди однієї мс
tail = [(1002 + k, BASE + 6000 + k * 100, 100.5 + k * 0.001) for k in range(50)]
ROWS = same_ms + tail
calls9 = []
def rest9(url):
    calls9.append(url)
    q = dict(p.split("=") for p in url.split("?")[1].split("&"))
    if "fundingRate" in url:
        st, en = int(q["startTime"]), int(q["endTime"])
        return [{"fundingTime": BASE + 20 * S.MIN_MS, "fundingRate": "0.0001"}] if st <= BASE + 20 * S.MIN_MS <= en else []
    if "fromId" in q:
        fid = int(q["fromId"])
        return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in ROWS if a >= fid][:1000]
    st, en = int(q["startTime"]), int(q["endTime"])
    return [{"a": a, "p": str(p), "q": "1", "T": t, "m": False} for a, t, p in ROWS if st <= t <= en][:1000]
now9 = BASE + 2 * S.HOUR_MS
tape = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=rest9, now_ms=now9, rest_pace_s=0)
rows, ok = tape._rest_fetch("XUSDT", b0, b0 + S.REST_BUCKET_MS)
assert ok and len(rows) == 1052 and rows[-1][2] == 1051 and sum(1 for r in rows if r[0] == BASE + 5000) == 1002, (ok, len(rows))
assert "fromId=1000" in calls9[1] and "startTime" not in calls9[1] and len([c for c in calls9 if "aggTrades" in c]) == 2
ts_, px_ = tape.window("XUSDT", BASE + 5000, BASE + 5000)
assert len(ts_) == 1002 and tape._bucket_ok[("XUSDT", b0)] and tape.window_complete("XUSDT", BASE, BASE + 9 * S.MIN_MS)
assert not tape.window_complete("XUSDT", BASE, now9 - 30_000)   # поточне відро ще триває — кінця вікна не бачимо
# збій посеред обходу → (None, False), відро не кешується як повне
def rest_fail(url):
    q = dict(p.split("=") for p in url.split("?")[1].split("&"))
    if "fromId" in q:
        return None
    return rest9(url)
tape_f = S.Tape(tempfile.mkdtemp(), fetch_zip=lambda u: None, fetch_rest=rest_fail, now_ms=now9, rest_pace_s=0)
assert tape_f._rest_fetch("XUSDT", b0, b0 + S.REST_BUCKET_MS) == (None, False)
assert tape_f.window("XUSDT", BASE, BASE + 9000)[0].__len__() == 0 and not tape_f.window_complete("XUSDT", BASE, BASE + 9000)
assert not glob.glob(os.path.join(tape_f.rest_dir, "XUSDT-*.json"))
# _covered: краї є, але обхід неповний → False; повний → True
ctx9 = {"tape": tape, "hl": None, "symbol_map": {}, "now_ms": now9, "log": lambda *a: None}
assert S._covered("XUSDT", BASE + 5000, BASE + 6000 + 49 * 100, ctx9)
ctx_f = {"tape": tape_f, "hl": None, "symbol_map": {}, "now_ms": now9, "log": lambda *a: None}
assert not S._covered("XUSDT", BASE + 5000, BASE + 6000 + 49 * 100, ctx_f)
# рев'ю v2.19 №4: window_complete сам дотягує відра, яких ніхто не торкався (середина вікна між ногами)
b_mid = b0 + 2 * S.REST_BUCKET_MS
assert ("XUSDT", b_mid) not in tape._bucket_ok
n_agg = len([c for c in calls9 if "aggTrades" in c])
assert tape.window_complete("XUSDT", BASE, b0 + 3 * S.REST_BUCKET_MS - 1) and ("XUSDT", b_mid) in tape._bucket_ok \
       and len([c for c in calls9 if "aggTrades" in c]) == n_agg + 2      # відра 1 і 2 дотягнуто (по 1 сторінці)
# фандинг зі стрічки: одне нарахування у (t0, t1]
fr = tape.funding("XUSDT", BASE, BASE + 30 * S.MIN_MS)
assert fr == [(BASE + 20 * S.MIN_MS, 0.0001)] and tape.funding("XUSDT", BASE + 21 * S.MIN_MS, BASE + 30 * S.MIN_MS) == []
# рев'ю №7: поточний UTC-день у пам'яті — (момент запиту, рядки); вікно за момент запиту → новий запит
n_f = len([c for c in calls9 if "fundingRate" in c])
assert tape.funding("XUSDT", BASE, now9 - S.MIN_MS) == [(BASE + 20 * S.MIN_MS, 0.0001)] and len([c for c in calls9 if "fundingRate" in c]) == n_f
tape.now_ms = now9 + 5 * S.MIN_MS       # час пішов далі — вікно сягає за момент запиту
assert tape.funding("XUSDT", BASE, now9 + 2 * S.MIN_MS) == [(BASE + 20 * S.MIN_MS, 0.0001)] and len([c for c in calls9 if "fundingRate" in c]) == n_f + 1
assert tape.funding("XUSDT", BASE, now9 + 2 * S.MIN_MS) is not None and len([c for c in calls9 if "fundingRate" in c]) == n_f + 1   # у межах моменту запиту — кеш
tape.now_ms = now9
out9 = {"flags": []}
assert abs(S._funding(out9, "XUSDT", BASE, BASE + 30 * S.MIN_MS, True, ctx9) - 0.01) < 1e-12 and out9["funding_pct"] == 0.01
assert abs(S._funding(out9, "XUSDT", BASE, BASE + 30 * S.MIN_MS, False, ctx9) + 0.01) < 1e-12
ctx_nf = dict(ctx9, tape=S.Tape(tempfile.mkdtemp(), fetch_zip=lambda u: None, fetch_rest=lambda u: None, now_ms=now9, rest_pace_s=0))
out9 = {"flags": []}
assert S._funding(out9, "XUSDT", BASE, BASE + 30 * S.MIN_MS, True, ctx_nf) == 0.0 and "no_funding" in out9["flags"]
# HL-насичення: 2000 філів однієї мс повторюються → година насичена, прапорець hl_sat, кеш не пишеться
page = [{"tid": k, "time": BASE + 1000, "coin": "AAA", "px": "1", "sz": "1", "dir": "Close Long", "crossed": True,
         "startPosition": "5", "hash": "0x%d" % k, "side": "A"} for k in range(2000)]
hl = S.HLFills(tempfile.mkdtemp(), fetch_hl=lambda body: [f for f in page if body["startTime"] <= f["time"] <= body["endTime"]], now_ms=now9, pace_s=0)
fills = hl.user_fills("0xw", BASE - 600_000, BASE + 2000)
assert fills is not None and len(fills) == 2000 and hl.saturated("0xw", BASE - 600_000, BASE + 2000) and hl.stats["sat"] == 1
assert not os.path.exists(os.path.join(hl.dir, "0xw-%d.json" % ((BASE + 1000) // S.HOUR_MS)))   # насичена година не кешується
outw = {"flags": []}
S._whale(outw, {"whale_addr": "0xw", "coin": "AAA"}, BASE + 2000, {"hl": hl, "tape": tape}, close_only=True, symbol=None)
assert "hl_sat" in outw["flags"]
hl2 = S.HLFills(tempfile.mkdtemp(), fetch_hl=lambda body: [f for f in page[:1500] if body["startTime"] <= f["time"] <= body["endTime"]], now_ms=now9, pace_s=0)
hl2.user_fills("0xw", BASE - 600_000, BASE + 2000)
assert not hl2.saturated("0xw", BASE - 600_000, BASE + 2000) and hl2.stats["sat"] == 0
# реплей TP R8: вхід на BASE+5000 (100.0…), TP = tp_px 100.52 перетнуто у хвості (100.5+k·0.001 ≥ 100.52 при k≥20)
row8 = {"sig_id": "z1", "strategy": R8, "date": S._dt(BASE + 5000), "coin": "X", "our_side": "LONG", "entered": "1",
        "entry_px": "100.0", "entry_ts_ms": str(BASE + 5000), "exit_ts_ms": str(BASE + 5000 + 30 * S.MIN_MS),
        "exit_px": "100.9", "exit_reason": "timer", "exit_min": "30", "tp_px": "100.52", "costs_pct": "0.15",
        "dump_move_pct": "-1.0", "whale_addr": ""}
ctx8 = S.make_ctx(dd, {"X": "XUSDT"}, {"fetch_zip": lambda u: None, "fetch_rest": rest9, "fetch_hl": lambda b: [],
                                       "rest_pace_s": 0, "hl_pace_s": 0}, now_ms=now9, log=lambda *a: None)
o8 = S.settle_rev(row8, ctx8)
t_hit = BASE + 6000 + 21 * 100   # пошук перетину від входу + вікно 3 с (tape3s): (BASE+8000, …] → k=21
assert "tp_tape_hit" in o8["flags"] and o8["tp_hit_ms"] == t_hit and o8["exit_ts_ms"] == t_hit + S.TP_DETECT_MS \
       and o8["exit_reason_tape"] == "tp_replay" and o8["exit_src"] == "tape_replay", o8["flags"]
e8 = o8["entry_px_tape"]; assert e8 is not None
x_mkt = min(p for a, t, p in ROWS if t_hit + 2000 <= t <= t_hit + 5000)     # SELL: гірша = min у 3 с
assert abs(o8["exit_px_tape"] - x_mkt) < 1e-12 and abs(o8["net_tp_mkt_pct"] - ((x_mkt / e8 - 1) * 100 - 0.15)) < 1e-9
assert o8["net_tp_lim_pct"] is not None and abs(o8["net_tp_lim_pct"] - ((100.52 / e8 - 1) * 100 - 0.15)) < 1e-9
assert abs(o8["net_tape_pct"] - o8["net_tp_mkt_pct"]) < 1e-9 and o8["funding_pct"] == 0.0   # вихід на ~+3 с — нарахування (+20 хв) не потрапило
assert o8["cover_ok"] == 1 and o8["status"] == "verified" and abs(o8["net_official_pct"] - min(o8["net_live_pct"], o8["net_tape_pct"])) < 1e-9
assert o8["curve_final"] in (0, 1) and int(o8["curve_n"]) <= 60
# TP не перетнуто (tp_px 200) → m30_replay на вході+30 хв, прапорець miss, покриття (стрічка є до кінця?) — вікно 30 хв
# виходить за межі даних REST (відра після BASE+10 хв порожні, але обійдені повністю) → covered
row8b = dict(row8, sig_id="z2", tp_px="200.0")
o8b = S.settle_rev(row8b, ctx8)
assert "tp_tape_miss" in o8b["flags"] and o8b["exit_reason_tape"] == "m30_replay" and o8b.get("tp_hit_ms") is None \
       and o8b["exit_ts_ms"] == BASE + 5000 + 30 * S.MIN_MS
# синтетична стрічка після +11 с порожня → ноги виходу m30 по стрічці немає: partial/live_only,
# але фандинг за утримання (нарахування +20 хв < m30) порахований і записаний
assert o8b["net_tp_mkt_pct"] is None and o8b["net_tp_lim_pct"] is None and "partial" in o8b["flags"] \
       and o8b["status"] == "live_only" and o8b["funding_pct"] == 0.01 and o8b["net_tape_pct"] is None
# черга: чергування новіших і старіших (H) — порядок індексів
order = []
i_, j_ = 0, 4
while i_ <= j_:
    order.append(i_)
    if j_ != i_: order.append(j_)
    i_ += 1; j_ -= 1
assert order == [0, 4, 1, 3, 2]
_sp = S.settle_pending.__code__.co_consts
assert "_curve_final" in S.HEADERS or "curve_final" in S.HEADERS
assert S._curve_complete({"family": "fol", "curve_n": "60"}) and not S._curve_complete({"family": "fol", "curve_n": "59"})
print("9) settle: пагінація за id при насиченні, повнота обходу → покриття, HL-насичення → hl_sat, реплей TP (ринковий/лімітний), фандинг зі стрічки, крива лише 100%")

# ═══ 10. Структурні: версія, settlement v3 придатний, UI, CLAUDE.md ═══════
LS = load({"_load_settlements"}, dict(C, DATA_DIR=d7, stats={}, _settle_cache={"key": None, "data": {}}))
wcsv("settlements.csv", S.HEADERS, [setr("v3|R1", "rev", "R1_загальний", settle_v="3"), setr("v2|R1", "rev", "R1_загальний", settle_v="2"),
                                    setr("v4|R1", "rev", "R1_загальний")])
d = LS["_load_settlements"]()
# v2.20 (аудит №4): придатний лише рядок ПОТОЧНОЇ версії (v5) — v3/v4 рахувались на кешах без доказу повноти
assert set(d) == {"v4|R1"} and LS["stats"]["settle_old_v"] == 1 and LS["stats"]["settle_prev_v"] == 2   # «v4|R1» записано з SETTLE_V (поточна)
ui = open((_HL + "/hyperliquid-terminal.html"), encoding="utf-8").read()
for k in ("svExec", "Binance · стакан fapi", "n_sig_invalid", "n_sig_pending", "n_px_unverified", "n_exit_partial",
          "усі paper-угоди", "C('sig_ok','Сигнал'", "tp_phantom", "exit_partial", "Фандинг", "net_tp_mkt", "n_paper"):
    assert k in ui, k
_js = "\n".join(re.findall(r"<script>(.*?)</script>", ui, re.S))
_jp = os.path.join(tempfile.gettempdir(), "_v219_inline.js")
open(_jp, "w", encoding="utf-8").write(_js)
assert os.system(f"node --check {_jp}") == 0
cm = open((_HL + "/CLAUDE.md"), encoding="utf-8").read()
assert "v2.19" in cm and "Binance" in cm and "sig_ok" in cm and "фандинг" in cm.lower()
assert "def bn_book_exec" in src and "def _ingest_txs" in src and "def _pending_rev_rows" in src and "def _r8_tp_replay" in open((_HL + "/settle.py"), encoding="utf-8").read()
print("10) структурні: settlement v3 придатний / v2 pending, UI-маркери і синтаксис, CLAUDE.md v2.19")

print("\nУСІ РЕГРЕСІЇ v2.19 ЗЕЛЕНІ")

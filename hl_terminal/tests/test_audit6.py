import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Регресія на знахідки шостого аудиту (v2.5 -> v2.6)."""
import ast, threading, time, json, os, re, sys, tempfile, csv, math

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
          "math": math, "print": lambda *a, **k: None}
    ns.update(_V28)   # v2.8: нові константи/стаби
    ns.update(extra or {})
    exec(compile(mod, "x", "exec"), ns)
    return ns

class RateLimited(Exception): pass
class APIError(Exception): pass
C = dict(PROFILE_ALGO_V=5, F4_MIN_EPISODES=2, F4_MIN_NOTIONAL=100_000.0,
         F4_MAX_UNLOAD_S=300.0, F4_CHUNK_PCT=0.05, DATA_ALGO_V="2.6",
         MIN_POS_USD=50_000.0, MIN_TX_USD=5_000.0,
         RateLimited=RateLimited, APIError=APIError)
T = 1_700_000_000_000

# ── 1. Пагінація філів: 8 сторінок, далі fail-closed ────
def mk_api(n_fills, t0=T):
    fills = [{"time": t0 + i, "tid": i, "hash": f"0x{i}", "oid": i,
              "coin": "X", "dir": "Close Long", "crossed": True,
              "twapId": None, "px": "2.0", "sz": "1.0"} for i in range(n_fills)]
    calls = []
    def post(body, retries=4):
        calls.append(body["startTime"])
        st = body["startTime"]
        return sorted([f for f in fills if f["time"] >= st],
                      key=lambda f: f["time"])[:2000]
    return post, calls
gmf = load({"get_recent_market_fills"}, dict(C, hl_post=mk_api(17_000)[0]))
try:
    gmf["get_recent_market_fills"]("0xa", "X", T - 10, "LONG")
    raise SystemExit("FAIL: 17k філів прийнято як повні")
except APIError as e:
    assert "incomplete" in str(e)
# 12k (сценарій аудиту: 5 старих сторінок губили 2k) тепер читається ВЕСЬ
post_ok, calls_ok = mk_api(12_000)
g2 = load({"get_recent_market_fills"}, dict(C, hl_post=post_ok))
txs = g2["get_recent_market_fills"]("0xa", "X", T - 10, "LONG")
assert len(calls_ok) >= 6, len(calls_ok)
assert sum(t["sz"] for t in txs) == 12_000.0, sum(t["sz"] for t in txs)
print("1) 12k філів читаються ПОВНІСТЮ; >16k = APIError (пропуск без алерту)")

# ── 2. Скан-діф: курсор рухається ДО порогів (структурно) ──
i0 = src.index("Той самий поріг, що і в realtime")
seg = src[i0 - 1200:i0 + 1000]
i_cur = seg.index('fill_cursor[key_ac] = max(f["ts"] for f in mfills)')
i_gate = seg.index("if not big_txs or not _ratio_ok(new_pos, _now)")
assert i_cur < i_gate, "курсор має рухатись ДО порогових перевірок"
assert seg.count('fill_cursor[key_ac] = max(f["ts"]') == 1
print("2) скан-діф: відхилений порогом філ не воскресає хибним алертом")

# ── 3. F4: NaN і відсутній coin = bad (fail-closed) ─────
def mkf(t, px, sz, sp, coin="AAA"):
    return {"time": t, "px": px, "sz": sz, "startPosition": sp,
            "dir": "Close Long", "coin": coin, "crossed": True,
            "twapId": None, "hash": f"0x{coin}{t}", "tid": t}
bp = load({"_build_profile", "_grade_episode"}, dict(C))
bp["_build_profile"] = _v28_with_opens(bp["_build_profile"])   # v2.18
good = ([mkf(T, 2.0, 50000, 100000, "DDD"),
         mkf(T + 60_000, 2.0, 50000, 50000, "DDD")] +
        [mkf(T + 10**7, 2.0, 60000, 60000, "EEE")])
assert bp["_build_profile"](good)["ok"]
p_nan = bp["_build_profile"](good + [mkf(T + 2*10**7, "nan", "nan", "nan", "FFF")])
assert not p_nan["ok"] and p_nan.get("err") == 1, p_nan
no_coin = mkf(T + 3*10**7, 2.0, 10, 100); del no_coin["coin"]
p_nc = bp["_build_profile"](good + [no_coin])
assert not p_nc["ok"] and p_nc.get("err") == 1, p_nc
print("3) NaN-числа і філ без coin = err-профіль, не тиха кваліфікація")

# ── 4. EOL-вартовий: обрив усередині останнього поля ────
csvw = load({"_strat_csv_append"}, dict(C, _csv_lock=threading.Lock()))
d4 = tempfile.mkdtemp(); p4 = os.path.join(d4, "t.csv")
H4 = ["trade_id", "net", "eol"]
assert csvw["_strat_csv_append"](p4, H4, ["trade-123456", 1.0]) is True
raw = open(p4).read()
assert raw.strip().endswith("^")   # вартовий дописано автоматично
# симулюємо обрив: рядок з обрізаним trade_id, колонок стільки ж, "^" нема
with open(p4, "a", newline="") as f:
    f.write("trade-123,1.0,\r\n")
# читання (як у strat2_api.read)
q = [0]
with open(p4, newline="") as f:
    rd = csv.reader(f); hdr = next(rd)
    keep = []
    for rr in rd:
        if len(rr) != len(hdr) or (hdr[-1] == "eol" and rr[-1] != "^"):
            q[0] += 1; continue
        keep.append(rr)
assert len(keep) == 1 and q[0] == 1, (keep, q)
print("4) обірваний хвіст без '^' карантиниться; цілий рядок живий")

# ── 5. prio: transient-збій знімає кулдаун адреси ───────
seg5 = src[src.index("def run_prio_fetcher"):src.index("def get_recent_market_fills")]
i_err = seg5.index('_prio_log(addr, coin0, pos_side, notional, depth0, thr,\n                          "error"')
assert '_prio_seen.pop(addr, None)' in seg5[:i_err + 200]
print("5) prio: помилка перевірки не глушить адресу на 10 хв")

# ── 6. watchdog: alerted лише після успішного TG ────────
wd = open((_HL + "/watchdog.py"), encoding="utf-8").read()
assert "return r.status == 200" in wd and "return False" in wd
assert "sent_ok = tg(a) and sent_ok" in wd and "cur = prev" in wd
compile(wd, "watchdog", "exec")
print("6) watchdog: невідправлений алерт повторюється наступним тиком")

# ── 7. Хедери: eol скрізь; UI: sht-фільтр лише для реверсів ──
for h in ("REV_SIG_HEADERS", "REV_HEADERS", "FOLLOW_HEADERS",
          "FOLLOW_OUT_HEADERS", "REV_OUT_HEADERS", "PRIO_HEADERS"):
    i = src.index(h + " ="); assert '"eol"' in src[i:i + 2600], h   # v2.8/v2.18: коментарі в FOLLOW_HEADERS
html = open((_HL + "/hyperliquid-terminal.html"),
            encoding="utf-8").read()
assert "isRev" in html.split("fSht!=='all'")[1][:60]   # v2.11: isRev
assert "НЕ лише входи цієї стратегії" in html
assert 'DATA_ALGO_V      = "2.20"' in src  # v2.14: незмінність результатів, ідентичність TWAP
print("7) eol у 6 заголовках; sht-фільтр не спустошує F-таблиці; версія 2.7")

# ── 8. Повне закриття дрібними шматками = сигнал (свідомо) ──
rev = src[src.index("def rev_on_close"):src.index("def _rev_samples")]
assert "СВІДОМИЙ виняток" in rev and "MIN_POS_USD" in rev
print("8) full-close виняток з MIN_TX_USD задокументований у коді")

print("\nУСІ РЕГРЕСІЇ ШОСТОГО АУДИТУ ЗАКРИТІ")

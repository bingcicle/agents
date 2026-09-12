#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Тести settle.py (SETTLE_V=2): реальні zip-стрічки, синтетична стрічка через
ін'єкцію fetch_zip/fetch_rest/fetch_hl, розрахунок follow/rev/twap за
ЗАПИСАНИМИ часами, філи кита (епізод 300 с + startPosition), status/curve_tape,
відкладання без стрічки (72 год), дозрівання кривої, ротація заголовка.
Запуск: python3 test_settle.py"""
import bisect
import csv
import io
import json
import os
import shutil
import sys
import zipfile

sys.path.insert(0, _HL)
import settle as S  # noqa: E402
# v2.19: фандинг — окремий блок у test_v219; тут стрічка фейкова, фандинг = 0 без прапорця
_REAL_FUNDING = S.Tape.funding
S.Tape.funding = lambda self, symbol, t0, t1: []

SP = _SPT
# проба стрічки (реальні zip aggTrades ~11 МБ) НЕ в git: шукаємо у tests/fixtures/tape_probe,
# інакше генеруємо синтетичну (той самий формат Binance) — див. _ensure_probe()
_PROBE_CANDS = [os.path.join(_HL, "tests", "fixtures", "tape_probe"),
                "/tmp/claude-0/-home-user-agents/a1265cd8-6a44-5e82-a006-741dba9fbe9b/scratchpad/tape_probe"]
PROBE = os.path.join(_SPT, "tape_probe")
PROBE_KIND = [None]
TD = os.path.join(SP, "settle_test_data")
DAY = S.DAY_MS
H = S.HOUR_MS
M = S.MIN_MS
NCHK = [0]


def check(cond, msg):
    NCHK[0] += 1
    assert cond, msg


def close(a, b, tol=1e-6):
    return a is not None and b is not None and abs(float(a) - float(b)) <= tol


def fresh(name):
    d = os.path.join(TD, name)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    return d


def zip_bytes(symbol, day, rows, header=True):
    """Синтетичний zip aggTrades у форматі Binance (agg_trade_id = порядок рядка)."""
    lines = []
    if header:
        lines.append("agg_trade_id,price,quantity,first_trade_id,last_trade_id,"
                     "transact_time,is_buyer_maker")
    for i, (t, p) in enumerate(rows):
        lines.append("%d,%s,1.0,%d,%d,%d,false" % (i, p, i, i, t))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("%s-aggTrades-%s.csv" % (symbol, day), "\n".join(lines) + "\n")
    return buf.getvalue()


class FakeZip:
    """fetch_zip: {(symbol, day): bytes}; решта → None (404)."""
    def __init__(self, files):
        self.files, self.calls = files, []

    def __call__(self, url):
        self.calls.append(url)
        for (s, d), b in self.files.items():
            if url.endswith("/%s/%s-aggTrades-%s.zip" % (s, s, d)):
                return b
        return None


def read_zip_rows(path):
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as f:
            rd = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            next(rd)
            return [(int(r[5]), float(r[1])) for r in rd]


def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(h, "") for h in header])


def _ensure_probe():
    """Реальна проба (якщо є) або синтетична стрічка дня 2026-09-09 у форматі Binance."""
    global PROBE
    for d in _PROBE_CANDS:
        if all(os.path.exists(os.path.join(d, s + ".zip")) for s in ("HYPEUSDT", "PUMPUSDT")):
            PROBE = d; PROBE_KIND[0] = "реальна (%s)" % d
            return
    import calendar, random
    os.makedirs(PROBE, exist_ok=True)
    day0 = calendar.timegm((2026, 9, 9, 0, 0, 0)) * 1000
    rnd = random.Random(909)
    def walk(n, p0, step, gap):
        rows, t, p = [], day0 + 10, p0
        for _ in range(n):
            rows.append((t, round(p, 6)))
            t += rnd.randint(1, gap)
            if t >= day0 + DAY - 1000:
                break
            p = max(p0 * 0.5, p + rnd.uniform(-step, step))
        return rows
    for sym, n, p0, step in (("HYPEUSDT", 60000, 40.0, 0.02), ("PUMPUSDT", 30000, 0.005, 0.00001)):
        with open(os.path.join(PROBE, sym + ".zip"), "wb") as f:
            f.write(zip_bytes(sym, "2026-09-09", walk(n, p0, step, 2800)))
    PROBE_KIND[0] = "синтетична (%s)" % PROBE


def curve(o):
    return o["curve_tape"].split(";") if o["curve_tape"] else []


NO_REST = lambda url: None       # noqa: E731
NO_HL = lambda body: None        # noqa: E731

# ── блок 1: реальні zip ─────────────────────────────────────────────────
_ensure_probe()
print("проба стрічки:", PROBE_KIND[0])
hype_rows = read_zip_rows(os.path.join(PROBE, "HYPEUSDT.zip"))
hype_ts = [r[0] for r in hype_rows]
DAY0 = (hype_ts[0] // DAY) * DAY
check(S._day_str(DAY0) == "2026-09-09", "день проби = 2026-09-09")
BASE = DAY0 + 12 * H             # 12:00 UTC синтетичного дня
NOW_OLD = DAY0 + 10 * DAY        # «зараз» через 10 днів: усе старе, стрічка зріла


def block1():
    dd = fresh("b1")
    os.makedirs(os.path.join(dd, "tape"))
    shutil.copy(os.path.join(PROBE, "HYPEUSDT.zip"),
                os.path.join(dd, "tape", "HYPEUSDT-aggTrades-2026-09-09.zip"))
    with open(os.path.join(PROBE, "PUMPUSDT.zip"), "rb") as f:
        pump_raw = f.read()
    shifted = [(t + DAY, p) for t, p in hype_rows[:5000]]
    fz = FakeZip({("PUMPUSDT", "2026-09-09"): pump_raw,
                  ("HYPEUSDT", "2026-09-10"): zip_bytes("HYPEUSDT", "2026-09-10", shifted)})
    tape = S.Tape(dd, fetch_zip=fz, fetch_rest=NO_REST, now_ms=NOW_OLD)
    for t in (hype_ts[0], hype_ts[len(hype_ts) // 3], DAY0 + 43200000, hype_ts[-1] - 5000):
        i, j = bisect.bisect_left(hype_ts, t), bisect.bisect_right(hype_ts, t + 3000)
        exp = [p for _, p in hype_rows[i:j]]
        check(exp, "у вікні 3с є трейди (t=%d)" % t)
        rb = S.exec_px("HYPEUSDT", t, "BUY", tape)
        rs = S.exec_px("HYPEUSDT", t, "SELL", tape)
        check(rb["src"] == "tape3s" and rs["src"] == "tape3s", "src tape3s")
        check(rb["n"] == j - i and rs["n"] == j - i, "n = кількість трейдів у [t, t+3с]")
        check(rb["px"] == max(exp), "BUY = max у вікні (t=%d)" % t)
        check(rs["px"] == min(exp), "SELL = min у вікні (t=%d)" % t)
        check(rb["age_ms"] == 0, "age 0 для tape3s")
    check(not any("HYPEUSDT-aggTrades-2026-09-09" in u for u in fz.calls),
          "HYPE 09-09 читається з дискового кешу без мережі")
    # t=00:00:00.010 → вікно [t, t+10с] не торкає попереднього дня (stale-вікна назад немає)
    check(not any("HYPEUSDT-aggTrades-2026-09-08" in u for u in fz.calls),
          "вікно [t, t+10с] не пробує 08.09")
    # PUMP через fetch_zip + запис у кеш
    pump_rows = read_zip_rows(os.path.join(PROBE, "PUMPUSDT.zip"))
    pts = [r[0] for r in pump_rows]
    t = pts[len(pts) // 2]
    i, j = bisect.bisect_left(pts, t), bisect.bisect_right(pts, t + 3000)
    r = S.exec_px("PUMPUSDT", t, "SELL", tape)
    check(r["px"] == min(p for _, p in pump_rows[i:j]) and r["n"] == j - i, "PUMP SELL min")
    check(os.path.exists(os.path.join(dd, "tape", "PUMPUSDT-aggTrades-2026-09-09.zip")),
          "zip закешований на диску")
    check(len([u for u in fz.calls if "PUMPUSDT" in u]) == 1, "один запит zip PUMP")
    # перехід через північ: два дні
    t0, t1 = DAY0 + DAY - 1000, DAY0 + DAY + 1000
    ts, px = S.get_trades("HYPEUSDT", t0, t1, tape)
    exp = (sum(1 for x in hype_ts if x >= t0)
           + sum(1 for x, _ in shifted if x <= t1))
    check(len(ts) == exp and exp > 0, "вікно через північ = трейди обох днів (%d)" % exp)
    check(all(ts[k] <= ts[k + 1] for k in range(len(ts) - 1)), "зшите вікно відсортоване")
    check(any("HYPEUSDT-aggTrades-2026-09-10.zip" in u for u in fz.calls),
          "другий день дотягнутий через fetch_zip")
    tm = DAY0 + DAY - 500
    r = S.exec_px("HYPEUSDT", tm, "BUY", tape)
    exp_px = ([p for x, p in hype_rows if x >= tm] + [p for x, p in shifted if x <= tm + 3000])
    check(r["src"] == "tape3s" and r["n"] == len(exp_px) and r["px"] == max(exp_px),
          "exec_px через північ (n=%d)" % r["n"])
    # zip без заголовка парситься так само
    a = S.parse_agg_zip(zip_bytes("X", "2026-09-09", hype_rows[:100], header=True))
    b = S.parse_agg_zip(zip_bytes("X", "2026-09-09", hype_rows[:100], header=False))
    check(list(a[0]) == list(b[0]) and list(a[1]) == list(b[1]) and len(a[0]) == 100,
          "zip без заголовка = zip із заголовком")
    # невідсортований вхід сортується за часом
    c = S.parse_agg_zip(zip_bytes("X", "2026-09-09", [(5, 1.0), (3, 2.0), (4, 3.0)]))
    check(list(c[0]) == [3, 4, 5] and list(c[1]) == [2.0, 3.0, 1.0], "сортування за часом")
    # одна мс: порядок файлу (agg_trade_id), НЕ за ціною — і в упорядкованому, і в
    # невпорядкованому файлі
    c = S.parse_agg_zip(zip_bytes("X", "2026-09-09", [(5, 2.0), (5, 1.0), (5, 3.0)]))
    check(list(c[1]) == [2.0, 1.0, 3.0], "одна мс, порядок файлу: %r" % list(c[1]))
    c = S.parse_agg_zip(zip_bytes("X", "2026-09-09", [(5, 2.0), (5, 1.0), (3, 9.0), (5, 0.5)]))
    check(list(c[0]) == [3, 5, 5, 5] and list(c[1]) == [9.0, 2.0, 1.0, 0.5],
          "невпорядкований файл: (ts, agg_id), ціни однієї мс не переставлені %r" % list(c[1]))
    # LRU: ліміт 1 → другий день витісняє перший
    small = S.Tape(fresh("b1lru"), fetch_zip=fz, fetch_rest=NO_REST, lru_max=1, now_ms=NOW_OLD)
    small.day("PUMPUSDT", "2026-09-09")
    small.day("HYPEUSDT", "2026-09-10")
    check(list(small._lru.keys()) == [("HYPEUSDT", "2026-09-10")], "LRU тримає lru_max днів")
    print("блок 1 (реальні zip, північ, парсер, порядок однієї мс): OK")


# ── блок 2: синтетична стрічка, правило вікон, REST ──────────────────────
def block2():
    dd = fresh("b2")
    rows = [(BASE + 1000, 100.0), (BASE + 2000, 101.0), (BASE + 2500, 99.5),
            (BASE + 60000 + 5000, 50.0), (BASE + 60000 + 8000, 51.0),
            (BASE + 200000, 70.0)]
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", rows)})
    tape = S.Tape(dd, fetch_zip=fz, fetch_rest=NO_REST, now_ms=NOW_OLD)
    rb, rs = S.exec_px("SYNUSDT", BASE, "BUY", tape), S.exec_px("SYNUSDT", BASE, "SELL", tape)
    check(rb == {"px": 101.0, "src": "tape3s", "n": 3, "age_ms": 0}, "3с BUY = max %r" % rb)
    check(rs == {"px": 99.5, "src": "tape3s", "n": 3, "age_ms": 0}, "3с SELL = min %r" % rs)
    rb = S.exec_px("SYNUSDT", BASE + 60000, "BUY", tape)
    rs = S.exec_px("SYNUSDT", BASE + 60000, "SELL", tape)
    check(rb["src"] == "tape10s" and rb["px"] == 51.0 and rb["n"] == 2, "10с BUY %r" % rb)
    check(rs["src"] == "tape10s" and rs["px"] == 50.0, "10с SELL %r" % rs)
    # трейд ДО t — не доказ виконання: stale прибрано, 30 с після трейду = none
    r = S.exec_px("SYNUSDT", BASE + 230000, "BUY", tape)
    check(r == {"px": None, "src": "none", "n": 0, "age_ms": None}, "без трейдів після t → none %r" % r)
    r = S.exec_px("SYNUSDT", BASE + 200001, "SELL", tape)
    check(r["src"] == "none", "трейд за 1 мс до t не рахується")
    r = S.exec_px("SYNUSDT", BASE + 200000, "SELL", tape)
    check(r["src"] == "tape3s" and r["px"] == 70.0, "трейд рівно в t рахується")
    r = S.exec_px("SYNUSDT", BASE + 400000, "SELL", tape)
    check(r == {"px": None, "src": "none", "n": 0, "age_ms": None}, "none %r" % r)
    r = S.exec_px("NOPEUSDT", BASE, "BUY", tape)
    check(r["src"] == "none", "невідомий символ → none")
    n_calls = len(fz.calls)
    S.exec_px("NOPEUSDT", BASE + 5000, "BUY", tape)
    check(len(fz.calls) == n_calls, "404 не повторюється у тому ж запуску")
    # _tape_last_px: останній трейд з ts ≤ t СТРОГО (first_ts−1 не бачить філа на first_ts)
    check(S._tape_last_px("SYNUSDT", BASE + 2000, tape) == 101.0, "last_px ≤ t включно")
    check(S._tape_last_px("SYNUSDT", BASE + 1999, tape) == 100.0, "last_px строго: t−1 не бачить трейд у t")
    check(S._tape_last_px("SYNUSDT", BASE + 999, tape) is None, "last_px: до першого трейду → None")
    check(S._tape_last_px("SYNUSDT", BASE + 2500 + 10000, tape) == 99.5
          and S._tape_last_px("SYNUSDT", BASE + 2500 + 10001, tape) is None, "вікно 10 с назад")
    # REST-фолбек: zip 404, день у межах 48 год → 10-хв відро з пагінацією
    rest_rows = [(BASE + k * 100, 100.0 + k * 0.001) for k in range(1500)]
    same_ms = [(1, "9.0"), (2, "4.5"), (3, "5.0")]      # (a, p) в одну мс: за a (як біржа), ціни не монотонні
    calls = []

    def fake_rest(url):
        calls.append(url)
        q = dict(p.split("=") for p in url.split("?")[1].split("&"))
        if "fundingRate" in url:
            return []
        if "fromId" in q:
            # v2.19 (аудит G): друга і далі сторінки — за id, без часу
            fid = int(q["fromId"])
            return [{"a": k, "p": str(p), "q": "1", "T": t, "m": False}
                    for k, (t, p) in enumerate(rest_rows) if k >= fid][:1000]
        st, en = int(q["startTime"]), int(q["endTime"])
        if q["symbol"] == "MSUSDT":
            out = [{"a": 0, "p": "7.0", "q": "1", "T": BASE + 4999, "m": False}]
            out += [{"a": a, "p": p, "q": "1", "T": BASE + 5000, "m": False} for a, p in same_ms]
            return [r for r in out if st <= r["T"] <= en]
        out = [{"a": k, "p": str(p), "q": "1", "T": t, "m": False}
               for k, (t, p) in enumerate(rest_rows) if st <= t <= en]
        return out[:1000]

    now = DAY0 + DAY + H                       # 10.09 01:00 → день 09.09 ще у 48 год
    tape2 = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=fake_rest, now_ms=now,
                   rest_pace_s=0)
    r = S.exec_px("RESTUSDT", BASE, "BUY", tape2)
    check(r["src"] == "tape3s" and r["n"] == 31 and close(r["px"], 100.030), "REST BUY %r" % r)
    qs = lambda u: dict(p.split("=") for p in u.split("?")[1].split("&"))   # noqa: E731
    # вікно [t, t+10с] — одне відро, 2 сторінки (стрічка назад більше не читається)
    cb = [c for c in calls if "aggTrades" in c and ("fromId" in c or int(qs(c)["startTime"]) >= BASE)]
    check(len(cb) == 2 and "limit=1000" in cb[0] and "startTime" in cb[0], "пагінація: 2 запити відра (%d)" % len(cb))
    check("fromId=1000" in cb[1] and "startTime" not in cb[1], "друга сторінка — за id (аудит v2.17 G), не за T")
    b0 = (BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS
    jp = os.path.join(dd, "tape", "rest", "RESTUSDT-%d.json" % b0)
    jrows = json.load(open(jp))
    jmeta = jrows if isinstance(jrows, dict) else {}
    jrows = jmeta.get("rows") if isinstance(jmeta, dict) else jrows
    check(os.path.exists(jp) and isinstance(jrows, list) and len(jrows) == 1500, "відро на диску (1500 без дублів)")
    check(jmeta.get("v") == S.CACHE_V and jmeta.get("complete") is True and jmeta.get("n") == 1500,
          "v2.20: кеш відра версійований (v3) з доказом повноти")
    check(all(len(x) == 3 for x in jrows) and jrows[0][2] == 0 and jrows[-1][2] == 1499,
          "кеш відра — рядки [ts, px, agg_id]")
    tape3 = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=fake_rest, now_ms=now, rest_pace_s=0)
    n = len(calls)
    r2 = S.exec_px("RESTUSDT", BASE + 100000, "SELL", tape3)
    check(len(calls) == n and r2["src"] == "tape3s" and close(r2["px"], 101.0), "REST з диска без мережі")
    # одна мс через REST: порядок (T, a), не за ціною; last_px = останній за a
    ts, px = S.get_trades("MSUSDT", BASE + 4999, BASE + 5000, tape3)
    check(list(px) == [7.0, 9.0, 4.5, 5.0], "REST одна мс: порядок за agg_id %r" % list(px))
    check(S._tape_last_px("MSUSDT", BASE + 5000, tape3) == 5.0
          and S._tape_last_px("MSUSDT", BASE + 4999, tape3) == 7.0, "last_px = останній за a, строго по t")
    check(S.exec_px("MSUSDT", BASE + 5000, "BUY", tape3)["px"] == 9.0, "BUY = max у мс")
    # старий кеш v1 (рядки [ts, px], без agg_id) НЕПРИДАТНИЙ (порядок однієї мс
    # втрачено): ігнорується, файл видаляється, відро перетягується (тут REST
    # мовчить → none); повний сценарій перетягування — блок 9 (F)
    old_jp = os.path.join(dd, "tape", "rest", "OLDUSDT-%d.json" % b0)
    with open(old_jp, "w") as f:
        json.dump([[BASE + 100, 1.5], [BASE + 200, 1.7]], f)
    logs_old = []
    tape_old = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=NO_REST, now_ms=now, rest_pace_s=0,
                      log=logs_old.append)
    r = S.exec_px("OLDUSDT", BASE, "BUY", tape_old)
    check(r["src"] == "none" and not os.path.exists(old_jp) and tape_old.stats["rest_req"] == 1
          and any("старого формату" in m for m in logs_old),
          "старий 2-елементний кеш відра ігнорується, файл видалено, спроба перетягнути %r" % r)
    # неповне (поточне) відро: не на диску, але в пам'яті на життя Tape
    dd2 = fresh("b2cur")
    tape5 = S.Tape(dd2, fetch_zip=lambda u: None, fetch_rest=fake_rest, now_ms=BASE + 5 * M,
                   rest_pace_s=0)
    n = len(calls)
    r = S.exec_px("RESTUSDT", BASE, "BUY", tape5)
    n1 = len(calls)
    check(r["src"] == "tape3s" and n1 > n, "поточне відро — з мережі")
    S.exec_px("RESTUSDT", BASE + 1000, "SELL", tape5)
    S.exec_px("RESTUSDT", BASE + 2000, "SELL", tape5)
    check(len(calls) == n1, "поточне відро в пам'яті — без повторних запитів")
    check(not os.path.exists(os.path.join(dd2, "tape", "rest", "RESTUSDT-%d.json" % b0)),
          "поточне відро не персиститься")
    # старий день без zip → REST не пробується
    tape4 = S.Tape(fresh("b2old"), fetch_zip=lambda u: None, fetch_rest=fake_rest,
                   now_ms=NOW_OLD, rest_pace_s=0)
    n = len(calls)
    r = S.exec_px("RESTUSDT", BASE, "BUY", tape4)
    check(r["src"] == "none" and len(calls) == n, "день старший за 48 год: без REST")
    print("блок 2 (правило 3с→10с→none, строгий last_px, REST: порядок мс, кеш): OK")


# ── блок 3: settle_follow ───────────────────────────────────────────────
FOL_HDR = ["date_open", "date_close", "strategy", "coin", "our_side", "whale_addr",
           "entry_px", "exit_px", "exit_reason", "hold_s", "gross_pct", "costs_pct",
           "net_pct", "algo_v", "trade_id", "open_ts_ms", "close_ts_ms", "fill_ts_ms", "eol"]   # v2.20: час тригера


def follow_tape():
    t1, t1x = BASE, BASE + 120000
    t2, t2x = BASE + 600000 + 123, BASE + 600000 + 123 + 180000
    return [(t1 + 500, 100.0), (t1 + 1500, 99.8), (t1x + 200, 99.0), (t1x + 900, 99.4),
            (t2 + 100, 50.0), (t2 + 2000, 50.2), (t2x + 100, 50.5), (t2x + 400, 50.3)]


def block3():
    dd = fresh("b3")
    t1, t1x = BASE, BASE + 120000
    t2, t2x = BASE + 600000 + 123, BASE + 600000 + 123 + 180000
    rows = [
        {"date_open": S._dt(t1), "date_close": S._dt(t1x), "strategy": "F1_1хв", "coin": "SYN",
         "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100.0", "exit_px": "99.0",
         "exit_reason": "silence", "hold_s": "120", "gross_pct": "1.0", "costs_pct": "0.15",
         "net_pct": "0.85", "algo_v": "2.15", "trade_id": "t1", "eol": "^"},
        {"date_open": S._dt(t2), "date_close": S._dt(t2x), "strategy": "F5_перший", "coin": "SYN",
         "our_side": "LONG", "whale_addr": "0xabc", "entry_px": "50.1", "exit_px": "50.5",
         "exit_reason": "full_close", "hold_s": "180", "gross_pct": "0.798", "costs_pct": "0.15",
         "net_pct": "0.648", "algo_v": "2.16", "trade_id": "t2",
         "open_ts_ms": str(t2), "close_ts_ms": str(t2x), "eol": "^"},
    ]
    write_csv(os.path.join(dd, "follow_trades.csv"), FOL_HDR, rows)
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    fetchers = {"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    res = S.settle_pending(dd, fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    check(res == (2, 0, 0), "settle_pending follow → (2,0,0) %r" % (res,))
    st = S.load_settlements(dd)
    with open(os.path.join(dd, "settlements.csv"), encoding="utf-8") as f:
        check(f.readline().strip() == ",".join(S.HEADERS), "заголовок settlements.csv")
    a = st["t1"]
    check(a["family"] == "fol" and a["symbol"] == "SYNUSDT" and a["settle_v"] == S.SETTLE_V, "fol/символ/поточна версія")
    check(a["ts_src"] == "local" and int(a["entry_ts_ms"]) == t1 and int(a["exit_ts_ms"]) == t1x,
          "локальні рядки → мс (%s)" % a["ts_src"])
    check(close(a["entry_px_tape"], 99.8) and a["entry_src"] == "tape3s", "SHORT вхід SELL = min")
    check(close(a["exit_px_tape"], 99.4) and a["exit_src"] == "tape3s", "SHORT вихід BUY = max")
    g = (99.4 / 99.8 - 1) * 100 * -1
    check(close(a["gross_tape_pct"], g, 1e-6) and close(a["net_tape_pct"], g - 0.15, 1e-6), "gross/net tape SHORT")
    check(close(a["net_live_pct"], 0.85) and close(a["net_official_pct"], g - 0.15, 1e-6),
          "official = min(live, tape) = tape")
    check(a["status"] == "verified" and a["flags"] == "no_fills", "status verified; flags %r" % a["flags"])
    check(close(a["entry_bias_pct"], (100.0 - 99.8) / 99.8 * 100, 1e-6) and float(a["entry_bias_pct"]) > 0,
          "SHORT: live вхід вищий за стрічку → зсув додатний")
    check(a["eol"] == "^" and a["strategy"] == "F1_1хв" and a["exit_reason_tape"] == "silence", "службові поля")
    cv = a["curve_tape"].split(";")
    check(len(cv) == 60 and cv[0] == "" and cv[1] == "%.4f" % (g - 0.15), "крива 60 хв: m2 = net виходу, m1 порожня")
    check(cv[9] == "%.4f" % ((50.2 / 99.8 - 1) * 100 * -1 - 0.15), "крива m10: BUY = max у [t+10хв, +10с]")
    b = st["t2"]
    check(b["ts_src"] == "ms" and int(b["entry_ts_ms"]) == t2, "мс-колонки мають пріоритет")
    check(close(b["entry_px_tape"], 50.2) and close(b["exit_px_tape"], 50.3), "LONG вхід BUY=max, вихід SELL=min")
    g2 = (50.3 / 50.2 - 1) * 100
    check(close(b["net_tape_pct"], g2 - 0.15, 1e-6) and close(b["net_official_pct"], g2 - 0.15, 1e-6),
          "LONG official = tape (гірший)")
    check(float(b["entry_bias_pct"]) > 0 and close(b["entry_bias_pct"], (50.1 - 50.2) / 50.2 * 100 * -1, 1e-6),
          "LONG: live вхід нижчий → зсув додатний")
    # official = live, якщо live гірший
    ctx = S.make_ctx(dd, fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    r3 = dict(rows[0], net_pct="-1.0", trade_id="t3x")
    o = S.settle_follow(r3, ctx)
    check(close(o["net_official_pct"], -1.0) and o["status"] == "verified", "official = live коли live гірший")
    o = S.settle_follow(dict(rows[0], date_close="", trade_id="t4x"), ctx)
    check(o is None, "незакрита угода → None")
    o = S.settle_follow(dict(rows[0], date_open="битий", trade_id="t5x"), ctx)
    check(o is None, "битий час → None")
    # вихід без стрічки → partial, official = live, status live_only
    r6 = dict(rows[0], trade_id="t6x", date_close=S._dt(BASE + 3 * H))
    o = S.settle_follow(r6, ctx)
    check(o["exit_src"] == "none" and "partial" in o["flags"] and close(o["net_official_pct"], 0.85)
          and o["status"] == "live_only" and o["net_tape_pct"] is None,
          "без стрічки на виході → partial, official = live, live_only")
    check(len(curve(o)) == 60, "крива є, поки є вхід по стрічці")
    # без live-результату, стрічка є → tape_only, official = tape
    o = S.settle_follow(dict(rows[0], trade_id="t7x", net_pct=""), ctx)
    check(o["status"] == "tape_only" and close(o["net_official_pct"], g - 0.15, 1e-6)
          and "partial" in o["flags"], "tape_only: official = tape")
    # ні стрічки, ні live → none; ні входу по стрічці → curve порожня
    o = S.settle_follow(dict(rows[0], trade_id="t8x", coin="NOPE", net_pct=""), ctx)
    check(o["status"] == "none" and o["net_official_pct"] is None and "no_tape" in o["flags"]
          and o["curve_tape"] == "", "none: без результату, крива порожня")
    print("блок 3 (settle_follow SHORT/LONG, min, зсув, status, крива): OK")


# ── блок 4: settle_rev ──────────────────────────────────────────────────
REV_HDR = (["sig_id", "strategy", "date", "coin", "our_side", "whale_addr", "detect_px",
            "entry_px", "entered", "move_3m_pct", "costs_pct", "algo_v", "dump_move_pct",
            "entry_ts_ms", "exit_ts_ms", "exit_px", "exit_reason", "exit_min", "tp_px",
            "entry_src", "exit_src"]
           + ["m%d" % i for i in range(1, 61)] + ["eol"])


def rev_row(**kw):
    r = {"coin": "SYN", "whale_addr": "0xabc", "entered": "1", "costs_pct": "0.15",
         "algo_v": "2.16", "eol": "^"}
    r.update(kw)
    return r


def block4():
    dd = fresh("b4")
    T1, T2, T3, T4, T5, T6 = (BASE + k * 2 * H for k in range(6))
    e2 = T2 + 45000
    H30, H60 = S.HOLD30_MS, S.HOLD60_MS
    tape = ([(T1 - 500, 100.0), (T1 + 100, 100.1), (T1 + H30 + 200, 101.1),
             (T1 + 31 * M + 300, 101.4), (T1 + H60 + 100, 102.0)]
            # T2−589000 / T3+399000 / T3+599000: трейди на краях вікон тригера, щоб стрічка
            # ПОКРИВАЛА вікно (інакше вердикт не no_trigger, а tape_gap — блок 9 C)
            + [(T2 - 600500, 200.0), (T2 - 589000, 200.0), (T2 - 300, 200.0), (T2 + 10000, 200.4),
               (e2, 200.7), (e2 + 1000, 200.9), (e2 + H30 + 50, 203.0), (e2 + 1000 + H30 + 100, 203.2)]
            + [(T3 - 100, 300.0), (T3 + 5000, 299.5), (T3 + 300000, 299.2), (T3 + 399000, 299.3),
               (T3 + 400500, 299.4), (T3 + 599000, 299.3), (T3 + 700000, 298.0),
               (T3 + 400000 + H30 + 100, 298.5)]
            + [(T4 - 100, 400.0), (T4 + 100, 400.0), (T4 + 1000, 400.4), (T4 + 120000, 405.0),
               (T4 + 300000, 407.0), (T4 + H30 + 100, 401.0)]
            + [(T5 - 100, 500.0), (T5 + 100, 500.0), (T5 + 500, 499.5), (T5 + 600000, 495.0),
               (T5 + H30 + 100, 498.0), (T5 + H30 + 900, 498.5)]
            + [(T6 - 100, 600.0), (T6 + 100, 600.0), (T6 + H30 + 100, 600.0)])
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", sorted(tape))})
    fetchers = {"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    ctx = S.make_ctx(dd, fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    # R1 старий рядок (лише date): вихід +30 хв, net60 на +60
    o = S.settle_rev(rev_row(sig_id="s1", strategy="R1_загальний", date=S._dt(T1), our_side="LONG",
                             detect_px="100.0", entry_px="100.0", move_3m_pct="1.5", m30="1.5"), ctx)
    check(o["key"] == "s1|R1_загальний" and o["family"] == "rev" and o["ts_src"] == "local", "ключ rev")
    check(close(o["entry_px_tape"], 100.1) and int(o["entry_ts_ms"]) == T1, "R1 вхід на детекті")
    check(int(o["exit_ts_ms"]) == T1 + H30 and close(o["exit_px_tape"], 101.1)
          and o["exit_reason_tape"] == "m30", "R1 вихід +30 хв")
    n30 = (101.1 / 100.1 - 1) * 100 - 0.15
    check(close(o["net_tape_pct"], n30, 1e-6) and close(o["net_live_pct"], 1.35)
          and close(o["net_official_pct"], n30, 1e-6) and o["status"] == "verified", "R1 net: official = tape")
    n60 = (102.0 / 100.1 - 1) * 100 - 0.15
    check(close(o["net60_tape_pct"], n60, 1e-6), "R1 net60 на +60 хв")
    check(close(o["exit_px_live"], 101.5), "R1 exit_px_live з m30")
    cv = curve(o)
    check(len(cv) == 60 and cv[0] == "" and cv[29] == "%.4f" % n30 and cv[59] == "%.4f" % n60
          and cv[30] == "%.4f" % ((101.4 / 100.1 - 1) * 100 - 0.15), "R1 крива: m30 = вихід, m31, m60 = net60")
    # R1 v2.16: записані entry_ts_ms/exit_ts_ms (пізній таймер +31 хв) — ціна на записаному виході
    o = S.settle_rev(rev_row(sig_id="s1b", strategy="R1_загальний", date=S._dt(T1), our_side="LONG",
                             entry_px="100.0", entry_ts_ms=str(T1), exit_ts_ms=str(T1 + 31 * M),
                             exit_px="101.3", exit_reason="timer_late", exit_min="31", m30="1.5"), ctx)
    check(o["ts_src"] == "ms" and int(o["exit_ts_ms"]) == T1 + 31 * M and close(o["exit_px_tape"], 101.4)
          and o["exit_reason_tape"] == "timer_late", "записаний вихід: ціна на exit_ts_ms, причина з рядка")
    nt1 = (101.4 / 100.1 - 1) * 100 - 0.15
    check(close(o["net_live_pct"], 1.15, 1e-9) and close(o["net_tape_pct"], nt1, 1e-9)
          and close(o["net_official_pct"], min(1.15, nt1), 1e-9) and o["status"] == "verified",
          "v2.16 net_live з exit_px, official = min")
    # R2 старий рядок: тригер = перший трейд ≥ p0*(1.003) у 10 хв
    o = S.settle_rev(rev_row(sig_id="s2", strategy="R2_breakout", date=S._dt(T2), our_side="LONG",
                             detect_px="200.0", entry_px="200.6", move_3m_pct="1.2", m30="0.5"), ctx)
    check(int(o["entry_ts_ms"]) == e2 and close(o["entry_px_tape"], 200.9), "R2 вхід на тригері (BUY=max)")
    check(int(o["exit_ts_ms"]) == e2 + H30 and close(o["exit_px_tape"], 203.0), "R2 вихід +30 від входу")
    check(close(o["net_official_pct"], 0.35) and "no_trigger" not in o["flags"], "R2 official = live (гірший)")
    # R2 старий без тригера (SHORT, рівень 299.1 не досягнуто у 600 с; 298.0 на 700 с — поза вікном)
    o = S.settle_rev(rev_row(sig_id="s3", strategy="R2_breakout", date=S._dt(T3), our_side="SHORT",
                             detect_px="300.0", entry_px="299.1", move_3m_pct="1.0", m30="0.4"), ctx)
    check("no_trigger" in o["flags"] and "no_tape" not in o["flags"] and o["entry_src"] == "none",
          "R2 без тригера → no_trigger (без no_tape) %r" % o["flags"])
    check(o["net_tape_pct"] is None and close(o["net_official_pct"], 0.25) and close(o["net_live_pct"], 0.25)
          and o["status"] == "live_only" and o["curve_tape"] == "",
          "R2 no_trigger: official = live (записаний результат не зникає), live_only")
    # R2 v2.16 із записаним входом, стрічка НЕ бачила пробою до входу → лише прапорець, ціни на записаних часах
    o = S.settle_rev(rev_row(sig_id="s3b", strategy="R2_breakout", date=S._dt(T3), our_side="SHORT",
                             entry_px="299.3", entry_ts_ms=str(T3 + 400000), exit_ts_ms=str(T3 + 400000 + H30),
                             exit_px="299.0", exit_reason="timer", exit_min="30"), ctx)
    check("no_trigger" in o["flags"] and int(o["entry_ts_ms"]) == T3 + 400000 and close(o["entry_px_tape"], 299.4)
          and close(o["exit_px_tape"], 298.5) and o["status"] == "verified",
          "R2 записаний вхід без пробою: no_trigger як прапорець, угода оцінена %r" % o["flags"])
    nt = (298.5 / 299.4 - 1) * 100 * -1 - 0.15
    nl = (299.0 / 299.3 - 1) * 100 * -1 - 0.15
    check(close(o["net_tape_pct"], nt, 1e-9) and close(o["net_live_pct"], nl, 1e-9)
          and close(o["net_official_pct"], min(nt, nl), 1e-9), "R2 записаний: official = min")
    # R2 v2.16 із записаним входом після пробою → без прапорця
    o = S.settle_rev(rev_row(sig_id="s2b", strategy="R2_breakout", date=S._dt(T2), our_side="LONG",
                             entry_px="200.8", entry_ts_ms=str(e2 + 1000), exit_ts_ms=str(e2 + 1000 + H30),
                             exit_px="203.1", exit_reason="timer", exit_min="30"), ctx)
    check("no_trigger" not in o["flags"] and int(o["entry_ts_ms"]) == e2 + 1000 and close(o["entry_px_tape"], 200.9)
          and close(o["exit_px_tape"], 203.2) and o["status"] == "verified", "R2 записаний вхід після пробою")
    # R2 v2.16 без date: вікно тригера entry−10 хв (пробій у ньому є)
    o = S.settle_rev(rev_row(sig_id="s2c", strategy="R2_breakout", date="", our_side="LONG",
                             entry_px="200.8", entry_ts_ms=str(e2 + 1000), exit_ts_ms=str(e2 + 1000 + H30),
                             exit_px="203.1", exit_reason="timer"), ctx)
    check(o is not None and "no_trigger" not in o["flags"] and o["status"] == "verified", "R2 без date: вікно від entry−10хв")
    o = S.settle_rev(rev_row(sig_id="s2d", strategy="R2_breakout", date="", our_side="LONG",
                             entry_px="200.5", entry_ts_ms=str(T2 + 10000), exit_ts_ms=str(T2 + 10000 + H30),
                             exit_px="201.0", exit_reason="timer"), ctx)
    check("no_trigger" in o["flags"] and close(o["entry_px_tape"], 200.4) and int(o["entry_ts_ms"]) == T2 + 10000,
          "R2 без date, вхід до пробою: прапорець no_trigger, ціна на записаному вході %r" % o["flags"])
    # R8 старий стиль (без записаного виходу): v2.19 (аудит F) — нога виходу по
    # стрічці = РЕПЛЕЙ правила: TP перетнуто → ринковий вихід після виявлення
    # (+TP_DETECT_MS, гірша ціна 3 с); інакше таймер m30 по стрічці
    o = S.settle_rev(rev_row(sig_id="s4", strategy="R8_тп", date=S._dt(T4), our_side="LONG",
                             detect_px="400.0", entry_px="400.0", move_3m_pct="2.0",
                             dump_move_pct="", m30="2.0"), ctx)
    check(close(o["entry_px_tape"], 400.4) and "tp_tape_hit" in o["flags"] and o["exit_reason_tape"] == "tp_replay"
          and o["exit_src"] == "tape_replay" and T4 < int(o["tp_hit_ms"]) <= T4 + H30
          and int(o["exit_ts_ms"]) == int(o["tp_hit_ms"]) + S.TP_DETECT_MS,
          "R8: реплей TP → вихід після перетину %r %r" % (o["flags"], o.get("exit_ts_ms")))
    check(o.get("net_tp_mkt_pct") is not None and close(o["net_tape_pct"], o["net_tp_mkt_pct"], 1e-9)
          and o.get("net60_tape_pct") is None and close(o["net_live_pct"], 1.85)
          and close(o["net_official_pct"], min(o["net_tape_pct"], 1.85), 1e-9),
          "R8: net по стрічці = ринковий реплей TP (%r), лімітний %r; без net60" % (o.get("net_tp_mkt_pct"), o.get("net_tp_lim_pct")))
    # R8 v2.16: записаний TP-вихід — ціна на exit_ts_ms, tp_px з рядка, tp_tape_hit
    o = S.settle_rev(rev_row(sig_id="s4b", strategy="R8_тп", date=S._dt(T4), our_side="LONG",
                             entry_px="400.0", entry_ts_ms=str(T4), exit_ts_ms=str(T4 + 300000),
                             exit_px="406.4", exit_reason="tp", exit_min="5", tp_px="406.4",
                             dump_move_pct="-2.0"), ctx)
    # v2.19 (аудит F): нога по стрічці — реплей правила (перетин tp_px 406.4 → вихід після виявлення),
    # не ціна на записаному live-часі; official = гірший з live і реплею
    check(o["exit_reason_tape"] == "tp_replay" and o["exit_src"] == "tape_replay" and "tp_tape_hit" in o["flags"]
          and T4 < int(o["tp_hit_ms"]) <= T4 + H30 and int(o["exit_ts_ms"]) == int(o["tp_hit_ms"]) + S.TP_DETECT_MS,
          "R8 TP: реплей по стрічці %r" % o["flags"])
    # v2.20 (аудит v2.19 №6): фікстурна стрічка рідка — після перетину у вікні виявлення трейдів
    # немає (tp_replay_thin) і шлях до перетину не покритий (tp_path_gap) → реплей на неповних
    # даних НЕ «verified», а tape_thin; офіційний net усе одно гірший із двох
    check(close(o["net_live_pct"], 1.45, 1e-9) and close(o["net_tape_pct"], o["net_tp_mkt_pct"], 1e-9)
          and close(o["net_official_pct"], min(1.45, o["net_tape_pct"]), 1e-9) and o["status"] == "tape_thin"
          and "tp_replay_thin" in o["flags"] and "tp_path_gap" in o["flags"],
          "R8 TP: official = min(live, реплей), статус tape_thin (%r, %r)" % (o.get("net_tp_mkt_pct"), o.get("status")))
    # R8 таймер (SHORT, TP 487.5 не досягнуто) → tp_tape_miss
    o = S.settle_rev(rev_row(sig_id="s5", strategy="R8_тп", date=S._dt(T5), our_side="SHORT",
                             entry_px="500.0", entry_ts_ms=str(T5), exit_ts_ms=str(T5 + H30),
                             exit_px="498.2", exit_reason="timer", exit_min="30", tp_px="487.5",
                             dump_move_pct="3.0"), ctx)
    check(close(o["entry_px_tape"], 499.5) and o["exit_reason_tape"] == "m30_replay"
          and int(o["exit_ts_ms"]) == T5 + H30 and close(o["exit_px_tape"], 498.5),
          "R8 таймер: TP не перетнуто → реплей m30 (той самий час), BUY = max")
    n = (498.5 / 499.5 - 1) * 100 * -1 - 0.15
    check("tp_tape_miss" in o["flags"] and close(o["net_tape_pct"], n, 1e-6)
          and close(o["net_official_pct"], n, 1e-6), "R8 таймер: tp_tape_miss, official = tape")
    o = S.settle_rev(rev_row(sig_id="s5b", strategy="R8_тп", date=S._dt(T5), our_side="SHORT",
                             entry_px="500.0", move_3m_pct="", dump_move_pct="", m30="0.1"), ctx)
    check("no_dump" in o["flags"] and "tp_tape_miss" not in o["flags"], "R8 без дампу і tp_px → no_dump")
    # no_price з записаним входом: вихід +30 по стрічці, net_live None → tape_only
    o = S.settle_rev(rev_row(sig_id="s6b", strategy="R1_загальний", date=S._dt(T6), our_side="LONG",
                             entry_px="600.0", entry_ts_ms=str(T6), exit_reason="no_price", m30=""), ctx)
    check(o["net_live_pct"] is None and int(o["exit_ts_ms"]) == T6 + H30 and o["exit_reason_tape"] == "m30"
          and close(o["net_tape_pct"], -0.15) and close(o["net_official_pct"], -0.15)
          and o["status"] == "tape_only" and "partial" in o["flags"], "no_price → tape_only, official = tape")
    # m30 порожній → m31
    o = S.settle_rev(rev_row(sig_id="s6", strategy="R4_великий", date=S._dt(T6), our_side="LONG",
                             detect_px="600.0", entry_px="600.0", move_3m_pct="2.0", m30="", m31="1.2"), ctx)
    check(close(o["net_live_pct"], 1.05) and close(o["net_tape_pct"], -0.15)
          and close(o["net_official_pct"], -0.15), "m30 '' → m31; official = tape")
    check(o["net60_tape_pct"] is None, "net60 без стрічки → None")
    # entered=0 → None; без стрічки на детекті → no_tape, live_only
    check(S.settle_rev(rev_row(sig_id="s7", strategy="R1_загальний", date=S._dt(T1), entered="0",
                               our_side="LONG", m30="1"), ctx) is None, "entered=0 → None")
    o = S.settle_rev(rev_row(sig_id="s8", strategy="R1_загальний", date=S._dt(BASE - 5 * H),
                             our_side="LONG", entry_px="1", m30="1.0"), ctx)
    check("no_tape" in o["flags"] and close(o["net_official_pct"], 0.85) and o["entry_src"] == "none"
          and o["status"] == "live_only" and o["curve_tape"] == "", "без стрічки → no_tape, official = live")
    # через settle_pending: 6 входів + 1 entered=0 (пропущений)
    rows = [rev_row(sig_id="s%d" % k, strategy="R1_загальний", date=S._dt(T1), our_side="LONG",
                    entry_px="100", m30="1") for k in range(1, 7)]
    rows.append(rev_row(sig_id="s9", strategy="R1_загальний", date=S._dt(T1), our_side="LONG",
                        entered="0"))
    write_csv(os.path.join(dd, "rev_trades.csv"), REV_HDR, rows)
    res = S.settle_pending(dd, fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    check(res == (6, 1, 0), "settle_pending rev → (6,1,0) %r" % (res,))
    print("блок 4 (settle_rev: записані часи, R2 no_trigger, R8 tp_tape_*, status, крива): OK")


# ── блок 5: settle_twap ─────────────────────────────────────────────────
TW_HDR = (["twap_id", "strategy", "date_entry", "coin", "our_side", "whale_addr", "entry_px",
           "net60_pct", "costs_pct", "exit_min", "exit_reason", "algo_v", "entry_ts_ms",
           "exit_ts_ms", "exit_px", "entry_src", "exit_src"]
          + ["m%d" % i for i in range(1, 61)] + ["eol"])


def block5():
    dd = fresh("b5")
    t = BASE
    tape = [(t + 100, 100.0), (t + 2000, 100.3), (t + 60 * M + 100, 100.6),
            (t + 5 * M + 100, 100.9), (t + 5 * M + 500, 100.7), (t + 90 * M + 100, 101.0)]
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", sorted(tape))})
    fetchers = {"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    base = {"coin": "SYN", "our_side": "LONG", "whale_addr": "0xabc", "entry_px": "100.0",
            "costs_pct": "0.15", "algo_v": "2.16", "eol": "^", "date_entry": S._dt(t)}
    rows = [dict(base, twap_id="w1", strategy="T1_твап", net60_pct="0.5", exit_min="60",
                 exit_reason="timer_60m", m60="0.65"),
            dict(base, twap_id="w2", strategy="T1_твап", net60_pct="", exit_min="",
                 exit_reason="no_price"),
            dict(base, twap_id="w3", strategy="T2_твап", coin="NOSYM", net60_pct="0.7",
                 exit_min="60", exit_reason="timer_60m"),
            dict(base, twap_id="w4", strategy="T1_твап", net60_pct="0.2", exit_min="5",
                 exit_reason="cancelled", entry_ts_ms=str(t), m5="0.35"),
            dict(base, twap_id="w5", strategy="T1_твап", net60_pct="0.6", exit_min="5",
                 exit_reason="cancelled", entry_ts_ms=str(t), exit_ts_ms=str(t + 5 * M + 400),
                 exit_px="100.75", m5="0.35")]
    write_csv(os.path.join(dd, "twap_trades.csv"), TW_HDR, rows)
    res = S.settle_pending(dd, fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    check(res == (4, 1, 0), "settle_pending twap → (4,1,0) %r" % (res,))
    st = S.load_settlements(dd)
    a = st["w1|T1_твап"]
    g = (100.6 / 100.3 - 1) * 100
    check(a["family"] == "twap" and close(a["entry_px_tape"], 100.3) and close(a["exit_px_tape"], 100.6),
          "TWAP вхід/вихід за стрічкою")
    check(int(a["exit_ts_ms"]) == t + 60 * M and a["exit_reason_tape"] == "timer_60m", "без exit_ts_ms: вихід = entry + 60 хв")
    check(close(a["net_tape_pct"], g - 0.15, 1e-6) and close(a["net_official_pct"], g - 0.15, 1e-6)
          and close(a["net_live_pct"], 0.5) and a["status"] == "verified", "TWAP official = tape")
    check(close(a["exit_px_live"], 100.65, 1e-9), "exit_px_live з m60")
    cv = a["curve_tape"].split(";")
    check(len(cv) == 120 and cv[59] == "%.4f" % (g - 0.15) and cv[4] == "%.4f" % ((100.7 / 100.3 - 1) * 100 - 0.15)
          and cv[89] == "%.4f" % ((101.0 / 100.3 - 1) * 100 - 0.15) and cv[119] == "",
          "TWAP крива 120 хв (m5, m60, m90)")
    check("w2|T1_твап" not in st, "no_price пропущено")
    c = st["w3|T2_твап"]
    check(c["symbol"] == "NOSYMUSDT" and "no_tape" in c["flags"] and c["entry_src"] == "none"
          and close(c["net_official_pct"], 0.7) and c["net_tape_pct"] == "" and c["status"] == "live_only",
          "без символу → no_tape, official=live")
    d = st["w4|T1_твап"]
    check(d["ts_src"] == "ms" and int(d["exit_ts_ms"]) == t + 5 * M
          and close(d["exit_px_tape"], 100.7) and close(d["exit_px_live"], 100.35, 1e-9),
          "cancelled без exit_ts_ms: вихід на exit_min (SELL = min), exit_px_live з m5")
    e = st["w5|T1_твап"]
    check(int(e["exit_ts_ms"]) == t + 5 * M + 400 and close(e["exit_px_tape"], 100.7)
          and close(e["exit_px_live"], 100.75) and e["exit_reason_tape"] == "cancelled",
          "записаний exit_ts_ms/exit_px мають пріоритет")
    # символи
    check(S.symbol_for("HYPE") == "HYPEUSDT" and S.symbol_for("kPEPE") == "1000PEPEUSDT"
          and S.symbol_for("KDOGS") == "1000DOGSUSDT" and S.symbol_for("kBONK") == "1000BONKUSDT",
          "symbol_for: kXXX → 1000XXXUSDT")
    check(S.symbol_for("KLUNC", {"KLUNC": "1000LUNCUSDT"}) == "1000LUNCUSDT"
          and S.symbol_for("kLUNC", {"KLUNC": "1000LUNCUSDT"}) == "1000LUNCUSDT", "symbol_map override")
    print("блок 5 (settle_twap: записаний вихід, крива 120, no_price, no_tape, символи): OK")


# ── блок 6: філи кита ───────────────────────────────────────────────────
def mk_fill(t, px, side, d, coin="SYN", sp=None, sz="1"):
    # hash — від часу: кожен філ = окремий ордер (найбільша ТРАНЗАКЦІЯ епізоду рахується по hash)
    f = {"coin": coin, "px": str(px), "sz": str(sz), "side": side, "time": t, "dir": d,
         "tid": t, "hash": "0x%x" % int(t), "crossed": True}
    if sp is not None:
        f["startPosition"] = str(sp)
    return f


def block6():
    dd = fresh("b6")
    t0 = BASE
    fills = [mk_fill(t0 - 560000, 100.0, "A", "Close Long"),
             mk_fill(t0 - 250000, 99.0, "A", "Close Long"),
             mk_fill(t0 - 200000, 98.5, "A", "Close Long"),
             mk_fill(t0 - 100000, 98.0, "A", "Close Long"),
             mk_fill(t0 - 30000, 97.0, "A", "Close Long"),
             mk_fill(t0 - 20000, 5.0, "B", "Open Long", coin="OTHER"),
             mk_fill(t0 + 5000, 96.0, "A", "Close Long")]
    calls = []

    def fake_hl(body):
        calls.append(body)
        check(body["type"] == "userFillsByTime" and "startTime" in body, "тіло запиту HL")
        if body["user"] != "0xabc":
            return []
        return [f for f in fills if body["startTime"] <= f["time"] <= body["endTime"]]

    hl = S.HLFills(dd, fetch_hl=fake_hl, now_ms=NOW_OLD, pace_s=0)
    got = hl.user_fills("0xabc", t0 - 600000, t0)
    check([f["time"] for f in got] == [f["time"] for f in fills[:6]], "user_fills: вікно [t0−600с, t0]")
    check(len(calls) == 2, "дві години → два запити (%d)" % len(calls))
    ep = S.whale_episode(got, "SYN", t0, close_only=True)
    check(ep["whale_fill_ts_ms"] == t0 - 30000 and close(ep["lag_s"], 30.0) and close(ep["whale_px"], 97.0),
          "останній філ перед t0 → lag 30 с")
    check(ep["dump_first_ts_ms"] == t0 - 250000 and ep["dump_last_ts_ms"] == t0 - 30000
          and close(ep["dump_dur_s"], 220.0), "епізод: паузи ≤300 с (310 с розриває)")
    check(close(ep["dump_move_pct"], (97.0 / 99.0 - 1) * 100 * -1, 1e-9) and ep["dump_move_pct"] > 0,
          "dump_move: кит продає, ціна впала → додатний (сирі ціни філів)")
    check(ep["dump_bucket"] == 4, "bucket = ceil(220/60) = 4")
    first, last, inep = S.close_episode(got, "SYN", t0, True)
    check(first["time"] == t0 - 250000 and last["time"] == t0 - 30000 and len(inep) == 4
          and [f["time"] for f in inep] == [t0 - 250000, t0 - 200000, t0 - 100000, t0 - 30000],
          "close_episode → (перший, останній, філи епізоду)")
    check(S.close_episode(got, "XYZ", t0, True) == (None, None, []), "close_episode без філів → (None, None, [])")
    # пауза рівно 300 с включається; 300 001 мс — ні
    e = S.whale_episode([mk_fill(t0 - 330000, 10.0, "A", "Close Long"), mk_fill(t0 - 30000, 9.0, "A", "Close Long")],
                        "SYN", t0, True)
    check(e["dump_first_ts_ms"] == t0 - 330000 and e["dump_bucket"] == 0, "пауза рівно EP_GAP_MS включається")
    e = S.whale_episode([mk_fill(t0 - 330001, 10.0, "A", "Close Long"), mk_fill(t0 - 30000, 9.0, "A", "Close Long")],
                        "SYN", t0, True)
    check(e["dump_first_ts_ms"] == t0 - 30000 and e["dump_bucket"] == 1, "пауза > EP_GAP_MS розриває")
    check(S.EP_GAP_MS == 300000, "EP_GAP_MS = 300 000 (FC_MAX_EPISODE_S бота)")
    # startPosition: ланцюг залишків тримає епізод; стрибок позиції (долив/реопен) розриває
    chain = [mk_fill(t0 - 200000, 100.0, "A", "Close Long", sp=100, sz=10),
             mk_fill(t0 - 100000, 99.0, "A", "Close Long", sp=90, sz=10),
             mk_fill(t0 - 30000, 98.0, "A", "Close Long", sp=80, sz=10)]
    e = S.whale_episode(chain, "SYN", t0, True)
    check(e["dump_first_ts_ms"] == t0 - 200000 and close(e["dump_dur_s"], 170.0), "ланцюг startPosition: один епізод")
    jump = chain[:2] + [mk_fill(t0 - 30000, 98.0, "A", "Close Long", sp=120, sz=10)]
    e = S.whale_episode(jump, "SYN", t0, True)
    check(e["dump_first_ts_ms"] == t0 - 30000 and e["dump_dur_s"] == 0 and e["dump_bucket"] == 1,
          "позиція виросла між філами (80 → 120) → епізод лише з останнього філа")
    tol_ok = [mk_fill(t0 - 100000, 100.0, "A", "Close Long", sp=100, sz=10),
              mk_fill(t0 - 30000, 99.0, "A", "Close Long", sp=90.5, sz=10)]
    check(S.whale_episode(tol_ok, "SYN", t0, True)["dump_first_ts_ms"] == t0 - 100000, "допуск 1%: 90.5 проти 90 — ок")
    tol_no = [tol_ok[0], mk_fill(t0 - 30000, 99.0, "A", "Close Long", sp=91.5, sz=10)]
    check(S.whale_episode(tol_no, "SYN", t0, True)["dump_first_ts_ms"] == t0 - 30000, "1.5 > 1% → розрив")
    mixed = [mk_fill(t0 - 100000, 100.0, "A", "Close Long", sp=100, sz=10),
             mk_fill(t0 - 30000, 99.0, "A", "Close Long")]
    check(S.whale_episode(mixed, "SYN", t0, True)["dump_first_ts_ms"] == t0 - 100000,
          "без startPosition в одного з філів — лише правило паузи")
    # ліквідація / ADL — close-філи; не-close посеред серії розриває run при close_only
    liq = [mk_fill(t0 - 100000, 100.0, "A", "Close Long"), mk_fill(t0 - 30000, 95.0, "A", "Liquidated Isolated Long")]
    e = S.whale_episode(liq, "SYN", t0, True)
    check(e["whale_fill_ts_ms"] == t0 - 30000 and e["dump_first_ts_ms"] == t0 - 100000, "ліквідація = close-філ")
    adl = [mk_fill(t0 - 30000, 95.0, "A", "Auto-Deleveraging")]
    check(S.whale_episode(adl, "SYN", t0, True)["whale_fill_ts_ms"] == t0 - 30000, "ADL = close-філ")
    run = [mk_fill(t0 - 100000, 100.0, "A", "Close Long"), mk_fill(t0 - 60000, 99.0, "A", "Open Short"),
           mk_fill(t0 - 30000, 98.0, "A", "Close Long")]
    check(S.whale_episode(run, "SYN", t0, True)["dump_first_ts_ms"] == t0 - 30000,
          "close_only: не-close філ того ж боку розриває run")
    check(S.whale_episode(run, "SYN", t0, False)["dump_first_ts_ms"] == t0 - 100000,
          "close_only=False: будь-який філ того ж боку")
    # кеш на диску, повторний виклик без мережі
    files = os.listdir(os.path.join(dd, "tape", "hl"))
    check(len(files) == 2, "дві години у кеші hl/ (%r)" % files)
    hl2 = S.HLFills(dd, fetch_hl=fake_hl, now_ms=NOW_OLD, pace_s=0)
    n = len(calls)
    check(hl2.user_fills("0xabc", t0 - 600000, t0) == got and len(calls) == n, "з диска без мережі")
    # неповна година (now всередині) не персиститься
    hl3 = S.HLFills(fresh("b6b"), fetch_hl=fake_hl, now_ms=t0 + 120000, pace_s=0)
    hl3.user_fills("0xabc", t0 - 600000, t0)
    check(len(os.listdir(os.path.join(TD, "b6b", "tape", "hl"))) == 1, "поточна година не на диску")
    # збій → None
    check(S.HLFills(fresh("b6c"), fetch_hl=NO_HL, pace_s=0).user_fills("0xabc", t0 - 1, t0) is None,
          "збій HL → None")
    # close_only: rev бере лише Close, follow — будь-який
    f2 = got + [mk_fill(t0 - 10000, 96.5, "A", "Open Short")]
    check(S.whale_episode(f2, "SYN", t0, True)["whale_fill_ts_ms"] == t0 - 30000, "rev: лише Close")
    e = S.whale_episode(f2, "SYN", t0, False)
    check(e["whale_fill_ts_ms"] == t0 - 10000 and close(e["lag_s"], 10.0)
          and e["dump_first_ts_ms"] == t0 - 250000, "follow: будь-який філ, епізод по боку")
    # bucket: dur ≥300 → 0; один філ → 1; 61 с → 2; кит купує, ціна росте → додатний
    long_ep = [mk_fill(t0 - 590000 + k * 100000, 100.0 + k, "B", "Close Short") for k in range(6)]
    e = S.whale_episode(long_ep, "SYN", t0, True)
    check(close(e["dump_dur_s"], 500.0) and e["dump_bucket"] == 0, "dur ≥ 300 с → bucket 0")
    check(close(e["dump_move_pct"], 5.0, 1e-9), "кит купує, ціна росте → додатний")
    e = S.whale_episode([mk_fill(t0 - 1000, 10.0, "B", "Close Short")], "SYN", t0, True)
    check(e["dump_dur_s"] == 0 and e["dump_bucket"] == 1 and e["dump_move_pct"] == 0, "один філ → bucket 1")
    e = S.whale_episode([mk_fill(t0 - 62000, 10.0, "B", "Close Short"),
                         mk_fill(t0 - 1000, 10.0, "B", "Close Short")], "SYN", t0, True)
    check(e["dump_bucket"] == 2, "61 с → bucket 2")
    check(S.whale_episode(got, "SYN", t0 - 700000, True) == {}, "без філів у вікні → {}")
    check(S.whale_episode(got, "XYZ", t0, True) == {}, "інша монета → {}")
    # інтеграція: settle_follow з філами (стрічки біля філів немає → dump_fills)
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    ctx = S.make_ctx(dd, fetchers={"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": fake_hl,
                                   "hl_pace_s": 0}, now_ms=NOW_OLD, log=lambda *a: None)
    row = {"date_open": S._dt(t0), "date_close": S._dt(t0 + 120000), "strategy": "F1", "coin": "SYN",
           "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
           "costs_pct": "0.15", "net_pct": "0.85", "trade_id": "tf",
           "fill_ts_ms": str(t0 - 30000)}   # v2.20 (аудит №3): рядок несе час тригерного філа — звірка САМЕ з ним
    o = S.settle_follow(row, ctx)
    check(o.get("trig_match") == "ts", "v2.20: тригер знайдено за записаним часом (%r)" % o.get("trig_match"))
    check(close(o["lag_s"], 30.0) and o["dump_bucket"] == 4 and "no_fills" not in o["flags"]
          and "dump_fills" in o["flags"], "settle_follow несе lag/dump (фолбек по філах)")
    o = S.settle_follow(dict(row, whale_addr="0xnew"), ctx)
    check("no_whale_fill" in o["flags"] and o.get("lag_s") is None, "філів нема → no_whale_fill")
    # рух по стрічці: референс = останній трейд СТРОГО до першого філа, кінець ≤ останнього філа
    tp = [(t0 - 250500, 60.0), (t0 - 250000, 50.0), (t0 - 30000, 40.0), (t0 - 29000, 30.0)]
    fz2 = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", tp)})
    ctx2 = S.make_ctx(fresh("b6d"), fetchers={"fetch_zip": fz2, "fetch_rest": NO_REST, "fetch_hl": fake_hl,
                                              "hl_pace_s": 0}, now_ms=NOW_OLD, log=lambda *a: None)
    out = {"flags": []}
    S._whale(out, {"whale_addr": "0xabc", "coin": "SYN"}, t0, ctx2, close_only=True, symbol="SYNUSDT")
    check("dump_tape" in out["flags"] and close(out["dump_move_pct"], (40.0 / 60.0 - 1) * 100 * -1, 1e-9),
          "dump_tape: ref 60 (не 50 на мс філа), кінець 40 (не 30 після), знак у бік тиску %r" % out["dump_move_pct"])
    print("блок 6 (філи кита: епізод 300 с + startPosition, close-філи, bucket, референс строго до філа): OK")


# ── блок 7: ідемпотентність, force, legacy, відкладання, дозрівання, ротація, CLI ──
def block7():
    dd = os.path.join(TD, "b3")
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    fetchers = {"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    kw = dict(fetchers=fetchers, now_ms=NOW_OLD, log=lambda *a: None)
    res = S.settle_pending(dd, **kw)
    check(res == (0, 0, 0), "повторний прогін нічого не робить %r" % (res,))
    n_lines = sum(1 for _ in open(os.path.join(dd, "settlements.csv"), encoding="utf-8"))
    res = S.settle_pending(dd, force=True, **kw)
    check(res == (2, 0, 0), "--force перераховує все %r" % (res,))
    n2 = sum(1 for _ in open(os.path.join(dd, "settlements.csv"), encoding="utf-8"))
    check(n2 == n_lines + 2 and len(S.load_settlements(dd)) == 2, "force дописує, ключі ті самі")
    # legacy-файл читається
    t3 = BASE + 600000 + 123
    write_csv(os.path.join(dd, "follow_trades.csv.legacy-1700000000.csv"),
              FOL_HDR[:15] + ["eol"],
              [{"date_open": S._dt(BASE), "date_close": S._dt(BASE + 120000), "strategy": "F2",
                "coin": "SYN", "our_side": "LONG", "whale_addr": "0xabc", "entry_px": "100",
                "exit_px": "99.5", "costs_pct": "0.15", "net_pct": "-0.65", "algo_v": "2.10",
                "trade_id": "t_legacy", "eol": "^"},
               {"date_open": S._dt(t3), "date_close": S._dt(BASE), "strategy": "F2", "coin": "SYN",
                "our_side": "LONG", "trade_id": "t_cut", "eol": "обірв"}])
    res = S.settle_pending(dd, **kw)
    st = S.load_settlements(dd)
    check(res == (1, 0, 0) and "t_legacy" in st and "t_cut" not in st,
          "legacy прочитано, обірваний рядок без вартового відкинуто %r" % (res,))
    # LONG: вхід BUY=max(100, 99.8)=100, вихід SELL=min(99, 99.4)=99 → tape −1.15 гірший за live −0.65
    check(close(st["t_legacy"]["net_official_pct"], -1.15, 1e-9) and close(st["t_legacy"]["net_live_pct"], -0.65)
          and st["t_legacy"]["family"] == "fol", "legacy LONG: official = tape −1.15")
    # limit і since
    os.remove(os.path.join(dd, "settlements.csv"))
    check(S.settle_pending(dd, limit=1, **kw) == (1, 0, 0), "limit=1")
    check(S.settle_pending(dd, since="2099-01-01", **kw) == (0, 0, 0), "since у майбутньому → 0")
    check(S.settle_pending(dd, **kw) == (2, 0, 0), "решта дораховується")
    # битий рядок не валить прогін
    write_csv(os.path.join(dd, "twap_trades.csv"), TW_HDR,
              [{"twap_id": "bad", "strategy": "T1", "date_entry": S._dt(BASE), "coin": "SYN",
                "our_side": "LONG", "entry_px": "100", "exit_min": "x60", "exit_reason": "timer_60m",
                "eol": "^"},
               {"twap_id": "ok", "strategy": "T1", "date_entry": S._dt(BASE), "coin": "SYN",
                "our_side": "LONG", "entry_px": "100", "net60_pct": "0.3", "exit_min": "60",
                "exit_reason": "timer_60m", "eol": "^"}])
    res = S.settle_pending(dd, **kw)
    check(res == (1, 1, 0) and "ok|T1" in S.load_settlements(dd), "битий exit_min → пропуск, решта йде")
    # ── відкладання без стрічки: молодше за 72 год — не фіксується, рахується як збій і спроба
    logs = []
    dd2 = fresh("b7d")
    t_new = BASE
    write_csv(os.path.join(dd2, "follow_trades.csv"), FOL_HDR,
              [{"date_open": S._dt(t_new), "date_close": S._dt(t_new + 60000), "strategy": "F1", "coin": "NOPE",
                "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
                "costs_pct": "0.15", "net_pct": "0.85", "algo_v": "2.16", "trade_id": "n1",
                "open_ts_ms": str(t_new), "close_ts_ms": str(t_new + 60000), "eol": "^"},
               {"date_open": S._dt(t_new), "date_close": S._dt(t_new + 3 * H), "strategy": "F1", "coin": "SYN",
                "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
                "costs_pct": "0.15", "net_pct": "0.85", "algo_v": "2.16", "trade_id": "p1",
                "open_ts_ms": str(t_new), "close_ts_ms": str(t_new + 3 * H), "eol": "^"}])
    kw2 = dict(fetchers=fetchers, log=logs.append)
    S.settle_pending.retry.clear()
    res = S.settle_pending(dd2, now_ms=t_new + 71 * H, **kw2)
    check(res == (0, 0, 2) and not os.path.exists(os.path.join(dd2, "settlements.csv")),
          "no_tape / вихід без стрічки молодше 72 год → відкладено (0,0,2) %r" % (res,))
    check(sum("стрічка ще недоступна — відкладено" in m for m in logs) == 2, "лог відкладання")
    # (E) відкладені — у бекофі 10 хв: у тому ж «зараз» пропускаються без витрати спроби
    check(S.settle_pending(dd2, now_ms=t_new + 71 * H, limit=1, **kw2) == (0, 0, 0)
          and S.settle_pending.last_waiting == 2 and S.settle_pending.last_pending == 2,
          "відкладені у бекофі не повторюються і не витрачають limit")
    check(S.settle_pending(dd2, now_ms=t_new + 71 * H + 11 * M, limit=1, **kw2) == (0, 0, 1),
          "після бекофу — знову спроба, відкладена рахується у limit")
    res = S.settle_pending(dd2, now_ms=t_new + 73 * H, **kw2)
    st2 = S.load_settlements(dd2)
    check(res == (2, 0, 0) and "no_tape" in st2["n1"]["flags"] and st2["n1"]["status"] == "live_only"
          and "partial" in st2["p1"]["flags"] and st2["p1"]["exit_src"] == "none",
          "старше 72 год без стрічки → фіксується назавжди %r" % (res,))
    check(not S.settle_pending.retry, "успішний запис знімає ключі з бекофу")
    check(S.settle_pending(dd2, now_ms=t_new + 73 * H, **kw2) == (0, 0, 0), "зафіксоване не перераховується")
    # ── дозрівання кривої: розрахунок до entry+60хв — тимчасовий, перерахунок після горизонту
    dd3 = fresh("b7c")
    write_csv(os.path.join(dd3, "follow_trades.csv"), FOL_HDR,
              [{"date_open": S._dt(BASE), "date_close": S._dt(BASE + 120000), "strategy": "F1", "coin": "SYN",
                "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
                "costs_pct": "0.15", "net_pct": "0.85", "algo_v": "2.16", "trade_id": "c1",
                "open_ts_ms": str(BASE), "close_ts_ms": str(BASE + 120000), "eol": "^"}])
    kw3 = dict(fetchers=fetchers, log=lambda *a: None)
    check(S.settle_pending(dd3, now_ms=BASE + 35 * M, **kw3) == (1, 0, 0), "свіжа угода рахується одразу")
    s = S.load_settlements(dd3)["c1"]
    check(s["status"] == "verified" and not S._settle_final(s), "результат є, але крива ще не дозріла")
    check(S.settle_pending(dd3, now_ms=BASE + 40 * M, **kw3) == (0, 0, 0), "до горизонту — не перераховується")
    check(S.settle_pending(dd3, now_ms=BASE + 60 * M + 20000, **kw3) == (0, 0, 0), "запас 30 с ще не минув")
    check(S.settle_pending(dd3, now_ms=BASE + 60 * M + 31000, **kw3) == (1, 0, 0), "горизонт минув → перерахунок")
    s = S.load_settlements(dd3)["c1"]
    # (D) крива є (60 точок), але заповнено лише 3/60 (m2/m10/m13 — рідка стрічка) → НЕ остаточний
    # до 72 год; повторний перерахунок з подвоюваними паузами (перша = settled − entry ≈ 61 хв)
    check(len(curve({"curve_tape": s["curve_tape"]})) == 60 and int(s["curve_n"]) == 3
          and not S._curve_complete(s) and not S._settle_final(s, now_ms=BASE + 61 * M),
          "після перерахунку крива неповна (3/60) → ще не остаточний")
    # v2.20 (аудит v2.19 №6): «остаточний за віком» лише після ПЕРЕРАХУНКУ, зробленого вже після 72 год —
    # рядок, порахований на 61-й хвилині, через 72 год не фінальний сам по собі, а перерахунок обов'язковий
    check(not S._settle_final(s, now_ms=BASE + 73 * H) and S._resettle_due(s, BASE + 73 * H),
          "…через 72 год — НЕ остаточний без останнього перерахунку; перерахунок належний")
    check(S._settle_final(dict(s, settled_at=S._dt(BASE + 73 * H)), now_ms=BASE + 73 * H),
          "…а порахований після 72 год — остаточний за віком")
    check(S.settle_pending(dd3, now_ms=BASE + 100 * M, **kw3) == (0, 0, 0), "пауза 61 хв ще не минула — пропуск")
    check(S.settle_pending(dd3, now_ms=BASE + 5 * H, **kw3) == (1, 0, 0), "пауза минула → перерахунок")
    # v2.20: після 72 год — ОСТАННІЙ перерахунок (рядок рахувався на 5-й годині), далі остаточний
    check(S.settle_pending(dd3, now_ms=BASE + 73 * H, **kw3) == (1, 0, 0), "72 год → останній перерахунок після горизонту стрічки")
    check(S.settle_pending(dd3, now_ms=BASE + 74 * H, **kw3) == (0, 0, 0), "…і далі остаточний, не перераховується")
    n_rows = sum(1 for r in csv.DictReader(open(os.path.join(dd3, "settlements.csv"), encoding="utf-8")))
    check(n_rows == 4, "чотири записи ключа (v2.20: + останній перерахунок після 72 год), останній виграє")
    # ── ротація чужого заголовка + перерахунок рядків старої версії
    dd4 = fresh("b7e")
    shutil.copy(os.path.join(dd, "follow_trades.csv"), os.path.join(dd4, "follow_trades.csv"))
    old_hdr = [h for h in S.HEADERS if h not in ("curve_tape", "status")]
    write_csv(os.path.join(dd4, "settlements.csv"), old_hdr,
              [{"key": "t1", "family": "fol", "settle_v": "1", "settled_at": S._dt(NOW_OLD),
                "entry_ts_ms": str(BASE), "net_official_pct": "9.9", "flags": "", "eol": "^"}])
    res = S.settle_pending(dd4, **kw)
    leg = [f for f in os.listdir(dd4) if f.startswith("settlements.csv.legacy-")]
    check(res == (2, 0, 0) and len(leg) == 1, "старий заголовок → ротація у legacy, v1-рядок перерахований %r" % (res,))
    with open(os.path.join(dd4, "settlements.csv"), encoding="utf-8") as f:
        check(f.readline().strip() == ",".join(S.HEADERS), "новий файл із новим заголовком")
    st4 = S.load_settlements(dd4)
    check(st4["t1"]["settle_v"] == S.SETTLE_V and not close(st4["t1"]["net_official_pct"], 9.9) and "t2" in st4,
          "новий запис t1 (v2) виграє над legacy v1")
    check(S.settle_pending(dd4, **kw) == (0, 0, 0), "після ротації — ідемпотентно")
    # CLI
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = S.main(["--data-dir", dd, "--since", "2099-01-01", "--limit", "5",
                     "--symbol-map", '{"KLUNC":"1000LUNCUSDT"}'])
    check(rc == 0 and "settled=0 skipped=0 failed=0" in buf.getvalue(), "CLI: %r" % buf.getvalue())
    # заголовки: curve_tape перед flags, status після flags, eol останній
    hd = S.HEADERS
    check(hd.index("curve_tape") == hd.index("flags") - 1 and hd.index("status") == hd.index("flags") + 1
          and hd[-1] == "eol" and hd.index("net_official_pct") > 0, "порядок HEADERS")
    check(hd.index("costs_live_pct") == hd.index("costs_pct") + 1
          and hd[hd.index("curve_tape") - 4:hd.index("curve_tape")]
          == ["whale_pos_start", "whale_pos_after", "whale_max_fill_pct", "whale_max_fill_usd"]
          and "curve_n" in hd, "нові колонки: costs_live_pct після costs_pct, whale_* перед curve_tape, curve_n")
    print("блок 7 (ідемпотентність, force, legacy, 72 год, дозрівання кривої, ротація, CLI): OK")


# ── блок 8: символ, якого немає на Binance (REST 400 Invalid symbol) ──────────
def block8():
    dd = fresh("b8")
    t_new = BASE
    write_csv(os.path.join(dd, "follow_trades.csv"), FOL_HDR,
              [{"date_open": S._dt(t_new), "date_close": S._dt(t_new + 60000), "strategy": "F1", "coin": "NOPE",
                "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
                "costs_pct": "0.15", "net_pct": "0.85", "algo_v": "2.16", "trade_id": "u1",
                "open_ts_ms": str(t_new), "close_ts_ms": str(t_new + 60000), "eol": "^"}])
    calls = []
    def rest400(url):
        calls.append(url)
        raise S.Transient("HTTP 400 Invalid symbol", 400)
    logs = []
    fetchers = {"fetch_zip": lambda u: None, "fetch_rest": rest400, "fetch_hl": NO_HL,
                "hl_pace_s": 0, "rest_pace_s": 0}
    now = t_new + 2 * H                      # молодше за 72 год, REST-вікно (<48 год)
    res = S.settle_pending(dd, fetchers=fetchers, now_ms=now, log=logs.append)
    st = S.load_settlements(dd)
    check(res == (1, 0, 0) and "u1" in st, "400 → не відкладається, фіксується одразу %r" % (res,))
    fl = st["u1"]["flags"]
    check("no_tape" in fl and "unlisted" in fl and st["u1"]["status"] == "live_only"
          and close(st["u1"]["net_official_pct"], 0.85), "прапорці no_tape+unlisted, live_only, official = live")
    check(len(calls) == 1, "один REST-запит на символ за прогін (%d)" % len(calls))
    check(os.path.exists(os.path.join(dd, "tape", "unlisted.json")), "unlisted.json записано")
    check(any("не торгується" in m for m in logs), "лог про unlisted")
    # новий прогін (новий Tape) — жодного запиту: unlisted.json прочитано з диска
    calls.clear()
    res = S.settle_pending(dd, fetchers=fetchers, now_ms=now + H, force=True, log=lambda *a: None)
    check(res == (1, 0, 0) and len(calls) == 0, "наступний прогін без запитів до Binance (%d)" % len(calls))
    # TTL: через тиждень символ перепитується (лістинг міг з'явитись)
    tp = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=rest400, now_ms=now + 8 * DAY, rest_pace_s=0)
    check(not tp.is_unlisted("NOPEUSDT"), "TTL 7 днів минув → знову невідомий")
    tp2 = S.Tape(dd, fetch_zip=lambda u: None, fetch_rest=rest400, now_ms=now + 6 * DAY, rest_pace_s=0)
    check(tp2.is_unlisted("NOPEUSDT") and tp2.window("NOPEUSDT", now, now + 1000) == (S.array("q"), S.array("d")),
          "до TTL — порожня стрічка без запитів")
    # інший код помилки (429/5xx/мережа) — як раніше: Transient нагору, рядок відкладено
    def rest429(url): raise S.Transient("HTTP 429", 429)
    dd2 = fresh("b8b")
    shutil.copy(os.path.join(dd, "follow_trades.csv"), os.path.join(dd2, "follow_trades.csv"))
    res = S.settle_pending(dd2, fetchers=dict(fetchers, fetch_rest=rest429), now_ms=now, log=lambda *a: None)
    check(res == (0, 0, 1) and not os.path.exists(os.path.join(dd2, "settlements.csv")),
          "429 → Transient, рядок не фіксується %r" % (res,))
    check(not os.path.exists(os.path.join(dd2, "tape", "unlisted.json")), "429 не позначає unlisted")
    print("блок 8 (unlisted: 400 → no_tape+unlisted одразу, кеш на диску, TTL, 429 як раніше): OK")


# ── блок 9: аудит — фліп (A), факти епізоду (B), покриття стрічки (C), повнота
#    кривої (D), бекоф + справедлива черга (E), v1-кеш REST (F), витрати за
#    ногами (G), TWAP no_price (H) ──────────────────────────────────────────
def block9():
    S.settle_pending.retry.clear()
    S._ZIP_MISS.clear()
    t0 = BASE
    quiet = lambda *a: None                                        # noqa: E731
    # ── A: фліп позиції = close-філ
    check(S._is_close({"dir": "Long > Short"}) and S._is_close({"dir": "Short > Long"})
          and S._is_close({"dir": "Close Long"}) and S._is_close({"dir": "Liquidated Isolated Long"})
          and not S._is_close({"dir": "Open Long"}) and not S._is_close({"dir": "Open Short"}),
          "_is_close: фліп «Long > Short» / «Short > Long» — закриття")
    flip = [mk_fill(t0 - 100000, 100.0, "A", "Close Long", sp=100, sz=10),
            mk_fill(t0 - 30000, 99.0, "A", "Long > Short", sp=90, sz=150)]
    first, last, inep = S.close_episode(flip, "SYN", t0, True)
    check(first is not None and last["dir"] == "Long > Short" and len(inep) == 2
          and first["time"] == t0 - 100000, "close_only: епізод закінчується фліпом і тягне попередній close")
    e = S.whale_episode(flip[1:], "SYN", t0, True)
    check(e and e["whale_fill_ts_ms"] == t0 - 30000 and e["whale_pos_after"] == 0
          and S._close_flag(e) == "full_close", "лише фліп → епізод є, залишок 0 (кламп) → full_close")
    check(S.whale_episode([mk_fill(t0 - 30000, 99.0, "A", "Open Short")], "SYN", t0, True) == {},
          "Open — не close-філ (без « > »)")
    # ── B: факти епізоду (приклад волту: 1000 од., продає 50@120 і 50@118 за 60 с)
    vault = [mk_fill(t0 - 60000, 120.0, "A", "Close Long", sp=1000, sz=50),
             mk_fill(t0 - 1000, 118.0, "A", "Close Long", sp=950, sz=50)]
    e = S.whale_episode(vault, "SYN", t0, True)
    check(close(e["whale_pos_start"], 1000) and close(e["whale_pos_after"], 900)
          and close(e["whale_max_fill_pct"], 0.05) and close(e["whale_max_fill_usd"], 6000.0)
          and e["dump_first_ts_ms"] == t0 - 60000, "волт: start 1000, after 900, max fill 5%% = $6000 (%r)" % e)
    check(S._close_flag(e) == "partial_close", "900 > 1% від 1000 → partial_close")
    full = [mk_fill(t0 - 100000, 100.0, "A", "Close Long", sp=100, sz=60),
            mk_fill(t0 - 30000, 99.0, "A", "Close Long", sp=40, sz=39.5)]
    e = S.whale_episode(full, "SYN", t0, True)
    check(close(e["whale_pos_after"], 0.5) and close(e["whale_max_fill_pct"], 0.6)
          and close(e["whale_max_fill_usd"], 6000.0) and S._close_flag(e) == "full_close",
          "залишок 0.5 ≤ 1% від 100 → full_close; найбільший філ 60 (перший)")
    e = S.whale_episode([full[0], mk_fill(t0 - 30000, 99.0, "A", "Close Long", sp=40, sz=38)], "SYN", t0, True)
    check(close(e["whale_pos_after"], 2.0) and S._close_flag(e) == "partial_close", "залишок 2 > 1 → partial_close")
    unk = [mk_fill(t0 - 30000, 99.0, "A", "Close Long", sz=3)]
    e = S.whale_episode(unk, "SYN", t0, True)
    check(e["whale_pos_start"] is None and e["whale_pos_after"] is None and e["whale_max_fill_pct"] is None
          and close(e["whale_max_fill_usd"], 297.0) and S._close_flag(e) == "close_unknown",
          "без startPosition → після/старт/частка None, usd є, close_unknown")
    e = S.whale_episode([mk_fill(t0 - 30000, 99.0, "A", "Close Long", sp=0, sz=0)], "SYN", t0, True)
    check(e["whale_max_fill_pct"] is None and e["whale_pos_after"] == 0 and S._close_flag(e) == "full_close",
          "start 0 → частка None (без ділення на нуль)")
    hl_fills = {"0xvault": vault, "0xfull": full, "0xunk": unk}

    def fake_hl(body):
        fs = hl_fills.get(body["user"], [])
        return [f for f in fs if body["startTime"] <= f["time"] <= body["endTime"]]

    dd = fresh("b9b")
    fz = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    fetch_b = {"fetch_zip": fz, "fetch_rest": NO_REST, "fetch_hl": fake_hl, "hl_pace_s": 0}
    ctx = S.make_ctx(dd, fetchers=fetch_b, now_ms=NOW_OLD, log=quiet)
    base_row = {"date_open": S._dt(t0), "date_close": S._dt(t0 + 120000), "strategy": "F1", "coin": "SYN",
                "our_side": "SHORT", "entry_px": "100", "exit_px": "99", "costs_pct": "0.15",
                "net_pct": "0.85", "algo_v": "2.16", "open_ts_ms": str(t0), "close_ts_ms": str(t0 + 120000),
                "eol": "^"}
    o = S.settle_follow(dict(base_row, trade_id="bv", whale_addr="0xvault", fill_ts_ms=str(t0 - 1000)), ctx)   # v2.20: рядок несе час тригера
    check("partial_close" in o["flags"] and close(o["whale_pos_after"], 900) and close(o["whale_pos_start"], 1000),
          "_whale: прапорець partial_close + колонки у рядку %r" % o["flags"])
    o = S.settle_follow(dict(base_row, trade_id="bf", whale_addr="0xfull", fill_ts_ms=str(t0 - 30000)), ctx)
    check("full_close" in o["flags"] and "partial_close" not in o["flags"], "_whale: full_close")
    o = S.settle_follow(dict(base_row, trade_id="bu", whale_addr="0xunk", fill_ts_ms=str(t0 - 30000)), ctx)
    check("close_unknown" in o["flags"] and o["whale_pos_after"] is None, "_whale: close_unknown")
    o = S.settle_follow(dict(base_row, trade_id="bn", whale_addr="0xnone"), ctx)
    check("no_whale_fill" in o["flags"] and not any(f in o["flags"] for f in ("full_close", "partial_close", "close_unknown")),
          "без епізоду — без прапорця закриття")
    write_csv(os.path.join(dd, "follow_trades.csv"), FOL_HDR, [dict(base_row, trade_id="bv", whale_addr="0xvault", fill_ts_ms=str(t0 - 1000))])
    check(S.settle_pending(dd, fetchers=fetch_b, now_ms=NOW_OLD, log=quiet) == (1, 0, 0), "запис")
    sv = S.load_settlements(dd)["bv"]
    check(close(sv["whale_pos_start"], 1000) and close(sv["whale_pos_after"], 900) and close(sv["whale_max_fill_pct"], 0.05)
          and close(sv["whale_max_fill_usd"], 6000) and "partial_close" in sv["flags"].split(";")
          and close(sv["costs_live_pct"], 0.15) and int(sv["curve_n"]) == 3,
          "CSV: whale_pos_* / whale_max_fill_* / costs_live_pct / curve_n записані")
    # ── C: покриття стрічки vs вердикт no_trigger (R2, SHORT, рівень 299.1)
    T = BASE + 6 * H
    H30 = S.HOLD30_MS
    gap_tape = [(T - 300, 300.0), (T + 5000, 299.5), (T + 30000, 299.6),      # лише перша хвилина вікна
                (T + 400500, 299.4), (T + 400000 + H30 + 100, 298.5)]         # ноги записаного входу/виходу
    ddc = fresh("b9c1")
    fzc = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", gap_tape)})
    fetch_c = {"fetch_zip": fzc, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    ctx = S.make_ctx(ddc, fetchers=fetch_c, now_ms=T + 2 * H, log=quiet)
    check(not S._covered("SYNUSDT", T, T + S.R2_WINDOW_MS, ctx) and S._covered("SYNUSDT", T, T + 30000, ctx)
          and S._covered("SYNUSDT", T + 4000, T + 400500, ctx) and not S._covered("SYNUSDT", T + 100000, T + 400500, ctx),
          "_covered: трейд у перших і останніх 60 с вікна")
    g1 = rev_row(sig_id="g1", strategy="R2_breakout", date=S._dt(T), our_side="SHORT",
                 detect_px="300.0", entry_px="299.1", move_3m_pct="1.0", m30="0.4")
    o = S.settle_rev(g1, ctx)
    check("tape_gap" in o["flags"] and "no_trigger" not in o["flags"] and "no_tape" not in o["flags"]
          and o["entry_src"] == "none" and o["status"] == "live_only" and close(o["net_official_pct"], 0.25)
          and o["curve_n"] == 0, "R2 старий рядок, вікно не покрите → tape_gap (не no_trigger), live_only %r" % o["flags"])
    check(S._tape_pending(o, T + 2 * H) and not S._tape_pending(o, T + 73 * H),
          "tape_gap: молодий рядок відкладається, старий фіксується")
    g3 = rev_row(sig_id="g3", strategy="R2_breakout", date=S._dt(T), our_side="SHORT", entry_px="299.3",
                 entry_ts_ms=str(T + 400000), exit_ts_ms=str(T + 400000 + H30), exit_px="299.0",
                 exit_reason="timer", exit_min="30")
    o = S.settle_rev(g3, ctx)
    check("tape_gap" in o["flags"] and "no_trigger" not in o["flags"] and o["status"] == "verified"
          and close(o["entry_px_tape"], 299.4) and S._tape_pending(o, T + 2 * H) and not S._tape_pending(o, T + 73 * H),
          "R2 записаний вхід, вікно [date, entry] не покрите → tape_gap як прапорець, ноги оцінені, молодий — відкладено")
    write_csv(os.path.join(ddc, "rev_trades.csv"), REV_HDR, [g1])
    S.settle_pending.retry.clear()
    check(S.settle_pending(ddc, fetchers=fetch_c, now_ms=T + 2 * H, log=quiet) == (0, 0, 1)
          and not os.path.exists(os.path.join(ddc, "settlements.csv")), "settle_pending: tape_gap молодий → відкладено")
    check(S.settle_pending(ddc, fetchers=fetch_c, now_ms=T + 73 * H, log=quiet) == (1, 0, 0), "старий → зафіксовано")
    sc = S.load_settlements(ddc)["g1|R2_breakout"]
    check("tape_gap" in sc["flags"].split(";") and sc["status"] == "live_only", "CSV: tape_gap, live_only")
    S.settle_pending.retry.clear()
    cov_tape = [(T - 300, 300.0), (T + 5000, 299.5), (T + 300000, 299.4), (T + 590000, 299.3)]   # без пробою
    ddc2 = fresh("b9c2")
    fzc2 = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", cov_tape)})
    fetch_c2 = {"fetch_zip": fzc2, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    ctx2 = S.make_ctx(ddc2, fetchers=fetch_c2, now_ms=T + 2 * H, log=quiet)
    o = S.settle_rev(g1, ctx2)
    check("no_trigger" in o["flags"] and "tape_gap" not in o["flags"] and o["status"] == "live_only"
          and not S._tape_pending(o, T + 2 * H), "вікно покрите, пробою немає → no_trigger; не відкладається")
    write_csv(os.path.join(ddc2, "rev_trades.csv"), REV_HDR, [g1])
    check(S.settle_pending(ddc2, fetchers=fetch_c2, now_ms=T + 2 * H, log=quiet) == (1, 0, 0)
          and "no_trigger" in S.load_settlements(ddc2)["g1|R2_breakout"]["flags"],
          "no_trigger молодий → зафіксовано одразу")
    # ── D: повнота кривої: 3/60 → не остаточний, перерахунок з подвоюваними паузами, повна стрічка → остаточний
    ddd = fresh("b9d")
    d_row = dict(base_row, trade_id="d1", whale_addr="0xnone")
    write_csv(os.path.join(ddd, "follow_trades.csv"), FOL_HDR, [d_row])
    sparse = {"fetch_zip": FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())}),
              "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    S.settle_pending.retry.clear()
    check(S.settle_pending(ddd, fetchers=sparse, now_ms=t0 + 61 * M, log=quiet) == (1, 0, 0), "D: перший розрахунок після горизонту")
    s = S.load_settlements(ddd)["d1"]
    check(int(s["curve_n"]) == 3 and s["status"] == "verified" and not S._curve_complete(s)
          and not S._settle_final(s, now_ms=t0 + 61 * M) and not S._settle_final(s, now_ms=t0 + 71 * H),
          "3/60 точок → крива неповна → не остаточний до 72 год")
    # v2.20: за віком остаточний лише рядок, ПЕРЕРАХОВАНИЙ після 72 год (settled_at − entry ≥ 72 год)
    check(not S._settle_final(s, now_ms=t0 + 72 * H)
          and S._settle_final(dict(s, settled_at=S._dt(t0 + 72 * H)), now_ms=t0 + 72 * H)
          and S._curve_complete({"family": "fol", "curve_n": "60"})
          and not S._curve_complete({"family": "fol", "curve_n": "54"})
          and not S._curve_complete({"family": "twap", "curve_tape": ";".join(["1"] * 108 + [""] * 12)})
          and S._curve_complete({"family": "twap", "curve_tape": ";".join(["1"] * 120)}),
          "_curve_complete: v2.19 (аудит G) — лише 100% горизонту (60/60, 120/120); 54/60 — ні")
    check(S.settle_pending(ddd, fetchers=sparse, now_ms=t0 + 121 * M, log=quiet) == (0, 0, 0), "пауза max(1 год, 61 хв) не минула")
    check(S.settle_pending(ddd, fetchers=sparse, now_ms=t0 + 122 * M, log=quiet) == (1, 0, 0), "минула → перерахунок")
    check(S.settle_pending(ddd, fetchers=sparse, now_ms=t0 + 243 * M, log=quiet) == (0, 0, 0), "наступна пауза 122 хв (подвоєння)")
    os.remove(os.path.join(ddd, "tape", "SYNUSDT-aggTrades-2026-09-09.zip"))      # стрічка «доїхала» повністю
    dense_rows = [(t0 + k * M + 100, 100.0 - k * 0.01) for k in range(0, 130)]
    dense = dict(sparse, fetch_zip=FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", dense_rows)}))
    check(S.settle_pending(ddd, fetchers=dense, now_ms=t0 + 244 * M, log=quiet) == (1, 0, 0), "пауза минула → перерахунок по повній стрічці")
    s = S.load_settlements(ddd)["d1"]
    check(int(s["curve_n"]) == 60 and S._curve_complete(s) and S._settle_final(s, now_ms=t0 + 245 * M)
          and close(s["entry_px_tape"], 100.0) and close(s["exit_px_tape"], 99.98), "повна крива 60/60 → остаточний одразу")
    check(S.settle_pending(ddd, fetchers=dense, now_ms=t0 + 10 * H, log=quiet) == (0, 0, 0)
          and S.settle_pending(ddd, fetchers=dense, now_ms=t0 + 80 * H, log=quiet) == (0, 0, 0), "остаточний не перераховується")
    ddd2 = fresh("b9d2")
    write_csv(os.path.join(ddd2, "follow_trades.csv"), FOL_HDR, [d_row])
    check(S.settle_pending(ddd2, fetchers=sparse, now_ms=t0 + 61 * M, log=quiet) == (1, 0, 0)
          and S.settle_pending(ddd2, fetchers=sparse, now_ms=t0 + 73 * H, log=quiet) == (1, 0, 0)
          and S.settle_pending(ddd2, fetchers=sparse, now_ms=t0 + 74 * H, log=quiet) == (0, 0, 0),
          "неповна крива, 72 год минуло → ОСТАННІЙ перерахунок (v2.20), далі остаточний")
    # ── E: бекоф + справедлива черга (сценарій аудиту: limit=2, два нових без стрічки, один старший з даними)
    check(S._backoff_ms(1) == 10 * M and S._backoff_ms(2) == 20 * M and S._backoff_ms(3) == 40 * M
          and S._backoff_ms(7) == 6 * H and S._backoff_ms(30) == 6 * H, "бекоф 10 хв × 2^(n−1), стеля 6 год")
    dde = fresh("b9e")
    S.settle_pending.retry.clear()
    S._ZIP_MISS.clear()
    e_rows = [dict(base_row, trade_id="eA", coin="NOPE", whale_addr="0x", date_open=S._dt(t0 + 2 * H),
                   date_close=S._dt(t0 + 2 * H + 60000), open_ts_ms=str(t0 + 2 * H), close_ts_ms=str(t0 + 2 * H + 60000)),
              dict(base_row, trade_id="eB", coin="NOPE", whale_addr="0x", date_open=S._dt(t0 + H),
                   date_close=S._dt(t0 + H + 60000), open_ts_ms=str(t0 + H), close_ts_ms=str(t0 + H + 60000)),
              dict(base_row, trade_id="eC", whale_addr="0x")]
    write_csv(os.path.join(dde, "follow_trades.csv"), FOL_HDR, e_rows)
    fze = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    fetch_e = {"fetch_zip": fze, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    now1 = t0 + 3 * H
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1, log=quiet)
    st = S.load_settlements(dde)
    check(res == (1, 0, 1) and list(st) == ["eC"] and S.settle_pending.last_pending == 3
          and S.settle_pending.last_waiting == 0, "прохід 1: новіший eA відкладено, старший eC (2-га половина) розраховано %r" % (res,))
    rt = S.settle_pending.retry
    check(set(rt) == {"eA"} and rt["eA"] == (now1 + 10 * M, 1), "бекоф eA: 10 хв, 1 збій %r" % rt)
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1 + M, log=quiet)
    check(res == (0, 0, 1) and S.settle_pending.last_pending == 2 and S.settle_pending.last_waiting == 1
          and set(rt) == {"eA", "eB"} and rt["eB"] == (now1 + 11 * M, 1),
          "прохід 2: eA у бекофі (не повторюється), спроба йде на eB; eC не кандидат %r" % (res,))
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1 + 2 * M, log=quiet)
    check(res == (0, 0, 0) and S.settle_pending.last_waiting == 2, "прохід 3: обидва у бекофі — limit не витрачено %r" % (res,))
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1 + 11 * M, log=quiet)
    check(res == (0, 0, 2) and rt["eA"] == (now1 + 31 * M, 2) and rt["eB"] == (now1 + 31 * M, 2),
          "прохід 4: next_retry минув → обидва повторені, бекоф 20 хв %r" % (res,))
    nope = [(t0 + H + 100, 10.0), (t0 + H + 60000 + 100, 9.9), (t0 + 2 * H + 100, 10.0), (t0 + 2 * H + 60000 + 100, 9.9)]
    fze.files[("NOPEUSDT", "2026-09-09")] = zip_bytes("NOPEUSDT", "2026-09-09", nope)   # стрічка з'явилась
    S._ZIP_MISS.clear()
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1 + 20 * M, log=quiet)
    check(res == (0, 0, 0) and S.settle_pending.last_waiting == 2, "у бекофі — не повторюються навіть коли дані вже є")
    res = S.settle_pending(dde, fetchers=fetch_e, limit=2, now_ms=now1 + 31 * M, log=quiet)
    st = S.load_settlements(dde)
    check(res == (2, 0, 0) and not rt and st["eA"]["status"] == "verified" and st["eB"]["status"] == "verified",
          "після бекофу — розраховано, ключі зняті %r" % (res,))
    # порядок спроб при limit=4 з 5 готових: 2 новіші першими, потім 2 старіші першими
    dde2 = fresh("b9e2")
    write_csv(os.path.join(dde2, "follow_trades.csv"), FOL_HDR,
              [dict(base_row, trade_id="q%d" % k, whale_addr="0x", date_open=S._dt(t0 + k * 10 * M),
                    date_close=S._dt(t0 + k * 10 * M + 60000), open_ts_ms=str(t0 + k * 10 * M),
                    close_ts_ms=str(t0 + k * 10 * M + 60000)) for k in range(5)])
    dense2 = {"fetch_zip": FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09",
                                    [(t0 + k * 30000 + 100, 100.0) for k in range(400)])}),
              "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    check(S.settle_pending(dde2, fetchers=dense2, limit=4, now_ms=NOW_OLD, log=quiet) == (4, 0, 0), "limit=4 з 5")
    order = [r["key"] for r in csv.DictReader(open(os.path.join(dde2, "settlements.csv"), encoding="utf-8"))]
    check(order == ["q4", "q0", "q3", "q1"], "черга v2.19 (аудит H): новіший · найстаріший · наступний новіший · … %r" % order)
    check(S.settle_pending(dde2, fetchers=dense2, now_ms=NOW_OLD, log=quiet) == (1, 0, 0), "решта (q2) без подвійної обробки")
    # Transient → той самий бекоф
    dde3 = fresh("b9e3")
    write_csv(os.path.join(dde3, "follow_trades.csv"), FOL_HDR, [dict(base_row, trade_id="tr1", whale_addr="0x")])

    def zip_boom(url):
        raise S.Transient("HTTP 503", 503)
    fetch_t = {"fetch_zip": zip_boom, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    check(S.settle_pending(dde3, fetchers=fetch_t, now_ms=now1, log=quiet) == (0, 0, 1) and rt["tr1"] == (now1 + 10 * M, 1)
          and S.settle_pending(dde3, fetchers=fetch_t, now_ms=now1 + M, log=quiet) == (0, 0, 0)
          and S.settle_pending.last_waiting == 1, "Transient → бекоф 10 хв, повтор пропущено без спроби")
    check(S.settle_pending(dde3, fetchers=fetch_t, now_ms=now1 + M, force=True, log=quiet) == (0, 0, 1)
          and rt["tr1"][1] == 2, "force ігнорує бекоф (спроба, лічильник росте)")
    S.settle_pending.retry.clear()
    # ── F: v1-кеш REST-відра (2 колонки) — ігнорується, файл видаляється, відро перетягується
    ddf = fresh("b9f")
    now_f = DAY0 + DAY + H                      # день 09.09 у REST-вікні 48 год
    b0 = (BASE // S.REST_BUCKET_MS) * S.REST_BUCKET_MS
    calls_f = []

    def rest_f(url):
        calls_f.append(url)
        q = dict(p.split("=") for p in url.split("?")[1].split("&"))
        st_, en = int(q["startTime"]), int(q["endTime"])
        rows = [{"a": 0, "p": "2.0", "q": "1", "T": BASE + 100, "m": False},
                {"a": 1, "p": "2.5", "q": "1", "T": BASE + 200, "m": False}]
        return [r for r in rows if st_ <= r["T"] <= en]
    logs_f = []
    tape_f = S.Tape(ddf, fetch_zip=lambda u: None, fetch_rest=rest_f, now_ms=now_f, rest_pace_s=0, log=logs_f.append)
    jp = os.path.join(ddf, "tape", "rest", "V1USDT-%d.json" % b0)
    with open(jp, "w") as f:
        json.dump([[BASE + 100, 1.5], [BASE + 200, 1.7]], f)
    r = S.exec_px("V1USDT", BASE, "BUY", tape_f)
    check(r["px"] == 2.5 and r["n"] == 2 and len(calls_f) == 1 and any("старого формату" in m for m in logs_f),
          "v1-кеш проігноровано, відро перетягнуто (ціни з REST, не з кешу) %r" % r)
    jmeta = json.load(open(jp)); jrows = jmeta.get("rows") if isinstance(jmeta, dict) else jmeta
    check(os.path.exists(jp) and isinstance(jmeta, dict) and jmeta.get("v") == S.CACHE_V and jmeta.get("complete") is True
          and len(jrows) == 2 and all(len(x) == 3 for x in jrows), "повне відро перезаписано у форматі v3 (доказ повноти)")
    tape_f2 = S.Tape(ddf, fetch_zip=lambda u: None, fetch_rest=rest_f, now_ms=now_f, rest_pace_s=0)
    check(S.exec_px("V1USDT", BASE, "BUY", tape_f2)["px"] == 2.5 and len(calls_f) == 1, "далі — з нового кешу без мережі")
    jp_old = os.path.join(ddf, "tape", "rest", "V1USDT-%d.json" % (b0 - 3 * S.REST_BUCKET_MS))
    with open(jp_old, "w") as f:
        json.dump([[BASE - 1800000 + 100, 1.5]], f)
    tape_f3 = S.Tape(ddf, fetch_zip=lambda u: None, fetch_rest=rest_f, now_ms=NOW_OLD, rest_pace_s=0, log=quiet)
    n_f = len(calls_f)
    check(tape_f3._rest_bucket("V1USDT", b0 - 3 * S.REST_BUCKET_MS) is None and not os.path.exists(jp_old)
          and len(calls_f) == n_f, "поза REST-вікном: файл видалено, без перетягування → None")
    # ── G: витрати за ногами як в API
    check(close(S._leg_costs(0.15, "book", "book"), 0.10) and close(S._leg_costs(0.15, "mid", "book"), 0.125)
          and close(S._leg_costs(0.15, "book_partial", "mid+whale"), 0.125) and close(S._leg_costs(0.15, "", ""), 0.15)
          and close(S._leg_costs(0.15, None, None), 0.15) and close(S._leg_costs("x", "mid", "mid"), 0.10)
          and close(S._leg_costs(0.05, "mid", "mid"), 0.10), "_leg_costs: comm 0.10 + сліпаж лише не-book ногам")
    TG = BASE + 8 * H
    H60 = S.HOLD60_MS
    g_tape = [(TG - 500, 100.0), (TG + 100, 100.1), (TG + 5 * M + 100, 100.9), (TG + H30 + 200, 101.1),
              (TG + H60 + 100, 102.0), (TG + 61 * M + 100, 101.9)]
    ddg = fresh("b9g")
    fetch_g = {"fetch_zip": FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", g_tape)}),
               "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0}
    ctxg = S.make_ctx(ddg, fetchers=fetch_g, now_ms=NOW_OLD, log=quiet)
    gb = rev_row(sig_id="gb", strategy="R1_загальний", date=S._dt(TG), our_side="LONG", entry_px="100.0",
                 entry_ts_ms=str(TG), exit_ts_ms=str(TG + H30), exit_px="101.0", exit_reason="timer",
                 exit_min="30", entry_src="book", exit_src="book", m30="1.5")
    o = S.settle_rev(gb, ctxg)
    nt = (101.1 / 100.1 - 1) * 100 - 0.15
    check(close(o["net_live_pct"], 0.90, 1e-9) and close(o["costs_live_pct"], 0.10) and close(o["net_tape_pct"], nt, 1e-9)
          and close(o["net_official_pct"], min(0.90, nt), 1e-9) and close(o["gross_tape_pct"], nt + 0.15, 1e-9),
          "rev book/book: net_live = gross − 0.10; стрічка — повні costs %r" % o["net_live_pct"])
    o = S.settle_rev(dict(gb, sig_id="gm", entry_src="mid", exit_src="book"), ctxg)
    check(close(o["net_live_pct"], 0.875, 1e-9) and close(o["costs_live_pct"], 0.125), "rev mid/book: gross − 0.10 − сліпаж 0.025")
    o = S.settle_rev(dict(gb, sig_id="gs", entry_src="book", exit_src="sample"), ctxg)
    check(close(o["net_live_pct"], 0.875, 1e-9), "rev book/sample: сліпаж на ногу виходу")
    o = S.settle_rev(dict(gb, sig_id="g30", exit_ts_ms="", exit_px="", exit_reason="", entry_src="book"), ctxg)
    check(close(o["net_live_pct"], 1.35) and close(o["costs_live_pct"], 0.15), "rev m30-шлях: повні costs 0.15")
    o = S.settle_follow(dict(base_row, trade_id="gf", whale_addr="0x"), ctx)
    check(close(o["net_live_pct"], 0.85) and close(o["costs_live_pct"], 0.15), "follow: net як записано, costs_live = costs рядка")
    tw_base = {"coin": "SYN", "our_side": "LONG", "whale_addr": "0x", "entry_px": "100.0", "costs_pct": "0.15",
               "algo_v": "2.16", "eol": "^", "date_entry": S._dt(TG), "entry_ts_ms": str(TG), "strategy": "T1_твап"}
    o = S.settle_twap(dict(tw_base, twap_id="gt", net60_pct="0.6", exit_min="5", exit_reason="cancelled",
                           exit_ts_ms=str(TG + 5 * M), exit_px="100.75", entry_src="book", exit_src="book"), ctxg)
    check(close(o["net_live_pct"], 0.65, 1e-9) and close(o["costs_live_pct"], 0.10) and close(o["exit_px_live"], 100.75),
          "twap з exit_px book/book: net_live = 0.75 − 0.10")
    o = S.settle_twap(dict(tw_base, twap_id="gt2", net60_pct="0.6", exit_min="5", exit_reason="cancelled",
                           exit_ts_ms=str(TG + 5 * M), exit_px="100.75", entry_src="mid", exit_src="book"), ctxg)
    check(close(o["net_live_pct"], 0.625, 1e-9) and close(o["costs_live_pct"], 0.125), "twap mid/book")
    o = S.settle_twap(dict(tw_base, twap_id="gt3", net60_pct="0.6", exit_min="60", exit_reason="timer_60m"), ctxg)
    check(close(o["net_live_pct"], 0.6) and close(o["costs_live_pct"], 0.15), "twap без exit_px: net60_pct як записано")
    write_csv(os.path.join(ddg, "rev_trades.csv"), REV_HDR, [gb])
    check(S.settle_pending(ddg, fetchers=fetch_g, now_ms=NOW_OLD, log=quiet) == (1, 0, 0), "запис rev")
    sg = S.load_settlements(ddg)["gb|R1_загальний"]
    check(close(sg["costs_live_pct"], 0.10) and close(sg["net_live_pct"], 0.90, 1e-9) and close(sg["costs_pct"], 0.15),
          "CSV: costs_live_pct 0.10 поруч із costs_pct 0.15")
    # ── H: TWAP no_price — не пропускається: вихід на exit_ts_ms або entry + exit_min, net_live None → tape_only
    o = S.settle_twap(dict(tw_base, twap_id="h1", net60_pct="", exit_min="60", exit_reason="no_price"), ctxg)
    g60 = (102.0 / 100.1 - 1) * 100
    check(o is not None and o["net_live_pct"] is None and int(o["exit_ts_ms"]) == TG + 60 * M
          and close(o["exit_px_tape"], 102.0) and close(o["net_tape_pct"], g60 - 0.15, 1e-9)
          and close(o["net_official_pct"], g60 - 0.15, 1e-9) and o["status"] == "tape_only" and "partial" in o["flags"]
          and o["exit_reason_tape"] == "no_price", "twap no_price: вихід entry+exit_min, tape_only, official = tape")
    o = S.settle_twap(dict(tw_base, twap_id="h2", net60_pct="", exit_min="60", exit_reason="no_price",
                           exit_ts_ms=str(TG + 61 * M)), ctxg)
    check(int(o["exit_ts_ms"]) == TG + 61 * M and close(o["exit_px_tape"], 101.9) and o["status"] == "tape_only",
          "twap no_price із записаним exit_ts_ms — вихід на ньому")
    check(S.settle_twap(dict(tw_base, twap_id="h3", net60_pct="", exit_min="", exit_reason="no_price"), ctxg) is None,
          "no_price без exit_min → пропуск, як раніше")
    o = S.settle_twap(dict(tw_base, twap_id="h4", coin="NOPE", net60_pct="", exit_min="60", exit_reason="no_price"), ctxg)
    check(o["status"] == "none" and "no_tape" in o["flags"] and o["net_official_pct"] is None, "no_price без стрічки → none")
    write_csv(os.path.join(ddg, "twap_trades.csv"), TW_HDR,
              [dict(tw_base, twap_id="h1", net60_pct="", exit_min="60", exit_reason="no_price")])
    check(S.settle_pending(ddg, fetchers=fetch_g, now_ms=NOW_OLD, log=quiet) == (1, 0, 0)
          and S.load_settlements(ddg)["h1|T1_твап"]["status"] == "tape_only", "settle_pending: no_price TWAP записано tape_only")
    S.settle_pending.retry.clear()
    # найбільша ТРАНЗАКЦІЯ епізоду — філи одного hash (ордер) сумуються: 2×48% одним ордером = 96% (R7-база)
    _fl = [{"coin": "Q", "time": 1000, "side": "A", "dir": "Close Long", "startPosition": "1000", "sz": "480", "px": "100", "hash": "0xone"},
           {"coin": "Q", "time": 1001, "side": "A", "dir": "Close Long", "startPosition": "520", "sz": "480", "px": "99", "hash": "0xone"},
           {"coin": "Q", "time": 5000, "side": "A", "dir": "Close Long", "startPosition": "40", "sz": "40", "px": "98", "hash": "0xtwo"}]
    _ep = S.whale_episode(_fl, "Q", 6000, True)
    check(close(_ep["whale_max_fill_pct"], 0.96) and close(_ep["whale_max_fill_usd"], 480 * 100 + 480 * 99)
          and close(_ep["whale_pos_start"], 1000) and close(_ep["whale_pos_after"], 0), "ордер з двох філів = 96%% (%r)" % (_ep,))
    # grow_only (live: лише тейкерські філи): «незрозуміле» зменшення позиції між філами — не розрив
    # (мейкерське закриття невидиме боту), зростання — розрив; абсолютний допуск (settlement) рве обидва
    _go = [{"coin": "Q", "time": 1000, "side": "A", "dir": "Close Long", "startPosition": "1000", "sz": "100", "px": "100", "hash": "0xa"},
           {"coin": "Q", "time": 60000, "side": "A", "dir": "Close Long", "startPosition": "700", "sz": "700", "px": "99", "hash": "0xb"}]
    f1, _, ep1 = S.close_episode(_go, "Q", 61000, True, grow_only=True)
    f2, _, ep2 = S.close_episode(_go, "Q", 61000, True)
    check(f1 is _go[0] and len(ep1) == 2 and f2 is _go[1] and len(ep2) == 1, "grow_only не рве на зменшенні; абсолютний — рве")
    _gr = [dict(_go[0]), dict(_go[1], startPosition="2000", sz="700")]
    f3, _, ep3 = S.close_episode(_gr, "Q", 61000, True, grow_only=True)
    check(f3 is _gr[1] and len(ep3) == 1, "grow_only рве на зростанні (долив)")
    # системні філи (ліквідація/ADL) з нульовим hash не склеюються в один «ордер» — групування по oid
    _sys = [{"coin": "Q", "time": 1000, "side": "A", "dir": "Liquidated Long", "startPosition": "1000", "sz": "500", "px": "100",
             "hash": "0x0000000000000000000000000000000000000000000000000000000000000000", "oid": 1},
            {"coin": "Q", "time": 5000, "side": "A", "dir": "Liquidated Long", "startPosition": "500", "sz": "500", "px": "99",
             "hash": "0x0000000000000000000000000000000000000000000000000000000000000000", "oid": 2}]
    _eps = S.whale_episode(_sys, "Q", 6000, True)
    check(close(_eps["whale_max_fill_pct"], 0.5) and close(_eps["whale_max_fill_usd"], 500 * 100), "нульовий hash → окремі ордери (%r)" % (_eps["whale_max_fill_pct"],))
    check(S.SETTLE_V == "5" and S.CACHE_V == 3, "SETTLE_V=5 (v2.20): тригерний філ, кеш v3 з доказом повноти, tape_thin, фандинг у кривій — придатні лише рядки поточної версії")
    # бюджетний Transient (кап HL-запитів у боті, code="budget") — без бекофу: наступний прогін пробує знову
    ddb = fresh("b9budget")
    write_csv(os.path.join(ddb, "follow_trades.csv"), FOL_HDR,
              [{"date_open": S._dt(BASE), "date_close": S._dt(BASE + 120000), "strategy": "F1", "coin": "SYN",
                "our_side": "SHORT", "whale_addr": "0xabc", "entry_px": "100", "exit_px": "99",
                "costs_pct": "0.15", "net_pct": "0.85", "algo_v": "2.16", "trade_id": "bg1",
                "open_ts_ms": str(BASE), "close_ts_ms": str(BASE + 120000), "eol": "^"}])
    fzb = FakeZip({("SYNUSDT", "2026-09-09"): zip_bytes("SYNUSDT", "2026-09-09", follow_tape())})
    def hl_budget(body): raise S.Transient("кап HL-запитів на цикл", "budget")
    S.settle_pending.retry.clear()
    res = S.settle_pending(ddb, fetchers={"fetch_zip": fzb, "fetch_rest": NO_REST, "fetch_hl": hl_budget, "hl_pace_s": 0},
                           now_ms=NOW_OLD, log=lambda *a: None)
    check(res == (0, 0, 1) and "bg1" not in S.settle_pending.retry, "бюджетний Transient не кладе рядок у бекоф %r" % (res,))
    res = S.settle_pending(ddb, fetchers={"fetch_zip": fzb, "fetch_rest": NO_REST, "fetch_hl": NO_HL, "hl_pace_s": 0},
                           now_ms=NOW_OLD, log=lambda *a: None)
    check(res == (1, 0, 0), "наступний прогін (той самий момент) рахує рядок %r" % (res,))
    print("блок 9 (фліп, факти епізоду, tape_gap/no_trigger, повнота кривої, бекоф/черга, v1-кеш, витрати за ногами, TWAP no_price, бюджет без бекофу): OK")


if __name__ == "__main__":
    shutil.rmtree(TD, ignore_errors=True)
    os.makedirs(TD)
    for b in (block1, block2, block3, block4, block5, block6, block7, block8, block9):
        b()
    print("УСІ ТЕСТИ ЗЕЛЕНІ: %d перевірок" % NCHK[0])

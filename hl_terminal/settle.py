#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""settle.py — «розрахунок» (settlement) paper-угод по стрічці aggTrades
Binance USDⓈ-M futures: кожна закрита угода (follow / rev / twap) детерміновано
переоцінюється за реальними трейдами біржі одним правилом для старих і нових
рядків; офіційний результат = ГІРШИЙ з live-запису і стрічки. Лише stdlib.
CLI: python3 settle.py --data-dir DIR [--since YYYY-MM-DD] [--limit N] [--force]
     [--symbol-map JSON]"""
import argparse, bisect, csv, glob, io, json, math, os, time, zipfile  # noqa: E401
import urllib.request
from array import array
from collections import OrderedDict

__all__ = ["Tape", "HLFills", "exec_px", "get_trades", "symbol_for", "parse_local", "settle_follow",
           "settle_rev", "settle_twap", "settle_pending", "load_settlements", "make_ctx",
           "whale_episode", "hl_user_fills", "HEADERS", "SETTLE_V"]

SETTLE_V = "1"
ZIP_URL = ("https://data.binance.vision/data/futures/um/daily/aggTrades/"
           "{s}/{s}-aggTrades-{d}.zip")
REST_URL = "https://fapi.binance.com/fapi/v1/aggTrades"
HL_URL = "https://api.hyperliquid.xyz/info"
DAY_MS, HOUR_MS, MIN_MS = 86400000, 3600000, 60000
REST_BUCKET_MS = 600000            # REST-кеш 10-хв відрами (вікно < 1 год)
REST_MAX_AGE_MS = 48 * HOUR_MS     # REST лише для днів без zip у межах 48 год
LRU_MAX = 24                       # символо-днів у пам'яті
COSTS_DEF = 0.15                   # витрати за круг, % (як у server.py)
R2_BREAKOUT = 0.003                # R2: вхід після руху +0.3% від p0
R2_WINDOW_MS = 600000              # R2: вікно 10 хв
R8_TP_K = 0.8                      # R8: TP = 0.8 × дамп
HOLD30_MS, HOLD60_MS = 30 * MIN_MS, 60 * MIN_MS
K1000 = ("PEPE", "BONK", "SHIB", "FLOKI", "LUNC", "DOGS", "NEIRO")

HEADERS = ["key", "family", "strategy", "coin", "symbol", "settle_v", "settled_at",
           "ts_src", "entry_ts_ms", "entry_px_live", "entry_px_tape", "entry_src",
           "entry_age_ms", "exit_ts_ms", "exit_px_live", "exit_px_tape", "exit_src",
           "exit_reason_tape", "gross_tape_pct", "costs_pct", "net_live_pct",
           "net_tape_pct", "net_official_pct", "net60_tape_pct", "entry_bias_pct",
           "whale_fill_ts_ms", "whale_px", "lag_s", "dump_first_ts_ms",
           "dump_last_ts_ms", "dump_dur_s", "dump_move_pct", "dump_bucket",
           "flags", "eol"]


# ── дрібні хелпери ──────────────────────────────────────────────────────
def fnum(v, default=None):
    """float або default (порожнє / бите / non-finite)."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def parse_local(s):
    """'YYYY-MM-DD HH:MM:SS' у ЛОКАЛЬНОМУ часі машини → epoch мс, або None."""
    try:
        return int(time.mktime(time.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S"))) * 1000
    except (TypeError, ValueError, OverflowError):
        return None


def _dt(ms):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000.0))


def _day_str(ms):
    return time.strftime("%Y-%m-%d", time.gmtime(ms // 1000))


def symbol_for(coin, symbol_map=None):
    """Монета HL → символ Binance futures: override з symbol_map, kXXX →
    1000XXXUSDT, інакше COIN+USDT."""
    c = (coin or "").strip()
    up = c.upper()
    if symbol_map:
        for k, v in symbol_map.items():
            if str(k).upper() == up:
                return v
    if len(c) > 1 and c[0] == "k" and c[1:].isupper():
        return "1000" + c[1:] + "USDT"
    if up[:1] == "K" and up[1:] in K1000:
        return "1000" + up[1:] + "USDT"
    return up + "USDT"


def _http(url, data=None, timeout=20):   # GET/POST → bytes; None при 404/помилці
    hdr = {"User-Agent": "Mozilla/5.0"}
    if data is not None:
        hdr["Content-Type"] = "application/json"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=hdr),
                                    timeout=timeout) as r:
            return r.read()
    except Exception:
        return None


def _json_list(b):
    try:
        j = json.loads(b) if b else None
    except ValueError:
        return None
    return j if isinstance(j, list) else None


def _load_json(path):
    """JSON з диска або None (нема / битий)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _save_json(path, obj):
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(path + ".tmp", path)


def _pace(t_last, gap_s):
    """Не частіше за один запит на gap_s → нова мітка monotonic."""
    wait = gap_s - (time.monotonic() - t_last)
    if wait > 0:
        time.sleep(wait)
    return time.monotonic()


def default_fetch_zip(url):
    return _http(url)


def default_fetch_rest(url):
    return _json_list(_http(url))


def default_fetch_hl(body):
    return _json_list(_http(HL_URL, data=json.dumps(body).encode()))


def parse_agg_zip(raw):
    """zip aggTrades → (array ts_ms, array px), відсортовані за часом.
    Рядок заголовка може бути відсутній (старі файли)."""
    ts, px = array("q"), array("d")
    last, ordered = -1, True
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")] or z.namelist()
        with z.open(names[0]) as f:
            for line in io.TextIOWrapper(f, encoding="utf-8", errors="replace"):
                p = line.split(",")
                if len(p) < 6:
                    continue
                try:
                    t, x = int(p[5]), float(p[1])
                except ValueError:
                    continue            # заголовок / битий рядок
                ts.append(t)
                px.append(x)
                if t < last:
                    ordered = False
                last = t
    if not ordered:
        pairs = sorted(zip(ts, px))
        ts, px = array("q", (a for a, _ in pairs)), array("d", (b for _, b in pairs))
    return ts, px


# ── стрічка трейдів ─────────────────────────────────────────────────────
class Tape:
    """Стрічка aggTrades: zip-день (диск <data_dir>/tape + LRU) з REST-фолбеком
    10-хв відрами (<data_dir>/tape/rest) для днів без zip у межах 48 год.
    Мережа — лише через fetch_zip / fetch_rest (ін'єкція для тестів)."""

    def __init__(self, data_dir, fetch_zip=None, fetch_rest=None, log=print,
                 now_ms=None, lru_max=LRU_MAX, rest_pace_s=1.0):
        self.dir = os.path.join(data_dir, "tape")
        self.rest_dir = os.path.join(self.dir, "rest")
        os.makedirs(self.rest_dir, exist_ok=True)
        self.fetch_zip = fetch_zip or default_fetch_zip
        self.fetch_rest = fetch_rest or default_fetch_rest
        self.log, self.now_ms, self.lru_max, self.pace = log, now_ms, lru_max, rest_pace_s
        self._lru = OrderedDict()       # (symbol, day) | ("rest", symbol, b0) → (ts, px)
        self._miss = set()              # ключі без даних у цьому запуску
        self._t_rest = 0.0
        self.stats = {"zip_fetch": 0, "zip_miss": 0, "rest_req": 0, "rest_fail": 0}

    def _now(self):
        return self.now_ms if self.now_ms is not None else int(time.time() * 1000)

    def _put(self, key, val):
        self._lru[key] = val
        self._lru.move_to_end(key)
        while len(self._lru) > self.lru_max:
            self._lru.popitem(last=False)

    def _get(self, key):
        if key in self._lru:
            self._lru.move_to_end(key)
        return self._lru.get(key)

    def day(self, symbol, day):
        """Масиви (ts, px) за UTC-день 'YYYY-MM-DD' або None (zip нема)."""
        key = (symbol, day)
        v = self._get(key)
        if v is not None or key in self._miss:
            return v
        path = os.path.join(self.dir, "%s-aggTrades-%s.zip" % (symbol, day))
        raw = None
        if os.path.exists(path):
            with open(path, "rb") as f:
                raw = f.read()
        else:
            self.stats["zip_fetch"] += 1
            raw = self.fetch_zip(ZIP_URL.format(s=symbol, d=day))
            if raw:
                with open(path + ".tmp", "wb") as f:
                    f.write(raw)
                os.replace(path + ".tmp", path)
        if raw:
            try:
                v = parse_agg_zip(raw)
                self._put(key, v)
                return v
            except Exception as e:           # битий/обірваний zip — геть із кешу
                self.log("[settle] битий zip %s: %r" % (path, e))
                if os.path.exists(path):
                    os.remove(path)
        self.stats["zip_miss"] += 1
        self._miss.add(key)
        return None

    def _rest_fetch(self, symbol, b0, b1):
        """Усі трейди [b0, b1) через REST (пагінація по T, ≤1 запит/с) →
        [[ts, px], …] або None при збої."""
        out, start, last_id, pages = [], b0, -1, 0
        while start < b1 and pages < 200:
            self._t_rest = _pace(self._t_rest, self.pace)
            self.stats["rest_req"] += 1
            pages += 1
            rows = self.fetch_rest("%s?symbol=%s&startTime=%d&endTime=%d&limit=1000"
                                   % (REST_URL, symbol, start, b1 - 1))
            if rows is None:
                self.stats["rest_fail"] += 1
                return None
            n = 0
            for r in rows:
                try:
                    a, t, p = int(r.get("a", -1)), int(r["T"]), float(r["p"])
                except (TypeError, ValueError, KeyError, AttributeError):
                    continue
                if a >= 0 and a <= last_id:
                    continue                 # дубль на межі сторінки
                last_id = max(last_id, a)
                out.append([t, p])
                n += 1
            if len(rows) < 1000 or n == 0:
                break
            start = out[-1][0] if last_id >= 0 else out[-1][0] + 1
        out.sort()
        return out

    def _rest_bucket(self, symbol, b0):
        """10-хв відро через REST: диск (лише повні відра) → пам'ять."""
        key = ("rest", symbol, b0)
        v = self._get(key)
        if v is not None or key in self._miss:
            return v
        path = os.path.join(self.rest_dir, "%s-%d.json" % (symbol, b0))
        rows = _load_json(path)
        complete = b0 + REST_BUCKET_MS <= self._now() - MIN_MS
        if rows is None:
            rows = self._rest_fetch(symbol, b0, b0 + REST_BUCKET_MS)
            if rows is None:
                self._miss.add(key)
                return None
            if complete:
                _save_json(path, rows)
        v = (array("q", (int(r[0]) for r in rows)), array("d", (float(r[1]) for r in rows)))
        if complete:
            self._put(key, v)
        return v

    def window(self, symbol, t0, t1):
        """Усі трейди symbol з ts у [t0, t1] (дні/відра зшиваються) → (ts, px)."""
        ts_out, px_out = array("q"), array("d")
        now = self._now()
        for dn in range(t0 // DAY_MS, t1 // DAY_MS + 1):
            day_ms = dn * DAY_MS
            v = self.day(symbol, _day_str(day_ms))
            if v is not None:
                i, j = bisect.bisect_left(v[0], t0), bisect.bisect_right(v[0], t1)
                ts_out.extend(v[0][i:j])
                px_out.extend(v[1][i:j])
                continue
            if day_ms + DAY_MS < now - REST_MAX_AGE_MS or day_ms > now:
                continue                     # старий день без zip / майбутнє
            lo, hi = max(t0, day_ms), min(t1, day_ms + DAY_MS - 1)
            b = (lo // REST_BUCKET_MS) * REST_BUCKET_MS
            while b <= hi and b <= now:
                v = self._rest_bucket(symbol, b)
                if v is not None:
                    i, j = bisect.bisect_left(v[0], lo), bisect.bisect_right(v[0], hi)
                    ts_out.extend(v[0][i:j])
                    px_out.extend(v[1][i:j])
                b += REST_BUCKET_MS
        return ts_out, px_out


def _tape(cfg):
    return cfg if isinstance(cfg, Tape) else cfg["tape"]


def get_trades(symbol, t0_ms, t1_ms, cfg):
    """Трейди символу з ts у [t0_ms, t1_ms] → (array ts, array px)."""
    return _tape(cfg).window(symbol, int(t0_ms), int(t1_ms))


def exec_px(symbol, t_ms, side, cfg):
    """Ціна виконання за стрічкою: гірша ціна в [t, t+3с] (tape3s), інакше
    [t, t+10с] (tape10s), інакше останній трейд до t не старший за 60 с
    (stale, age_ms), інакше none. side: BUY → max, SELL → min.
    → {px, src, n, age_ms}"""
    t_ms = int(t_ms)
    ts, px = _tape(cfg).window(symbol, t_ms - MIN_MS, t_ms + 10000)
    worst = max if side == "BUY" else min
    i = bisect.bisect_left(ts, t_ms)
    for w, src in ((3000, "tape3s"), (10000, "tape10s")):
        j = bisect.bisect_right(ts, t_ms + w)
        if j > i:
            return {"px": worst(px[i:j]), "src": src, "n": j - i, "age_ms": 0}
    if i > 0 and t_ms - ts[i - 1] <= MIN_MS:
        return {"px": px[i - 1], "src": "stale", "n": 1, "age_ms": t_ms - ts[i - 1]}
    return {"px": None, "src": "none", "n": 0, "age_ms": None}


def _ref_px(symbol, t0, cfg):
    """Референс на детекті: останній трейд у [t0−60с, t0]."""
    ts, px = _tape(cfg).window(symbol, t0 - MIN_MS, t0)
    return px[-1] if len(px) else None


def _first_cross(symbol, t_from, t_to, level, above, cfg):
    """Перший трейд у (t_from, t_to] з ціною ≥ level (above) / ≤ level →
    (ts, px) або None."""
    ts, px = _tape(cfg).window(symbol, t_from + 1, t_to)
    for k in range(len(ts)):
        if (px[k] >= level) if above else (px[k] <= level):
            return ts[k], px[k]
    return None


# ── філи кита (Hyperliquid) ─────────────────────────────────────────────
class HLFills:
    """userFillsByTime з кешем по (адреса, година) на диску <data_dir>/tape/hl;
    ≤2 запити/с; мережа лише через fetch_hl(body) → list | None."""

    def __init__(self, data_dir, fetch_hl=None, log=print, now_ms=None, pace_s=0.5):
        self.dir = os.path.join(data_dir, "tape", "hl")
        os.makedirs(self.dir, exist_ok=True)
        self.fetch_hl = fetch_hl or default_fetch_hl
        self.log, self.now_ms, self.pace = log, now_ms, pace_s
        self._mem, self._miss, self._t = {}, set(), 0.0
        self.stats = {"req": 0, "fail": 0}

    def _now(self):
        return self.now_ms if self.now_ms is not None else int(time.time() * 1000)

    def _hour(self, addr, h):
        key = (addr, h)
        if key in self._mem:
            return self._mem[key]
        if key in self._miss:
            return None
        path = os.path.join(self.dir, "%s-%d.json" % (addr, h))
        rows = _load_json(path)
        complete = (h + 1) * HOUR_MS <= self._now() - MIN_MS
        if rows is None:
            rows, start, seen, pages = [], h * HOUR_MS, set(), 0
            while pages < 8:
                self._t = _pace(self._t, self.pace)
                self.stats["req"] += 1
                pages += 1
                page = self.fetch_hl({"type": "userFillsByTime", "user": addr,
                                      "startTime": start, "endTime": (h + 1) * HOUR_MS - 1})
                if page is None:
                    self.stats["fail"] += 1
                    self._miss.add(key)
                    return None
                n = 0
                for f in page:
                    if not isinstance(f, dict):
                        continue
                    k = (f.get("tid"), f.get("time"), f.get("coin"))
                    if k in seen:
                        continue
                    seen.add(k)
                    rows.append(f)
                    n += 1
                if len(page) < 2000 or n == 0:
                    break
                start = int(page[-1].get("time") or start + 1)
            rows.sort(key=lambda f: int(f.get("time") or 0))
            if complete:
                _save_json(path, rows)
        if complete:
            self._mem[key] = rows
        return rows

    def user_fills(self, addr, t0_ms, t1_ms):
        """Філи адреси з time у [t0, t1] (за часом) або None при збої."""
        out = []
        for h in range(int(t0_ms) // HOUR_MS, int(t1_ms) // HOUR_MS + 1):
            rows = self._hour(addr, h)
            if rows is None:
                return None
            out.extend(f for f in rows if t0_ms <= int(f.get("time") or -1) <= t1_ms)
        out.sort(key=lambda f: int(f.get("time") or 0))
        return out


def hl_user_fills(addr, t0_ms, t1_ms, ctx):
    hl = ctx.get("hl") if isinstance(ctx, dict) else ctx
    return hl.user_fills(addr, t0_ms, t1_ms) if hl is not None else None


def whale_episode(fills, coin, t0_ms, close_only):
    """Останній філ кита по монеті у [t0−600с, t0] (close_only → лише dir з
    «Close») → lag; епізод = філи того ж боку з паузами ≤120 с, що ним
    закінчується → dump_*. dump_move_pct > 0 = ціна пішла у бік тиску кита.
    → dict колонок або {}."""
    fs = sorted((f for f in fills if f.get("coin") == coin
                 and t0_ms - 600000 <= int(f.get("time") or -1) <= t0_ms),
                key=lambda f: int(f.get("time") or 0))
    idx = None
    for k in range(len(fs) - 1, -1, -1):
        if not close_only or "close" in str(fs[k].get("dir", "")).lower():
            idx = k
            break
    if idx is None:
        return {}
    last, side, k = fs[idx], fs[idx].get("side"), idx
    while k > 0 and fs[k - 1].get("side") == side \
            and int(fs[k]["time"]) - int(fs[k - 1]["time"]) <= 120000:
        k -= 1
    first = fs[k]
    t_last, t_first = int(last["time"]), int(first["time"])
    px_last, px_first = fnum(last.get("px")), fnum(first.get("px"))
    dur = (t_last - t_first) / 1000.0
    move = None
    if px_last and px_first:
        move = (px_last / px_first - 1) * 100 * (1 if side == "B" else -1)
    return {"whale_fill_ts_ms": t_last, "whale_px": px_last,
            "lag_s": (t0_ms - t_last) / 1000.0,
            "dump_first_ts_ms": t_first, "dump_last_ts_ms": t_last, "dump_dur_s": dur,
            "dump_move_pct": move,
            "dump_bucket": 0 if dur >= 300 else min(5, max(1, int(math.ceil(dur / 60.0))))}


# ── розрахунок сімей ────────────────────────────────────────────────────
def _ts(row, ms_cols, local_col):
    """Час рядка: перша числова epoch-мс колонка → 'ms', інакше локальний
    рядок → 'local'; (None, None) якщо нічого."""
    for c in ms_cols:
        v = fnum(row.get(c))
        if v is not None and v > 1e12:
            return int(v), "ms"
    t = parse_local(row.get(local_col))
    return (t, "local") if t else (None, None)


def _base(key, family, row, ctx, symbol, ts_src, costs):
    return {"key": key, "family": family, "strategy": row.get("strategy") or "",
            "coin": row.get("coin") or "", "symbol": symbol, "settle_v": SETTLE_V,
            "settled_at": _dt(ctx.get("now_ms") or int(time.time() * 1000)),
            "ts_src": ts_src, "costs_pct": costs, "flags": [], "eol": "^"}


def _leg(out, prefix, symbol, t_ms, side, ctx):
    """Нога (entry/exit) через exec_px → ціна або None; пише *_ts_ms/*_px_tape/*_src."""
    r = exec_px(symbol, t_ms, side, ctx)
    out[prefix + "_ts_ms"] = int(t_ms)
    out[prefix + "_px_tape"], out[prefix + "_src"] = r["px"], r["src"]
    if prefix == "entry":
        out["entry_age_ms"] = r["age_ms"]
    return r["px"]


def _net(e, x, long, costs):
    return None if not (e and x) else (x / e - 1) * 100 * (1 if long else -1) - costs


def _result(out, long, e_tape, x_tape, e_live, net_live, costs):
    """Спільний хвіст: gross/net по стрічці, зсув входу, офіційний = гірший."""
    net_tape = None
    if e_tape is None:
        out["flags"].append("no_tape")
    else:
        if e_live:
            out["entry_bias_pct"] = (e_live - e_tape) / e_tape * 100 * (-1 if long else 1)
        net_tape = _net(e_tape, x_tape, long, costs)
        if net_tape is not None:
            out["gross_tape_pct"] = net_tape + costs
    out["net_live_pct"], out["net_tape_pct"] = net_live, net_tape
    if net_live is not None and net_tape is not None:
        out["net_official_pct"] = min(net_live, net_tape)
    else:
        out["net_official_pct"] = net_live if net_live is not None else net_tape
        if e_tape is not None:
            out["flags"].append("partial")
    return out


def _tape_last_px(symbol, t_ms, ctx):
    """Ціна останнього трейду стрічки НЕ пізніше t_ms (вікно 10 с назад)."""
    try:
        _, px = get_trades(symbol, t_ms - 10000, t_ms + 1, ctx)
    except Exception:
        return None
    return px[-1] if len(px) else None


def _whale(out, row, t0, ctx, close_only, symbol=None):
    """Філи кита перед t0 → lag/dump-колонки; збій → flag no_fills.
    Рух епізоду (dump_move_pct) — як у live (server.py _rev_ref_px): від
    ціни ДО першого філа до ціни останнього; тут референс і кінець — зі
    стрічки Binance (останній трейд перед першим філом / на останньому),
    бо історії мідів HL немає; лише філи (перший→останній) — фолбек з
    прапорцем dump_fills (одним пострілом = 0%)."""
    hl, addr = ctx.get("hl"), (row.get("whale_addr") or "").strip()
    fills = hl.user_fills(addr, t0 - 600000, t0) if (hl is not None and addr) else None
    if fills is None:
        out["flags"].append("no_fills")
        return
    ep = whale_episode(fills, row.get("coin"), t0, close_only)
    if not ep:
        out["flags"].append("no_whale_fill")
        return
    if symbol and ep.get("dump_first_ts_ms") and ep.get("dump_last_ts_ms"):
        ref = _tape_last_px(symbol, ep["dump_first_ts_ms"] - 1, ctx)
        end = _tape_last_px(symbol, ep["dump_last_ts_ms"], ctx)
        side_first = None
        for f in fills:
            if int(f.get("time") or 0) == ep["dump_last_ts_ms"] and f.get("coin") == row.get("coin"):
                side_first = f.get("side")
                break
        if ref and end:
            # знак як у whale_episode: додатний = ціна пішла у бік тиску
            # кита (продає → впала; купує → зросла)
            mv = (end / ref - 1.0) * 100.0 * (1 if side_first == "B" else -1)
            ep["dump_move_pct"] = mv
            out["flags"].append("dump_tape")
        else:
            out["flags"].append("dump_fills")
    else:
        out["flags"].append("dump_fills")
    out.update(ep)


def settle_follow(row, ctx):
    """follow_trades.csv → колонки settlement або None (не закрита / без часу)."""
    t_in, s1 = _ts(row, ("open_ts_ms",), "date_open")
    t_out, s2 = _ts(row, ("close_ts_ms",), "date_close")
    if t_in is None or t_out is None or not row.get("trade_id"):
        return None
    long = (row.get("our_side") or "").upper() != "SHORT"
    costs = fnum(row.get("costs_pct")) or COSTS_DEF
    symbol = symbol_for(row.get("coin"), ctx.get("symbol_map"))
    out = _base(row["trade_id"], "fol", row, ctx, symbol, s1 if s1 == s2 else "mixed", costs)
    e_live = fnum(row.get("entry_px"))
    out["entry_px_live"], out["exit_px_live"] = e_live, fnum(row.get("exit_px"))
    out["exit_reason_tape"] = row.get("exit_reason") or ""
    e = _leg(out, "entry", symbol, t_in, "BUY" if long else "SELL", ctx)
    x = _leg(out, "exit", symbol, t_out, "SELL" if long else "BUY", ctx)
    _result(out, long, e, x, e_live, fnum(row.get("net_pct")), costs)
    _whale(out, row, t_in, ctx, close_only=False, symbol=symbol)
    return out


def _m30(row):
    """m30, або перша з m31..m33 (пізній вихід, як у server.py)."""
    for k in (30, 31, 32, 33):
        v = fnum(row.get("m%d" % k))
        if v is not None:
            return v
    return None


def settle_rev(row, ctx):
    """rev_trades.csv (entered=1) → колонки settlement або None.
    R2 — реплей тригера +0.3% від референсу стрічки у 10 хв; R8 — TP
    0.8×дамп або таймер 30 хв; решта R* — вихід на +30 хв (+ net60)."""
    if fnum(row.get("entered"), 0) != 1 or not row.get("sig_id"):
        return None
    strat = row.get("strategy") or ""
    is_r2, is_r8 = strat.startswith("R2"), ("R8" in strat)
    # t0 = детекція: detect_ts_ms, для не-R2 entry_ts_ms (вхід на детекті), інакше date
    t0, src = _ts(row, ("detect_ts_ms",) if is_r2 else ("detect_ts_ms", "entry_ts_ms"), "date")
    if t0 is None:
        return None
    long = (row.get("our_side") or "").upper() != "SHORT"
    costs = fnum(row.get("costs_pct")) or COSTS_DEF
    symbol = symbol_for(row.get("coin"), ctx.get("symbol_map"))
    out = _base("%s|%s" % (row["sig_id"], strat), "rev", row, ctx, symbol, src, costs)
    e_live, m30 = fnum(row.get("entry_px")), _m30(row)
    out["entry_px_live"] = e_live
    net_live = (m30 - costs) if m30 is not None else None
    if e_live and m30 is not None:
        out["exit_px_live"] = e_live * (1 + m30 / 100.0 * (1 if long else -1))
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    p0 = _ref_px(symbol, t0, ctx)
    entry_t = t0
    if p0 is not None and is_r2:
        hit = _first_cross(symbol, t0, t0 + R2_WINDOW_MS,
                           p0 * (1 + R2_BREAKOUT if long else 1 - R2_BREAKOUT), long, ctx)
        if hit is None:                  # стрічка тригера не бачила
            out.update({"entry_ts_ms": t0, "entry_src": "none", "net_live_pct": net_live,
                        "net_tape_pct": None, "net_official_pct": None})
            out["flags"].append("no_trigger")
            _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
            return out
        entry_t = hit[0]
    e = _leg(out, "entry", symbol, entry_t, side_in, ctx) if p0 is not None else None
    if e is None:
        out["entry_ts_ms"], out["entry_src"] = t0, "none"
    x = None
    if e is not None and is_r8:
        dump = abs(fnum(row.get("dump_move_pct")) or fnum(row.get("move_3m_pct")) or 0.0)
        tp = e * (1 + R8_TP_K * dump / 100.0 * (1 if long else -1))
        scan_from = entry_t + {"tape3s": 3000, "tape10s": 10000}.get(out["entry_src"], 0)
        hit = _first_cross(symbol, scan_from, entry_t + HOLD30_MS, tp, long, ctx) if dump > 0 else None
        if dump <= 0:
            out["flags"].append("no_dump")
        if hit is not None:
            out.update({"exit_ts_ms": hit[0], "exit_px_tape": tp, "exit_src": "tp",
                        "exit_reason_tape": "tp"})
            x = tp
        else:
            x = _leg(out, "exit", symbol, entry_t + HOLD30_MS, side_out, ctx)
            out["exit_reason_tape"] = "timer"
    elif e is not None:
        x = _leg(out, "exit", symbol, entry_t + HOLD30_MS, side_out, ctx)
        out["exit_reason_tape"] = "m30"
        x60 = exec_px(symbol, entry_t + HOLD60_MS, side_out, ctx)["px"]
        out["net60_tape_pct"] = _net(e, x60, long, costs)
    _result(out, long, e, x, e_live, net_live, costs)
    _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
    return out


def settle_twap(row, ctx):
    """twap_trades.csv → колонки settlement або None (no_price / без часу)."""
    em = fnum(row.get("exit_min"))
    if (row.get("exit_reason") or "") == "no_price" or em is None or not row.get("twap_id"):
        return None
    t_in, src = _ts(row, ("entry_ts_ms",), "date_entry")
    if t_in is None:
        return None
    t_out = t_in + int(em * MIN_MS)
    long = (row.get("our_side") or "").upper() != "SHORT"
    costs = fnum(row.get("costs_pct")) or COSTS_DEF
    symbol = symbol_for(row.get("coin"), ctx.get("symbol_map"))
    out = _base("%s|%s" % (row["twap_id"], row.get("strategy") or ""), "twap", row, ctx,
                symbol, src, costs)
    e_live, mx = fnum(row.get("entry_px")), fnum(row.get("m%d" % int(em)))
    out["entry_px_live"] = e_live
    if e_live and mx is not None:
        out["exit_px_live"] = e_live * (1 + mx / 100.0 * (1 if long else -1))
    out["exit_reason_tape"] = row.get("exit_reason") or ""
    e = _leg(out, "entry", symbol, t_in, "BUY" if long else "SELL", ctx)
    x = _leg(out, "exit", symbol, t_out, "SELL" if long else "BUY", ctx)
    _result(out, long, e, x, e_live, fnum(row.get("net60_pct")), costs)
    _whale(out, row, t_in, ctx, close_only=False, symbol=symbol)
    return out


# ── читання/запис CSV ───────────────────────────────────────────────────
def read_family(data_dir, name):
    """Рядки <name>.csv + усіх <name>.legacy-*.csv (legacy першими)."""
    base = os.path.join(data_dir, name)
    for path in sorted(glob.glob(base + ".legacy-*.csv")) + [base]:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            for r in csv.DictReader(f):
                if "eol" in r and r.get("eol") != "^":
                    continue                  # обірваний рядок (вартовий)
                yield r


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return "%.10g" % v
    if isinstance(v, (list, tuple)):
        return ";".join(str(x) for x in v)
    return str(v)


def append_settlement(data_dir, out):
    """Дописати рядок (заголовок — якщо файл новий; обірваний хвіст закривається)."""
    path = os.path.join(data_dir, "settlements.csv")
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    with open(path, "ab+") as f:
        f.seek(0, os.SEEK_END)
        if f.tell() == 0:
            w.writerow(HEADERS)
        else:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                f.write(b"\n")
        w.writerow([_fmt(out.get(h)) for h in HEADERS])
        f.write(buf.getvalue().encode("utf-8"))


def load_settlements(data_dir):
    """{key: рядок} з settlements.csv (+legacy); останній запис ключа виграє."""
    out = {}
    for r in read_family(data_dir, "settlements.csv"):
        if r.get("key"):
            out[r["key"]] = r
    return out


def make_ctx(data_dir, symbol_map=None, fetchers=None, now_ms=None, log=print):
    """Контекст розрахунку: {tape, hl, symbol_map, now_ms, log}. fetchers:
    {fetch_zip, fetch_rest, fetch_hl, rest_pace_s, hl_pace_s} — ін'єкція."""
    f = fetchers or {}
    tape = Tape(data_dir, f.get("fetch_zip"), f.get("fetch_rest"), log=log, now_ms=now_ms,
                rest_pace_s=f.get("rest_pace_s", 1.0))
    hl = HLFills(data_dir, f.get("fetch_hl"), log=log, now_ms=now_ms,
                 pace_s=f.get("hl_pace_s", 0.5))
    return {"tape": tape, "hl": hl, "symbol_map": symbol_map or {}, "now_ms": now_ms,
            "log": log}


FAMILIES = (   # (файл, fn, ключ, ms-колонка сортування, локальна дата)
    ("follow_trades.csv", settle_follow,
     lambda r: r.get("trade_id") or None, ("open_ts_ms",), "date_open"),
    ("rev_trades.csv", settle_rev,
     lambda r: ("%s|%s" % (r["sig_id"], r.get("strategy") or "")) if r.get("sig_id") else None,
     ("detect_ts_ms", "entry_ts_ms"), "date"),
    ("twap_trades.csv", settle_twap,
     lambda r: ("%s|%s" % (r["twap_id"], r.get("strategy") or "")) if r.get("twap_id") else None,
     ("entry_ts_ms",), "date_entry"),
)


def settle_pending(data_dir, symbol_map=None, limit=None, force=False, log=print,
                   fetchers=None, now_ms=None, since=None):
    """Розрахувати ще не розраховані закриті угоди (найстаріші першими).
    → (n_done, n_skipped, n_failed); окремий битий рядок логується, не валить
    прогін. force — перерахувати все заново (новий запис ключа виграє)."""
    ctx = make_ctx(data_dir, symbol_map, fetchers, now_ms, log)
    done = {} if force else load_settlements(data_dir)
    cands = []
    for name, fn, keyf, ms_cols, dcol in FAMILIES:
        seen = set()
        for row in read_family(data_dir, name):
            key = keyf(row)
            if not key or key in seen:
                continue
            seen.add(key)
            if key in done or (since and (row.get(dcol) or "")[:10] < since):
                continue
            cands.append((_ts(row, ms_cols, dcol)[0] or 0, key, fn, row))
    cands.sort(key=lambda c: c[0])
    if limit:
        cands = cands[:int(limit)]
    n_done = n_skip = n_fail = 0
    for _, key, fn, row in cands:
        try:
            out = fn(row, ctx)
            if out is None:
                n_skip += 1
                continue
            append_settlement(data_dir, out)
            n_done += 1
        except Exception as e:
            n_fail += 1
            log("[settle] збій %s: %r" % (key, e))
    log("[settle] done=%d skipped=%d failed=%d tape=%s hl=%s"
        % (n_done, n_skip, n_fail, ctx["tape"].stats, ctx["hl"].stats))
    return n_done, n_skip, n_fail


def main(argv=None):
    ap = argparse.ArgumentParser(description="Settlement paper-угод по стрічці Binance")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (локальна дата рядка)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--symbol-map", default=None, help='JSON, напр. {"KLUNC":"1000LUNCUSDT"}')
    a = ap.parse_args(argv)
    smap = json.loads(a.symbol_map) if a.symbol_map else None
    n = settle_pending(a.data_dir, symbol_map=smap, limit=a.limit, force=a.force,
                       since=a.since)
    print("settled=%d skipped=%d failed=%d" % n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

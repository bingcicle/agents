#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""settle.py — «розрахунок» (settlement) paper-угод по стрічці aggTrades
Binance USDⓈ-M futures: кожна закрита угода (follow / rev / twap) детерміновано
переоцінюється за реальними трейдами біржі одним правилом для старих і нових
рядків. Це АУДИТ ЗАПИСАНОГО виконання (час входу/виходу — з рядка угоди), а не
переторговування. Офіційний результат = ГІРШИЙ з live-запису і стрічки, коли
є обидва; інакше той, що є (колонка status: verified / tape_only / live_only /
none). Лише stdlib.
CLI: python3 settle.py --data-dir DIR [--since YYYY-MM-DD] [--limit N] [--force]
     [--symbol-map JSON]"""
import argparse, bisect, csv, glob, io, json, math, os, time, zipfile  # noqa: E401
import urllib.request
from array import array
from collections import OrderedDict

__all__ = ["Tape", "HLFills", "exec_px", "get_trades", "symbol_for", "parse_local", "settle_follow",
           "settle_rev", "settle_twap", "settle_pending", "load_settlements", "make_ctx",
           "whale_episode", "close_episode", "hl_user_fills", "HEADERS", "SETTLE_V", "Transient",
           "EP_GAP_MS", "CURVE_H", "TAPE_WAIT_MS"]


class Transient(Exception):
    """Транзиторний збій джерела (бюджет/мережа/429): fetcher КИДАЄ його —
    рядок НЕ фіксується (n_failed), наступний цикл спробує знову.
    None / b"" від fetcher-а = даних немає (no_tape / no_fills — назавжди,
    до --force). Дефолтні fetcher-и: 404 → b"", решта помилок → Transient
    (code = HTTP-статус, якщо був; 400 від aggTrades = символ не торгується
    на Binance → Tape запам'ятовує його як unlisted, див. UNLISTED_TTL_MS)."""

    def __init__(self, msg="", code=None):
        super().__init__(msg)
        self.code = code

SETTLE_V = "2"                     # v2: без stale-цін, епізод 300 с/startPosition, записані
                                   # часи виходу, status, curve_tape; рядки v1 перераховуються
UNLISTED_TTL_MS = 7 * 86400000     # символ, на який Binance відповів 400 (Invalid symbol):
                                   # стрічки немає — не питати тиждень (нові лістинги підхопляться)
ZIP_URL = ("https://data.binance.vision/data/futures/um/daily/aggTrades/"
           "{s}/{s}-aggTrades-{d}.zip")
REST_URL = "https://fapi.binance.com/fapi/v1/aggTrades"
HL_URL = "https://api.hyperliquid.xyz/info"
DAY_MS, HOUR_MS, MIN_MS = 86400000, 3600000, 60000
REST_BUCKET_MS = 600000            # REST-кеш 10-хв відрами (вікно < 1 год)
REST_MAX_AGE_MS = 48 * HOUR_MS     # REST лише для днів без zip у межах 48 год
LRU_MAX = 6                        # символо-днів у пам'яті (ZEC-день ≈ 45 МБ масивів)
REST_LRU_MAX = 32                  # 10-хв відер у пам'яті (крива 60–120 хв = до 13 відер)
ZIP_MISS_TTL_S = 3600              # 404 zip (день ще не викладений) — не перепитувати годину
TAPE_KEEP_DAYS = 14                # zip-и на диску старші за це — видаляються
TAPE_WAIT_MS = 72 * HOUR_MS        # без стрічки, а угода молодша — відкласти (zip ще не викладений)
_ZIP_MISS = {}                     # (symbol, day) -> ts 404
COSTS_DEF = 0.15                   # витрати за круг, % (як у server.py)
R2_BREAKOUT = 0.003                # R2: вхід після руху +0.3% від p0
R2_WINDOW_MS = 600000              # R2: вікно 10 хв
R8_TP_K = 0.8                      # R8: TP = 0.8 × дамп
HOLD30_MS, HOLD60_MS = 30 * MIN_MS, 60 * MIN_MS
EP_GAP_MS = 300000                 # епізод кита: пауза між філами ≤ 5 хв (= FC_MAX_EPISODE_S бота)
EP_POS_TOL = 0.01                  # епізод: допуск «позиція не виросла» — 1% від startPosition
CURVE_H = {"fol": 60, "rev": 60, "twap": 120}   # горизонт кривої по стрічці, хв
CURVE_MARGIN_MS = 30000            # крива остаточна, якщо розрахована після entry+H+запас
K1000 = ("PEPE", "BONK", "SHIB", "FLOKI", "LUNC", "DOGS", "NEIRO")

HEADERS = ["key", "family", "strategy", "coin", "symbol", "settle_v", "settled_at",
           "ts_src", "entry_ts_ms", "entry_px_live", "entry_px_tape", "entry_src",
           "entry_age_ms", "exit_ts_ms", "exit_px_live", "exit_px_tape", "exit_src",
           "exit_reason_tape", "gross_tape_pct", "costs_pct", "net_live_pct",
           "net_tape_pct", "net_official_pct", "net60_tape_pct", "entry_bias_pct",
           "whale_fill_ts_ms", "whale_px", "lag_s", "dump_first_ts_ms",
           "dump_last_ts_ms", "dump_dur_s", "dump_move_pct", "dump_bucket",
           "curve_tape", "flags", "status", "eol"]


# ── дрібні хелпери ──────────────────────────────────────────────────────
def fnum(v, default=None):
    """float або default (порожнє / бите / non-finite)."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _ms(v):
    """epoch-мс колонка → int або None (порожнє / не epoch-мс)."""
    x = fnum(v)
    return int(x) if x is not None and x > 1e12 else None


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
    except Exception as e:
        code = getattr(e, "code", None)
        if code == 404:
            return b""           # даних справді немає (день без zip) — не збій
        # мережа/429/5xx — рядок відкладається; 400 — Tape вирішує (unlisted)
        raise Transient("%s: %r" % (url[:80], e), code)


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
    """zip aggTrades → (array ts_ms, array px) у порядку файлу (Binance пише
    його за agg_trade_id — трейди однієї мс у порядку біржі). Якщо час у файлі
    не монотонний — сортування за (ts, agg_trade_id), НЕ за ціною.
    Рядок заголовка може бути відсутній (старі файли)."""
    ts, px, aid = array("q"), array("d"), array("q")
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
                try:
                    a = int(p[0])
                except ValueError:
                    a = len(ts)         # без id — порядок файлу
                ts.append(t)
                px.append(x)
                aid.append(a)
                if t < last:
                    ordered = False
                last = t
    if not ordered:
        order = sorted(range(len(ts)), key=lambda i: (ts[i], aid[i], i))
        ts, px = array("q", (ts[i] for i in order)), array("d", (px[i] for i in order))
    return ts, px


# ── стрічка трейдів ─────────────────────────────────────────────────────
class Tape:
    """Стрічка aggTrades: zip-день (диск <data_dir>/tape + LRU) з REST-фолбеком
    10-хв відрами (<data_dir>/tape/rest) для днів без zip у межах 48 год.
    Мережа — лише через fetch_zip / fetch_rest (ін'єкція для тестів).
    Неповні (поточні) відра тримаються лише в пам'яті на життя об'єкта —
    один цикл settle_pending (крива 60–120 хв = десятки вікон на рядок)."""

    def __init__(self, data_dir, fetch_zip=None, fetch_rest=None, log=print,
                 now_ms=None, lru_max=LRU_MAX, rest_pace_s=1.0, rest_lru_max=REST_LRU_MAX):
        self.dir = os.path.join(data_dir, "tape")
        self.rest_dir = os.path.join(self.dir, "rest")
        os.makedirs(self.rest_dir, exist_ok=True)
        self.fetch_zip = fetch_zip or default_fetch_zip
        self.fetch_rest = fetch_rest or default_fetch_rest
        self.log, self.now_ms, self.lru_max, self.pace = log, now_ms, lru_max, rest_pace_s
        self.rest_lru_max = rest_lru_max
        self._lru = OrderedDict()       # (symbol, day) → (ts, px)
        self._lru_rest = OrderedDict()  # (symbol, b0) → (ts, px)
        self._miss = set()              # ключі без даних у цьому запуску
        self._t_rest = 0.0
        self.stats = {"zip_fetch": 0, "zip_miss": 0, "rest_req": 0, "rest_fail": 0, "unlisted": 0}
        # символи, що не торгуються на Binance (REST 400 Invalid symbol):
        # <tape>/unlisted.json {symbol: ts_ms}; без цього кожна монета, якої
        # нема на Binance, коштувала б запит і рядок логу КОЖНИЙ цикл 72 год
        self._unl_path = os.path.join(self.dir, "unlisted.json")
        u = _load_json(self._unl_path)
        self._unlisted = u if isinstance(u, dict) else {}

    def _now(self):
        return self.now_ms if self.now_ms is not None else int(time.time() * 1000)

    def is_unlisted(self, symbol):
        """True, якщо Binance відповів 400 на symbol не давніше за UNLISTED_TTL_MS."""
        t = fnum(self._unlisted.get(symbol))
        return t is not None and self._now() - t < UNLISTED_TTL_MS

    def _mark_unlisted(self, symbol):
        self._unlisted[symbol] = self._now()
        self.stats["unlisted"] += 1
        try:
            _save_json(self._unl_path, self._unlisted)
        except OSError as e:
            self.log("[settle] unlisted.json: %r" % e)
        self.log("[settle] %s: Binance 400 (символ не торгується) — стрічки немає, "
                 "не питати %d дн." % (symbol, UNLISTED_TTL_MS // DAY_MS))

    @staticmethod
    def _put(lru, cap, key, val):
        lru[key] = val
        lru.move_to_end(key)
        while len(lru) > cap:
            lru.popitem(last=False)

    @staticmethod
    def _get(lru, key):
        if key in lru:
            lru.move_to_end(key)
        return lru.get(key)

    def day(self, symbol, day):
        """Масиви (ts, px) за UTC-день 'YYYY-MM-DD' або None (zip нема)."""
        key = (symbol, day)
        v = self._get(self._lru, key)
        if v is not None or key in self._miss:
            return v
        path = os.path.join(self.dir, "%s-aggTrades-%s.zip" % (symbol, day))
        raw = None
        if os.path.exists(path):
            with open(path, "rb") as f:
                raw = f.read()
        else:
            miss_ts = _ZIP_MISS.get(key)
            if miss_ts is not None and time.time() - miss_ts < ZIP_MISS_TTL_S:
                self._miss.add(key)          # 404 недавно — не смикати CDN щоциклу
                return None
            self.stats["zip_fetch"] += 1
            raw = self.fetch_zip(ZIP_URL.format(s=symbol, d=day))   # Transient — нагору
            if raw is not None and raw == b"":
                _ZIP_MISS[key] = time.time()
            if raw:
                with open(path + ".tmp", "wb") as f:
                    f.write(raw)
                os.replace(path + ".tmp", path)
        if raw:
            try:
                v = parse_agg_zip(raw)
                self._put(self._lru, self.lru_max, key, v)
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
        [[ts, px, agg_id], …] у порядку (ts, agg_id) або None при збої.
        Трейди однієї мс — у порядку біржі (agg_id), не за ціною; без id
        (a = −1) лишається порядок надходження."""
        out, start, last_id, pages = [], b0, -1, 0
        while start < b1 and pages < 200:
            self._t_rest = _pace(self._t_rest, self.pace)
            self.stats["rest_req"] += 1
            pages += 1
            try:
                rows = self.fetch_rest("%s?symbol=%s&startTime=%d&endTime=%d&limit=1000"
                                       % (REST_URL, symbol, start, b1 - 1))
            except Transient as e:
                if getattr(e, "code", None) != 400:
                    raise                    # мережа/429/5xx — рядок відкладається
                self._mark_unlisted(symbol)  # Invalid symbol — стрічки немає взагалі
                rows = None
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
                out.append([t, p, a])
                n += 1
            if len(rows) < 1000 or n == 0:
                break
            start = out[-1][0] if last_id >= 0 else out[-1][0] + 1
        return [r for _, r in sorted(enumerate(out), key=lambda ir: (ir[1][0], ir[1][2], ir[0]))]

    def _rest_bucket(self, symbol, b0):
        """10-хв відро через REST: диск (лише повні відра) → пам'ять (і неповні —
        на життя об'єкта). Рядки кешу: [ts, px, agg_id] (v2) або [ts, px] (v1)."""
        key = (symbol, b0)
        v = self._get(self._lru_rest, key)
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
        self._put(self._lru_rest, self.rest_lru_max, key, v)
        return v

    def window(self, symbol, t0, t1):
        """Усі трейди symbol з ts у [t0, t1] (дні/відра зшиваються) → (ts, px);
        для unlisted-символу — порожньо без жодного запиту."""
        ts_out, px_out = array("q"), array("d")
        if self.is_unlisted(symbol):
            return ts_out, px_out
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
    [t, t+10с] (tape10s), інакше none — трейд ДО t не є доказом виконання
    (stale-ціни немає). side: BUY → max, SELL → min. → {px, src, n, age_ms}"""
    t_ms = int(t_ms)
    ts, px = _tape(cfg).window(symbol, t_ms, t_ms + 10000)
    worst = max if side == "BUY" else min
    for w, src in ((3000, "tape3s"), (10000, "tape10s")):
        j = bisect.bisect_right(ts, t_ms + w)
        if j > 0:
            return {"px": worst(px[:j]), "src": src, "n": j, "age_ms": 0}
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
        # у пам'яті — і неповна година: ctx живе один цикл, а всі рядки одного
        # епізоду кита ділять адресу/годину (6–15 рядків = 1 запит, не 15)
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


def _ftime(f):
    x = fnum(f.get("time"))
    return int(x) if x is not None else -1


def _is_close(f):
    """Філ закриває позицію: dir з «Close», ліквідація / ADL (текст dir або
    поле liquidation) — як класифікує live-бот."""
    d = str(f.get("dir") or "")
    return ("Close" in d) or ("Liquidat" in d) or ("Auto-Delever" in d) or bool(f.get("liquidation"))


def _sp_sz(f):
    """(|startPosition|, |sz|) філа; None, якщо поля немає / бите."""
    sp, sz = fnum(f.get("startPosition")), fnum(f.get("sz"))
    return (abs(sp) if sp is not None else None), (abs(sz) if sz is not None else None)


def close_episode(fills, coin, t0_ms, close_only):
    """ЄДИНЕ правило епізоду закриття кита (спільне з live-ботом, чиста функція).
    Кандидати — філи coin з time у [t0−600с, t0]. Кінець епізоду = останній
    CLOSE-філ (dir містить «Close», або ліквідація/ADL: «Liquidat» /
    «Auto-Delever» / liquidation); при close_only=False — будь-який останній
    філ. Далі назад включається попередній філ j перед уже включеним i, поки:
      • той самий бік («B»/«A») і (при close_only) теж close-філ;
      • пауза time_i − time_j ≤ EP_GAP_MS (300 000 мс = FC_MAX_EPISODE_S бота);
      • позиція між ними не виросла: sp = |startPosition|, sz = розмір —
        |sp_i − (sp_j − sz_j)| ≤ 0.01·max(sp_j, 1e-9) + 1e-9, тобто стартова
        позиція пізнішого філа = залишок після попереднього (без доливу /
        перевідкриття посередині); без startPosition у будь-якого з двох —
        лише правило паузи.
    → (first_fill, last_fill, fills_in_episode) або (None, None, [])."""
    fs = sorted((f for f in fills if f.get("coin") == coin
                 and t0_ms - 600000 <= _ftime(f) <= t0_ms), key=_ftime)
    idx = None
    for k in range(len(fs) - 1, -1, -1):
        if not close_only or _is_close(fs[k]):
            idx = k
            break
    if idx is None:
        return None, None, []
    side, k = fs[idx].get("side"), idx
    while k > 0:
        i, j = fs[k], fs[k - 1]
        if j.get("side") != side or (close_only and not _is_close(j)):
            break
        if _ftime(i) - _ftime(j) > EP_GAP_MS:
            break
        sp_i, _ = _sp_sz(i)
        sp_j, sz_j = _sp_sz(j)
        if sp_i is not None and sp_j is not None and sz_j is not None \
                and abs(sp_i - (sp_j - sz_j)) > EP_POS_TOL * max(sp_j, 1e-9) + 1e-9:
            break                            # долив / перевідкриття між філами
        k -= 1
    return fs[k], fs[idx], fs[k:idx + 1]


def _episode_cols(first, last, t0_ms):
    """Колонки епізоду з першого/останнього філа: сирі ціни філів (не VWAP);
    dump_move_pct > 0 = ціна пішла у бік тиску кита (продає → впала)."""
    t_last, t_first = _ftime(last), _ftime(first)
    px_last, px_first = fnum(last.get("px")), fnum(first.get("px"))
    dur = (t_last - t_first) / 1000.0
    move = None
    if px_last and px_first:
        move = (px_last / px_first - 1) * 100 * (1 if last.get("side") == "B" else -1)
    return {"whale_fill_ts_ms": t_last, "whale_px": px_last,
            "lag_s": (t0_ms - t_last) / 1000.0,
            "dump_first_ts_ms": t_first, "dump_last_ts_ms": t_last, "dump_dur_s": dur,
            "dump_move_pct": move,
            "dump_bucket": 0 if dur >= 300 else min(5, max(1, int(math.ceil(dur / 60.0))))}


def whale_episode(fills, coin, t0_ms, close_only):
    """Епізод закриття кита перед t0 (правило — close_episode) → dict колонок
    whale_fill_ts_ms / whale_px / lag_s / dump_first_ts_ms / dump_last_ts_ms /
    dump_dur_s / dump_move_pct / dump_bucket, або {} без філів."""
    first, last, _ = close_episode(fills, coin, t0_ms, close_only)
    return _episode_cols(first, last, t0_ms) if first is not None else {}


# ── розрахунок сімей ────────────────────────────────────────────────────
def _ts(row, ms_cols, local_col):
    """Час рядка: перша числова epoch-мс колонка → 'ms', інакше локальний
    рядок → 'local'; (None, None) якщо нічого."""
    for c in ms_cols:
        v = _ms(row.get(c))
        if v is not None:
            return v, "ms"
    t = parse_local(row.get(local_col)) if local_col else None
    return (t, "local") if t else (None, None)


def _base(key, family, row, ctx, symbol, ts_src, costs):
    return {"key": key, "family": family, "strategy": row.get("strategy") or "",
            "coin": row.get("coin") or "", "symbol": symbol, "settle_v": SETTLE_V,
            "settled_at": _dt(ctx.get("now_ms") or int(time.time() * 1000)),
            "ts_src": ts_src, "costs_pct": costs, "flags": [], "curve_tape": "", "eol": "^"}


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


def _result(out, long, e_tape, x_tape, e_live, net_live, costs, tape_flag="no_tape"):
    """Спільний хвіст: gross/net по стрічці, зсув входу, офіційний результат і
    status. net_official = min(net_live, net_tape), коли є обидва; інакше той,
    що є (записаний результат ніколи не зникає). status: verified — обидві ноги
    по стрічці і є net_live; tape_only — стрічка є, net_live немає; live_only —
    ноги по стрічці немає (no_tape / partial / no_trigger), live є; none — нічого.
    flags: no_tape (нема входу по стрічці; tape_flag=None — не ставити, напр.
    no_trigger), partial (вхід є, а виходу або net_live немає)."""
    net_tape = None
    if e_tape is None:
        if tape_flag:
            out["flags"].append(tape_flag)
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
    if net_tape is not None:
        out["status"] = "verified" if net_live is not None else "tape_only"
    else:
        out["status"] = "live_only" if net_live is not None else "none"
    return out


def _curve(out, symbol, entry_t, e, side_out, long, costs, h, ctx):
    """Крива по стрічці: для k=1..h net (gross − costs) від ціни входу e зі
    стрічки до exec_px(entry_t + k хв, side_out); "" де трейдів немає; колонка
    curve_tape = ";"-з'єднані "%.4f". Без входу по стрічці — порожня."""
    if e is None or entry_t is None:
        out["curve_tape"] = ""
        return
    vals = []
    for k in range(1, h + 1):
        x = exec_px(symbol, entry_t + k * MIN_MS, side_out, ctx)["px"]
        n = _net(e, x, long, costs)
        vals.append("" if n is None else "%.4f" % n)
    out["curve_tape"] = ";".join(vals)


def _tape_last_px(symbol, t_ms, ctx):
    """Ціна останнього трейду стрічки з ts ≤ t_ms СТРОГО (вікно 10 с назад).
    Виклик з first_ts−1 = референс ДО першого філа. Transient — нагору."""
    try:
        _, px = get_trades(symbol, t_ms - 10000, t_ms, ctx)
    except Transient:
        raise
    except Exception:
        return None
    return px[-1] if len(px) else None


def _whale(out, row, t0, ctx, close_only, symbol=None):
    """Філи кита перед t0 → lag/dump-колонки; збій → flag no_fills.
    Епізод — close_episode (одне правило з live-ботом). Рух епізоду
    (dump_move_pct) — як у live (server.py _rev_ref_px): від ціни ДО першого
    філа до ціни на останньому; тут референс = останній трейд Binance СТРОГО
    перед першим філом, кінець = останній трейд ≤ останнього філа (прапорець
    dump_tape; знак: додатний = у бік тиску кита); лише філи (сирі ціни
    перший→останній) — фолбек dump_fills (одним пострілом = 0%)."""
    hl, addr = ctx.get("hl"), (row.get("whale_addr") or "").strip()
    fills = hl.user_fills(addr, t0 - 600000, t0) if (hl is not None and addr) else None
    if fills is None:
        out["flags"].append("no_fills")
        return
    first, last, _ = close_episode(fills, row.get("coin"), t0, close_only)
    if first is None:
        out["flags"].append("no_whale_fill")
        return
    ep = _episode_cols(first, last, t0)
    ref = end = None
    if symbol and ep.get("dump_first_ts_ms") and ep.get("dump_last_ts_ms"):
        ref = _tape_last_px(symbol, ep["dump_first_ts_ms"] - 1, ctx)
        end = _tape_last_px(symbol, ep["dump_last_ts_ms"], ctx)
    if ref and end:
        ep["dump_move_pct"] = (end / ref - 1.0) * 100.0 * (1 if last.get("side") == "B" else -1)
        out["flags"].append("dump_tape")
    else:
        out["flags"].append("dump_fills")
    out.update(ep)


def settle_follow(row, ctx):
    """follow_trades.csv → колонки settlement або None (не закрита / без часу).
    Вхід/вихід — записані open_ts_ms/close_ts_ms (старі рядки — локальні
    дати); крива по стрічці 60 хв."""
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
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    e = _leg(out, "entry", symbol, t_in, side_in, ctx)
    x = _leg(out, "exit", symbol, t_out, side_out, ctx)
    _result(out, long, e, x, e_live, fnum(row.get("net_pct")), costs)
    _curve(out, symbol, t_in, e, side_out, long, costs, CURVE_H["fol"], ctx)
    _whale(out, row, t_in, ctx, close_only=False, symbol=symbol)
    return out


def _m30(row):
    """m30, або перша з m31..m33 (пізній вихід, як у server.py)."""
    for k in (30, 31, 32, 33):
        v = fnum(row.get("m%d" % k))
        if v is not None:
            return v
    return None


def _r8_tp_flag(out, row, symbol, entry_t, e, long, ctx):
    """R8: реплей TP по стрічці від входу — ЛИШЕ прапорець tp_tape_hit /
    tp_tape_miss, ціни не міняє. TP = записаний tp_px (те, чого чекав бот);
    без нього — 0.8×дамп від ціни входу зі стрічки; без дампу — no_dump."""
    tp = fnum(row.get("tp_px"))
    if tp is None:
        dump = abs(fnum(row.get("dump_move_pct")) or fnum(row.get("move_3m_pct")) or 0.0)
        if dump <= 0:
            out["flags"].append("no_dump")
            return
        tp = e * (1 + R8_TP_K * dump / 100.0 * (1 if long else -1))
    scan_from = entry_t + {"tape3s": 3000, "tape10s": 10000}.get(out.get("entry_src"), 0)
    hit = _first_cross(symbol, scan_from, entry_t + HOLD30_MS, tp, long, ctx)
    out["flags"].append("tp_tape_hit" if hit is not None else "tp_tape_miss")


def settle_rev(row, ctx):
    """rev_trades.csv (entered=1) → колонки settlement або None.
    Аудит ЗАПИСАНОГО виконання: вхід = entry_ts_ms (v2.16; для R2 — вхід на
    пробої), у старих рядків — локальний date; вихід = exit_ts_ms (таймер / TP /
    пізній), без нього або при no_price — рівно +30 хв від входу (m30).
    R2 без entry_ts_ms — реплей тригера +0.3% від референсу стрічки у 10 хв
    (не знайдено → no_trigger, результат лише live). R2 з entry_ts_ms — той
    самий реплей від date (або entry−10 хв) до записаного входу ЛИШЕ як
    прапорець no_trigger; ціни — на записаних часах. R8 — ціна на записаному
    виході + прапорець tp_tape_hit/miss (реплей TP по стрічці). Не-R8 — ще
    net60 на +60 хв. Крива по стрічці 60 хв від входу."""
    if fnum(row.get("entered"), 0) != 1 or not row.get("sig_id"):
        return None
    strat = row.get("strategy") or ""
    is_r2, is_r8 = strat.startswith("R2"), ("R8" in strat)
    t_rec = _ms(row.get("entry_ts_ms"))          # записаний вхід (v2.16)
    # t0 = детекція: detect_ts_ms, для не-R2 entry_ts_ms (вхід на детекті), інакше date
    t0, _ = _ts(row, ("detect_ts_ms",) if is_r2 else ("detect_ts_ms", "entry_ts_ms"), "date")
    if t0 is None and t_rec is None:
        return None
    if t0 is None:
        t0 = t_rec - R2_WINDOW_MS                # R2 без date: вікно тригера до входу
    long = (row.get("our_side") or "").upper() != "SHORT"
    costs = fnum(row.get("costs_pct")) or COSTS_DEF
    symbol = symbol_for(row.get("coin"), ctx.get("symbol_map"))
    out = _base("%s|%s" % (row["sig_id"], strat), "rev", row, ctx, symbol,
                "ms" if t_rec is not None else "local", costs)
    e_live, m30 = fnum(row.get("entry_px")), _m30(row)
    out["entry_px_live"] = e_live
    x_live_px, x_reason = fnum(row.get("exit_px")), (row.get("exit_reason") or "")
    x_rec = _ms(row.get("exit_ts_ms")) if x_reason not in ("", "no_price") else None
    if e_live and x_live_px and x_reason and x_reason != "no_price":
        # v2.16: вихід зі стакану (таймер/TP) — як рахує API
        g_live = (x_live_px / e_live - 1) * 100.0 * (1 if long else -1)
        net_live, out["exit_px_live"] = g_live - costs, x_live_px
    elif x_reason == "no_price":
        net_live = None
    else:
        net_live = (m30 - costs) if m30 is not None else None
        if e_live and m30 is not None:
            out["exit_px_live"] = e_live * (1 + m30 / 100.0 * (1 if long else -1))
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    entry_t, ref_ok = (t_rec if t_rec is not None else t0), True
    if is_r2:
        p0 = _ref_px(symbol, t0, ctx)
        level = p0 * (1 + R2_BREAKOUT if long else 1 - R2_BREAKOUT) if p0 else None
        if t_rec is None:
            # старий рядок без часу входу: вхід = перший трейд, що перетнув рівень
            ref_ok = p0 is not None
            hit = _first_cross(symbol, t0, t0 + R2_WINDOW_MS, level, long, ctx) if ref_ok else None
            if ref_ok and hit is None:       # стрічка тригера не бачила — угоди по стрічці немає
                out.update({"entry_ts_ms": t0, "entry_src": "none", "exit_reason_tape": ""})
                out["flags"].append("no_trigger")
                _result(out, long, None, None, e_live, net_live, costs, tape_flag=None)
                _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
                return out
            if hit is not None:
                entry_t = hit[0]
        elif level and _first_cross(symbol, t0, t_rec, level, long, ctx) is None:
            out["flags"].append("no_trigger")    # стрічка не бачила пробою до записаного входу
    e = _leg(out, "entry", symbol, entry_t, side_in, ctx) if ref_ok else None
    if e is None:
        out["entry_ts_ms"], out["entry_src"] = entry_t, "none"
    x_t = x_rec if x_rec is not None else entry_t + HOLD30_MS
    out["exit_reason_tape"] = x_reason if x_rec is not None else "m30"
    x = None
    if e is not None:
        x = _leg(out, "exit", symbol, x_t, side_out, ctx)
        if is_r8:
            _r8_tp_flag(out, row, symbol, entry_t, e, long, ctx)
        else:
            x60 = exec_px(symbol, entry_t + HOLD60_MS, side_out, ctx)["px"]
            out["net60_tape_pct"] = _net(e, x60, long, costs)
    else:
        out["exit_ts_ms"] = x_t
    _result(out, long, e, x, e_live, net_live, costs)
    _curve(out, symbol, entry_t, e, side_out, long, costs, CURVE_H["rev"], ctx)
    _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
    return out


def settle_twap(row, ctx):
    """twap_trades.csv → колонки settlement або None (no_price / без часу).
    Вихід = записаний exit_ts_ms, без нього entry + exit_min; exit_px_live =
    записаний exit_px, без нього з m{exit_min}. Крива по стрічці 120 хв."""
    em = fnum(row.get("exit_min"))
    if (row.get("exit_reason") or "") == "no_price" or em is None or not row.get("twap_id"):
        return None
    t_in, src = _ts(row, ("entry_ts_ms",), "date_entry")
    if t_in is None:
        return None
    x_rec = _ms(row.get("exit_ts_ms"))
    t_out = x_rec if x_rec is not None else t_in + int(em * MIN_MS)
    long = (row.get("our_side") or "").upper() != "SHORT"
    costs = fnum(row.get("costs_pct")) or COSTS_DEF
    symbol = symbol_for(row.get("coin"), ctx.get("symbol_map"))
    out = _base("%s|%s" % (row["twap_id"], row.get("strategy") or ""), "twap", row, ctx,
                symbol, src, costs)
    e_live, x_live = fnum(row.get("entry_px")), fnum(row.get("exit_px"))
    mx = fnum(row.get("m%d" % int(em)))
    out["entry_px_live"] = e_live
    if x_live:
        out["exit_px_live"] = x_live
    elif e_live and mx is not None:
        out["exit_px_live"] = e_live * (1 + mx / 100.0 * (1 if long else -1))
    out["exit_reason_tape"] = row.get("exit_reason") or ""
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    e = _leg(out, "entry", symbol, t_in, side_in, ctx)
    x = _leg(out, "exit", symbol, t_out, side_out, ctx)
    _result(out, long, e, x, e_live, fnum(row.get("net60_pct")), costs)
    _curve(out, symbol, t_in, e, side_out, long, costs, CURVE_H["twap"], ctx)
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
    """Дописати рядок (заголовок — якщо файл новий; обірваний хвіст закривається).
    Файл з ЧУЖИМ заголовком (стара версія колонок) ротується у
    settlements.csv.legacy-<ts>.csv — read_family/load_settlements його читають."""
    path = os.path.join(data_dir, "settlements.csv")
    hdr = ",".join(HEADERS)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            first = f.readline().rstrip("\r\n")
        if first != hdr:
            os.replace(path, "%s.legacy-%d.csv" % (path, int(time.time())))
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


MIN_ALGO_V = (2, 10)               # рядки старіших версій ніде не показуються


def _vt(v):
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except (ValueError, AttributeError):
        return (0,)


def _prune_tape(data_dir, log=print):
    """zip-и стрічки старші за TAPE_KEEP_DAYS — геть (ZEC-день = 36 МБ; повторний
    розрахунок --force їх завантажить знову)."""
    try:
        cut = time.time() - TAPE_KEEP_DAYS * 86400
        n = 0
        for f in glob.glob(os.path.join(data_dir, "tape", "*.zip")):
            if os.path.getmtime(f) < cut:
                os.remove(f)
                n += 1
        if n:
            log("[settle] видалено старих zip: %d" % n)
    except OSError as e:
        log("[settle] чистка zip: %r" % e)


def _curve_due_ms(srow):
    """Момент, після якого крива по стрічці рядка остаточна: entry + H хв + запас;
    None, якщо часу входу немає."""
    e = fnum(srow.get("entry_ts_ms"))
    if e is None:
        return None
    return int(e) + CURVE_H.get(srow.get("family"), 60) * MIN_MS + CURVE_MARGIN_MS


def _settle_final(srow):
    """Рядок settlements остаточний: поточна версія розрахунку І розрахований
    після горизонту кривої (інакше крива обірвана — перерахунок, коли дозріє)."""
    if srow.get("settle_v") != SETTLE_V:
        return False
    due, at = _curve_due_ms(srow), parse_local(srow.get("settled_at"))
    return due is None or at is None or at >= due


def _tape_pending(out, now_ms):
    """Стрічки немає (вхід або вихід без трейдів), а угода молодша за
    TAPE_WAIT_MS (72 год) — zip ще не викладений / REST-прогалина: не фіксувати,
    спробувати наступним циклом. no_trigger — вердикт стрічки, не її брак;
    unlisted — символу на Binance немає, чекати нема чого."""
    fl = out.get("flags") or []
    if "no_trigger" in fl or "unlisted" in fl:
        return False
    missing = ("no_tape" in fl) or (out.get("entry_px_tape") is not None
                                    and out.get("exit_px_tape") is None)
    if not missing:
        return False
    t = fnum(out.get("entry_ts_ms"))
    return t is not None and now_ms - t < TAPE_WAIT_MS


def settle_pending(data_dir, symbol_map=None, limit=None, force=False, log=print,
                   fetchers=None, now_ms=None, since=None):
    """Розрахувати ще не розраховані закриті угоди (НОВІШІ першими).
    → (n_done, n_skipped, n_failed); окремий битий рядок логується, не валить
    прогін. force — перерахувати все заново (новий запис ключа виграє).
    Рядок без стрічки (вхід/вихід) молодший за 72 год — не фіксується
    (n_failed, рахується у limit спроб), старший — фіксується назавжди.
    Рядок, розрахований до горизонту кривої (entry + 60/120 хв), або старої
    версії SETTLE_V — перераховується, коли горизонт минув."""
    ctx = make_ctx(data_dir, symbol_map, fetchers, now_ms, log)
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    prev = {} if force else load_settlements(data_dir)
    cands = []
    n_old = 0
    for name, fn, keyf, ms_cols, dcol in FAMILIES:
        seen = set()
        for row in read_family(data_dir, name):
            key = keyf(row)
            if not key or key in seen:
                continue
            seen.add(key)
            if since and (row.get(dcol) or "")[:10] < since:
                continue
            sr = prev.get(key)
            if sr is not None:
                if _settle_final(sr):
                    continue                 # розраховано остаточно
                due = _curve_due_ms(sr)
                if sr.get("settle_v") == SETTLE_V and due is not None and now < due:
                    continue                 # крива ще не дозріла — перерахунок пізніше
            if (row.get("algo_v") or "").strip() and _vt(row.get("algo_v")) < MIN_ALGO_V:
                n_old += 1               # рядки до 2.10 ніде не показуються — не рахуємо
                continue
            cands.append((_ts(row, ms_cols, dcol)[0] or 0, key, fn, row))
    # НОВІШІ першими: свіжі угоди отримують офіційний net за хвилини, бек-лог
    # доїжджає далі; limit — лише на СПРОБИ (рядки, що дають None, без мережі)
    cands.sort(key=lambda c: c[0], reverse=True)
    settle_pending.last_pending = len(cands)     # бек-лог (/status)
    n_done = n_skip = n_fail = n_try = 0
    for _, key, fn, row in cands:
        if limit and n_try >= int(limit):
            break
        try:
            out = fn(row, ctx)
            if out is None:
                n_skip += 1
                continue
            n_try += 1
            if out.get("symbol") and ctx["tape"].is_unlisted(out["symbol"]) \
                    and "unlisted" not in out["flags"]:
                out["flags"].append("unlisted")  # стрічки не буде — фіксуємо як live_only
            if _tape_pending(out, now):
                n_fail += 1
                log("[settle] %s: стрічка ще недоступна — відкладено" % key)
                continue
            append_settlement(data_dir, out)
            n_done += 1
        except Transient as e:
            # джерело недоступне (мережа/429/бюджет) — рядок не фіксуємо,
            # наступний цикл спробує знову
            n_fail += 1
            n_try += 1
            log("[settle] %s: відкладено — %s" % (key, e))
        except Exception as e:
            n_fail += 1
            n_try += 1
            log("[settle] збій %s: %r" % (key, e))
    if n_old:
        log("[settle] пропущено рядків до v%s: %d" % (".".join(map(str, MIN_ALGO_V)), n_old))
    _prune_tape(data_dir, log)
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

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
           "EP_GAP_MS", "CURVE_H", "TAPE_WAIT_MS", "SIM_COMMISSION"]


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

SETTLE_V = "5"                     # v2: без stale-цін, епізод 300 с/startPosition, записані
                                   # часи виходу, status, curve_tape; v3 (аудит-3): факти
                                   # позиції кита (full_close/partial_close, база R7), tape_gap,
                                   # curve_n, витрати за ногою; v4 (аудит v2.17 F/G): реплей TP
                                   # R8 (ринковий після перетину + лімітний), фандинг Binance за
                                   # утримання, повнота стрічки за завершеним обходом (cover_ok),
                                   # насичення історії HL (hl_sat), curve_final — рядки v1/v2
                                   # перераховуються (сервер показує їх як pending), v3 —
                                   # придатні (ціни ті самі), перераховуються у чергу;
                                   # v5 (аудит v2.19 №3/№4/№6/№10): Follow звіряється з
                                   # ТРИГЕРНИМ філом (trig_hash / fill_ts_ms), версійований
                                   # кеш REST/HL із доказом повноти (старі файли — знову),
                                   # R8: діра на шляху до TP / без ціни після виявлення →
                                   # status tape_thin (не verified), фінал лише після
                                   # перерахунку за 72 год, фандинг у кривій і net60, live-net
                                   # мінус фандинг рядка — рядки <5 сервер показує як pending
CACHE_V = 3                        # формат кеш-файлів REST-відер / HL-годин (dict із доказом повноти)
TP_DETECT_MS = 2000                # R8-реплей: затримка виявлення перетину TP (поллер міда 5 с/2)
FUND_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
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
CURVE_FULL = 1.0                   # крива «повна» лише при 100% точок (аудит v2.17 G: 54/60
                                   # робили рядок остаточним назавжди); PnL остаточний окремо
COVER_EDGE_MS = 60000              # покриття вікна стрічкою: трейд у перших/останніх 60 с
RETRY_BASE_MS = 10 * MIN_MS        # бекоф відкладеного рядка: 10 хв × 2^(n−1), стеля 6 год
RETRY_CAP_MS = 6 * HOUR_MS
SIM_COMMISSION = 0.0005            # Binance taker 0.05% за сторону (як у server.py)
K1000 = ("PEPE", "BONK", "SHIB", "FLOKI", "LUNC", "DOGS", "NEIRO")

HEADERS = ["key", "family", "strategy", "coin", "symbol", "settle_v", "settled_at",
           "ts_src", "entry_ts_ms", "entry_px_live", "entry_px_tape", "entry_src",
           "entry_age_ms", "exit_ts_ms", "exit_px_live", "exit_px_tape", "exit_src",
           "exit_reason_tape", "gross_tape_pct", "costs_pct", "costs_live_pct", "net_live_pct",
           "net_tape_pct", "net_official_pct", "net60_tape_pct", "entry_bias_pct",
           "whale_fill_ts_ms", "whale_px", "lag_s", "dump_first_ts_ms",
           "dump_last_ts_ms", "dump_dur_s", "dump_move_pct", "dump_bucket",
           "whale_pos_start", "whale_pos_after", "whale_max_fill_pct", "whale_max_fill_usd",
           "curve_tape", "flags", "status", "curve_n",
           # v4 (аудит v2.17): реплей TP R8, фандинг, повнота стрічки/кривої
           "tp_hit_ms", "net_tp_mkt_pct", "net_tp_lim_pct", "funding_pct", "cover_ok",
           "curve_final",
           # v5 (аудит v2.19): як знайдено тригерний філ Follow (hash / ts / none / unmatched)
           "trig_match", "eol"]


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
        self._bucket_ok = {}            # (symbol, b0) → обхід відра завершено (аудит G)
        self._partial = set()           # рев'ю v2.20 №3: відра, зняті ще ВІДКРИТИМИ (знімок неповний)
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
        """Усі трейди [b0, b1) через REST (≤1 запит/с) → ([[ts, px, agg_id], …]
        у порядку (ts, agg_id), complete) або (None, False) при збої.
        Аудит v2.17 G: перша сторінка — за часом, ДАЛІ — за ідентифікатором
        (fromId = останній id + 1): повторний запит за тією самою мс при
        насиченні (1000 трейдів однієї мс) повертав ті самі id, n=0 «завершував»
        збір і хвіст губився, а відро кешувалось як повне. complete = обхід
        дійшов до кінця вікна (коротка сторінка або трейд за межею), а не
        обірвався капом сторінок / збоєм."""
        out, start, last_id, pages, complete = [], b0, -1, 0, False
        by_id = False
        while pages < 200:
            self._t_rest = _pace(self._t_rest, self.pace)
            self.stats["rest_req"] += 1
            pages += 1
            try:
                if by_id:
                    url = "%s?symbol=%s&fromId=%d&limit=1000" % (REST_URL, symbol, last_id + 1)
                else:
                    url = "%s?symbol=%s&startTime=%d&endTime=%d&limit=1000" % (REST_URL, symbol, start, b1 - 1)
                rows = self.fetch_rest(url)
            except Transient as e:
                if getattr(e, "code", None) != 400:
                    raise                    # мережа/429/5xx — рядок відкладається
                self._mark_unlisted(symbol)  # Invalid symbol — стрічки немає взагалі
                rows = None
            if rows is None:
                self.stats["rest_fail"] += 1
                return None, False
            n, crossed = 0, False
            for r in rows:
                try:
                    a, t, p = int(r.get("a", -1)), int(r["T"]), float(r["p"])
                except (TypeError, ValueError, KeyError, AttributeError):
                    continue
                if a >= 0 and a <= last_id:
                    continue                 # дубль на межі сторінки
                if t >= b1:
                    crossed = True           # за межею вікна — кінець обходу
                    break
                if t < b0:
                    continue
                last_id = max(last_id, a)
                out.append([t, p, a])
                n += 1
            if crossed or len(rows) < 1000:
                complete = True
                break
            if last_id < 0:
                # без id (a = −1) — лише за часом: n == 0 при повній сторінці =
                # насичення, продовжити нема як → неповне
                if n == 0:
                    break
                start = out[-1][0] + 1
            else:
                by_id = True                 # далі — за id, насичена мс не губиться
        srt = [r for _, r in sorted(enumerate(out), key=lambda ir: (ir[1][0], ir[1][2], ir[0]))]
        return srt, complete

    def _rest_bucket(self, symbol, b0):
        """10-хв відро через REST: диск (лише повні відра) → пам'ять (і неповні —
        на життя об'єкта). Рядки кешу: [ts, px, agg_id] (v2). Старий кеш v1
        ([ts, px], без agg_id) НЕПРИДАТНИЙ: його рядки зберігались без id
        угоди, тож порядок трейдів однієї мілісекунди втрачено (при читанні
        він виходить за ціною, а не за біржею) — «останній трейд ≤ t»
        (_tape_last_px: референс/кінець дампу) і межа вікна стають хибними.
        Такий файл видаляється, відро тягнеться знову, якщо ще у REST-вікні."""
        key = (symbol, b0)
        now = self._now()
        complete = b0 + REST_BUCKET_MS <= now - MIN_MS
        v = self._get(self._lru_rest, key)
        if v is not None and key in self._partial and complete:
            # рев'ю v2.20 №3: відро знято, поки ще тривало (короткий обхід =
            # «повний» лише до цієї миті); тепер воно закрилось — знімок у
            # пам'яті неповний, друга половина відра порожня не як факт ринку:
            # перетягнути, а не судити повноту вікна за ним
            self._partial.discard(key)
            self._lru_rest.pop(key, None)
            self._bucket_ok.pop(key, None)
            v = None
        if v is not None or key in self._miss:
            return v
        path = os.path.join(self.rest_dir, "%s-%d.json" % (symbol, b0))
        raw = _load_json(path)
        in_window = b0 + REST_BUCKET_MS >= now - REST_MAX_AGE_MS
        rows, legacy = None, False
        if isinstance(raw, dict) and raw.get("v") == CACHE_V and isinstance(raw.get("rows"), list):
            # v5 (аудит v2.19 №4): версійований кеш із ДОКАЗОМ повноти —
            # complete записує сам обхід (fromId дійшов до кінця вікна)
            rows = raw["rows"]
            self._bucket_ok[key] = bool(raw.get("complete"))
        elif isinstance(raw, (list, dict)):
            # старий формат (v2.17: список рядків без доказу повноти — сторінка
            # 1000 трейдів обрізалась мовчки, v2.19 читала її як повну): у
            # REST-вікні — видалити й тягнути знову; поза вікном — рядки
            # лишаються як БЕЗ доказу (відро не «ок» → tape_gap/cover_ok=0)
            legacy = True
            if in_window or not isinstance(raw, list) or any(not isinstance(r, list) or len(r) < 3 for r in raw):
                self.log("[settle] REST-кеш %s старого формату — видалено, відро тягнеться знову"
                         % os.path.basename(path))
                try:
                    os.remove(path)
                except OSError:
                    pass
                self.stats["cache_legacy_refetch"] = self.stats.get("cache_legacy_refetch", 0) + 1
            else:
                rows = raw
                self._bucket_ok[key] = False
                self.stats["cache_legacy_kept"] = self.stats.get("cache_legacy_kept", 0) + 1
        if rows is None:
            if not in_window:
                self._miss.add(key)          # поза REST-вікном — перетягнути нема як
                return None
            rows, crawl_ok = self._rest_fetch(symbol, b0, b0 + REST_BUCKET_MS)
            if rows is None:
                self._miss.add(key)
                return None
            # аудит G: на диск — лише ПОВНЕ відро (час минув І обхід завершено);
            # неповний обхід не оголошується повним кешем; рев'ю v2.20 №3:
            # відкрите відро — не «ок» (кінця ще нема) і позначене як частковий знімок
            self._bucket_ok[key] = bool(crawl_ok) and complete
            if not complete:
                self._partial.add(key)
            if complete and crawl_ok:
                _save_json(path, {"v": CACHE_V, "symbol": symbol, "b0": b0, "complete": True,
                                  "n": len(rows), "last_id": (rows[-1][2] if rows else None),
                                  "saved_ms": now, "rows": rows})
        v = (array("q", (int(r[0]) for r in rows)), array("d", (float(r[1]) for r in rows)))
        self._put(self._lru_rest, self.rest_lru_max, key, v)
        return v

    def window_complete(self, symbol, t0, t1):
        """Аудит v2.17 G: стрічка на [t0, t1] зібрана ПОВНІСТЮ — кожен день
        із zip, а без zip кожне 10-хв відро вікна отримане повним обходом
        (не обірваним капом/збоєм, не «поточне» без кінця). Лише тоді
        відсутність трейдів у вікні — факт ринку, а не діра збору."""
        if self.is_unlisted(symbol):
            return False
        now = self._now()
        for dn in range(int(t0) // DAY_MS, int(t1) // DAY_MS + 1):
            day_ms = dn * DAY_MS
            if self._get(self._lru, (symbol, _day_str(day_ms))) is not None \
                    or os.path.exists(os.path.join(self.dir, "%s-aggTrades-%s.zip" % (symbol, _day_str(day_ms)))):
                continue
            lo, hi = max(int(t0), day_ms), min(int(t1), day_ms + DAY_MS - 1)
            if (hi // REST_BUCKET_MS) * REST_BUCKET_MS + REST_BUCKET_MS > now - MIN_MS:
                return False                 # останнє відро ще триває — кінця вікна не
                                             # бачимо; перевірка ДО будь-яких запитів
            b = (lo // REST_BUCKET_MS) * REST_BUCKET_MS
            while b <= hi:
                if (symbol, b) not in self._bucket_ok or (symbol, b) in self._partial:
                    # рев'ю v2.19 №4: відро, якого ніхто ще не торкався (середина
                    # вікна між двома ногами), — не «неповне», а невідоме:
                    # тягнемо (кеш/диск/REST) і лише тоді судимо; рев'ю v2.20 №3:
                    # частковий знімок відкритого відра — теж перепитуємо (відро
                    # могло закритись → _rest_bucket перетягне повним обходом)
                    self._rest_bucket(symbol, b)
                if not self._bucket_ok.get((symbol, b)):
                    return False
                b += REST_BUCKET_MS
        return True

    def funding(self, symbol, t0, t1):
        """Аудит v2.17 (фандинг Binance): суми ставок фандингу з fundingTime у
        (t0, t1] — REST fundingRate (вага 1), кеш по (символ, UTC-день) на
        диску <tape>/funding (лише минулі дні). → список (ts, rate) або None
        при збої/unlisted (викликач ставить no_funding)."""
        if self.is_unlisted(symbol):
            return None
        fdir = os.path.join(self.dir, "funding")
        os.makedirs(fdir, exist_ok=True)
        out = []
        now = self._now()
        for dn in range(int(t0) // DAY_MS, int(t1) // DAY_MS + 1):
            day_ms = dn * DAY_MS
            key = (symbol, "fund", dn)
            rows = self._get(self._lru_rest, key)
            if isinstance(rows, tuple):
                # рев'ю v2.19 №7: поточний день у пам'яті — (момент запиту, рядки);
                # вікно, що сягає за момент запиту, могло отримати нове
                # нарахування (08:00 UTC посеред довгого циклу) — тягнемо знову
                rows = rows[1] if int(t1) <= rows[0] else None
            path = os.path.join(fdir, "%s-%s.json" % (symbol, _day_str(day_ms)))
            if rows is None:
                rows = _load_json(path)
                if not isinstance(rows, list):
                    self._t_rest = _pace(self._t_rest, self.pace)
                    self.stats["rest_req"] += 1
                    raw = self.fetch_rest("%s?symbol=%s&startTime=%d&endTime=%d&limit=100"
                                          % (FUND_URL, symbol, day_ms, day_ms + DAY_MS - 1))
                    if raw is None:
                        self.stats["rest_fail"] += 1
                        return None
                    rows = []
                    for r in raw:
                        try:
                            rows.append([int(r["fundingTime"]), float(r["fundingRate"])])
                        except (TypeError, ValueError, KeyError, AttributeError):
                            continue
                    if day_ms + DAY_MS <= now - MIN_MS:
                        _save_json(path, rows)   # минулий день — незмінний
                if day_ms + DAY_MS <= now - MIN_MS:
                    self._put(self._lru_rest, self.rest_lru_max, key, rows)
                else:
                    self._put(self._lru_rest, self.rest_lru_max, key, (now, rows))
            for ft, fr in rows:
                if int(t0) < ft <= int(t1):
                    out.append((ft, fr))
        return out

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


def _covered(symbol, t_from, t_to, ctx, edge_ms=COVER_EDGE_MS):
    """Стрічка ПОКРИВАЄ вікно [t_from, t_to]: є трейд у [t_from, t_from+edge]
    І трейд у [t_to−edge, t_to] І (аудит v2.17 G) стрічка вікна зібрана
    ПОВНІСТЮ (window_complete: zip або повний обхід кожного відра) — два
    крайні трейди не доводять, що середина завантажена. Без цього «тригера
    не було» (no_trigger) не відрізнити від «стрічки не було» (tape_gap)."""
    tape = _tape(ctx)
    ts, _ = tape.window(symbol, int(t_from), int(t_from) + edge_ms)
    if not len(ts):
        return False
    ts, _ = tape.window(symbol, int(t_to) - edge_ms, int(t_to))
    if not len(ts):
        return False
    return tape.window_complete(symbol, int(t_from), int(t_to))


def _leg_costs(costs_full, entry_src, exit_src):
    """Витрати за ногами — як у server.py (аудит v2.16 №12): нога зі стакану
    (src починається з «book») уже містить спред і вплив ордера — модельний
    сліпаж додається ЛИШЕ до ніг за мідом/семплом. costs_full = комісія×2 +
    сліпаж×2 (колонка costs_pct); comm = 2·SIM_COMMISSION·100 = 0.10 %."""
    comm = 2 * SIM_COMMISSION * 100.0
    try:
        slip_each = max(0.0, (float(costs_full) - comm) / 2.0)
    except (TypeError, ValueError):
        slip_each = 0.0

    def _book(src):
        return str(src or "").startswith("book")
    return comm + (0.0 if _book(entry_src) else slip_each) \
                + (0.0 if _book(exit_src) else slip_each)


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
        self._sat = set()               # (addr, h): година насичена (2000 філів однієї мс / кап сторінок)
        self.stats = {"req": 0, "fail": 0, "sat": 0}

    def _now(self):
        return self.now_ms if self.now_ms is not None else int(time.time() * 1000)

    def _hour(self, addr, h):
        key = (addr, h)
        if key in self._mem:
            return self._mem[key]
        if key in self._miss:
            return None
        path = os.path.join(self.dir, "%s-%d.json" % (addr, h))
        raw = _load_json(path)
        complete = (h + 1) * HOUR_MS <= self._now() - MIN_MS
        rows = None
        if isinstance(raw, dict) and raw.get("v") == CACHE_V and isinstance(raw.get("fills"), list):
            rows = raw["fills"]              # v5: версійований кеш (лише ненасичені години)
        elif raw is not None:
            # v5 (аудит v2.19 №4): старий список без доказу повноти (v2.17 міг
            # зберегти насичену годину як повну) — видалити, тягнути знову
            try:
                os.remove(path)
            except OSError:
                pass
            self.stats["cache_legacy_refetch"] = self.stats.get("cache_legacy_refetch", 0) + 1
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
                if len(page) < 2000:
                    break
                if n == 0 or pages >= 8:
                    # аудит v2.17 G: 2000 філів однієї мс (повторна сторінка з
                    # тими самими id) або кап сторінок — історія години НЕПОВНА:
                    # явна невизначеність (hl_sat), не «зібрано»
                    self._sat.add(key)
                    self.stats["sat"] += 1
                    break
                start = int(page[-1].get("time") or start + 1)
            rows.sort(key=lambda f: int(f.get("time") or 0))
            if complete and key not in self._sat:
                _save_json(path, {"v": CACHE_V, "addr": addr, "h": h, "complete": True,
                                  "sat": False, "n": len(rows), "pages": pages,
                                  "saved_ms": self._now(), "fills": rows})
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

    def saturated(self, addr, t0_ms, t1_ms):
        """Чи якась година вікна насичена (історія неповна) — після user_fills."""
        return any((addr, h) in self._sat
                   for h in range(int(t0_ms) // HOUR_MS, int(t1_ms) // HOUR_MS + 1))


def hl_user_fills(addr, t0_ms, t1_ms, ctx):
    hl = ctx.get("hl") if isinstance(ctx, dict) else ctx
    return hl.user_fills(addr, t0_ms, t1_ms) if hl is not None else None


def _ftime(f):
    x = fnum(f.get("time"))
    return int(x) if x is not None else -1


def _is_close(f):
    """Філ закриває позицію: dir з «Close», ФЛІП («Long > Short» / «Short >
    Long» — стара позиція закрита повністю), ліквідація / ADL (текст dir або
    поле liquidation) — як класифікує live-бот."""
    d = str(f.get("dir") or "")
    return ("Close" in d) or (" > " in d) or ("Liquidat" in d) or ("Auto-Delever" in d) \
        or bool(f.get("liquidation"))


def _sp_sz(f):
    """(|startPosition|, |sz|) філа; None, якщо поля немає / бите."""
    sp, sz = fnum(f.get("startPosition")), fnum(f.get("sz"))
    return (abs(sp) if sp is not None else None), (abs(sz) if sz is not None else None)


def close_episode(fills, coin, t0_ms, close_only, grow_only=False):
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
        if sp_i is not None and sp_j is not None and sz_j is not None:
            exp_i, tol = sp_j - sz_j, EP_POS_TOL * max(sp_j, 1e-9) + 1e-9
            # grow_only (live-бот): у потоці лише ТЕЙКЕРСЬКІ філи — мейкерське
            # закриття між ними виглядає як «незрозуміле» зменшення позиції,
            # це той самий злив, не новий епізод; розрив лише на ЗРОСТАННІ
            # (долив / перевідкриття). Settlement бачить усі філи — абсолютний
            # допуск (зменшення без філа = чужий епізод / діра)
            if (sp_i > exp_i + tol) if grow_only else (abs(sp_i - exp_i) > tol):
                break                        # долив / перевідкриття між філами
        k -= 1
    return fs[k], fs[idx], fs[k:idx + 1]


def _episode_cols(first, last, t0_ms, fills=()):
    """Колонки епізоду з першого/останнього філа: сирі ціни філів (не VWAP);
    dump_move_pct > 0 = ціна пішла у бік тиску кита (продає → впала).
    Факти позиції (для повторної валідації старих рядків сервером):
    whale_pos_start = |startPosition| ПЕРШОГО філа; whale_pos_after =
    |startPosition| − |sz| ОСТАННЬОГО (залишок після епізоду, ≥0; None без
    полів); whale_max_fill_pct = найбільший філ епізоду / whale_pos_start
    (None без старту); whale_max_fill_usd = px·sz того самого філа."""
    t_last, t_first = _ftime(last), _ftime(first)
    px_last, px_first = fnum(last.get("px")), fnum(first.get("px"))
    dur = (t_last - t_first) / 1000.0
    move = None
    if px_last and px_first:
        move = (px_last / px_first - 1) * 100 * (1 if last.get("side") == "B" else -1)
    sp_first, _ = _sp_sz(first)
    sp_last, sz_last = _sp_sz(last)
    after = None
    if sp_last is not None and sz_last is not None:
        after = max(0.0, sp_last - sz_last)
    # найбільша ТРАНЗАКЦІЯ епізоду (ордер = філи з одним hash; без hash —
    # окремий філ): live-R7 звіряє частку ордера, не окремого філа
    big_sz = big_usd = None
    groups = {}
    for i, f in enumerate(fills or (first, last)):
        _, sz = _sp_sz(f)
        px = fnum(f.get("px"))
        if sz is None or px is None:
            continue
        h = str(f.get("hash") or "")
        if not h or set(h[2:] if h.startswith("0x") else h) <= {"0"}:
            h = None                          # системний філ (ліквідація/ADL): hash нульовий
        gk = h or (("oid:%s" % f.get("oid")) if f.get("oid") is not None else None) \
            or (("tid:%s" % f.get("tid")) if f.get("tid") is not None else None) or ("_f%d" % i)
        g = groups.setdefault(gk, [0.0, 0.0])
        g[0] += sz
        g[1] += px * sz
    for gsz, gusd in groups.values():
        if big_sz is None or gsz > big_sz:
            big_sz, big_usd = gsz, gusd
    max_pct = big_sz / sp_first if (big_sz is not None and sp_first) else None
    return {"whale_fill_ts_ms": t_last, "whale_px": px_last,
            "lag_s": (t0_ms - t_last) / 1000.0,
            "dump_first_ts_ms": t_first, "dump_last_ts_ms": t_last, "dump_dur_s": dur,
            "dump_move_pct": move,
            "dump_bucket": 0 if dur >= 300 else min(5, max(1, int(math.ceil(dur / 60.0)))),
            "whale_pos_start": sp_first, "whale_pos_after": after,
            "whale_max_fill_pct": max_pct, "whale_max_fill_usd": big_usd}


def _close_flag(ep):
    """Прапорець епізоду за залишком позиції: full_close — залишок ≤ 1% від
    старту (EP_POS_TOL; без старту — ≈0); partial_close — більший;
    close_unknown — залишок невідомий (без startPosition / sz)."""
    after, start = ep.get("whale_pos_after"), ep.get("whale_pos_start")
    if after is None:
        return "close_unknown"
    tol = EP_POS_TOL * start if start else 0.0
    return "full_close" if after <= tol + 1e-9 else "partial_close"


def whale_episode(fills, coin, t0_ms, close_only):
    """Епізод закриття кита перед t0 (правило — close_episode) → dict колонок
    whale_fill_ts_ms / whale_px / lag_s / dump_first_ts_ms / dump_last_ts_ms /
    dump_dur_s / dump_move_pct / dump_bucket / whale_pos_start /
    whale_pos_after / whale_max_fill_pct / whale_max_fill_usd, або {} без філів."""
    first, last, inep = close_episode(fills, coin, t0_ms, close_only)
    return _episode_cols(first, last, t0_ms, inep) if first is not None else {}


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
            "ts_src": ts_src, "costs_pct": costs, "costs_live_pct": costs, "flags": [],
            "curve_tape": "", "curve_n": 0, "eol": "^"}


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


def _funding(out, symbol, t0, t1, long, ctx):
    """v4: фандинг Binance за утримання (t0, t1] у % (додатний = ми платимо):
    Σ ставок × знак (LONG платить додатну). Збій/unlisted → 0 і прапорець
    no_funding (net по стрічці без фандингу — не приховано)."""
    if t0 is None or t1 is None or t1 <= t0:
        out["funding_pct"] = 0.0
        return 0.0
    tape = _tape(ctx)
    try:
        rows = tape.funding(symbol, int(t0), int(t1)) if hasattr(tape, "funding") else None
    except Transient:
        raise
    except Exception:
        rows = None
    if rows is None:
        out["flags"].append("no_funding")
        out["funding_pct"] = 0.0
        return 0.0
    f = sum(r for _, r in rows) * 100.0 * (1.0 if long else -1.0)
    out["funding_pct"] = f
    return f


THIN_FLAGS = ("tp_path_gap", "tp_replay_thin")   # v5: реплей на неповних даних → status tape_thin

def _result(out, long, e_tape, x_tape, e_live, net_live, costs, tape_flag="no_tape", funding=0.0):
    """Спільний хвіст: gross/net по стрічці, зсув входу, офіційний результат і
    status. net_official = min(net_live, net_tape), коли є обидва; інакше той,
    що є (записаний результат ніколи не зникає). status: verified — обидві ноги
    по стрічці і є net_live; tape_only — стрічка є, net_live немає; live_only —
    ноги по стрічці немає (no_tape / partial / no_trigger), live є; none — нічого.
    flags: no_tape (нема входу по стрічці; tape_flag=None — не ставити, напр.
    no_trigger), partial (вхід є, а виходу або net_live немає). v4: net по
    стрічці — мінус фандинг за утримання (funding)."""
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
            net_tape -= float(funding or 0.0)
    out["net_live_pct"], out["net_tape_pct"] = net_live, net_tape
    if net_live is not None and net_tape is not None:
        out["net_official_pct"] = min(net_live, net_tape)
    else:
        out["net_official_pct"] = net_live if net_live is not None else net_tape
        if e_tape is not None:
            out["flags"].append("partial")
    if net_tape is not None:
        out["status"] = "verified" if net_live is not None else "tape_only"
        if any(f in (out.get("flags") or []) for f in THIN_FLAGS):
            # v5 (аудит v2.19 №6): нога по стрічці порахована на неповних даних
            # (діра до першого перетину TP / без ціни після виявлення) —
            # не «перевірено»; офіційний net лишається гіршим із двох
            out["status"] = "tape_thin"
    else:
        out["status"] = "live_only" if net_live is not None else "none"
    return out


def _fund_events(symbol, t0, t1, ctx, out=None):
    """Нарахування фандингу у (t0, t1] — список (ts, rate); збій → [] і
    прапорець no_funding у out (рев'ю v2.20: пропуск фандингу у кривій/net60
    видимий так само, як у net по стрічці)."""
    tape = _tape(ctx)
    try:
        rows = tape.funding(symbol, int(t0), int(t1)) if hasattr(tape, "funding") else None
    except Transient:
        raise
    except Exception:
        rows = None
    if rows is None:
        if out is not None and "no_funding" not in out.get("flags", []):
            out.setdefault("flags", []).append("no_funding")
        return []
    return list(rows)


def _fund_until(events, t0, t1, long):
    """Фандинг (%; додатний = платимо) з подій у (t0, t1]."""
    return sum(r for ts, r in events if int(t0) < int(ts) <= int(t1)) * 100.0 * (1.0 if long else -1.0)


def _curve(out, symbol, entry_t, e, side_out, long, costs, h, ctx):
    """Крива по стрічці: для k=1..h net (gross − costs − фандинг до k-ї хвилини,
    v5 аудит №10 — та сама формула, що net по стрічці) від ціни входу e зі
    стрічки до exec_px(entry_t + k хв, side_out); "" де трейдів немає; колонка
    curve_tape = ";"-з'єднані "%.4f", curve_n = кількість заповнених точок.
    Без входу по стрічці — порожня (curve_n = 0)."""
    if e is None or entry_t is None:
        out["curve_tape"], out["curve_n"] = "", 0
        return
    ev = _fund_events(symbol, entry_t, entry_t + h * MIN_MS, ctx, out)
    vals = []
    for k in range(1, h + 1):
        x = exec_px(symbol, entry_t + k * MIN_MS, side_out, ctx)["px"]
        n = _net(e, x, long, costs)
        if n is not None and ev:
            n -= _fund_until(ev, entry_t, entry_t + k * MIN_MS, long)
        vals.append("" if n is None else "%.4f" % n)
    out["curve_tape"] = ";".join(vals)
    out["curve_n"] = sum(1 for v in vals if v)
    out["curve_final"] = int(out["curve_n"] >= h)   # аудит G: 100% точок, окремо від PnL


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


NEG_LAG_LOOK_MS = 120_000   # рев'ю v2.20: наскільки ПІСЛЯ входу шукати записаний тригер (від'ємний лаг)


def _whale(out, row, t0, ctx, close_only, symbol=None, trigger=False):
    """Філи кита перед t0 → lag/dump-колонки + факти позиції (whale_pos_* /
    whale_max_fill_*) і прапорець full_close / partial_close / close_unknown
    (_close_flag); збій → flag no_fills.
    Епізод — close_episode (одне правило з live-ботом). Рух епізоду
    (dump_move_pct) — як у live (server.py _rev_ref_px): від ціни ДО першого
    філа до ціни на останньому; тут референс = останній трейд Binance СТРОГО
    перед першим філом, кінець = останній трейд ≤ останнього філа (прапорець
    dump_tape; знак: додатний = у бік тиску кита); лише філи (сирі ціни
    перший→останній) — фолбек dump_fills (одним пострілом = 0%)."""
    hl, addr = ctx.get("hl"), (row.get("whale_addr") or "").strip()
    # рев'ю v2.20 (причинність): рядок із записаним тригером (trig_hash /
    # fill_ts_ms) — філи дивимось і ТРОХИ ПІСЛЯ входу: тригер, що стався
    # після ціни входу (від'ємний лаг), має бути знайдений і дати lag_s<0
    # (→ sig_ok=0 neg_lag), а не «зникнути» за межею вікна
    has_trig = bool((row.get("trig_hash") or "").strip()) or _ms(row.get("fill_ts_ms")) is not None
    look = NEG_LAG_LOOK_MS if has_trig else 0
    fills = hl.user_fills(addr, t0 - 600000, t0 + look) if (hl is not None and addr) else None
    if fills is None:
        out["flags"].append("no_fills")
        return
    if hasattr(hl, "saturated") and hl.saturated(addr, t0 - 600000, t0 + look):
        out["flags"].append("hl_sat")        # аудит G: історія неповна — епізод не доведений
    t_end = t0
    anchor = None
    if not trigger and look and fills:
        # реверс: тригер шукаємо лише щоб виявити від'ємний лаг; у звичайному
        # випадку (тригер до входу) епізод — до входу, як раніше
        anchor, _how = _trigger_fill(fills, row)
        if anchor is not None and _ftime(anchor) > t0:
            t_end = _ftime(anchor)
            out["flags"].append("neg_lag")
        else:
            anchor = None
    if trigger and fills:
        # v5 (аудит v2.19 №3): Follow звіряється з ТРИГЕРНОЮ транзакцією, яку
        # записав бот (trig_hash, а для старих рядків — fill_ts_ms), не з
        # «останнім підхожим філом перед входом»: інший (мейкерський, дрібний)
        # філ за секунду до входу «виправляв» лаг непридатного сигналу.
        # Не знайдено — trigger_unmatched: перевірка НЕПОВНА, не дозвіл.
        # Рев'ю v2.20 №2: лише Follow (trigger=True) — TWAP-рядки тригера не
        # записують (нема trig_hash/fill_ts_ms), їхній епізод — як раніше
        anchor, how = _trigger_fill(fills, row)
        out["trig_match"] = how
        if anchor is None:
            out["flags"].append("trigger_unmatched")
            return
        t_end = _ftime(anchor)
        if t_end > t0:
            out["flags"].append("neg_lag")   # тригер після входу — lag_s<0 → sig_ok=0
    fills = [f for f in fills if _ftime(f) <= t_end]
    first, last, inep = close_episode(fills, row.get("coin"), t_end, close_only)
    if first is None:
        out["flags"].append("no_whale_fill")
        return
    ep = _episode_cols(first, last, t0, inep)
    out["flags"].append(_close_flag(ep))     # full_close / partial_close / close_unknown
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


def _trigger_fill(fills, row):
    """Тригерний філ Follow серед філів кита: (а) за hash транзакції
    (trig_hash, v2.20 — усі філи одного hash = одна транзакція, беремо
    останній за часом), (б) для рядків без hash — за записаним часом
    (fill_ts_ms, точно та сама мс) і напрямом закриття, (в) системний
    ідентифікатор sys:… — за часом. → (філ, спосіб) або (None, спосіб)."""
    h = str(row.get("trig_hash") or "").strip()
    t = _ms(row.get("fill_ts_ms"))
    coin = row.get("coin")
    cand = [f for f in fills if f.get("coin") == coin]
    if h and not h.startswith("sys:") and set(h[2:] if h.startswith("0x") else h) - {"0"}:
        same = [f for f in cand if str(f.get("hash") or "") == h]
        if same:
            return max(same, key=_ftime), "hash"
        return None, "unmatched"
    if t is not None:
        same = [f for f in cand if _ftime(f) == int(t) and _is_close(f)]
        if same:
            return max(same, key=lambda f: fnum(f.get("sz")) or 0.0), "ts"
        return None, "unmatched"
    return None, "none"


def settle_follow(row, ctx):
    """follow_trades.csv → колонки settlement або None (не закрита / без часу).
    Вхід/вихід — записані open_ts_ms/close_ts_ms (старі рядки — локальні
    дати); крива по стрічці 60 хв. net_live — записаний net_pct рядка (як є);
    costs_live_pct = витрати рядка (costs_pct)."""
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
    fund = _funding(out, symbol, t_in, t_out, long, ctx) if e is not None else 0.0
    _result(out, long, e, x, e_live, fnum(row.get("net_pct")), costs, funding=fund)
    _curve(out, symbol, t_in, e, side_out, long, costs, CURVE_H["fol"], ctx)
    _whale(out, row, t_in, ctx, close_only=False, symbol=symbol, trigger=True)
    return out


def _m30(row):
    """m30, або перша з m31..m33 (пізній вихід, як у server.py)."""
    for k in (30, 31, 32, 33):
        v = fnum(row.get("m%d" % k))
        if v is not None:
            return v
    return None


def _r8_tp_replay(out, row, symbol, entry_t, e, long, costs, side_out, ctx):
    """R8 (аудит v2.17 F): НЕЗАЛЕЖНИЙ реплей всього правила виходу по стрічці
    від входу — не лише прапорець. TP = записаний tp_px (те, чого чекав бот);
    без нього — 0.8×дамп від ціни входу зі стрічки; без дампу — no_dump.
    Дві моделі: (1) РИНКОВИЙ вихід після виявлення перетину — перший трейд
    ≥TP (LONG) у (вхід, +30 хв], виявлення через TP_DETECT_MS, ціна = гірша у
    наступні 3 с (net_tp_mkt_pct) — ЦЕ нога виходу по стрічці для R8;
    (2) ЛІМІТКА на TP — виконана, якщо трейд пройшов КРІЗЬ рівень (строго за
    TP), ціна = TP (net_tp_lim_pct, ті самі витрати — консервативно). Без
    перетину — таймер m30 по стрічці. → (x_tape, x_t, reason_tape)."""
    tp = fnum(row.get("tp_px"))
    if tp is None:
        dump = abs(fnum(row.get("dump_move_pct")) or fnum(row.get("move_3m_pct")) or 0.0)
        if dump <= 0:
            out["flags"].append("no_dump")
            x_t = entry_t + HOLD30_MS
            return exec_px(symbol, x_t, side_out, ctx)["px"], x_t, "m30"
        tp = e * (1 + R8_TP_K * dump / 100.0 * (1 if long else -1))
    scan_from = entry_t + {"tape3s": 3000, "tape10s": 10000}.get(out.get("entry_src"), 0)
    hit = _first_cross(symbol, scan_from, entry_t + HOLD30_MS, tp, long, ctx)
    if hit is None:
        out["flags"].append("tp_tape_miss")
        if not _covered(symbol, scan_from, entry_t + HOLD30_MS, ctx):
            out["flags"].append("tape_gap")  # «не перетнув» має право лише на повній стрічці
        x_t = entry_t + HOLD30_MS
        x = exec_px(symbol, x_t, side_out, ctx)["px"]
        out["net_tp_mkt_pct"] = _net(e, x, long, costs)
        out["net_tp_lim_pct"] = out["net_tp_mkt_pct"]
        return x, x_t, "m30_replay"
    t_hit, px_hit = hit
    out["flags"].append("tp_tape_hit")
    out["tp_hit_ms"] = int(t_hit)
    if not _covered(symbol, scan_from, int(t_hit), ctx):
        # v5 (аудит v2.19 №6): діра на шляху до першого видимого перетину —
        # ранішого перетину могло не бути видно; результат не «verified»
        out["flags"].append("tp_path_gap")
    x_t = int(t_hit) + TP_DETECT_MS
    xm = exec_px(symbol, x_t, side_out, ctx)["px"]
    if xm is None:
        # рідка стрічка після перетину (тест/тонка монета): ціна перетину —
        # реальний принт за TP; позначаємо, що вікно 10 с порожнє
        xm = px_hit
        out["flags"].append("tp_replay_thin")
    out["net_tp_mkt_pct"] = _net(e, xm, long, costs)
    # лімітка: заповнена лише якщо ціна пройшла крізь рівень (строго за TP)
    through = _first_cross(symbol, scan_from, entry_t + HOLD30_MS,
                           tp * (1 + 1e-6) if long else tp * (1 - 1e-6), long, ctx)
    out["net_tp_lim_pct"] = _net(e, tp, long, costs) if through is not None else None
    return xm, x_t, "tp_replay"


def settle_rev(row, ctx):
    """rev_trades.csv (entered=1) → колонки settlement або None.
    Аудит ЗАПИСАНОГО виконання: вхід = entry_ts_ms (v2.16; для R2 — вхід на
    пробої), у старих рядків — локальний date; вихід = exit_ts_ms (таймер / TP /
    пізній), без нього або при no_price — рівно +30 хв від входу (m30).
    R2 без entry_ts_ms — реплей тригера +0.3% від референсу стрічки у 10 хв
    (не знайдено → no_trigger, результат лише live). R2 з entry_ts_ms — той
    самий реплей від date (або entry−10 хв) до записаного входу ЛИШЕ як
    прапорець no_trigger; ціни — на записаних часах. Вердикт no_trigger має
    право лише на ПОКРИТОМУ стрічкою вікні (_covered); інакше — tape_gap
    (стрічки бракує: молодий рядок відкладається, як no_tape). R8 — ціна на
    записаному виході + прапорець tp_tape_hit/miss (реплей TP по стрічці).
    Не-R8 — ще net60 на +60 хв. Крива по стрічці 60 хв від входу.
    net_live при виході зі стакану (v2.16 exit_px) — gross − витрати за
    ногами (_leg_costs, як в API); m30-шлях — повні costs_pct; використані
    витрати — колонка costs_live_pct."""
    if fnum(row.get("entered"), 0) != 1 or not row.get("sig_id"):
        return None
    strat = row.get("strategy") or ""
    is_r2, is_r8 = strat.startswith("R2"), ("R8" in strat)
    t_rec = _ms(row.get("entry_ts_ms"))          # записаний вхід (v2.16)
    # t0 = детекція: detect_ms (v2.20, мс виявлення філа) / detect_ts_ms, для не-R2
    # entry_ts_ms (вхід на детекті), інакше date
    t0, _ = _ts(row, ("detect_ms", "detect_ts_ms") if is_r2 else ("detect_ms", "detect_ts_ms", "entry_ts_ms"), "date")
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
    costs_live = costs
    if e_live and x_live_px and x_reason and x_reason != "no_price":
        # v2.16: вихід зі стакану (таймер/TP) — як рахує API: витрати за ногами
        costs_live = _leg_costs(costs, row.get("entry_src"), row.get("exit_src"))
        g_live = (x_live_px / e_live - 1) * 100.0 * (1 if long else -1)
        # v5 (аудит v2.19 №10): live-net — як у сервері, мінус фандинг рядка
        net_live, out["exit_px_live"] = g_live - costs_live - (fnum(row.get("funding_pct")) or 0.0), x_live_px
    elif x_reason == "no_price":
        net_live = None
    else:
        net_live = (m30 - costs) if m30 is not None else None
        if e_live and m30 is not None:
            out["exit_px_live"] = e_live * (1 + m30 / 100.0 * (1 if long else -1))
    out["costs_live_pct"] = costs_live
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    entry_t, ref_ok = (t_rec if t_rec is not None else t0), True
    if is_r2:
        p0 = _ref_px(symbol, t0, ctx)
        level = p0 * (1 + R2_BREAKOUT if long else 1 - R2_BREAKOUT) if p0 else None
        if t_rec is None:
            # старий рядок без часу входу: вхід = перший трейд, що перетнув рівень
            ref_ok = p0 is not None
            hit = _first_cross(symbol, t0, t0 + R2_WINDOW_MS, level, long, ctx) if ref_ok else None
            if ref_ok and hit is None:
                # пробою у вікні немає: на покритій стрічці — вердикт no_trigger
                # (угоди по стрічці немає), на непокритій — tape_gap (стрічки
                # бракує — як no_tape: молодий рядок відкладається)
                out.update({"entry_ts_ms": t0, "entry_src": "none", "exit_reason_tape": ""})
                covered = _covered(symbol, t0, t0 + R2_WINDOW_MS, ctx)
                out["flags"].append("no_trigger" if covered else "tape_gap")
                _result(out, long, None, None, e_live, net_live, costs, tape_flag=None)
                _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
                return out
            if hit is not None:
                entry_t = hit[0]
        elif level and _first_cross(symbol, t0, t_rec, level, long, ctx) is None:
            # стрічка не бачила пробою до записаного входу: прапорець лише на
            # покритому вікні, інакше — tape_gap (ціни все одно на записаних часах)
            out["flags"].append("no_trigger" if _covered(symbol, t0, t_rec, ctx) else "tape_gap")
    e = _leg(out, "entry", symbol, entry_t, side_in, ctx) if ref_ok else None
    if e is None:
        out["entry_ts_ms"], out["entry_src"] = entry_t, "none"
    x_t = x_rec if x_rec is not None else entry_t + HOLD30_MS
    out["exit_reason_tape"] = x_reason if x_rec is not None else "m30"
    x = None
    if e is not None:
        if is_r8:
            # аудит F: нога виходу R8 по стрічці — реплей правила (TP після
            # перетину / таймер m30), а не записаний live-час
            x, x_t, out["exit_reason_tape"] = _r8_tp_replay(out, row, symbol, entry_t, e, long,
                                                             costs, side_out, ctx)
            out["exit_ts_ms"], out["exit_px_tape"] = int(x_t), x
            out["exit_src"] = "tape_replay" if x is not None else "none"
        else:
            x = _leg(out, "exit", symbol, x_t, side_out, ctx)
            x60 = exec_px(symbol, entry_t + HOLD60_MS, side_out, ctx)["px"]
            n60 = _net(e, x60, long, costs)
            if n60 is not None:   # v5 (№10): net60 — мінус фандинг за 60 хв, як net по стрічці
                n60 -= _fund_until(_fund_events(symbol, entry_t, entry_t + HOLD60_MS, ctx, out),
                                   entry_t, entry_t + HOLD60_MS, long)
            out["net60_tape_pct"] = n60
    else:
        out["exit_ts_ms"] = x_t
    fund = _funding(out, symbol, entry_t, x_t, long, ctx) if e is not None else 0.0
    _result(out, long, e, x, e_live, net_live, costs, funding=fund)
    _curve(out, symbol, entry_t, e, side_out, long, costs, CURVE_H["rev"], ctx)
    # рев'ю v2.19 №4: покриття — ПІСЛЯ кривої (вона торкається кожної хвилини
    # вікна; window_complete і сам дотягує невідомі відра)
    out["cover_ok"] = int(_covered(symbol, entry_t, x_t, ctx)) if e is not None else 0
    _whale(out, row, t0, ctx, close_only=True, symbol=symbol)
    return out


def settle_twap(row, ctx):
    """twap_trades.csv → колонки settlement або None (без exit_min / часу).
    Вихід = записаний exit_ts_ms, без нього entry + exit_min (хвилина
    закриття); exit_px_live = записаний exit_px, без нього з m{exit_min}.
    net_live: з exit_px (v2.16) — gross − витрати за ногами (_leg_costs, як в
    API), інакше записаний net60_pct; no_price — net_live None (стрічка є →
    tape_only), рядок НЕ пропускається. Крива по стрічці 120 хв."""
    em = fnum(row.get("exit_min"))
    if em is None or not row.get("twap_id"):
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
    x_reason = row.get("exit_reason") or ""
    mx = fnum(row.get("m%d" % int(em)))
    out["entry_px_live"] = e_live
    if x_live:
        out["exit_px_live"] = x_live
    elif e_live and mx is not None:
        out["exit_px_live"] = e_live * (1 + mx / 100.0 * (1 if long else -1))
    out["exit_reason_tape"] = x_reason
    costs_live = costs
    if x_reason == "no_price":
        net_live = None                      # ціни виходу не було — лише стрічка
    elif e_live and x_live:
        costs_live = _leg_costs(costs, row.get("entry_src"), row.get("exit_src"))
        net_live = ((x_live / e_live - 1) * 100.0 * (1 if long else -1) - costs_live
                    - (fnum(row.get("funding_pct")) or 0.0))   # v5: мінус фандинг рядка (№10)
    else:
        net_live = fnum(row.get("net60_pct"))
    out["costs_live_pct"] = costs_live
    side_in, side_out = ("BUY", "SELL") if long else ("SELL", "BUY")
    e = _leg(out, "entry", symbol, t_in, side_in, ctx)
    x = _leg(out, "exit", symbol, t_out, side_out, ctx)
    fund = _funding(out, symbol, t_in, t_out, long, ctx) if e is not None else 0.0
    _result(out, long, e, x, e_live, net_live, costs, funding=fund)
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
     ("detect_ms", "detect_ts_ms", "entry_ts_ms"), "date"),
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


def _curve_n(srow):
    """Кількість заповнених точок кривої: колонка curve_n, а для рядків без
    неї — підрахунок по curve_tape."""
    n = fnum(srow.get("curve_n"))
    if n is not None:
        return int(n)
    c = srow.get("curve_tape") or ""
    return sum(1 for v in str(c).split(";") if v) if c else 0


def _curve_complete(srow):
    """Крива по стрічці достатньо повна: заповнено ≥ CURVE_FULL (90%) точок
    горизонту сім'ї (60 хв R/F, 120 хв TWAP)."""
    h = CURVE_H.get(srow.get("family"), 60)
    return _curve_n(srow) >= CURVE_FULL * h


def _settle_final(srow, now_ms=None):
    """Рядок settlements остаточний: поточна версія розрахунку І розрахований
    після горизонту кривої І (крива повна АБО угода старша за TAPE_WAIT_MS —
    стрічка вже не доповниться) — інакше крива обірвана / діркувата
    (REST-відро ще неповне, zip ще не викладений): перерахунок за
    _resettle_due. Без часу входу / settled_at судити нема з чого — остаточний;
    unlisted — стрічки не буде, чекати повної кривої нема чого."""
    if srow.get("settle_v") != SETTLE_V:
        return False
    due, at = _curve_due_ms(srow), parse_local(srow.get("settled_at"))
    if due is None or at is None:
        return True
    if at < due:
        return False
    if _curve_complete(srow) or "unlisted" in (srow.get("flags") or ""):
        return True
    # v5 (аудит v2.19 №6): остаточний «за віком» — лише якщо ОСТАННІЙ перерахунок
    # стався вже після 72 год від входу (стрічка вже не доповниться): рядок,
    # порахований на 2-й годині з кривою 54/60, не стає фінальним сам по собі
    e = int(fnum(srow.get("entry_ts_ms")))
    return at - e >= TAPE_WAIT_MS


def _resettle_due(srow, now_ms):
    """Чи (пере)раховувати рядок settlements зараз: стара версія — так;
    остаточний — ні; крива ще не дозріла (now < entry+H+запас) — ні, чекати
    горизонту; розрахований ДО горизонту — так (один раз, коли дозрів);
    розрахований після горизонту з неповною кривою — з подвоюваними паузами:
    лише коли now − settled_at ≥ max(1 год, settled_at − entry) (≈7 спроб до
    72 год, далі рядок остаточний за віком)."""
    if _settle_final(srow, now_ms):
        return False
    if srow.get("settle_v") != SETTLE_V:
        return True
    due, at = _curve_due_ms(srow), parse_local(srow.get("settled_at"))
    if due is None or at is None:
        return True
    if now_ms < due:
        return False
    if at < due:
        return True
    e = int(fnum(srow.get("entry_ts_ms")))
    if now_ms - e >= TAPE_WAIT_MS and at - e < TAPE_WAIT_MS:
        return True          # v5: останній перерахунок після 72 год — обов'язковий
    return now_ms - at >= max(HOUR_MS, at - e)


def _tape_pending(out, now_ms):
    """Стрічки бракує (вхід без трейдів — no_tape, вікно тригера не покрите —
    tape_gap, або вихід без трейдів), а угода молодша за TAPE_WAIT_MS (72 год)
    — zip ще не викладений / REST-прогалина: не фіксувати, спробувати пізніше.
    no_trigger сам по собі — вердикт покритої стрічки, не її брак; unlisted —
    символу на Binance немає, чекати нема чого."""
    fl = out.get("flags") or []
    if "unlisted" in fl:
        return False
    missing = ("no_tape" in fl) or ("tape_gap" in fl) or any(f in fl for f in THIN_FLAGS) or (
        out.get("entry_px_tape") is not None and out.get("exit_px_tape") is None)
    if not missing:
        return False
    t = fnum(out.get("entry_ts_ms"))
    return t is not None and now_ms - t < TAPE_WAIT_MS


def _backoff_ms(n_fail):
    """Пауза перед повторною спробою після n_fail збоїв поспіль:
    10 хв × 2^(n_fail−1), стеля 6 год."""
    return min(RETRY_CAP_MS, RETRY_BASE_MS * (2 ** max(0, min(n_fail - 1, 12))))


def settle_pending(data_dir, symbol_map=None, limit=None, force=False, log=print,
                   fetchers=None, now_ms=None, since=None):
    """Розрахувати ще не розраховані закриті угоди.
    → (n_done, n_skipped, n_failed); окремий битий рядок логується, не валить
    прогін. force — перерахувати все заново (новий запис ключа виграє).
    Рядок без стрічки (вхід/вихід/вікно тригера) молодший за 72 год — не
    фіксується (n_failed), старший — фіксується назавжди. Перерахунок рядків
    старої версії / з недозрілою чи неповною кривою — _resettle_due.
    БЕКОФ: settle_pending.retry {key: (next_retry_ms, n_fail)} — відкладений
    рядок або збій (Transient/інший) → наступна спроба через 10 хв × 2^(n−1)
    (стеля 6 год); до того кандидат пропускається БЕЗ витрати спроби
    (settle_pending.last_waiting); успішний запис знімає ключ.
    last_pending = усі кандидати (бек-лог для /status).
    ЧЕРГА: limit — лише на СПРОБИ (рядки, що дають None, без мережі);
    перші ceil(limit/2) спроб — НОВІШІ першими (свіжі угоди отримують
    офіційний net за хвилини), решта — СТАРІШІ першими (бек-лог не голодує);
    без limit — усе, новіші першими."""
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
            if sr is not None and not _resettle_due(sr, now):
                continue                     # остаточно / ще не дозріло / пауза
            if (row.get("algo_v") or "").strip() and _vt(row.get("algo_v")) < MIN_ALGO_V:
                n_old += 1               # рядки до 2.10 ніде не показуються — не рахуємо
                continue
            cands.append((_ts(row, ms_cols, dcol)[0] or 0, key, fn, row))
    cands.sort(key=lambda c: c[0], reverse=True)      # новіші першими
    retry = settle_pending.retry
    keys = set(c[1] for c in cands)
    for k in [k for k in retry if k not in keys]:
        retry.pop(k)                                  # ключ уже не в черзі
    ready = cands if force else [c for c in cands if retry.get(c[1], (0, 0))[0] <= now]
    settle_pending.last_pending = len(cands)          # бек-лог (/status)
    settle_pending.last_waiting = len(cands) - len(ready)   # у бекофі
    n_done = n_skip = n_fail = n_try = 0
    lim = int(limit) if limit else None
    done_ix = set()

    def _defer(key):
        nf = retry.get(key, (0, 0))[1] + 1
        retry[key] = (now + _backoff_ms(nf), nf)
        return nf

    def _run(ix_iter, cap):
        nonlocal n_done, n_skip, n_fail, n_try
        for ix in ix_iter:
            if ix in done_ix:
                continue
            if cap is not None and n_try >= cap:
                break
            done_ix.add(ix)
            _, key, fn, row = ready[ix]
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
                    nf = _defer(key)
                    log("[settle] %s: стрічка ще недоступна — відкладено (спроба %d, повтор "
                        "через %d хв)" % (key, nf, _backoff_ms(nf) // MIN_MS))
                    continue
                append_settlement(data_dir, out)
                retry.pop(key, None)
                n_done += 1
            except Transient as e:
                # джерело недоступне (мережа/429) — рядок не фіксуємо,
                # наступна спроба після бекофу. Вичерпаний БЮДЖЕТ циклу
                # (code="budget": кап HL-запитів у боті) — не вина рядка:
                # без бекофу, наступний цикл спробує знову
                n_fail += 1
                n_try += 1
                if getattr(e, "code", None) != "budget":
                    _defer(key)
                log("[settle] %s: відкладено — %s" % (key, e))
            except Exception as e:
                n_fail += 1
                n_try += 1
                _defer(key)
                log("[settle] збій %s: %r" % (key, e))

    # аудит v2.17 H: НОВІ і СТАРІ рядки чергуються (новіший, найстаріший,
    # наступний новіший, …) — квота на кількість спроб без чергування не
    # гарантувала бек-логу мережевого бюджету (перші 20 нових з'їдали кап
    # HL-запитів циклу, старі отримували лише budget-винятки)
    n_r = len(ready)
    order, i, j = [], 0, n_r - 1
    while i <= j:
        order.append(i)
        if j != i:
            order.append(j)
        i += 1; j -= 1
    _run(order, lim)
    if n_old:
        log("[settle] пропущено рядків до v%s: %d" % (".".join(map(str, MIN_ALGO_V)), n_old))
    _prune_tape(data_dir, log)
    log("[settle] done=%d skipped=%d failed=%d waiting=%d tape=%s hl=%s"
        % (n_done, n_skip, n_fail, settle_pending.last_waiting, ctx["tape"].stats,
           ctx["hl"].stats))
    return n_done, n_skip, n_fail


settle_pending.retry = {}          # key → (next_retry_ms, n_fail): бекоф відкладених рядків
settle_pending.last_pending = 0    # кандидатів у черзі (бек-лог)
settle_pending.last_waiting = 0    # з них у бекофі (пропущені без витрати спроби)


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

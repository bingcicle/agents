import http.server
import urllib.request
import urllib.error
import json
import os
import time
import random
import threading
import socket
import ssl
import struct
import math
import queue
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# ── ЛОГИ З ЧАСОМ: усі print у файлі проходять тут. terminal.log без
#    міток часу був сліпий для розслідувань ("КОЛИ впав WS?" — невідомо).
#    flush=True: systemd-append не буферизує, лог живий одразу.
_print_raw = print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _print_raw(time.strftime("%d.%m %H:%M:%S"), *args, **kwargs)

PORT      = 3000
DIR       = os.path.dirname(os.path.abspath(__file__))

# ── ПАПКА ДАНИХ: окремо від коду, щоб заміна/видалення папки бота
#    не чіпала статистику, стан і секрети. При першому старті все,
#    що знайдено у старій папці, переїжджає саме. Якщо прав немає
#    (локальний запуск) — тихо працюємо по-старому в папці коду.
DATA_DIR = "/home/hl_data"
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    _p = os.path.join(DATA_DIR, ".probe")
    open(_p, "w").close(); os.remove(_p)
except Exception:
    DATA_DIR = DIR
if DATA_DIR != DIR:
    for _fn in ("state.json", "sim_trades.csv", "fc_trades.csv",
                "strat_trades.csv", "strat_signals.csv", "tx1_trades.csv",
                "rev_trades.csv", "rev_signals.csv", "rev_outcomes.csv",
                "follow_trades.csv",
                "wallet_profiles.json", "prio_fetch.csv",
                "follow_outcomes.csv",
                "tg_token.txt", "tg_chat.json", "ws_proxy.txt",
                "rest_proxy.txt", "creds.json"):
        try:
            _src, _dst = os.path.join(DIR, _fn), os.path.join(DATA_DIR, _fn)
            if os.path.exists(_src) and not os.path.exists(_dst):
                os.replace(_src, _dst)
                print(f"  [DATA] {_fn} -> {DATA_DIR}")
        except Exception as _e:
            print(f"  [DATA] migrate {_fn}: {_e}")
REFRESH_S = 30 * 60
SCAN_TOP  = 60000   # весь лідерборд (вже 40к+), із запасом на ріст
# Скан це фоновий перепис, йому нікуди спішити. 5 воркерів дають
# ~6 гаманців/с, скан 4600 за ~13 хв при циклі 30 хв. Головне:
# майже нуль 429, а 429 у скані ще й отруював smart-skip,
# записуючи живих китів як порожніх.
WORKERS   = 5
DELAY     = 0.05   # затримка між запитами (сек)

# Пропускати порожні гаманці через SKIP_AFTER порожніх сканів,
# повертатись до них раз на CHECK_EVERY сканів
SKIP_AFTER  = 3
CHECK_EVERY = 5
# ВИНЯТОК (аудит покриття 04.09): топ-N лідерборду за accountValue
# сканується КОЖЕН скан, без smart-skip. Хронічно "порожній" гаманець з
# $50M екваті — це кит між позиціями, а не мертвий акаунт; смарт-скіп
# бачив його раз на ~3.7 год нарівні з нульовими. Ціна: ~+6.5% часу
# циклу (виміряно на лозі 944 год)
VIP_TOP_N   = 2000

# Скільки WS-знайдених гаманців (поза лідербордом) максимум додаємо
# до одного скану. Запобіжник від сплеску нових адрес у стрімі трейдів.
WS_EXTRA_MAX = 5000

# Пауза між WS-підписками і стеля backoff реконекту. IP, засвічений
# штормом реконектів, сервер ріже за будь-яку агресію: підписки шлемо
# повільно, а між невдалими спробами чекаємо аж до години, щоб
# бан устиг злетіти (частий ретрай може продовжувати його вічно).
WS_SUB_DELAY   = 0.25
WS_BACKOFF_CAP = 3600
WS_STALE_S     = 180    # «протухле» з'єднання: TCP живий, pong-и йдуть,
                        # підписки підтверджені, але жодного ТРЕЙДА довше
                        # 3 хв — біржа перестала годувати старий сокет
                        # (01.09: 17+ хв тиші при 114/114 підписках, новий
                        # конект дав трейди за секунду). Рвемо і
                        # перепідключаємось самі, не чекаючи розриву
WS_STALE_MAX_S = 1800   # ескалація порога, якщо після реконекту трейдів
                        # так і не було (3→6→12→24→30 хв): мовчить сама
                        # біржа — не влаштовуємо шторм реконектів

# Проксі ТІЛЬКИ для WS (обхід бана IP): env WS_PROXY або файл
# ws_proxy.txt у папці даних. Формат: host:port або
# user:pass@host:port (HTTP CONNECT). Трафік всередині — TLS до
# Hyperliquid, проксі його не читає. REST-запити йдуть напряму.
WS_PROXY = os.environ.get("WS_PROXY", "").strip()
if not WS_PROXY:
    try:
        with open(os.path.join(DATA_DIR, "ws_proxy.txt")) as _f:
            WS_PROXY = _f.read().strip()
    except Exception:
        pass

# Проксі для ПРІОРИТЕТНИХ REST-перевірок невідомих гаманців: окремий
# IP, щоб ці запити не їли rate-limit основного каналу (скан/sweep).
# env REST_PROXY або файл rest_proxy.txt. Формати: host:port,
# user:pass@host:port або host:port:user:pass (як дають постачальники).
def _norm_proxy(p):
    p = (p or "").strip()
    if not p or "@" in p:
        return p
    parts = p.split(":")
    if len(parts) == 4:   # host:port:user:pass
        return f"{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    return p

REST_PROXY = _norm_proxy(os.environ.get("REST_PROXY", ""))
if not REST_PROXY:
    try:
        with open(os.path.join(DATA_DIR, "rest_proxy.txt")) as _f:
            REST_PROXY = _norm_proxy(_f.read())
    except Exception:
        pass

# Пріоритетна перевірка: WS бачить великий трейд невідомої адреси ->
# негайний REST по ній замість чекання скану (лаг до 30 хв губив перші
# закриття свіжих китів). Поріг ПО-МОНЕТНИЙ від кешованої глибини:
# 10% глибини сторони = мінімальний алерт-шматок (5%) × мінімальний
# ratio (2); флор проти пилу на надтонких монетах.
PRIO_K_DEPTH        = 0.10
PRIO_FLOOR_USD      = 15_000.0
PRIO_COOLDOWN_S     = 600   # одна адреса — не частіше разу на 10 хв
PRIO_MAX_PER_MIN    = 10    # стеля тригерів; надлишок дропається з логом
PRIO_DIRECT_PER_MIN = 4     # без проксі прямі запити ще скупіші

CUSTOM_WALLETS = []

# Блекліст монет: не моніторимо позиції по цих монетах.
# Ліквідність величезна, навіть великі позиції ціну не рухають.
COIN_BLACKLIST = {"BTC", "ETH", "XRP", "BNB"}

# ── TELEGRAM ─────────────────────────────────────────────
# Токен бота НЕ тримаємо в коді (старий уже світився у бекапах —
# його треба ротувати через BotFather). Шукаємо у env TG_TOKEN,
# потім у файлі tg_token.txt поруч із server.py.
TG_TOKEN = os.environ.get("TG_TOKEN", "").strip()
if not TG_TOKEN:
    try:
        with open(os.path.join(DATA_DIR, "tg_token.txt")) as _f:
            TG_TOKEN = _f.read().strip()
    except Exception:
        pass
if not TG_TOKEN:
    print("  [TG] УВАГА: токен не знайдено (env TG_TOKEN або tg_token.txt). "
          "Алерти в Telegram не працюватимуть.")
TG_CHAT_FILE = os.path.join(DATA_DIR, "tg_chat.json")
TG_CHAT_ID = None          # заповнюється після /start, зберігається у файл

def _tg_save_chat(cid):
    try:
        with open(TG_CHAT_FILE, "w") as _f:
            json.dump({"chat_id": cid}, _f)
    except Exception as _e:
        # без збереження chat_id рестарт "забуде" користувача
        print(f"  [TG] chat_id НЕ збережено: {_e}")

try:
    with open(TG_CHAT_FILE) as _f:
        TG_CHAT_ID = json.load(_f).get("chat_id")
    if TG_CHAT_ID:
        print(f"  [TG] chat_id відновлено з файлу: {TG_CHAT_ID}")
except Exception:
    pass
MIN_DELTA_PCT = 0.01       # дельта, з якої взагалі перевіряємо fills
# Хард-фільтри (вимога користувача, 30.08, після серії MET з позицією
# $17k і ratio 0.15): дрібнота не алертиться І не пише статистику
# стратегій. SIM/FC-легасі не чіпаємо — їхні ряди порівнянні з історією.
MIN_POS_USD = 50_000.0     # позиція кита менша — не сигнал ніде
MIN_TX_USD  = 5_000.0      # транзакція закриття менша — не сигнал ніде
MIN_CLOSE_PCT = 0.05       # поріг АЛЕРТУ: ОДНА маркет-транзакція >= 5%
                           # позиції (вимога користувача, 27.08). Кумулятивні
                           # епізоди і поріг за глибиною для алертів вимкнені:
                           # нарізка 10 x 1% свідомо не алертиться. Кожна
                           # достатня транзакція шле ОКРЕМЕ повідомлення
                           # (дедуп лише за hash транзакції, alerted_txs).
EPISODE_TTL_S = 600        # (для чистки старих записів close_episodes у стані)
CLOSE_DEPTH_RATIO = 1.0    # (вимкнено 27.08: більше не впливає на алерти)

# Стеля ws_wallets: WS додає обидві сторони кожного трейда, без
# капу словник ріс би необмежено. Витіснення LRU: новий трейд
# пересуває адресу в кінець, першою випадає найдавніше активна.
WS_WALLETS_CAP = 30000

tg_chat_lock = threading.Lock()

def tg_send(text):
    """3 спроби з паузами: алерт — це продукт системи, разовий збій
    мережі чи Telegram не має право його з'їсти назавжди."""
    if not TG_TOKEN:
        return None   # нема токена — постійно, ретрай безглуздий
    with tg_chat_lock:
        chat_id = TG_CHAT_ID
    if not chat_id:
        print(f"  [TG] No chat_id yet. Message: {text[:60]}")
        return None   # до /start доставляти нікуди — не ретраїмо
    data = json.dumps({"chat_id": chat_id, "text": text,
                       "parse_mode": "HTML", "disable_web_page_preview": True}).encode()
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data=data, headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = json.loads(r.read())
            if resp.get("ok"):
                return True
            print(f"  [TG] Error: {resp}")
        except urllib.error.HTTPError as e:
            # Telegram віддає помилки саме HTTP-статусом (400 битий HTML,
            # 403 бот заблокований) — urlopen кидає HTTPError ще до
            # читання тіла, тож ловимо окремо
            _body = ""
            try:
                _body = e.read()[:200].decode("utf-8", "ignore")
            except Exception:
                pass
            print(f"  [TG] HTTP {e.code} (спроба {attempt+1}/3): {_body}")
            if e.code in (400, 403):
                # постійна помилка (битий HTML / бот заблокований):
                # None = «не ретраїти» — sender дропне одразу, а не
                # молотитиме 11 приречених циклів (рев'ю v2.7 №2)
                stats["tg_errors"] = stats.get("tg_errors", 0) + 1
                return None
        except Exception as e:
            print(f"  [TG] Send error (спроба {attempt+1}/3): {e}")
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
    stats["tg_errors"] = stats.get("tg_errors", 0) + 1
    return False

def tg_poll_updates():
    """Поллінг Telegram для отримання chat_id після /start."""
    global TG_CHAT_ID
    if not TG_TOKEN:
        print("  [TG] Без токена поллінг не стартує.")
        return
    offset = 0
    print("  [TG] Polling for updates (send /start to bot to get alerts)...")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={offset}&timeout=30"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=35) as r:
                data = json.loads(r.read())
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                text = msg.get("text", "")
                cid  = msg.get("chat", {}).get("id")
                if cid and text.startswith("/start"):
                    with tg_chat_lock:
                        current = TG_CHAT_ID
                    if current and cid != current:
                        # чужий /start НЕ перехоплює алерти: інакше будь-хто,
                        # знайшовши бота, забирав би сигнали собі назавжди.
                        # Свідома зміна чату: видали tg_chat.json і перезапусти.
                        print(f"  [TG] /start від стороннього чату {cid} — ігнорую")
                        continue
                    with tg_chat_lock:
                        TG_CHAT_ID = cid
                    _tg_save_chat(cid)
                    print(f"  [TG] Chat ID set: {cid}")
                    tg_send("✅ <b>Hyperliquid Terminal</b>\n\nАлерти активовані. Ти отримаєш повідомлення коли кит починає закривати позицію з ratio ≥ 2x.")
        except Exception as e:
            time.sleep(5)


SYMBOL_MAP = {
    "KPEPE":  "1000PEPEUSDT",
    "KBONK":  "1000BONKUSDT",
    "KSHIB":  "1000SHIBUSDT",
    "KFLOKI": "1000FLOKIUSDT",
}
DEPTH_PCT  = 0.01   # 1% від best price
DEPTH_LIMIT = 500   # рівнів стакану

def get_bn_symbol(coin):
    return SYMBOL_MAP.get(coin.upper(), coin.upper() + "USDT")

def fetch_binance_depth(coin, retries=3):
    symbol = get_bn_symbol(coin)
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={DEPTH_LIMIT}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if not bids or not asks:
                return None
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            ask_depth = sum(float(p)*float(q) for p,q in asks if float(p) <= best_ask*(1+DEPTH_PCT))
            bid_depth = sum(float(p)*float(q) for p,q in bids if float(p) >= best_bid*(1-DEPTH_PCT))
            return {"ask": ask_depth, "bid": bid_depth, "max": max(ask_depth, bid_depth)}
        except Exception as e:
            if attempt < retries-1:
                time.sleep(1)
    return None

def depth_for_side(d, side):
    """Глибина тієї сторони стакану, яку атакує закриття позиції:
    LONG закривається продажем у bid, SHORT — купівлею з ask.
    Старий max(ask, bid) застосовував більшу сторону і применшував
    тиск: позиція у 2.4x від "своєї" сторони могла виглядати як 0.6x
    і не потрапляти у watchlist. max лишається запасним варіантом."""
    if not d:
        return 0
    v = d.get("bid" if side == "LONG" else "ask", 0)
    return v or d.get("max", 0)

def fetch_hl_coins_list():
    """Всі perp монети з Hyperliquid — ТОЧНІ назви, як їх віддає API.
    Раніше тут стояло .upper(), і воно тихо ламало всі mixed-case
    монети (kPEPE, kBONK...): позиції приходили як "kPEPE", а глибина
    лежала під ключем "KPEPE" → ratio завжди 0, у watchlist такі
    монети не потрапляли ніколи (за місяць у лозі жодної), і
    WS-підписка на "KPEPE" теж була битою."""
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info",
        data=json.dumps({"type":"metaAndAssetCtxs"}).encode(),
        headers={"Content-Type":"application/json","User-Agent":"Mozilla/5.0"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    return [u["name"] for u in data[0]["universe"]
            if "/" not in u.get("name","")]

def fetch_binance_symbols_set():
    req = urllib.request.Request(
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        headers={"User-Agent":"Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        info = json.loads(r.read())
    return {s["symbol"] for s in info.get("symbols",[])
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT"}

def fetch_all_depth(coins=None):
    """Завантажує глибину для всіх HL монет що є на Binance."""
    t0 = time.time()
    print(f"  [DEPTH] ── START ──────────────────────────────")

    # Якщо монети не передані — беремо весь HL universe
    if coins is None:
        try:
            coins = fetch_hl_coins_list()
            print(f"  [DEPTH] HL universe: {len(coins)} coins")
        except Exception as e:
            print(f"  [DEPTH] HL fetch error: {e}")
            return {}

    # Binance symbols
    try:
        bn_symbols = fetch_binance_symbols_set()
        print(f"  [DEPTH] Binance perpetuals: {len(bn_symbols)} symbols")
    except Exception as e:
        print(f"  [DEPTH] Binance exchangeInfo error: {e}")
        bn_symbols = set()

    coins = [c for c in coins if c.upper() not in COIN_BLACKLIST]
    ok_coins = [c for c in coins if get_bn_symbol(c) in bn_symbols]
    skip     = [c for c in coins if get_bn_symbol(c) not in bn_symbols]
    print(f"  [DEPTH] Will fetch: {len(ok_coins)} | Not on Binance: {len(skip)}")
    if skip:
        print(f"  [DEPTH] Skipped: {', '.join(skip[:15])}{'...' if len(skip)>15 else ''}")

    depth_map = {}
    errors    = []

    def fetch_one_depth(coin):
        d = fetch_binance_depth(coin)
        if d:
            depth_map[coin] = d
        else:
            errors.append(coin)

    # 2 запити/сек — Binance weight limit
    total = len(ok_coins)
    for i in range(0, total, 2):
        batch = ok_coins[i:i+2]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(fetch_one_depth, batch))
        done = min(i+2, total)
        pct  = done/total*100
        if done % 20 == 0 or done == total:
            print(f"  [DEPTH] {done}/{total} ({pct:.0f}%) | got: {len(depth_map)} | errors: {len(errors)}")
        if i+2 < total:
            time.sleep(1.1)

    elapsed = time.time() - t0
    print(f"  [DEPTH] Done in {elapsed:.0f}s — {len(depth_map)} coins, {len(errors)} errors")
    if errors:
        print(f"  [DEPTH] Errors: {', '.join(errors[:10])}{'...' if len(errors)>10 else ''}")
    print(f"  [DEPTH] ── END ────────────────────────────────")
    return depth_map

def run_depth_loop():
    """Запускається першим, до сканування гаманців. Потім оновлюється кожні 30 хвилин."""
    while True:
        print(f"\n  [DEPTH] Starting depth fetch (parallel with wallet scan)...")
        depth = fetch_all_depth()
        with cache_lock:
            if depth:
                # Зливаємо ПО-МОНЕТНО: монета, що цього разу не
                # завантажилась (разова помилка Binance), тримає стару
                # глибину, а не випадає з watchlist на цілий цикл.
                # Ціна питання: делістнута монета висить зі старою
                # глибиною — нешкідливо, позицій у ній вже не буде.
                cache["depth_prev"] = dict(cache.get("depth", {}))
                merged = dict(cache.get("depth", {}))
                merged.update(depth)
                cache["depth"] = merged
                carried = len(merged) - len(depth)
                print(f"  [DEPTH] Cache updated: {len(depth)} fresh"
                      + (f", {carried} carried over" if carried > 0 else ""))
            else:
                # Binance не відповів: НЕ затираємо робочі дані порожнім
                print(f"  [DEPTH] Fetch returned empty, keeping old data "
                      f"({len(cache.get('depth', {}))} coins)")
        time.sleep(REFRESH_S)


# ── СТАН ─────────────────────────────────────────────────
# addr_lower -> {empty_streak, scan_count, last_had_pos}
wallet_stats = {}
stats_lock   = threading.Lock()
scan_number  = 0   # лічильник сканувань

cache = {
    "data": None, "wallets": [], "updated_at": 0,
    "scanning": False,
    "progress": {"done": 0, "total": 0, "phase": "", "skipped": 0},
    "ws_discovered": 0, "lb_total": 0,
    "scan_number": 0,
    "depth":      {},   # coin -> {ask, bid, max}
    "depth_prev": {},   # попередній depth як fallback
}
cache_lock = threading.Lock()

# ── HTTP helpers ─────────────────────────────────────────
class RateLimited(Exception):
    """Запит впав через 429 rate limit після всіх retry."""
    pass

class APIError(Exception):
    """Запит впав через мережеву помилку/таймаут після всіх retry."""
    pass

def hl_post(body, retries=4):
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=data,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        method="POST"
    )
    last_was_429 = False
    last_err = ""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code == 429:
                last_was_429 = True
                time.sleep(2 ** attempt)
            else:
                last_was_429 = False
                time.sleep(1)
        except Exception as e:
            # причина зберігається для повідомлення винятку: інакше у лозі
            # лише «max retries», і таймаут від DNS не відрізнити (рев'ю
            # v2.8 — важкий шлях історії профілів робить до 18 спроб)
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            last_was_429 = False
            time.sleep(1)
    # Вичерпали retry — кидаємо конкретний тип помилки
    if last_was_429:
        raise RateLimited("429 after retries")
    raise APIError(f"max retries ({last_err})")

def hl_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

# Окремий opener для пріоритетних запитів: увесь їхній трафік іде через
# REST_PROXY (HTTP CONNECT, TLS наскрізний — проксі вміст не читає).
_prio_opener = None
if REST_PROXY:
    _prio_opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"https": "http://" + REST_PROXY,
                                     "http":  "http://" + REST_PROXY}))

def hl_post_prio(body, retries=2, direct=False):
    """hl_post пріоритетного каналу: через REST_PROXY, щоб перевірки
    невідомих китів не їли ліміт основної IP. direct=True (або без
    проксі) — прямий запит; викликач тоді сам тримає жорсткіший кап
    PRIO_DIRECT_PER_MIN."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "Mozilla/5.0"},
        method="POST")
    opener = (urllib.request.urlopen
              if direct or not _prio_opener else _prio_opener.open)
    last_was_429 = False
    last_err = ""
    for attempt in range(retries):
        try:
            with opener(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            last_was_429 = e.code == 429
            if attempt < retries - 1:   # після останньої спроби не спати
                time.sleep(2 ** attempt if last_was_429 else 1)
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            last_was_429 = False
            if attempt < retries - 1:
                time.sleep(1)
    if last_was_429:
        raise RateLimited("prio 429 after retries")
    raise APIError(f"prio max retries ({last_err})")

def _hl_post_prio_direct(body, retries=2):
    return hl_post_prio(body, retries, direct=True)

def _prio_probe():
    """Разова перевірка проксі на старті: одразу видно в лозі, чи канал
    живий, а не через годину мовчазних збоїв."""
    if not REST_PROXY:
        print("  [PRIO] rest_proxy.txt немає — пріоритетні перевірки "
              f"підуть НАПРЯМУ (кап {PRIO_DIRECT_PER_MIN}/хв)")
        return
    try:
        r = hl_post_prio({"type": "allMids"}, retries=1)
        ok = isinstance(r, dict) and r
        print(f"  [PRIO] проксі {REST_PROXY.split('@')[-1]}: "
              f"{'OK' if ok else 'відповідь дивна: ' + str(r)[:80]}")
    except Exception as e:
        print(f"  [PRIO] проксі {REST_PROXY.split('@')[-1]} НЕ працює: {e} "
              f"— після 3 збоїв поспіль воркер сам перемкнеться на прямий "
              f"канал ({PRIO_DIRECT_PER_MIN}/хв) і пробуватиме проксі "
              f"кожні 30 хв")

# ── LEADERBOARD ──────────────────────────────────────────
def load_leaderboard():
    print("  [LB] Fetching leaderboard...")
    lb   = hl_get("https://stats-data.hyperliquid.xyz/Mainnet/leaderboard")
    rows = lb.get("leaderboardRows", lb if isinstance(lb, list) else [])

    wallets = []
    for r in rows:
        addr = r.get("ethAddress", "")
        if not addr: continue
        perf = {w: v for w, v in (r.get("windowPerformances") or [])}
        wallets.append({
            "addr":      addr,
            "name":      r.get("displayName") or "",
            "account":   float(r.get("accountValue") or 0),
            "pnl_at":    float((perf.get("allTime") or {}).get("pnl", 0) or 0),
            "pnl_day":   float((perf.get("day")     or {}).get("pnl", 0) or 0),
            "vol_day":   float((perf.get("day")     or {}).get("vlm", 0) or 0),
            "roi_day":   float((perf.get("day")     or {}).get("roi", 0) or 0),
            "source":    "leaderboard",
            "pos_count": 0,
        })

    wallets.sort(key=lambda w: w["account"], reverse=True)
    top = wallets[:SCAN_TOP]
    with cache_lock:
        cache["lb_total"] = len(wallets)
    print(f"  [LB] {len(wallets)} total, scanning {len(top)} "
          f"(${top[0]['account']:,.0f} → ${top[-1]['account']:,.0f})")
    return top

# ── WEBSOCKET ────────────────────────────────────────────
ws_wallets = {}
ws_lock    = threading.Lock()

def add_ws_wallet(addr):
    if not addr or not addr.startswith("0x") or len(addr) != 42: return False
    k = addr.lower()
    with ws_lock:
        existed = k in ws_wallets
        if existed:
            # LRU: свіжий трейд пересуває адресу в кінець черги, інакше
            # давно доданий, але АКТИВНИЙ кит був би першим на витіснення
            ws_wallets[k] = ws_wallets.pop(k)
        else:
            if len(ws_wallets) >= WS_WALLETS_CAP:
                # витісняємо адресу з найдавнішою активністю
                ws_wallets.pop(next(iter(ws_wallets)), None)
            ws_wallets[k] = {"addr": addr, "source": "websocket",
                             "account": 0, "pnl_at": 0, "pnl_day": 0,
                             "vol_day": 0, "roi_day": 0, "name": "",
                             "pos_count": 0}
    # Гаманець щойно ТОРГНУВ: якщо smart-skip списав його як хронічно
    # порожній — повертаємо в чергу перевірки. Стосується і ВЖЕ відомих
    # адрес: раніше return False стояв до скидання, і жива адреса зі
    # streak 4-5 чекала планової перевірки годинами.
    s = wallet_stats.get(k)
    if s and s.get("empty_streak", 0) >= SKIP_AFTER:
        with stats_lock:
            s["empty_streak"] = 0
    return not existed

def _ws_open_tcp(host, port, timeout=30):
    """TCP до host:port напряму або через HTTP CONNECT проксі (WS_PROXY)."""
    if not WS_PROXY:
        return socket.create_connection((host, port), timeout=timeout)
    creds, _, hp = WS_PROXY.rpartition("@")
    phost, _, pport = hp.rpartition(":")
    raw = socket.create_connection((phost, int(pport)), timeout=timeout)
    try:
        req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n"
        if creds:
            b64 = __import__("base64").b64encode(creds.encode()).decode()
            req += f"Proxy-Authorization: Basic {b64}\r\n"
        raw.sendall((req + "\r\n").encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = raw.recv(4096)
            if not chunk or len(resp) > 65536:
                raise ConnectionError("proxy CONNECT: обірвана відповідь")
            resp += chunk
        status = resp.split(b"\r\n", 1)[0]
        if b" 200" not in status:
            raise ConnectionError(f"proxy відмовив: {status[:60]!r}")
        return raw
    except Exception:
        try: raw.close()
        except Exception: pass
        raise

def ws_handshake(sock, host, path):
    """Повертає (ok, leftover). leftover — байти ПІСЛЯ заголовків:
    це вже початок першого фрейма, їх треба віддати в читання,
    інакше потік розсинхронізується."""
    key = __import__("base64").b64encode(os.urandom(16)).decode()
    # User-Agent і Origin як у браузера: WAF перед api може різати
    # "голі" апгрейди без них, особливо з IP із поганою історією
    sock.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
                  f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                  f"Sec-WebSocket-Version: 13\r\n"
                  f"User-Agent: Mozilla/5.0\r\n"
                  f"Origin: https://app.hyperliquid.xyz\r\n\r\n").encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(4096)
        if not chunk or len(resp) > 65536:
            # сервер закрив сокет: recv віддає b"" миттєво, без цієї
            # перевірки цикл крутився б вічно впустую
            return False, b""
        resp += chunk
    head, _, leftover = resp.partition(b"\r\n\r\n")
    # Саме статусна стрічка, а не "101 десь у тілі 403-ї сторінки"
    ok = head.split(b"\r\n", 1)[0].startswith((b"HTTP/1.1 101", b"HTTP/1.0 101"))
    return ok, leftover

def ws_recv(sock, send_lock=None, rbuf=None):
    """Читає одне ПОВНЕ повідомлення, збираючи фрагменти (FIN=0 +
    continuation-фрейми) — раніше фрагментований JSON тихо губився.
    None: з'єднання закрите або помилка. b"": службовий фрейм.
    rbuf: bytearray із хвостом, що прийшов разом із handshake.
    На ping сервера відповідаємо pong, інакше сервер рве з'єднання."""
    try:
        def read_exact(n):
            buf = b""
            if rbuf:
                take = bytes(rbuf[:n]); del rbuf[:n]
                buf += take
            while len(buf) < n:
                chunk = sock.recv(min(65536, n - len(buf)))
                if not chunk:
                    # recv повертає b"" на закритому сокеті: без цієї перевірки
                    # цикл читання крутився б вічно впустую
                    raise ConnectionError("closed")
                buf += chunk
            return buf
        message = b""
        in_msg  = False
        while True:
            h = read_exact(2)
            fin    = bool(h[0] & 0x80)
            opcode = h[0] & 0x0F
            length = h[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", read_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", read_exact(8))[0]
            if length + len(message) > 16 * 1024 * 1024:
                # сміттєва довжина = розсинхрон парсера; краще реконект,
                # ніж спроба зачитати "мультигігабайтний фрейм" до OOM
                raise ConnectionError(f"frame too large: {length}")
            payload = read_exact(length) if length else b""
            if opcode == 8:
                return None
            if opcode == 9:
                # ping → pong; керуючі фрейми можуть прилітати
                # і МІЖ фрагментами одного повідомлення
                if send_lock is not None:
                    with send_lock:
                        ws_send_frame(sock, 0xA, payload)
                else:
                    ws_send_frame(sock, 0xA, payload)
                if not in_msg:
                    return b""
                continue
            if opcode == 10:          # pong на наш ping
                if not in_msg:
                    return b""
                continue
            if opcode in (1, 2):
                in_msg  = True
                message = payload
            elif opcode == 0 and in_msg:
                message += payload    # continuation-фрагмент
            else:
                if not in_msg:
                    return b""
                continue
            if fin:
                return message
    except Exception:
        return None

def ws_send_frame(sock, opcode, payload=b""):
    n = len(payload)
    if n < 126:
        hdr = bytes([0x80 | opcode, 0x80 | n])
    elif n < 65536:
        hdr = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack(">H", n)
    else:
        hdr = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack(">Q", n)
    mask = os.urandom(4)
    sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

def ws_send(sock, msg):
    ws_send_frame(sock, 0x1, msg.encode())

def _ws_conn(coins, label, initial_delay=0):
    """
    Одна WS-сесія на свою половину монет. Реконект вічний.
    Hyperliquid рве з'єднання, якщо клієнт ~60с нічого не шле,
    тому окремий потік шле {"method":"ping"} кожні 30с.
    Підписки шле окремий потік ПОВІЛЬНО, а читання стартує одразу:
    якщо слати всі підряд і не читати, вхідний буфер забивається
    трейдами вже підписаних монет, сервер бачить повільного клієнта
    і рве з'єднання — Broken pipe посеред підписки.
    Реконект з експоненційним backoff аж до години: шторм раз на 5с
    (193k реконектів у старому лозі) тримав IP забаненим тижнями.
    """
    if initial_delay:
        time.sleep(initial_delay)
    backoff = 5
    while True:
        sock = None
        alive = threading.Event()
        connected_at = 0.0
        try:
            ctx = ssl.create_default_context()
            raw = _ws_open_tcp("api.hyperliquid.xyz", 443)
            sock = ctx.wrap_socket(raw, server_hostname="api.hyperliquid.xyz")
            sock.settimeout(60)
            hs_ok, leftover = ws_handshake(sock, "api.hyperliquid.xyz", "/ws")
            if not hs_ok:
                raise ConnectionError("handshake failed")
            rbuf = bytearray(leftover)
            send_lock = threading.Lock()
            # sock/send_lock/alive фіксуємо через дефолтні аргументи:
            # інакше замикання після реконекту бачили б уже НОВІ об'єкти,
            # старі потоки не помирали б і накопичувались.
            def _send_json(obj, sock=sock, lock=send_lock):
                with lock:
                    ws_send(sock, json.dumps(obj))
            def _kill(sock=sock):
                # Збій відправки: фрейм міг піти наполовину, стан потоку
                # невідомий. Тихо жити далі не можна — інакше частина
                # підписок губиться назавжди. Саме shutdown, НЕ close:
                # close з чужого потоку не будить reader, що вже висить
                # у recv (чекав би 60с таймауту), і звільняє fd, який
                # може перевикористати паралельний REST-запит. shutdown
                # будить recv одразу (EOF) → звичайний реконект.
                try: sock.shutdown(socket.SHUT_RDWR)
                except Exception: pass
            connected_at = time.time()
            alive.set()
            stats[f"ws_subs_{label}"] = 0
            stats[f"ws_expected_{label}"] = len(coins)
            def _subscriber(alive=alive, send=_send_json, kill=_kill):
                for coin in coins:
                    if not alive.is_set():
                        return
                    try:
                        send({"method": "subscribe",
                              "subscription": {"type": "trades", "coin": coin}})
                    except Exception:
                        kill()
                        return
                    time.sleep(WS_SUB_DELAY)
            def _pinger(alive=alive, send=_send_json, kill=_kill):
                while alive.is_set():
                    time.sleep(30)
                    if not alive.is_set():
                        break
                    try:
                        send({"method": "ping"})
                    except Exception:
                        kill()
                        break
            confirmed = set()   # монети з підтвердженою підпискою
            def _sub_checker(alive=alive, send=_send_json, kill=_kill,
                             confirmed=confirmed):
                """Раніше часткове підтвердження (100 із 115) мовчало
                вічно: монети без підписки глухли, а статус був зелений.
                Тепер: дочекатись дедлайну, доподписати мовчазні; якщо
                провалилась третина+ — реконект, кілька штук — гучний лог
                (sweep їх прикриває)."""
                time.sleep(len(coins) * WS_SUB_DELAY + 60)
                if not alive.is_set():
                    return
                missing = [c for c in coins if c not in confirmed]
                if not missing:
                    return
                print(f"  [WS-{label}] без підтвердження {len(missing)} "
                      f"підписок — повторюю")
                for c in missing:
                    if not alive.is_set():
                        return
                    try:
                        send({"method": "subscribe",
                              "subscription": {"type": "trades", "coin": c}})
                    except Exception:
                        kill()
                        return
                    time.sleep(WS_SUB_DELAY)
                time.sleep(30)
                if not alive.is_set():
                    return
                missing = [c for c in coins if c not in confirmed]
                if len(missing) >= max(3, len(coins) // 3):
                    print(f"  [WS-{label}] досі мовчать {len(missing)} — реконект")
                    kill()
                elif missing:
                    print(f"  [WS-{label}] монети без підписки: "
                          f"{', '.join(missing[:10])}"
                          f"{'...' if len(missing) > 10 else ''} (sweep прикриє)")
            threading.Thread(target=_subscriber, daemon=True).start()
            threading.Thread(target=_pinger, daemon=True).start()
            threading.Thread(target=_sub_checker, daemon=True).start()
            # Лічильники з суфіксом label: кожен пише лише свій потік,
            # інакше A, вдало перепідключившись, обнуляв би стрік B
            stats[f"ws_connects_{label}"] = stats.get(f"ws_connects_{label}", 0) + 1
            stats[f"ws_up_{label}"] = connected_at
            # «мовчить з»: початок серії конектів БЕЗ жодного трейда на
            # цьому лейблі. Форс-реконекти його не переставляють (рев'ю
            # v2.8: ws_up скидався кожні 3 хв, і _ws_dead_labels ніколи не
            # бачив «5 хв без трейдів» — TG-алерт не приходив)
            if not stats.get(f"ws_stale_streak_{label}") \
               or not stats.get(f"ws_silent_since_{label}"):
                stats[f"ws_silent_since_{label}"] = connected_at
            print(f"  [WS-{label}] connected, шлю {len(coins)} підписок "
                  f"по {WS_SUB_DELAY}s"
                  + (f" через проксі" if WS_PROXY else ""))
            # Поріг тиші — ДИНАМІЧНИЙ від стріку у stats: базові 3 хв,
            # подвоюється за кожен форс-реконект поспіль без трейдів,
            # перший трейд скидає стрік — і поріг одразу знову 3 хв
            # (рев'ю v2.8: ліміт, захоплений при конекті, лишав 30 хв
            # на всю добу життя сокета, а стрік зі старого знімка ріс
            # храповиком попри трейди)
            def _stale_limit(label=label):
                sk = stats.get(f"ws_stale_streak_{label}", 0)
                return min(WS_STALE_S * (2 ** min(sk, 4)), WS_STALE_MAX_S)
            def _stale(label=label, connected_at=connected_at):
                """True = з'єднання формально живе (фрейми йдуть), але
                трейдів немає довше за поріг. Рахується від пізнішого з
                двох: конект або останній трейд цього з'єднання. Лише
                при підтверджених підписках — інакше це справа
                _sub_checker, а не «протухання»."""
                now_s = time.time()
                limit = _stale_limit()
                if now_s - connected_at < limit:
                    return False
                if stats.get(f"ws_subs_{label}", 0) <= 0:
                    return False
                last = stats.get(f"ws_last_{label}", 0) / 1000.0
                return not last or now_s - last >= limit
            def _stale_fire(label=label, connected_at=connected_at,
                            n_coins=len(coins)):
                sk = f"ws_stale_streak_{label}"
                stats[sk] = stats.get(sk, 0) + 1
                stats[f"ws_stale_reconnects_{label}"] = \
                    stats.get(f"ws_stale_reconnects_{label}", 0) + 1
                _last = stats.get(f"ws_last_{label}", 0) / 1000.0
                # тиша САМЕ цього сокета (не від трейда попереднього)
                _age = time.time() - max(connected_at, _last)
                _nxt = min(WS_STALE_S * (2 ** min(stats[sk], 4)),
                           WS_STALE_MAX_S)
                print(f"  [WS-{label}] тиша {_age:.0f}с при живому "
                      f"з'єднанні ({stats.get(f'ws_subs_{label}', 0)}/"
                      f"{n_coins} підписок) — форс-реконект "
                      f"#{stats[f'ws_stale_reconnects_{label}']}, "
                      f"стрік {stats[sk]}, наступний поріг {_nxt:.0f}с")
            while True:
                frame = ws_recv(sock, send_lock, rbuf)
                if frame is None: break
                if not frame:
                    # службовий фрейм (pong-контроль): саме такі фрейми
                    # тримають «живим» сокет, у який біржа перестала слати
                    # трейди (60с таймаут recv його ніколи не зловить).
                    # Тишу перевіряємо ТІЛЬКИ тут і на службових JSON-
                    # повідомленнях нижче — не перед розбором даних (рев'ю
                    # v2.8: перший трейд після паузи інакше сам ставав
                    # приводом для розриву і губився)
                    if _stale():
                        _stale_fire()
                        break
                    continue
                try:
                    obj = json.loads(frame.decode("utf-8", errors="ignore"))
                except Exception:
                    continue
                ch = obj.get("channel", "")
                if ch == "subscriptionResponse":
                    # Пам'ятаємо, ЯКІ саме монети підтверджені: чекер
                    # доподпише мовчазні, /status покаже ws_subs_A/B
                    try:
                        confirmed.add(obj["data"]["subscription"]["coin"])
                    except Exception:
                        confirmed.add(f"?{len(confirmed)}")
                    stats[f"ws_subs_{label}"] = len(confirmed)
                    if len(confirmed) == len(coins):
                        print(f"  [WS-{label}] всі {len(confirmed)} підписок підтверджені")
                    if _stale():
                        _stale_fire()
                        break
                    continue
                if ch == "pong":
                    if _stale():
                        _stale_fire()
                        break
                    continue
                if ch == "error":
                    print(f"  [WS-{label}] server error: {str(obj)[:160]}")
                    if _stale():
                        _stale_fire()
                        break
                    continue
                # Сюди доходять лише реальні дані (трейди). Тільки вони
                # оновлюють ws_last_ms: якщо рахувати й pong-и, з'єднання
                # з порізаними підписками виглядало б "живим" вічно.
                stats["ws_last_ms"] = time.time() * 1000
                stats[f"ws_last_{label}"] = stats["ws_last_ms"]
                if stats.get(f"ws_stale_streak_{label}"):
                    # трейд прийшов = форс-реконект допоміг; поріг тиші
                    # повертається до базових 3 хв
                    stats[f"ws_stale_streak_{label}"] = 0
                if stats.get(f"ws_fails_{label}"):
                    # трейди йдуть = з'єднання здорове. Раніше стрік
                    # скидався лише при НАСТУПНОМУ розриві, і /status
                    # годинами брехав "мертве" про живе з'єднання
                    stats[f"ws_fails_{label}"] = 0
                data = obj.get("data", [])
                if not isinstance(data, list): data = [data]
                for t in data:
                    if not isinstance(t, dict): continue
                    ws_trade_fastpath(t)   # миттєвий детект закриттів китів
                    for u in list(t.get("users") or []) + ([t["user"]] if t.get("user") else []):
                        if add_ws_wallet(u):
                            with cache_lock:
                                cache["ws_discovered"] = cache.get("ws_discovered", 0) + 1
            print(f"  [WS-{label}] disconnected")
        except Exception as e:
            print(f"  [WS-{label}] {e}")
        alive.clear()
        try:
            if sock: sock.close()
        except: pass
        # Пожило довше 2 хв — проблема була разова, стартуємо швидко.
        # Інакше подвоюємо паузу аж до WS_BACKOFF_CAP: якщо бан
        # продовжується від кожної спроби, тільки довга пауза дає
        # йому шанс злетіти. Джитер ВНИЗ: розносить A і B у часі,
        # не перевищуючи стелю. Стрік — свій на кожне з'єднання.
        fs_key = f"ws_fails_{label}"
        stats[f"ws_subs_{label}"] = 0   # мертве з'єднання = 0 підписок,
                                        # інакше /status бреше "57/57"
        if connected_at and time.time() - connected_at > 120:
            backoff = 5
            stats[fs_key] = 0
        else:
            stats[fs_key] = stats.get(fs_key, 0) + 1
            backoff = min(backoff * 2, WS_BACKOFF_CAP)
        pause = backoff - random.uniform(0, backoff / 4)
        print(f"  [WS-{label}] reconnect in {pause:.0f}s "
              f"(fail streak {stats[fs_key]})")
        time.sleep(pause)

def run_websocket():
    """
    Дві паралельні WS-сесії, половина монет на кожну. Дві причини:
    впала одна, друга тримає детект без дірки на перепідписку,
    і можливий ліміт підписок на одне з'єднання не ріже мовчки
    монети з кінця списку (VVV і решта пізніх лістингів).
    """
    try:
        coins = [c for c in fetch_hl_coins_list() if c.upper() not in COIN_BLACKLIST]
    except Exception as e:
        print(f"  [WS] coin list failed ({e}), fallback")
        coins = ["SOL","DOGE","AVAX","LINK","ARB","OP","SUI","APT",
                 "INJ","TIA","ATOM","NEAR","HYPE","WIF","PEPE","JUP"]
    half = (len(coins) + 1) // 2
    a, b = coins[:half], coins[half:]
    proxy_note = ""
    if WS_PROXY:
        proxy_note = f" | проксі: {WS_PROXY.rpartition('@')[2]}"  # без кредів
    print(f"  [WS] {len(coins)} coins → A:{len(a)} + B:{len(b)} (два з'єднання)"
          + proxy_note)
    threading.Thread(target=_ws_conn, args=(a, "A"), daemon=True).start()
    if b:
        # B стартує пізніше: два одночасні конекти з одного IP
        # виглядають агресивніше для тротлінгу
        threading.Thread(target=_ws_conn, args=(b, "B", 15), daemon=True).start()

# ── FETCH ONE WALLET ─────────────────────────────────────
def fetch_one(addr_str):
    try:
        # Кит щойно торгнув: пропускаємо fast-перевірку вперед. Але зі
        # СТЕЛЕЮ: кожен трейд відсуває hold ще на 3с, і при безперервному
        # потоці скан чекав би необмежено
        _hd = time.time() + 10
        while time.time() < fast_hold[0] and time.time() < _hd:
            time.sleep(0.2)
        time.sleep(DELAY)
        data = hl_post({"type": "clearinghouseState", "user": addr_str})
        all_positions = data.get("assetPositions", [])

        result = []
        for p in all_positions:
            pos = p.get("position", {})
            sz  = float(pos.get("szi", 0))
            if not sz: continue
            coin = pos.get("coin", "?")
            if coin.upper() in COIN_BLACKLIST:
                continue   # блекліст монет
            lev = pos.get("leverage", {})
            result.append({
                "addr":  addr_str,
                "coin":  coin,
                "side":  "LONG" if sz > 0 else "SHORT",
                "size":  abs(sz),
                "val":   abs(float(pos.get("positionValue", 0))),
                "pnl":   float(pos.get("unrealizedPnl", 0)),
                "entry": float(pos.get("entryPx", 0)),
                "liq":   float(pos.get("liquidationPx") or 0),
                "lev":   lev.get("value","?") if isinstance(lev, dict) else "?",
            })
        return result
    except:
        # 429 або мережа: це НЕ "порожній гаманець". None каже скану
        # не чіпати лічильник empty_streak, інакше живі кити
        # отруюються і випадають у chronic-skip
        return None

# ── SMART SKIP LOGIC ─────────────────────────────────────
def should_skip(addr_lower, scan_num):
    """True якщо гаманець порожній достатньо разів і ще не час його перевіряти."""
    with stats_lock:
        s = wallet_stats.get(addr_lower, {})
        streak = s.get("empty_streak", 0)
        last_checked = s.get("last_checked", 0)

        if streak < SKIP_AFTER:
            return False
        # Перевіряємо раз на CHECK_EVERY сканів
        return (scan_num - last_checked) < CHECK_EVERY

def update_stats(addr_lower, had_positions, scan_num):
    with stats_lock:
        s = wallet_stats.setdefault(addr_lower, {"empty_streak": 0, "last_checked": 0})
        s["last_checked"] = scan_num
        if had_positions:
            s["empty_streak"] = 0
        else:
            s["empty_streak"] = s.get("empty_streak", 0) + 1

# ── SCAN ─────────────────────────────────────────────────

# ── POSITION TRACKING (для детекції закриття) ───────────
# addr_lower -> {coin -> {"val", "side", "ratio"}}
prev_positions = {}
tracking_lock  = threading.Lock()


# ── REAL-TIME WATCHLIST ──────────────────────────────────
# Два шляхи детекції:
#   FAST: WebSocket trades несе адреси обох сторін кожної угоди.
#         Кит з watchlist зробив тейкер-угоду в бік закриття → миттєвий
#         точковий чек саме цього гаманця. Детект ~0.3-0.5с після блоку.
#   SWEEP: повний обхід усіх гаманців раз на WATCH_INTERVAL як резерв,
#          якщо WS впав або пропустив повідомлення.
watchlist      = {}   # addr_lower -> {coin -> {val, side, ratio, entry}}
recent_alerts  = []   # останні 50 алертів для термінала
alerts_lock    = threading.Lock()
watchlist_lock = threading.Lock()
sent_alerts    = set()   # addr:coin, спільний дедуп для realtime і скан-діфа
alerted_txs    = {}      # tx_hash -> ts: щоб одна транзакція не алертилась
                         # двічі (realtime + скан-діф); чиститься за годину
alert_queue    = queue.Queue()   # алерти шле ОДИН потік по черзі: інакше
                                 # потік-на-алерт міняв повідомлення місцями
                                 # (29% -> 71% -> 41% замість 29 -> 41 -> 71)

def run_alert_sender():
    while True:
        a = alert_queue.get()
        # Ретраїмо ЦЕЙ алерт до успіху/капу, НЕ беручи наступний:
        # порядок доставки — інваріант v1.3 (черга існує саме для
        # нього), реенкью в хвіст його ламав (рев'ю v2.7 №1).
        # Head-of-line затримка свіжих алертів при мертвому TG —
        # свідома ціна порядку. Дубль можливий, якщо TG прийняв, а
        # таймаут з'їв відповідь — дубль кращий за втрату.
        for _try in range(11):
            try:
                ok = send_close_alert(a)
            except Exception as e:
                print(f"  [ALERT] sender err: {e}")
                ok = False
            if ok:
                break
            if ok is None:
                # постійна помилка TG (400/403/нема chat_id):
                # ретрай приречений — дроп одразу (рев'ю v2.7 №2)
                print(f"  [ALERT] DROP (постійна помилка TG): "
                      f"{a.get('coin', '?')} {str(a.get('addr', '?'))[:10]}…")
                break
            time.sleep(min(30, 5 * (_try + 1)))
        else:
            print(f"  [ALERT] DROP після 11 спроб доставки: "
                  f"{a.get('coin', '?')} {str(a.get('addr', '?'))[:10]}…")

# Лічильники з моменту старту. Дивитись: localhost:3000/status або щогодинний рядок у лозі
stats = {
    "started":        time.time(),
    "ws_last_ms":     0,   # час останнього трейда з WS: якщо давно, WS мертвий
    "ws_matched":     0,   # трейди китів з watchlist, спіймані fast-path
    "checks":         0,   # точкових перевірок гаманців
    "delta_events":   0,   # зафіксованих зменшень позиції
    "fills_confirmed": 0,  # підтверджених маркет-закриттів
    "fills_empty":    0,   # дельта є, а маркет-філів немає (лімітки або пасив)
    "alerts_sent":    0,
    "rate_limited":   0,
    # Далі динамічні ключі з суфіксом з'єднання (A/B), кожен пише
    # лише свій потік: ws_connects_X, ws_fails_X (невдалі спроби
    # поспіль, 0 = ок), ws_subs_X / ws_expected_X (підтверджені
    # підписки), ws_last_X (останній ТРЕЙД, не pong).
}

def _ws_age_s():
    """Скільки секунд від останнього трейда з WS. Якщо трейдів не було
    ВЗАГАЛІ — вік дорівнює аптайму, але мінімум 61с: інакше сервер, що
    так і не підключився, вічно виглядав би 'ще не стартував' (алерт не
    приходив ніколи — саме так було на проді), а перші 60с після
    рестарту /status брехав би 'ws_alive: true'."""
    if stats["ws_last_ms"]:
        return (time.time() * 1000 - stats["ws_last_ms"]) / 1000
    return max(time.time() - stats["started"], 61.0)

def _ws_dead_labels():
    """Список з'єднань (A/B), що виглядають мертвими: серія невдалих
    реконектів або давно без жодного трейда, хоча колись трейди йшли.
    Глобальний _ws_age_s() цього не бачить: поки A живий, трейди
    оновлюють спільний ws_last_ms, і мертвий B ховається за ним."""
    dead = []
    now_ms = time.time() * 1000
    for lb in ("A", "B"):
        if f"ws_connects_{lb}" not in stats and f"ws_fails_{lb}" not in stats:
            continue   # це з'єднання ніколи не запускалось
        if stats.get(f"ws_fails_{lb}", 0) >= 6:
            dead.append(lb)
            continue
        last = stats.get(f"ws_last_{lb}", 0)
        if last:
            if (now_ms - last) / 1000 > 300:
                dead.append(lb)
        else:
            # підключений, але жодного трейда за 5 хв: підписки порізані
            # або біржа мовчить. Рахуємо від початку СЕРІЇ мовчазних
            # конектів (ws_silent_since), а не від останнього конекту —
            # форс-реконекти v2.8 переставляли ws_up кожні 3 хв, і поріг
            # 5 хв не наставав ніколи (рев'ю v2.8)
            up = (stats.get(f"ws_silent_since_{lb}", 0)
                  or stats.get(f"ws_up_{lb}", 0))
            if up and time.time() - up > 300:
                dead.append(lb)
    return dead
delta_seen     = {}      # addr:coin -> коли вперше побачили дельту (анти-рейс)
fill_cursor    = {}      # addr:coin -> ts(ms) останнього ОБРОБЛЕНОГО філа.
                         # Без нього стара агресивна транзакція з 5-хвилинного
                         # вікна "підтверджувала" нову, не пов'язану дельту
                         # (пасивне закриття) — і йшов фальшивий алерт.
close_episodes = {}      # addr:coin -> {start_size, acc_sz, last_ts, side}:
                         # накопичення нарізаного закриття до порога 5%
scan_tombstones = {}     # addr:coin -> ts повного закриття; захист від
                         # "воскресіння" позиції застарілим снапшотом скану
WATCH_INTERVAL = 20   # резервний обхід. Першу лінію тримає WS,
                      # 20с звільняє ~5 запитів/с постійного навантаження

# Fast path: WS кладе сюди (addr, coin) і будить монітор
fast_pending = {}     # (addr, coin) -> час блоку першого трейда, ms
fast_hold    = [0.0]  # до цього моменту скан-воркери поступаються дорогою
fast_lock    = threading.Lock()
fast_event   = threading.Event()
fast_last    = {}     # (addr, coin) -> ts останнього тригера, дебаунс 2с
_fastpath_err_ts = [0.0]   # дросель логів помилок fastpath

# ── Пріоритетний фетч невідомих китів ───────────────────
_prio_lock   = threading.Lock()
_prio_q      = deque()
_prio_event  = threading.Event()
_prio_seen   = {}        # addr -> ts останнього тригера (кулдаун)
_prio_minute = deque()   # ts тригерів за останню хвилину (стеля)
_prio_direct = deque()   # ts прямих (без проксі) запитів за хвилину
_prio_proxy_state = {"streak": 0, "dead_since": 0.0}  # фолбек мертвої проксі
prio_stats   = {"triggers": 0, "added": 0, "dropped": 0, "errors": 0}
PRIO_CSV     = os.path.join(DATA_DIR, "prio_fetch.csv")
PRIO_HEADERS = ["date", "addr", "trigger_coin", "pos_side", "notional_usd",
                "depth_usd", "threshold_usd", "result", "best_ratio",
                "coins_added", "via_proxy", "eol"]

def _prio_note(addr, coin, taker_side, t):
    """WS-потік: великий трейд НЕвідомої адреси -> у чергу перевірки.
    Тут ЖОДНИХ REST-запитів — лише поріг, кулдаун і стеля за хвилину."""
    if coin.upper() in COIN_BLACKLIST:
        return
    try:
        notional = float(t.get("px", 0)) * float(t.get("sz", 0))
    except (TypeError, ValueError):
        return
    if notional < PRIO_FLOOR_USD:
        return   # дешевий вихід ДО кеш-лока глибини: 99% трейдів дрібні
    # тейкер продав (A) -> якщо це закриття, закривається ЛОНГ -> біди;
    # купив (B) -> шорт -> аски. Та сама шкала, що у ratio watchlist.
    pos_side = "LONG" if taker_side == "A" else "SHORT"
    depth = _sim_depth(coin, pos_side) or 0
    if not depth:
        return   # монета поза нашим всесвітом глибини — ratio не порахувати
    thr = max(PRIO_FLOOR_USD, PRIO_K_DEPTH * depth)
    if notional < thr:
        return
    now = time.time()
    with _prio_lock:
        if now - _prio_seen.get(addr, 0) < PRIO_COOLDOWN_S:
            return
        while _prio_minute and now - _prio_minute[0] > 60:
            _prio_minute.popleft()
        if len(_prio_minute) >= PRIO_MAX_PER_MIN:
            prio_stats["dropped"] += 1
            return   # стеля: краще пропустити, ніж спалити ліміт IP
        _prio_minute.append(now)
        _prio_seen[addr] = now
        prio_stats["triggers"] += 1
        _prio_q.append((addr, coin, pos_side, notional, depth, thr))
    _prio_event.set()
    print(f"  [PRIO] {coin} {addr[:10]}… трейд ${notional:,.0f} "
          f"(поріг ${thr:,.0f}) — перевіряю позиції")

def ws_trade_fastpath(t):
    """
    Викликається з WS-потоку на кожен трейд.
    Якщо тейкер є у watchlist по цій монеті і угода закриває його позицію,
    будимо монітор для миттєвої перевірки. Жодних REST-запитів тут.
    """
    try:
        coin  = t.get("coin", "")
        side  = t.get("side", "")          # сторона тейкера: B=купив, A=продав
        users = t.get("users") or []
        if len(users) < 2 or side not in ("B", "A"):
            return
        # users = [buyer, seller]; тейкер визначається стороною
        taker = ((users[0] if side == "B" else users[1]) or "").lower()
        if not taker:
            return
        with watchlist_lock:
            pos = watchlist.get(taker, {}).get(coin)
        if pos is None:
            # невідома адреса з ВЕЛИКИМ трейдом: пріоритетна перевірка
            # замість чекання скану (лаг до 30 хв губив перші закриття)
            _prio_note(taker, coin, side, t)
            return
        holder_side = pos["side"]
        # Закриття: лонг продає (A), шорт купує (B). Інакше він доливає.
        closing = (holder_side == "LONG" and side == "A") or \
                  (holder_side == "SHORT" and side == "B")
        if not closing:
            return
        key = (taker, coin)
        now = time.time()
        if now - fast_last.get(key, 0) < 1.0:   # дебаунс: серія філів = один чек
            return
        fast_last[key] = now
        t_ms = t.get("time", 0)
        lat  = time.time() * 1000 - t_ms if t_ms else 0
        stats["ws_matched"] += 1
        with fast_lock:
            fast_pending.setdefault(key, t_ms)
        fast_hold[0] = time.time() + 3   # скан відступає на 3с, дорога fast-перевірці
        fast_event.set()
        print(f"  [FAST] {coin} {taker[:10]} taker-{'sell' if side=='A' else 'buy'} "
              f"{t.get('sz','?')} | ws lat {lat:.0f}ms")
    except Exception as _e:
        # шлях гарячий (кожен трейд) — лог із дроселем раз на хвилину,
        # але НЕ мовчання: зламаний fastpath виглядав як "WS живий,
        # а детекція повільна", і причину не було видно ніде
        if time.time() - _fastpath_err_ts[0] > 60:
            _fastpath_err_ts[0] = time.time()
            print(f"  [FAST] handler err (дросель 60с): {_e}")

def update_watchlist(result, depth_snap, scan_start=0, failed_addrs=None,
                     fetch_times=None):
    """Оновлює watchlist після повного скану. fetch_times: addr -> коли
    скан реально зчитав цей гаманець (для звірки з tombstone-ами)."""
    # Гард глибини, ЯК у check_position_changes (аудит покриття 04.09):
    # без нього порожній depth-знімок (Binance-бан довший за перший
    # цикл глибини) дав би кожній позиції ratio=0 і СТЕР би весь
    # watchlist. Старий watchlist кращий за порожній: sweep продовжить
    # вести живі пари, наступний скан перебудує чесно.
    if len(depth_snap) < 10:
        print(f"  [WATCH] depth-знімок неповний ({len(depth_snap)} монет) "
              f"— watchlist НЕ перезаписується")
        return
    new_wl = {}
    for coin, positions in result.items():
        if coin.upper() in COIN_BLACKLIST:
            continue
        d = depth_snap.get(coin)
        for pos in positions:
            if not pos.get("size"): continue
            ds = depth_for_side(d, pos["side"])
            ratio = pos["val"] / ds if ds else 0
            if ratio < 2.0: continue  # watchlist тільки ratio >= 2
            addr = pos["addr"].lower()
            if addr not in new_wl: new_wl[addr] = {}
            new_wl[addr][coin] = {
                "size":  pos["size"],
                "val":   pos["val"],
                "side":  pos["side"],
                "ratio": ratio,
                "entry": pos["entry"],
                "liq":   pos.get("liq", 0),
            }

    with watchlist_lock:
        # Tombstone-и: позиції, які realtime ПОВНІСТЮ закрив (і, можливо,
        # заалертив) уже ПІСЛЯ старту цього скану. Снапшот скану їх ще
        # містить — без цієї перевірки закрита позиція "воскресала" і
        # могла дати повторний алерт про те саме закриття.
        for _k, _ts in list(scan_tombstones.items()):
            if _ts >= scan_start > 0:
                _ta, _, _tc = _k.partition(":")
                # Гасимо лише ЗАСТАРІЛИЙ знімок: якщо скан зчитав цей
                # гаманець уже ПІСЛЯ закриття, у знімку свіжий стан
                # (наприклад, кит перевідкрився) — його не чіпаємо
                if (fetch_times or {}).get(_ta, scan_start) <= _ts \
                   and _ta in new_wl and _tc in new_wl[_ta]:
                    del new_wl[_ta][_tc]
                    if not new_wl[_ta]:
                        del new_wl[_ta]
            elif scan_start > 0:
                scan_tombstones.pop(_k, None)   # старіші за скан — зайві

        # Записи, які realtime ДОДАВ під час скану (новий бік після
        # розвороту), а снапшот скану їх ще не бачив: переносимо,
        # інакше вони губились би при повній заміні watchlist.
        for _la, _lcoins in watchlist.items():
            for _lc, _lp in _lcoins.items():
                if _lp.get("upd", 0) >= scan_start > 0 \
                   and _lc not in new_wl.get(_la, {}):
                    new_wl.setdefault(_la, {})[_lc] = dict(_lp)

        # Гаманці, яких цей скан НЕ зчитав успішно, — і ті, чий запит
        # впав, і ті, що НЕ потрапили у випадкову вибірку WS_EXTRA_MAX:
        # їхній стан НЕВІДОМИЙ, а не "порожній". Раніше несканована
        # WS-адреса мовчки випадала з watchlist разом з активними
        # позиціями. Переносимо старі записи як є — найближчий sweep
        # сам їх перевірить і поправить або прибере.
        carried_unscanned = 0
        for _wa, _wcoins in list(watchlist.items()):
            if _wa in (fetch_times or {}) or not _wcoins:
                continue
            # ПО ПАРАХ адреса+монета, не по адресах: якщо realtime під час
            # скану оновив одну монету гаманця, адреса вже є у new_wl —
            # і перенесення "по адресах" губило решту його монет
            for _wc, _wp in _wcoins.items():
                if _wc not in new_wl.get(_wa, {}):
                    new_wl.setdefault(_wa, {})[_wc] = dict(_wp)
                    carried_unscanned += 1

        # Merge за ЧАСОМ, а не за розміром (last-write-wins): якщо
        # realtime торкався пари ПІСЛЯ того, як скан зчитав цей гаманець,
        # live-запис новіший і виграє повністю. Старий варіант брав live
        # лише з МЕНШИМ розміром — тому долив 100→200 під час скану
        # відкочувався знімком до 100, і наступне закриття 200→150
        # виглядало як долив і губилось.
        for _a, _coins in new_wl.items():
            live_c = watchlist.get(_a, {})
            _ft = (fetch_times or {}).get(_a, scan_start)
            for _c, _p in _coins.items():
                lv = live_c.get(_c)
                if lv is None:
                    # Нова пара гаманець-монета: дедуп скидаємо, а курсор
                    # філів ставимо на момент ЗНІМКА — все, що сталося до
                    # нього, вже враховане в базі і не має права
                    # "підтверджувати" майбутні дельти (фальшиві алерти
                    # старими філами у свіжих записів). Старий епізод теж
                    # геть: недограні 4% зниклої позиції не мають
                    # приклеюватись до нової з тим самим тикером
                    _k = f"{_a}:{_c}"
                    sent_alerts.discard(_k)
                    close_episodes.pop(_k, None)
                    delta_seen.pop(_k, None)
                    fill_cursor[_k] = max(fill_cursor.get(_k, 0),
                                          int(_ft * 1000))
                    continue
                if lv.get("upd", 0) >= _ft and lv.get("upd", 0) >= scan_start > 0:
                    _coins[_c] = dict(lv)
        watchlist.clear()
        watchlist.update(new_wl)
    print(f"  [WATCH] Watchlist updated: {len(new_wl)} wallets, "
          f"{sum(len(v) for v in new_wl.values())} positions with ratio>=2x"
          + (f" | {carried_unscanned} пар carried (не скановані/помилки)"
             if carried_unscanned else ""))

def check_one_wallet(addr, post=None):
    """
    Запитує поточний стан гаманця.
    Повертає dict{coin->pos} якщо OK (порожній {} = позицій немає).
    Кидає RateLimited / APIError при помилці запиту — НЕ плутати з "закрито".
    post: канал запиту (за замовчуванням основний hl_post; пріоритетний
    фетч передає hl_post_prio, щоб іти через свою проксі).
    """
    data = (post or hl_post)({"type": "clearinghouseState", "user": addr})
    # Якщо формат не той — це теж помилка, а не "немає позицій"
    if not isinstance(data, dict) or "assetPositions" not in data:
        raise APIError(f"unexpected response format for {addr[:10]}")
    result = {}
    for p in data.get("assetPositions", []):
        pos = p.get("position", {})
        sz  = float(pos.get("szi", 0))
        if not sz: continue
        coin = pos.get("coin", "?")
        result[coin] = {
            "size":  abs(sz),
            "val":   abs(float(pos.get("positionValue", 0))),
            "side":  "LONG" if sz > 0 else "SHORT",
            "entry": float(pos.get("entryPx", 0)),
            "liq":   float(pos.get("liquidationPx") or 0),
        }
    return result

def _prio_log(addr, coin, side, notional, depth, thr, result, ratio,
              added, via):
    _strat_csv_append(PRIO_CSV, PRIO_HEADERS,
        [_dt(time.time()), addr, coin, side, round(notional, 0),
         round(depth, 0), round(thr, 0), result, round(ratio, 2),
         added, via])

def run_prio_fetcher():
    """Воркер пріоритетних перевірок: читає чергу _prio_q, тягне позиції
    адреси через hl_post_prio (окрема проксі) і додає у watchlist пари з
    ratio>=2 — далі їх веде штатний конвеєр (WS fast-path, sweep,
    алерти, стратегії). Чесна межа: шматок, що ТРИГЕРНУВ перевірку, сам
    не алертиться (базою стає стан ПІСЛЯ нього) — але всі наступні
    шматки цього кита ловляться в секундах замість 30 хв."""
    while True:
        _prio_event.wait(timeout=5)
        _prio_event.clear()
        while True:
            with _prio_lock:
                if not _prio_q:
                    break
                addr, coin0, pos_side, notional, depth0, thr = _prio_q.popleft()
            via_proxy = 1 if REST_PROXY else 0
            if via_proxy and _prio_proxy_state["dead_since"]:
                # проксі визнана мертвою: працюємо напряму, але раз на
                # 30 хв ОДИН запит іде через проксі як проба оживлення
                if time.time() - _prio_proxy_state["dead_since"] >= 1800:
                    _prio_proxy_state["dead_since"] = time.time()  # re-arm
                else:
                    via_proxy = 0
            if not via_proxy:
                now = time.time()
                with _prio_lock:
                    while _prio_direct and now - _prio_direct[0] > 60:
                        _prio_direct.popleft()
                    if len(_prio_direct) >= PRIO_DIRECT_PER_MIN:
                        prio_stats["dropped"] += 1
                        # запиту НЕ БУЛО — але кулдаун не знімаємо
                        # повністю (рев'ю v2.7 №4: гіперактивна адреса
                        # з ratio<2 монополізувала б стелю тригерів у
                        # пікові хвилини), а вкорочуємо до ~75с — вікна
                        # рефілу бюджету. Глухоти 10 хв нема (аудит
                        # v2.6 №8), монополії теж
                        _prio_seen[addr] = now - PRIO_COOLDOWN_S + 75
                        _prio_log(addr, coin0, pos_side, notional, depth0,
                                  thr, "budget", 0, "", via_proxy)
                        continue
                    _prio_direct.append(now)
            best_ratio, added, marks = 0.0, [], []
            now_ms = int(time.time() * 1000)   # курсор = час ДО запиту
            try:
                positions = check_one_wallet(
                    addr, post=(hl_post_prio if via_proxy
                                else _hl_post_prio_direct))
                if via_proxy and _prio_proxy_state["streak"]:
                    _prio_proxy_state["streak"] = 0
                    if _prio_proxy_state["dead_since"]:
                        _prio_proxy_state["dead_since"] = 0.0
                        print("  [PRIO] проксі ожила — повертаюсь на неї")
            except Exception as e:
                prio_stats["errors"] += 1
                if via_proxy:
                    _prio_proxy_state["streak"] += 1
                    if (_prio_proxy_state["streak"] >= 3
                            and not _prio_proxy_state["dead_since"]):
                        _prio_proxy_state["dead_since"] = time.time()
                        print("  [PRIO] проксі мертва (3 збої поспіль) — "
                              f"перемикаюсь на прямий канал "
                              f"{PRIO_DIRECT_PER_MIN}/хв, ретрай проксі "
                              f"за 30 хв")
                print(f"  [PRIO] {addr[:10]}… перевірка впала: {e}")
                with _prio_lock:
                    # transient збій НЕ має глушити адресу на 10 хв
                    # (аудит v2.5): наступний великий трейд цього кита
                    # тригерне перевірку знову
                    _prio_seen.pop(addr, None)
                _prio_log(addr, coin0, pos_side, notional, depth0, thr,
                          "error", 0, "", via_proxy)
                continue
            for c, p in positions.items():
                if c.upper() in COIN_BLACKLIST:
                    continue
                ds = _sim_depth(c, p["side"]) or 0
                ratio = p["val"] / ds if ds else 0
                best_ratio = max(best_ratio, ratio)
                if ratio < 2.0:
                    continue
                with watchlist_lock:
                    if c in watchlist.get(addr, {}):
                        # ВІДОМУ пару НЕ чіпаємо: перезапис size/side
                        # обходив би конвеєр підтвердження монітора —
                        # незвірене закриття 30% губилось би назавжди,
                        # а перезаписаний side ховав фліп (рев'ю v2.5 п.1)
                        continue
                    watchlist.setdefault(addr, {})[c] = {
                        "size": p["size"], "val": p["val"],
                        "side": p["side"], "ratio": ratio,
                        "entry": p["entry"], "liq": p.get("liq", 0),
                        "upd": time.time()}
                    k = f"{addr}:{c}"
                    # ініціалізація нової пари ЯК У СКАНА: курсор на час
                    # ДО запиту (now_ms) — філ, що впав у вікно RTT, не
                    # опиниться позаду курсора (рев'ю v2.5 п.8, той
                    # самий урок, що й у скана)
                    sent_alerts.discard(k)
                    close_episodes.pop(k, None)
                    delta_seen.pop(k, None)
                    fill_cursor[k] = max(fill_cursor.get(k, 0), now_ms)
                added.append(c)
                if c == coin0 and p["side"] == pos_side:
                    # тригерний трейд = закриття цього боку, і воно вже
                    # позаду курсора: для F5 воно і є «першим пострілом»
                    # пари, тож мітку ставимо тут — інакше НАСТУПНА tx
                    # через 30с–59хв виглядала б першою (рев'ю v2.8);
                    # поза watchlist_lock — порядок локів як усюди
                    marks.append(k)
            if marks:
                with strat2_lock:
                    for k in marks:
                        follow_last_close[k] = time.time()
            if added:
                prio_stats["added"] += 1
                result = "added"
                print(f"  [PRIO] {addr[:10]}… У WATCHLIST: {'+'.join(added)} "
                      f"(ratio до {best_ratio:.1f})")
            elif positions:
                result = "known" if best_ratio >= 2.0 else "small_ratio"
            else:
                result = "no_pos"   # встиг усе закрити одним пострілом
            _prio_log(addr, coin0, pos_side, notional, depth0, thr,
                      result, best_ratio, "+".join(added), via_proxy)

def get_recent_market_fills(addr, coin, since_ms, side=None):
    """
    Повертає АГРЕСИВНІ закриття для addr/coin після since_ms,
    згруповані по ТРАНЗАКЦІЯХ (hash). side — бік позиції, яку
    відстежуємо: для LONG закриттям є "Close Long" і "Long > Short",
    а "Short > Long" — це ВІДКРИТТЯ лонга, його приймати не можна,
    інакше філ, що відкрив позицію, підтверджував би її "закриття".

    Логіка:
    - Один блок Hyperliquid = один hash = одна транзакція в explorer.
    - Всередині блоку може бути багато fills та ордерів, всі з одним hash.
    - Агресивне закриття = crossed=true (тейкер: угода перетнула спред
      і рухала ціну) + напрямок закриття + не TWAP. Розворот
      ("Long > Short") — це теж повне закриття старої позиції.
    - Тип ордера НЕ перевіряємо: маркетабельна Gtc-лімітка, поставлена
      в ціну, б'є по стакану так само, як кнопка "маркет". Стара
      перевірка через historicalOrders викидала такі закриття
      (реальні тейкер-закриття губились) і коштувала зайвого запиту.

    Кидає RateLimited / APIError при помилці — щоб не плутати з "немає fills".
    """
    # Пагінація: одна відповідь — максимум 2000 філів. Без ММ-фільтра
    # у watchlist бувають гіперактивні гаманці, і потрібний філ міг
    # не влізти у першу сторінку — тоді закриття тихо губилось.
    # Наступна сторінка стартує з ОСТАННЬОЇ мілісекунди (перекриття),
    # а не з +1: інакше філи, що ділять одну мс на межі сторінок,
    # губились би. Дублі знімає dedup за (time, tid, hash, oid).
    fills = []
    _seen = set()
    _start = since_ms
    _complete = False
    for _page in range(8):
        batch = hl_post({"type": "userFillsByTime",
                         "user": addr,
                         "startTime": _start})
        if batch and len(batch) > 0 and not isinstance(batch[0], dict):
            raise APIError(f"userFillsByTime unexpected format for {addr[:10]}")
        batch = batch or []
        # пагінація спирається на зростання time: якщо API раптом
        # віддав інший порядок, "остання сторінка коротша" означала б
        # НЕ "все прочитано" (аудит v2.6 №4в) — fail-closed
        _tprev = 0
        for _bf in batch:
            _bt = _bf.get("time", 0)
            if _bt < _tprev:
                raise APIError("fills not ascending — unexpected order")
            _tprev = _bt
        fresh = 0
        for f in batch:
            _k = (f.get("time"), f.get("tid"), f.get("hash"), f.get("oid"))
            if _k in _seen:
                continue
            _seen.add(_k)
            fills.append(f)
            fresh += 1
        if len(batch) < 2000:
            _complete = True
            break
        if fresh == 0:
            # 2000+ філів в одну мілісекунду: хвіст ФІЗИЧНО недоступний
            # через API — беремо, що є (fail-closed тут дав би вічний
            # клінч: since_ms не зрушить ніколи)
            print(f"  [FILLS] {addr[:10]}: 2000+ філів в одну мс, "
                  f"хвіст вікна недоступний")
            _complete = True
            break
        _start = max(f.get("time", 0) for f in batch)
    if not _complete:
        # 8 повних сторінок і дані ще Є: результат НЕПОВНИЙ. Прийняти
        # його "як є" означало б підтверджувати закриття огризком
        # історії (аудит v2.5 №1: 5 сторінок обривались мовчки).
        # Fail-closed: помилка -> викликач пропускає цикл БЕЗ алерту і
        # БЕЗ руху курсора, наступний sweep спробує знову.
        print(f"  [FILLS] {addr[:10]}: вікно >16k філів, НЕ повне — "
              f"пропускаю цикл (fail-closed)")
        raise APIError(f"fills window incomplete for {addr[:10]}")

    # Групуємо по HASH (транзакція)
    txs = {}  # hash -> {sz, cost, ts, oids, dir}
    for f in (fills or []):
        # СТРОГА схема (аудит v2.6 №2): це головний детектор, битий
        # рядок тут = НЕ "пропустимо", а "результату довіряти не можна"
        # (fail-closed -> викликач пропускає цикл без алерту і без руху
        # курсора/бази). Раніше: два hashless-філи по 3% клеїлись у
        # фальшиву транзакцію 6%; crossed="false" (рядок) був truthy;
        # px="inf" проходив доларовий поріг; філ без dir тихо зникав.
        if "coin" not in f:
            raise APIError("fill without coin")
        if f.get("coin") != coin: continue
        for _k in ("time", "px", "sz", "dir", "hash", "crossed"):
            if _k not in f:
                raise APIError(f"fill missing '{_k}'")
        _cr = f.get("crossed")
        if not isinstance(_cr, bool):
            raise APIError("fill 'crossed' is not bool")
        if not f.get("hash"):
            raise APIError("fill with empty hash")
        try:
            _vpx = float(f.get("px")); _vsz = float(f.get("sz"))
        except (TypeError, ValueError):
            raise APIError("fill px/sz not numeric")
        if not (math.isfinite(_vpx) and math.isfinite(_vsz)) \
           or _vpx <= 0 or _vsz <= 0:
            raise APIError("fill px/sz non-finite or <=0")
        is_taker = _cr
        d = f.get("dir", "")
        f_liq = bool(f.get("liquidation")) or ("Liquidat" in d)
        if side == "LONG":
            is_close = d.startswith("Close Long") or d.startswith("Long >") or f_liq
        elif side == "SHORT":
            is_close = d.startswith("Close Short") or d.startswith("Short >") or f_liq
        else:
            is_close = ("Close" in d) or (">" in d) or f_liq
        is_twap  = (f.get("twapId") is not None)
        if not ((is_taker or f_liq) and is_close and not is_twap):
            continue
        oid = f.get("oid")
        h  = f.get("hash")
        px, sz = _vpx, _vsz   # уже валідовані finite > 0
        if h not in txs:
            txs[h] = {
                "hash": h, "sz": 0.0, "cost": 0.0,
                "ts": f.get("time", 0), "dir": f.get("dir", ""),
                "oids": set(), "liq": False, "liq_method": "",
            }
        t = txs[h]
        t["sz"]   += sz
        t["cost"] += px * sz
        t["oids"].add(oid)
        if f_liq:
            t["liq"] = True
            _lm = (f.get("liquidation") or {})
            if isinstance(_lm, dict) and _lm.get("method"):
                t["liq_method"] = str(_lm["method"])
        if f.get("time", 0) >= t["ts"]:
            t["ts"] = f.get("time", 0)

    # Перетворюємо на список транзакцій
    market_txs = []
    for h, t in txs.items():
        if t["sz"] <= 0: continue
        market_txs.append({
            "hash":    h,
            "px":      t["cost"] / t["sz"],   # середня ціна транзакції
            "sz":      t["sz"],               # сумарний розмір
            "ts":      t["ts"],
            "dir":     t["dir"],
            "n_orders": len(t["oids"]),
            "liq":     t["liq"],
            "liq_method": t["liq_method"],
        })
    market_txs.sort(key=lambda x: x["ts"], reverse=True)
    return market_txs

def _insert_flipped(addr, coin, pos, alert_key, snap_ms=None):
    """Кит розвернувся (LONG↔SHORT): старий бік закритий і зааалертований,
    а НОВИЙ бік одразу повертаємо у watchlist, якщо він тягне на ratio>=2.
    Раніше нова позиція чекала наступного скану — до ~45 хв сліпоти по
    щойно розвернутому киту."""
    with cache_lock:
        d = cache["depth"].get(coin) or cache.get("depth_prev", {}).get(coin)
    ds = depth_for_side(d, pos.get("side"))
    ratio = pos["val"] / ds if ds else 0
    if ratio < 2.0:
        return
    with watchlist_lock:
        watchlist.setdefault(addr, {})[coin] = {
            "size":  pos["size"],
            "val":   pos["val"],
            "side":  pos["side"],
            "ratio": ratio,
            "entry": pos.get("entry", 0),
            "liq":   pos.get("liq", 0),
            "upd":   time.time(),
        }
    # нова позиція = нова історія: дедуп і епізод старого боку скидаємо,
    # курсор — на момент ЗНІМКА нової позиції (не "зараз": філ, що
    # прилетів після знімка, не має опинитись позаду курсора)
    sent_alerts.discard(alert_key)
    close_episodes.pop(alert_key, None)
    fill_cursor[alert_key] = max(fill_cursor.get(alert_key, 0),
                                 snap_ms or int(time.time() * 1000))
    print(f"  [WATCH] {coin} {addr[:10]} розворот: новий {pos['side']} "
          f"ratio {ratio:.1f}x одразу під наглядом")

def run_realtime_monitor():
    """Моніторить watchlist кожні WATCH_INTERVAL секунд."""
    # Чекаємо поки перший скан заповнить watchlist
    while True:
        with watchlist_lock:
            n = len(watchlist)
        if n > 0:
            break
        time.sleep(5)

    print(f"  [WATCH] Monitor started: FAST via WS + sweep every {WATCH_INTERVAL}s")
    cycle_rl_lock = threading.Lock()
    cycle_rl      = [0]     # rate limit hits (list для мутації з потоків)

    def check_wallet_worker(item):
            """Перевіряє один гаманець. Викликається паралельно."""
            addr, coins = item
            stats["checks"] += 1
            # Момент ЗНІМКА позицій: усі синхронізації бази і курсор
            # філів прив'язуються саме до нього, а не до "зараз" —
            # цикл по монетах може тривати секунди, і філ, що прилетів
            # після знімка, не має права опинитись позаду курсора
            snap_ms = int(time.time() * 1000)
            try:
                current = check_one_wallet(addr)
            except RateLimited:
                stats["rate_limited"] += 1
                with cycle_rl_lock:
                    cycle_rl[0] += 1
                return
            except Exception:
                return

            for coin, old in coins.items():
                new_pos = current.get(coin)
                alert_key = f"{addr}:{coin}"

                # Розворот LONG↔SHORT: стара позиція закрита ПОВНІСТЮ,
                # а решта — вже нова позиція в інший бік. Зберігаємо її:
                # після алерту про закриття новий бік одразу піде у
                # watchlist через _insert_flipped.
                flipped_pos = None
                if new_pos is not None and new_pos.get("side") != old.get("side"):
                    flipped_pos = new_pos
                    new_pos = None

                if new_pos is None:
                    close_pct = 1.0
                    full_close = True
                    delta_seen.setdefault(alert_key, time.time())
                else:
                    old_size = old["size"]
                    new_size = new_pos["size"]
                    delta_size = old_size - new_size
                    if delta_size <= 0:
                        # Кит ДОЛИВ (або без змін): синхронізуємо базу і
                        # МЕТАДАНІ. Розмір міг не змінитись, а вартість,
                        # entry, ліквідація і ratio — так; інакше алерт
                        # показував би ціни тижневої давнини. upd рухаємо
                        # лише при реальній зміні розміру, щоб merge скану
                        # й далі міг оновлювати запис свіжою глибиною.
                        with cache_lock:
                            _dd = (cache["depth"].get(coin)
                                   or cache.get("depth_prev", {}).get(coin))
                        _ds = depth_for_side(_dd, old.get("side"))
                        with watchlist_lock:
                            if addr in watchlist and coin in watchlist[addr]:
                                _w = watchlist[addr][coin]
                                _w["val"]   = new_pos["val"]
                                _w["entry"] = new_pos.get("entry",
                                                          _w.get("entry", 0))
                                _w["liq"]   = new_pos.get("liq",
                                                          _w.get("liq", 0))
                                if _ds:
                                    _w["ratio"] = new_pos["val"] / _ds
                                if delta_size < 0:
                                    _w["size"] = new_size
                                    _w["upd"]  = time.time()
                        if delta_size < 0:
                            # Долив обриває епізод розвантаження: інакше
                            # відсотки рахувались би від старої, меншої
                            # бази і поріг 5% спрацьовував би зарано
                            close_episodes.pop(alert_key, None)
                        else:
                            # Розмір той самий, а ціна входу ІНША: позицію
                            # закрили і перевідкрили тим самим розміром між
                            # sweep-ами. Старий епізод належить мертвій
                            # позиції — інакше його 4% приклеїлись би до
                            # нової і 1% закриття дав би фальшиві "5%"
                            _eo = old.get("entry", 0)
                            _en = new_pos.get("entry", 0)
                            if _eo and _en and abs(_en - _eo) / _eo > 1e-4:
                                close_episodes.pop(alert_key, None)
                        # Позиція звірена зі знімком — і при доливі, і при
                        # НУЛЬОВІЙ дельті ("закрив 10 і перевідкрив рівно
                        # 10"): усе до знімка вже враховане в базі, старий
                        # агресивний філ не має підтверджувати майбутні
                        # пасивні дельти
                        fill_cursor[alert_key] = max(
                            fill_cursor.get(alert_key, 0), snap_ms)
                        sent_alerts.discard(alert_key)
                        delta_seen.pop(alert_key, None)
                        continue
                    if delta_size < old_size * MIN_DELTA_PCT:
                        # < 1%: шум, fills не тягнемо, базу не рухаємо
                        sent_alerts.discard(alert_key)
                        delta_seen.pop(alert_key, None)
                        continue
                    close_pct = delta_size / old_size
                    full_close = False
                    delta_seen.setdefault(alert_key, time.time())

                # ── Підтвердження через fills ──
                # Курсор: беремо лише філи, НОВІШІ за останній оброблений.
                # Інакше стара агресивна транзакція з вікна "підтверджувала"
                # пізнішу пасивну дельту — фальшивий алерт. Вікно — ВІД
                # КУРСОРА (зі стелею година), а не жорсткі -5 хв: якщо
                # перевірка запізнилась (великий watchlist, збій), закриття
                # старше 5 хв інакше ставало непідтверджуваним. Звичайний
                # стан: нульові дельти рухають курсор щоsweep, тож вікно
                # і так коротке; пагінація з дедуплікацією витягне решту.
                stats["delta_events"] += 1
                _cur = fill_cursor.get(alert_key, 0)
                if _cur:
                    since_ms = max(_cur + 1, int((time.time() - 3600) * 1000))
                else:
                    since_ms = int((time.time() - 300) * 1000)
                try:
                    mfills = get_recent_market_fills(addr, coin, since_ms,
                                                     old.get("side"))
                except RateLimited:
                    stats["rate_limited"] += 1
                    with cycle_rl_lock:
                        cycle_rl[0] += 1
                    sent_alerts.discard(alert_key)
                    continue
                except Exception:
                    sent_alerts.discard(alert_key)
                    continue

                if not mfills:
                    stats["fills_empty"] += 1
                    sent_alerts.discard(alert_key)
                    # АНТИ-РЕЙС: ми бачимо зміну позиції за пів секунди,
                    # а філи в API з'являються трохи пізніше. Раніше код
                    # тут одразу списував базу, і алерт губився назавжди.
                    # Саме так пропадали закриття по частині монет.
                    # Свіжій дельті (до 45с) даємо ще спроби
                    if time.time() - delta_seen.get(alert_key, 0) < 45:
                        continue
                    delta_seen.pop(alert_key, None)
                    with watchlist_lock:
                        if full_close:
                            watchlist.get(addr, {}).pop(coin, None)
                            if not watchlist.get(addr):
                                # порожній гаманець без монет: приберемо,
                                # інакше sweep вічно палив би на нього запит
                                watchlist.pop(addr, None)
                        elif addr in watchlist and coin in watchlist[addr]:
                            # Розмір реально змінився, але лімiткою: алерт
                            # не шлемо, а базу оновлюємо, інакше ця дельта
                            # перевірялась би вічно кожні 10 секунд
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                    if full_close:
                        # тихе повне закриття (лімітками): tombstone проти
                        # воскресіння снапшотом скану + новий бік фліпа
                        scan_tombstones[alert_key] = time.time()
                        close_episodes.pop(alert_key, None)
                        # follow-позиції теж мають вийти: позиція кита
                        # зникла, хай і без нових тейкер-філів (аудит п.8)
                        try:
                            with strat2_lock:
                                for _fp in follow_open.values():
                                    if _fp["key"] == alert_key:
                                        _fp["force_exit"] = "full_close"
                        except Exception as _qe:
                            print(f"  [FOLLOW] quiet-close hook err: {_qe}")
                        if flipped_pos is not None:
                            _insert_flipped(addr, coin, flipped_pos,
                                            alert_key, snap_ms)
                    else:
                        # база звірена зі знімком без агресивних філів —
                        # усе до знімка вже враховане, курсор на знімок
                        fill_cursor[alert_key] = max(
                            fill_cursor.get(alert_key, 0), snap_ms)
                    continue
                stats["fills_confirmed"] += 1
                delta_seen.pop(alert_key, None)
                fill_cursor[alert_key] = max(f["ts"] for f in mfills)

                # Фліп-транзакція ("Long > Short") містить і закриття, і
                # відкриття нового боку одним філом: закритого в ній не
                # більше, ніж було в позиції — ріжемо, щоб алерт і
                # симулятор не завищували обсяг
                for _f in mfills:
                    if ">" in _f.get("dir", "") and _f["sz"] > old["size"]:
                        _f["sz"] = old["size"]

                # ── Симулятор: кожна підтверджена маркет-транзакція ──
                try:
                    sim_on_market_txs(addr, coin, old, mfills,
                                      (new_pos["size"] if new_pos else 0.0),
                                      full_close)
                except Exception as _se:
                    print(f"  [SIM] hook err: {_se}")

                # ── Стратегія відкату (п.6): годуємо епізоди ──
                try:
                    fc_on_txs(addr, coin, old, mfills)
                except Exception as _fe:
                    print(f"  [FC] txs hook err: {_fe}")

                # ── ОБОЛОНКА СТРАТЕГІЙ: реверс + вхід у бік. rev читає
                #    епізод ДО того, як fc_on_full_close його зніме
                try:
                    rev_on_close(addr, coin, old, mfills, full_close)
                except Exception as _re:
                    print(f"  [REV] hook err: {_re}")
                try:
                    follow_on_txs(addr, coin, old, mfills, full_close)
                except Exception as _fo:
                    print(f"  [FOLLOW] hook err: {_fo}")

                try:
                    if full_close:
                        fc_on_full_close(addr, coin, old)
                except Exception as _fe:
                    print(f"  [FC] full hook err: {_fe}")

                # ПОРІГ (повернено на вимогу користувача): алерт лише
                # коли ОДНА маркет-транзакція закрила >= MIN_CLOSE_PCT
                # позиції; ліквідації — завжди. Кумулятивні епізоди та
                # поріг за глибиною для алертів ВИМКНЕНІ (нарізка
                # 10 x 1% більше не алертиться — свідомий вибір).
                # Повне закриття без такої транзакції — теж тиша:
                # позиція, злита лімітками з маркет-пилом 0.01 токена,
                # інакше давала алерт "повністю закрив $1.5M".
                _base = old["size"] or 1e-12
                # Ліквідації БЕЗ винятку: часткова ліквідація на 0.01%
                # позиції ($273 пилу) — не сигнал, поріг один для всіх.
                # Хард-фільтр $: транзакція < MIN_TX_USD — пил незалежно
                # від відсотка (5% позиції на $30k — це $1.5k шуму)
                big_txs = [f for f in mfills
                           if f["sz"] >= _base * MIN_CLOSE_PCT
                           and f["px"] * f["sz"] >= MIN_TX_USD]

                # Пара могла пережити у watchlist падіння ratio нижче 2
                # (carry живої серії з оновленими метаданими): закриття
                # записуємо (SIM/FC вже отримали, база оновиться), але
                # алерт не шлемо — сигнал це позиція, ВЕЛИКА відносно
                # ліквідності, а ratio 1.09 нею не є.
                # Хард-фільтр $: позиція < MIN_POS_USD — теж не сигнал
                if big_txs and ((old.get("ratio") or 0) < 2.0
                                or (old.get("val") or 0) < MIN_POS_USD):
                    big_txs = []

                if not big_txs:
                    # агресія є, але кожна транзакція дрібна: без алерту,
                    # базу рухаємо (курсор уже пересунутий вище)
                    with watchlist_lock:
                        if full_close:
                            watchlist.get(addr, {}).pop(coin, None)
                            if not watchlist.get(addr):
                                watchlist.pop(addr, None)
                        elif addr in watchlist and coin in watchlist[addr]:
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                    if full_close:
                        scan_tombstones[alert_key] = time.time()
                        close_episodes.pop(alert_key, None)
                        if flipped_pos is not None:
                            _insert_flipped(addr, coin, flipped_pos,
                                            alert_key, snap_ms)
                    continue

                # КОЖНА достатня транзакція = окреме повідомлення
                # (анти-спам дедуп по парі прибраний на вимогу). Від
                # дублів між realtime і скан-діфом захищає LRU за hash.
                if full_close:
                    # повне закриття: одне повідомлення з усіма новими
                    # транзакціями батча (маркет-обсяг у ньому чесний)
                    _batches = [(mfills, 1.0, True)]
                else:
                    _batches = [([f], min(f["sz"] / _base, 1.0), False)
                                for f in big_txs]
                for _txs, _pct, _fc in _batches:
                    _new_h = [f.get("hash", "") for f in _txs
                              if f.get("hash", "") not in alerted_txs]
                    if not _new_h:
                        continue   # усе з цього батча вже алертилось
                    for _h in _new_h:
                        alerted_txs[_h] = time.time()
                    a = {
                        "addr":      addr,
                        "coin":      coin,
                        "side":      old["side"],
                        "old_val":   old["val"],
                        "old_size":  old["size"],
                        "close_pct": _pct,
                        "ratio":     old["ratio"],
                        "entry":     old["entry"],
                        "full_close": _fc,
                        "fills":     list(_txs),
                    }
                    alert_queue.put(a)

                with watchlist_lock:
                    if full_close:
                        watchlist.get(addr, {}).pop(coin, None)
                        if not watchlist.get(addr):
                            # порожній гаманець: приберемо, інакше sweep
                            # вічно палив би на нього запит
                            watchlist.pop(addr, None)
                    else:
                        if addr in watchlist and coin in watchlist[addr]:
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                if full_close:
                    # tombstone: скан, що почався до закриття, не воскресить
                    # позицію своїм застарілим снапшотом (повторний алерт)
                    scan_tombstones[alert_key] = time.time()
                    close_episodes.pop(alert_key, None)
                    if flipped_pos is not None:
                        _insert_flipped(addr, coin, flipped_pos,
                                        alert_key, snap_ms)

            # НОВІ монети відомого кита (аудит покриття 04.09): відповідь
            # clearinghouseState вже містить УСІ його позиції, але цикл
            # вище дивиться лише на ті, що ВЖЕ у watchlist — нова монета
            # чекала повного скану (до 30+ хв) при нулі додаткових
            # запитів. ratio>=2 -> одразу під нагляд; курсор філів на
            # момент знімка: все до нього — історія нової пари, не сигнал
            for _nc, _np in current.items():
                if _nc in coins or _nc.upper() in COIN_BLACKLIST:
                    continue
                with cache_lock:
                    _dd = (cache["depth"].get(_nc)
                           or cache.get("depth_prev", {}).get(_nc))
                _ds = depth_for_side(_dd, _np.get("side"))
                _r = _np["val"] / _ds if _ds else 0
                if _r < 2.0:
                    continue
                _nk = f"{addr}:{_nc}"
                with watchlist_lock:
                    if _nc in watchlist.get(addr, {}):
                        continue   # realtime додав її після нашого знімка
                    if scan_tombstones.get(_nk, 0) >= snap_ms / 1000.0:
                        # пару закрито ПІСЛЯ нашого знімка (prio/fast
                        # встигли за час циклу по монетах): знімок
                        # застарілий, не воскрешаємо (рев'ю v2.9 C1)
                        continue
                    watchlist.setdefault(addr, {})[_nc] = {
                        "size":  _np["size"],
                        "val":   _np["val"],
                        "side":  _np["side"],
                        "ratio": _r,
                        "entry": _np.get("entry", 0),
                        "liq":   _np.get("liq", 0),
                        "upd":   time.time(),
                    }
                    # ініціалізація ПІД тим самим локом, що і видимість
                    # пари (урок prio-фетчера, рев'ю v2.9 C3); delta_seen
                    # теж чиститься — як у update_watchlist (C2), інакше
                    # застаріла мітка минулого життя пари вимикала
                    # анти-рейс «філи запізнюються» і закриття губилось
                    sent_alerts.discard(_nk)
                    close_episodes.pop(_nk, None)
                    delta_seen.pop(_nk, None)
                    fill_cursor[_nk] = max(fill_cursor.get(_nk, 0), snap_ms)
                print(f"  [WATCH] {_nc} {addr[:10]} нова монета кита зі "
                      f"sweep: {_np['side']} ratio {_r:.1f}x під наглядом")

    def _safe_worker(item):
        try:
            check_wallet_worker(item)
        except Exception as _we:
            print(f"  [WATCH] worker err: {_we}")

    # ── Головний цикл: FAST-події миттєво, повний обхід як резерв ──
    last_sweep = 0.0
    while True:
        woke = fast_event.wait(timeout=1.0)

        if woke:
            fast_event.clear()
            with fast_lock:
                pend = dict(fast_pending)
                fast_pending.clear()
            if pend:
                # Групуємо по гаманцю: один clearinghouseState на адресу
                by_addr = {}
                with watchlist_lock:
                    for a, c in pend:
                        if a in watchlist and c in watchlist[a]:
                            by_addr.setdefault(a, {})[c] = dict(watchlist[a][c])
                if by_addr:
                    t0 = time.time()
                    with ThreadPoolExecutor(max_workers=min(10, len(by_addr))) as pool:
                        list(pool.map(_safe_worker, by_addr.items()))
                    oldest = min(pend.values()) if pend else 0
                    chain  = time.time() * 1000 - oldest if oldest else 0
                    print(f"  [FAST] checked {len(by_addr)} wallet(s) in "
                          f"{time.time()-t0:.2f}s | блок→готово {chain:.0f}ms")

        # Резервний повний обхід. Базовий інтервал WATCH_INTERVAL, але коли
        # watchlist виростає (без ММ-фільтра гаманців стало більше),
        # розтягуємо обхід, аби тримати ~7 запитів/с і не ловити 429.
        now = time.time()
        with watchlist_lock:
            wl_size = len(watchlist)
        sweep_interval = max(WATCH_INTERVAL, wl_size / 7.0)
        if now - last_sweep >= sweep_interval:
            last_sweep = now
            cycle_rl[0] = 0
            with watchlist_lock:
                wl_snap = {addr: dict(coins)
                           for addr, coins in watchlist.items() if coins}
            if wl_snap:
                with ThreadPoolExecutor(max_workers=10) as pool:
                    list(pool.map(_safe_worker, wl_snap.items()))
            if cycle_rl[0] > 0:
                print(f"  [WATCH] sweep rate limited {cycle_rl[0]}x")

def check_position_changes(new_result, depth_snap):
    """
    Порівнює нові позиції з попередніми (між повними сканами), шукає закриття.
    КОЖЕН алерт підтверджується через fills щоб уникнути фейків.
    """
    global prev_positions

    # Guard: якщо depth не завантажений — не можемо рахувати ratio, пропускаємо
    if len(depth_snap) < 10:
        return []

    with tracking_lock:
        prev = dict(prev_positions)

    new_by_addr = {}  # addr -> {coin -> pos}
    failed_keep = {}  # addr -> {coin -> СТАРА позиція}: пари, чиє
                      # підтвердження впало — база НЕ рухається
    for coin, positions in new_result.items():
        if coin.upper() in COIN_BLACKLIST: continue
        d = depth_snap.get(coin)
        if not d: continue
        for pos in positions:
            ds = depth_for_side(d, pos["side"])
            if not ds: continue
            ratio = pos["val"] / ds
            if ratio < 2.0: continue
            if not pos.get("size"): continue
            addr = pos["addr"].lower()
            if addr not in new_by_addr: new_by_addr[addr] = {}
            new_by_addr[addr][coin] = {
                "size":  pos["size"],
                "val":   pos["val"],
                "side":  pos["side"],
                "ratio": ratio,
                "entry": pos["entry"],
            }

    alerts = []
    for addr, coins in prev.items():
        for coin, old in coins.items():
            if not old.get("size"):  # некоректні дані
                continue
            new_addr_pos = new_by_addr.get(addr, {})
            new_pos = new_addr_pos.get(coin)

            if new_pos is None:
                # Повне закриття між сканами звідси не шлемо: закриття могло
                # статись до 30+ хв тому, а fills-підтвердження дивиться лише
                # 5 хв назад. Повні закриття ловить real-time монітор (~20с).
                if old.get("_pending"):
                    # пара з незвіреною дельтою зникла зі знімка: дельта
                    # втрачена — хоча б ГОЛОСНО (рев'ю v2.7 №5а);
                    # realtime міг встигнути покрити її незалежно
                    print(f"  [DIFF] незвірена дельта {addr[:10]}:{coin} "
                          f"втрачена: пара зникла зі знімка")
                continue
            else:
                if new_pos.get("side") != old.get("side"):
                    # розворот: повне закриття, його ловить realtime
                    continue
                delta_size = old["size"] - new_pos["size"]
                if delta_size <= 0: continue
                close_pct = delta_size / old["size"]
                # від 1%: рішення "чи алертити" приймає подвійний фільтр
                # нижче (>=5% позиції АБО >=1x глибини сторони) — інакше
                # закриття на 4% позиції, але на весь стакан, губилось би
                # у цьому резервному шляху
                if close_pct < MIN_DELTA_PCT: continue
                full_close = False

            # ── ПІДТВЕРДЖЕННЯ через fills (обов'язково) ──
            key_ac = f"{addr}:{coin}"
            _cur = fill_cursor.get(key_ac, 0)
            if _cur:
                since_ms = max(_cur + 1, int((time.time() - 3600) * 1000))
            else:
                since_ms = int((time.time() - 300) * 1000)
            try:
                mfills = get_recent_market_fills(addr, coin, since_ms,
                                                 old.get("side"))
            except (RateLimited, APIError, Exception):
                # Не можемо підтвердити — пропускаємо без алерту, а БАЗУ
                # пари лишаємо СТАРОЮ (аудит v2.6 №1: інакше "наступний
                # sweep спробує знову" було брехнею — знімок безумовно
                # затирав стару позицію, 100→80 ставало 80→80 і дельта
                # губилась назавжди)
                _kept = dict(old)
                _kept["_pending"] = time.time()   # маркер незвіреної дельти
                failed_keep.setdefault(addr, {})[coin] = _kept
                continue
            if not mfills:
                # Немає маркет fills — не шлемо алерт
                continue
            # Курсор рухаємо ОДРАЗУ після підтвердження, ЯК У REALTIME
            # (аудит v2.5 №2): інакше філ 4.9%, відхилений порогом,
            # лишався ПЕРЕД курсором і на наступній перевірці (база вже
            # менша: 4.9/96=5.1%) породжував хибний алерт СТАРОЮ
            # транзакцією
            fill_cursor[key_ac] = max(f["ts"] for f in mfills)
            for _f in mfills:   # фліп: закритого не більше за позицію
                if ">" in _f.get("dir", "") and _f["sz"] > old["size"]:
                    _f["sz"] = old["size"]

            # Той самий поріг, що і в realtime: ОДНА маркет-транзакція
            # >= MIN_CLOSE_PCT позиції (ліквідації без винятку) і
            # ratio пари не нижче 2
            _base = old["size"] or 1e-12
            big_txs = [f for f in mfills
                       if f["sz"] >= _base * MIN_CLOSE_PCT
                       and f["px"] * f["sz"] >= MIN_TX_USD]
            # ratio беремо СВІЖИЙ (new_pos, той самий знімок): база,
            # збережена failed_keep через кілька сканів, несла б ratio
            # годинної давнини — v1.3 вимагає "на момент алерту"
            # (рев'ю v2.7 №5б); розмір ПОДІЇ гейтиться старою позицією
            if not big_txs or (new_pos.get("ratio") or 0) < 2.0 \
               or (old.get("val") or 0) < MIN_POS_USD:
                continue

            # окреме повідомлення на кожну достатню транзакцію;
            # дублі з realtime знімає LRU за hash транзакції
            for _f in big_txs:
                _h = _f.get("hash", "")
                if _h in alerted_txs:
                    continue   # realtime вже відправив цю транзакцію
                alerted_txs[_h] = time.time()
                alerts.append({
                    "addr":      addr,
                    "coin":      coin,
                    "side":      old["side"],
                    "old_val":   old["val"],
                    "old_size":  old["size"],
                    "close_pct": min(_f["sz"] / _base, 1.0),
                    "ratio":     new_pos.get("ratio", old["ratio"]),
                    "entry":     old["entry"],
                    "full_close": full_close,
                    "fills":     [_f],
                })

    # Оновлюємо попередні позиції; пари зі збоєм підтвердження
    # зберігають СТАРУ базу — наступний прохід побачить дельту знову
    with tracking_lock:
        for _fa, _fcoins in failed_keep.items():
            for _fc, _fp in _fcoins.items():
                new_by_addr.setdefault(_fa, {})[_fc] = _fp
        prev_positions = new_by_addr

    return alerts

def send_close_alert(a):
    def fmt(n):
        n = abs(float(n))
        if n >= 1e6: return f"${n/1e6:.2f}M"
        if n >= 1e3: return f"${n/1e3:.1f}K"
        return f"${n:.0f}"

    side_emoji = "🟢" if a["side"] == "SHORT" else "🔴"
    old_size   = a.get("old_size", 0)
    close_size = old_size * a["close_pct"] if old_size else 0
    if a["full_close"]:
        size_info  = f" ({old_size:.4f} токенів)" if old_size else ""
        close_type = f"повністю закрив{size_info}"
    else:
        if old_size:
            close_type = f"закрив {a['close_pct']*100:.1f}% ({close_size:.4f} з {old_size:.4f} токенів)"
        else:
            close_type = f"закрив {a['close_pct']*100:.1f}%"
    addr_short = a["addr"][:6] + "…" + a["addr"][-4:]
    scan_link  = f"https://hypurrscan.io/address/{a['addr']}"

    fills     = a.get("fills", [])   # список ТРАНЗАКЦІЙ епізоду (по hash)
    fill_info = ""
    if fills:
        # Підсумки — з агрегатів усього епізоду: список fills може бути
        # обрізаний до 60, а числа мають описувати ВЕСЬ епізод
        total_sz   = a.get("ep_sz") or sum(f["sz"] for f in fills)
        closed_usd = a.get("ep_usd") or sum(f["px"] * f["sz"] for f in fills)
        avg_px     = closed_usd / total_sz if total_sz else 0
        # Унікальні hash — кожен веде на окрему транзакцію в explorer
        seen_hashes = []
        for f in fills:
            h = f.get("hash", "")
            if h and h not in seen_hashes:
                seen_hashes.append(h)
        tx_links = " ".join(
            f'<a href="https://app.hyperliquid.xyz/explorer/tx/{h}">tx{i+1}</a>'
            for i, h in enumerate(seen_hashes[:5])
        )
        n_tx = a.get("ep_n") or len(seen_hashes)
        tx_word = "маркет транзакція" if n_tx == 1 else "маркет транзакцій"
        fill_info = f"\n💸 <b>Маркет:</b> {n_tx} {tx_word}, avg ${avg_px:,.4f}, {total_sz:.4f} токенів"
        # Ratio вгорі описує ПОЗИЦІЮ; тут — сила самого закриття проти
        # свіжої глибини сторони, яку воно б'є (LONG→bid, SHORT→ask)
        side_depth = 0
        try:
            d_live = fetch_binance_depth(a["coin"], retries=1)
            side_depth = depth_for_side(d_live, a["side"]) if d_live else 0
        except Exception as _de:
            print(f"  [ALERT] live-глибина {a['coin']} недоступна: {_de}")
        if not side_depth:
            with cache_lock:
                d_c = (cache["depth"].get(a["coin"])
                       or cache.get("depth_prev", {}).get(a["coin"]))
            side_depth = depth_for_side(d_c, a["side"])
        if side_depth:
            _bside = "bid" if a["side"] == "LONG" else "ask"
            fill_info += (f"\n💥 <b>Закрито:</b> {fmt(closed_usd)} = "
                          f"{closed_usd/side_depth:.2f}× глибини {_bside}")
        if tx_links:
            fill_info += f"\n🔗 {tx_links}"
    try:
        if is_vault(a["addr"]):
            fill_info += ("\n🏦 <b>ВОЛТ</b>: можливо, механічний вивід "
                          "коштів вкладника, а не рішення кита")
    except Exception as _ve:
        print(f"  [ALERT] vault-перевірка {a['addr'][:10]}: {_ve}")
    liq_txs = [f for f in fills if f.get("liq")]
    if liq_txs:
        _m = (liq_txs[0].get("liq_method") or "").lower()
        if "backstop" in _m:
            fill_info += ("\n🔫 <b>Ліквідація:</b> ринок не проковтнув, позицію "
                          "перейняв ліквідатор HLP\n"
                          f"👤 <a href=\"https://hypurrscan.io/address/{HLP_LIQUIDATOR}\">"
                          f"{HLP_LIQUIDATOR[:6]}…{HLP_LIQUIDATOR[-4:]}</a>")
        else:
            fill_info += "\n🔫 <b>Ліквідація</b> (маркет у стакан)"

    msg = (
        f"{side_emoji} <b>#{a['coin']} — {a['side']} position closing</b>\n"
        f"\n"
        f"📊 <b>Ratio:</b> {a['ratio']:.2f}x\n"
        f"💰 <b>Позиція:</b> {fmt(a['old_val'])}\n"
        f"📉 <b>Дія:</b> {close_type}{fill_info}\n"
        f"🎯 <b>Entry:</b> ${a['entry']:,.4f}\n"
        f"\n"
        f"👛 <b>Гаманець:</b> <a href=\"{scan_link}\">{addr_short}</a>\n"
        f"\n"
        f"⚡️ <i>Вхід: {'SHORT' if a['side']=='LONG' else 'LONG'} #{a['coin']}</i>"
    )
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    # ── ОСТАННЯ ЛІНІЯ ЗАХИСТУ: без fills алерт не йде ──
    if not fills:
        print(f"  [BLOCKED][{now}] coin={a['coin']} side={a['side']} full_close={a['full_close']} — fills=0, НЕ відправляємо")
        return True   # свідомий не-алерт, ретрай не потрібен
    _lat = time.time() * 1000 - max(f["ts"] for f in fills)
    print(f"  [ALERT][{now}] coin={a['coin']} side={a['side']} ratio={a['ratio']:.2f}x "
          f"close_pct={a['close_pct']*100:.1f}% fills={len(fills)} | блок→алерт {_lat:.0f}ms")
    tg_result = tg_send(msg)
    print(f"  [ALERT]  tg_sent={'ok' if tg_result else 'FAIL'}")
    if tg_result is None:
        return None    # постійна помилка — sender дропне без ретраїв
    if not tg_result:
        return False   # transient — sender ретраїть ЦЕЙ алерт; стата чесна
    # історія і лічильник — ЛИШЕ після реальної доставки (аудит v2.6
    # №3: "alerts_sent: 1" при невідправленому повідомленні)
    with alerts_lock:
        recent_alerts.insert(0, {
            "ts":        time.time(),
            "coin":      a["coin"],
            "side":      a["side"],
            "ratio":     a["ratio"],
            "close_pct": a["close_pct"],
            "full":      a["full_close"],
            "val":       a["old_val"],
            "addr":      a["addr"],
        })
        del recent_alerts[50:]
    stats["alerts_sent"] += 1
    return True


# ═════════════════════════════════════════════════════════
#  PAPER SIMULATOR: віртуальні входи $1000 за китом
#  Вхід після 2-ї маркет-транзакції. Вихід за правилами нижче.
#  Результат: лог, sim_trades.csv, Google Таблиця (webhook).
# ═════════════════════════════════════════════════════════
SIM_ENABLED        = True
SIM_POSITION_USD   = 1000.0
SIM_ENTRY_AFTER_TX = 2        # входимо після цієї к-сті маркет-транзакцій
SIM_EXIT_RATIO     = 2.0      # вихід A: залишок кита < 2 ratio...
SIM_EXIT_REMAIN    = 0.20     # ...і одночасно < 20% від стартової позиції
SIM_SILENCE_MULT   = 3.0      # вихід B: тиша довше ніж unload_time * 3
SIM_SILENCE_CAP_S  = 1800     # але не більше 30 хв (щоб не висіти годинами)
SIM_SILENCE_MIN_S  = 30       # і не менше 30 секунд
SIM_TRACKER_TTL_S  = 300      # tx1 без tx2 за 5 хв: серія скидається
SIM_MAX_OPEN       = 5        # максимум одночасних симуляційних позицій
SIM_COMMISSION     = 0.0005   # Binance taker 0.05% за сторону
SIM_SPREAD         = 0.0002   # базовий спред у сліпаж-моделі

# Google Таблиця через gspread (pip install gspread google-auth)
GSPREAD_CREDS_FILE = os.path.join(DATA_DIR, "creds.json")  # ключ сервісного акаунта
GSPREAD_SHEET_KEY  = "17NQV-7Ob76XjIUx69K490WvjZv8w6PzR62PPhaTsa3A"   # id таблиці з URL: docs.google.com/spreadsheets/d/<ОЦЕ_ID>/edit
GSPREAD_WORKSHEET  = "trades_imba_bot"  # назва аркуша; якщо такого немає, створиться сам
SIM_CSV = os.path.join(DATA_DIR, "sim_trades.csv")

SIM_HEADERS = ["date_open","date_close","coin","our_side","whale_addr",
               "entry_px","exit_px","gross_move_pct","costs_pct","net_pnl_pct",
               "net_pnl_usd","peak_move_pct","duration_s","exit_reason",
               "speed_s","unload_time_s","ratio_per_min","liq_dist","whale_pnl_pct",
               "whale_ratio","whale_pos_usd","whale_start_size","depth_1pct_usd"]

sim_lock      = threading.Lock()
sim_trackers  = {}   # (addr,coin) -> серія транзакцій кита
sim_positions = {}   # (addr,coin) -> відкрита симуляційна позиція
sim_closed    = []   # закриті, останні 100 для /sim

def sim_all_mids():
    """Поточні mid-ціни всіх монет одним запитом. None при помилці."""
    try:
        data = hl_post({"type": "allMids"})
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def _sim_depth(coin, side=None):
    """Глибина монети; із side — по стороні, яку атакує закриття кита,
    щоб вихід 'whale_exhausted' рахувався в тій самій шкалі, що і
    side-aware ratio у watchlist."""
    with cache_lock:
        d = cache["depth"].get(coin) or cache.get("depth_prev", {}).get(coin)
    if side:
        return depth_for_side(d, side)
    return d["max"] if d and d.get("max") else 0

def _sim_slip(depth):
    """Сліпаж за сторону: спред + прохід по стакану нашим розміром."""
    if depth <= 0:
        return SIM_SPREAD + 0.001
    return SIM_SPREAD + (SIM_POSITION_USD / depth) * 0.005

def _csv_append(row):
    try:
        import csv as _csv
        new = not os.path.exists(SIM_CSV)
        with open(SIM_CSV, "a", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            if new: w.writerow(SIM_HEADERS)
            w.writerow(row)
    except Exception as e:
        print(f"  [SIM] csv err: {e}")

_gs_lock   = threading.Lock()
_gs_client = None
_gs_ws     = {}      # назва аркуша -> worksheet
_gs_failed = False

def _gs_get_ws(name, headers):
    """Аркуш за назвою. Створює і пише заголовки, якщо треба."""
    global _gs_client, _gs_failed
    if name in _gs_ws: return _gs_ws[name]
    if _gs_failed or not GSPREAD_SHEET_KEY: return None
    try:
        import gspread
        if _gs_client is None:
            _gs_client = gspread.service_account(filename=GSPREAD_CREDS_FILE)
        sh = _gs_client.open_by_key(GSPREAD_SHEET_KEY)
        try:
            ws = sh.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=name, rows=2000, cols=len(headers) + 5)
        if not (ws.acell("A1").value or "").strip():
            ws.append_row(headers, value_input_option="RAW")
        _gs_ws[name] = ws
        print(f"  [SHEETS] підключено, аркуш '{name}'")
        return ws
    except ImportError:
        print("  [SHEETS] gspread не встановлено: pip install gspread google-auth. Пишу тільки в CSV")
        _gs_failed = True
    except Exception as e:
        print(f"  [SHEETS] не підключились: {e}. Пишу тільки в CSV")
        _gs_failed = True
    return None

def _sheets_append(name, headers, row):
    def _go():
        with _gs_lock:
            ws = _gs_get_ws(name, headers)
            if ws is None: return
            try:
                ws.append_row(row, value_input_option="RAW")
            except Exception as e:
                print(f"  [SHEETS] append err ({name}): {e}")
    threading.Thread(target=_go, daemon=True).start()

def _sheets_post(row):
    # сумісність зі старим викликом симулятора
    _sheets_append(GSPREAD_WORKSHEET, SIM_HEADERS, row)

def sim_on_market_txs(addr, coin, old, mfills, remaining_size, full_close):
    """Викликається з монітора на кожну підтверджену пачку маркет-транзакцій."""
    if not SIM_ENABLED or not mfills:
        return
    key = (addr, coin)

    with sim_lock:
        tr = sim_trackers.get(key)
        # Позиція розвернулась (LONG↔SHORT): стара серія належить уже
        # мертвому боку — з нею симулятор входив би в протилежний бік
        if tr is not None and key not in sim_positions \
           and tr.get("whale_side") != old.get("side", tr.get("whale_side")):
            sim_trackers.pop(key, None)
            tr = None
        if tr is None:
            tr = {
                "txs": [], "seen": set(),
                "start_size": old.get("size", 0), "start_val": old.get("val", 0),
                "whale_side": old.get("side", "?"), "whale_ratio": old.get("ratio", 0),
                "whale_entry": old.get("entry", 0), "whale_liq": old.get("liq", 0),
                "remaining": remaining_size, "last_tx_ms": 0,
            }
            sim_trackers[key] = tr

        new_txs = sorted([t for t in mfills if t.get("hash") not in tr["seen"]],
                         key=lambda t: t.get("ts", 0))
        if not new_txs and not full_close:
            tr["remaining"] = remaining_size
            return

        # Стара серія видихлась: нова транзакція починає нову серію.
        # Оновлюємо ВСІ поля кита, не лише розмір: entry/liq/ratio теж
        # могли змінитись, інакше вхід рахувався б від мертвих даних
        if tr["txs"] and tr["last_tx_ms"] > 0 and new_txs and \
           (new_txs[0]["ts"] - tr["last_tx_ms"]) / 1000 > SIM_TRACKER_TTL_S and \
           key not in sim_positions:
            tr["txs"] = []
            tr["start_size"]  = old.get("size", tr["start_size"])
            tr["start_val"]   = old.get("val",  tr["start_val"])
            tr["whale_side"]  = old.get("side", tr["whale_side"])
            tr["whale_ratio"] = old.get("ratio", tr["whale_ratio"])
            tr["whale_entry"] = old.get("entry", tr["whale_entry"])
            tr["whale_liq"]   = old.get("liq", tr["whale_liq"])

        for t in new_txs:
            tr["seen"].add(t.get("hash"))
            tr["txs"].append(t)
            tr["last_tx_ms"] = max(tr["last_tx_ms"], t.get("ts", 0))
        tr["remaining"] = 0.0 if full_close else remaining_size

        pos = sim_positions.get(key)
        n_open = len(sim_positions)

    if pos is not None:
        with sim_lock:
            pos["last_tx_ms"] = tr["last_tx_ms"]
            pos["remaining"]  = tr["remaining"]
        _sim_check_exhausted(key)
        return

    if len(tr["txs"]) >= SIM_ENTRY_AFTER_TX and n_open < SIM_MAX_OPEN \
       and not full_close and tr["remaining"] > 0:
        threading.Thread(target=_sim_enter, args=(key,), daemon=True).start()

def _sim_enter(key):
    addr, coin = key
    with sim_lock:
        tr = sim_trackers.get(key)
        if tr is None or key in sim_positions or len(tr["txs"]) < SIM_ENTRY_AFTER_TX:
            return
        t1, t2 = tr["txs"][0], tr["txs"][1]
        start_size, remaining = tr["start_size"], tr["remaining"]
        w_side, w_ratio  = tr["whale_side"], tr["whale_ratio"]
        w_entry, w_liq   = tr["whale_entry"], tr["whale_liq"]
        start_val = tr["start_val"]
        last_tx_ms = tr["last_tx_ms"]

    depth = _sim_depth(coin, w_side)
    if depth <= 0:
        print(f"  [SIM] {coin}: немає depth, вхід пропущено")
        return
    mids = sim_all_mids()
    if not mids or coin not in mids:
        print(f"  [SIM] {coin}: немає mid-ціни, вхід пропущено")
        return
    mid = float(mids[coin])

    # ── Змінні на момент входу ──
    speed_s  = max((t2["ts"] - t1["ts"]) / 1000.0, 0.5)
    avg_sz   = (t1["sz"] + t2["sz"]) / 2.0
    rate     = avg_sz / speed_s                       # токенів за секунду
    unload_s = remaining / rate if rate > 0 else 0
    ratio_per_min = (rate * 60 * mid) / depth
    if w_liq > 0:
        liq_dist = (mid - w_liq) / mid if w_side == "LONG" else (w_liq - mid) / mid
    else:
        liq_dist = ""
    if w_entry > 0:
        whale_pnl = (mid - w_entry) / w_entry * (1 if w_side == "LONG" else -1)
    else:
        whale_pnl = ""

    our_side = "SHORT" if w_side == "LONG" else "LONG"
    slip = _sim_slip(depth)
    entry_eff = mid * (1 + slip) if our_side == "LONG" else mid * (1 - slip)
    silence_s = min(max(unload_s * SIM_SILENCE_MULT, SIM_SILENCE_MIN_S), SIM_SILENCE_CAP_S)

    pos = {
        "addr": addr, "coin": coin, "our_side": our_side,
        "open_ts": time.time(), "entry_mid": mid, "entry_eff": entry_eff,
        "slip": slip, "depth": depth,
        "start_size": start_size, "remaining": remaining, "last_tx_ms": last_tx_ms,
        "silence_s": silence_s, "peak": 0.0,
        "speed_s": speed_s, "unload_s": unload_s, "ratio_per_min": ratio_per_min,
        "liq_dist": liq_dist, "whale_pnl": whale_pnl,
        "whale_ratio": w_ratio, "whale_val": start_val,
    }
    with sim_lock:
        if key in sim_positions or len(sim_positions) >= SIM_MAX_OPEN:
            return
        sim_positions[key] = pos

    threading.Thread(target=save_state, daemon=True).start()
    print(f"  [SIM] ENTER {our_side} {coin} @ {mid:.6g} | whale {w_side} "
          f"{addr[:10]} | speed={speed_s:.1f}s unload={unload_s:.0f}s "
          f"ratio/min={ratio_per_min:.2f} liq_dist={liq_dist if liq_dist=='' else f'{liq_dist:.3f}'} "
          f"silence_exit={silence_s:.0f}s")

def _sim_gross(pos, mid):
    if pos["our_side"] == "LONG":
        return (mid - pos["entry_mid"]) / pos["entry_mid"]
    return (pos["entry_mid"] - mid) / pos["entry_mid"]

def _sim_check_exhausted(key, mid=None):
    """Вихід A: у кита лишилось < SIM_EXIT_RATIO і < 20% стартової позиції."""
    with sim_lock:
        pos = sim_positions.get(key)
        if pos is None: return
        remaining, start_size, depth = pos["remaining"], pos["start_size"], pos["depth"]
    if mid is None:
        mids = sim_all_mids()
        mid = float(mids.get(key[1], 0) or 0) if mids else 0
    if mid <= 0: return
    rem_ratio = (remaining * mid) / depth if depth > 0 else 0
    rem_pct   = remaining / start_size if start_size > 0 else 0
    if rem_ratio < SIM_EXIT_RATIO and rem_pct < SIM_EXIT_REMAIN:
        _sim_exit(key, "whale_exhausted", mid)

def _sim_exit(key, reason, mid):
    with sim_lock:
        pos = sim_positions.pop(key, None)
        sim_trackers.pop(key, None)
    if pos is None: return

    slip = pos["slip"]
    exit_eff = mid * (1 - slip) if pos["our_side"] == "LONG" else mid * (1 + slip)
    gross = _sim_gross(pos, mid)
    costs = 2 * SIM_COMMISSION + 2 * slip
    net_pct = gross - costs
    net_usd = SIM_POSITION_USD * net_pct
    dur = time.time() - pos["open_ts"]

    row = [
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(pos["open_ts"])),
        time.strftime("%Y-%m-%d %H:%M:%S"),
        pos["coin"], pos["our_side"], pos["addr"],
        round(pos["entry_eff"], 8), round(exit_eff, 8),
        round(gross * 100, 4), round(costs * 100, 4), round(net_pct * 100, 4),
        round(net_usd, 2), round(pos["peak"] * 100, 4), round(dur, 1), reason,
        round(pos["speed_s"], 2), round(pos["unload_s"], 1),
        round(pos["ratio_per_min"], 3),
        pos["liq_dist"] if pos["liq_dist"] == "" else round(pos["liq_dist"], 4),
        pos["whale_pnl"] if pos["whale_pnl"] == "" else round(pos["whale_pnl"], 4),
        round(pos["whale_ratio"], 2), round(pos["whale_val"], 0),
        round(pos["start_size"], 4), round(pos["depth"], 0),
    ]
    _csv_append(row)
    _sheets_post(row)
    with sim_lock:
        sim_closed.insert(0, dict(zip(SIM_HEADERS, row)))
        del sim_closed[100:]

    threading.Thread(target=save_state, daemon=True).start()
    icon = "✅" if net_usd >= 0 else "❌"
    print(f"  [SIM] {icon} EXIT {pos['coin']} {reason} | gross {gross*100:+.2f}% "
          f"costs {costs*100:.2f}% net {net_pct*100:+.2f}% (${net_usd:+.2f}) "
          f"| peak {pos['peak']*100:+.2f}% | {dur:.0f}s")

def run_sim_loop():
    """Кожні 2с: оновлює ціни відкритих позицій, перевіряє виходи."""
    print(f"  [SIM] Paper simulator on: ${SIM_POSITION_USD:.0f}/угода, "
          f"вхід після {SIM_ENTRY_AFTER_TX} tx | sheets: "
          f"{'on' if GSPREAD_SHEET_KEY else 'off (тільки CSV)'} | {SIM_CSV}")
    while True:
        time.sleep(2)
        # Прибираємо трекери без позиції, у яких година тиші
        _cut = time.time() * 1000 - 3600 * 1000
        with sim_lock:
            for _k in list(sim_trackers):
                _t = sim_trackers[_k]
                if _k not in sim_positions and _t["last_tx_ms"] and _t["last_tx_ms"] < _cut:
                    del sim_trackers[_k]
            keys = list(sim_positions.keys())
        if not keys:
            continue
        mids = sim_all_mids()
        if not mids:
            continue
        now_ms_ = time.time() * 1000
        for key in keys:
            with sim_lock:
                pos = sim_positions.get(key)
                if pos is None: continue
                coin = pos["coin"]
                mid = float(mids.get(coin, 0) or 0)
                if mid <= 0: continue
                g = _sim_gross(pos, mid)
                if g > pos["peak"]: pos["peak"] = g
                silent = pos["last_tx_ms"] > 0 and \
                         (now_ms_ - pos["last_tx_ms"]) / 1000 > pos["silence_s"]
            try:
                if silent:
                    _sim_exit(key, "silence", mid)
                else:
                    _sim_check_exhausted(key, mid)
            except Exception as _le:
                print(f"  [SIM] loop err {key[1]}: {_le}")


# ═════════════════════════════════════════════════════════
#  STATE: пережиття рестарту
#  Раз на хвилину все важливе скидається у state.json.
#  При старті підхоплюється назад: детект працює одразу,
#  відкриті сим-позиції продовжують жити, а не зникають.
# ═════════════════════════════════════════════════════════
STATE_FILE   = os.path.join(DATA_DIR, "state.json")
STATE_SAVE_S = 60
STATE_MAX_AGE_S = 3600   # старіший за годину стан не відновлюємо

def save_state():
    # під _state_save_lock ЦІЛКОМ (знімок + запис + replace): без нього
    # старіший знімок міг завершити os.replace після новішого і відкотити
    # стан на диску (аудит v2.2 п.3)
    with _state_save_lock:
        _save_state_locked()

def _save_state_locked():
    try:
        with watchlist_lock:
            wl = {a: {c: dict(p) for c, p in coins.items()}
                  for a, coins in watchlist.items()}
        with sim_lock:
            positions = [[k[0], k[1], dict(p)] for k, p in sim_positions.items()]
            trackers  = []
            for k, t in sim_trackers.items():
                tt = dict(t); tt["seen"] = list(t["seen"])
                trackers.append([k[0], k[1], tt])
            closed = list(sim_closed)
        with alerts_lock:
            ra = list(recent_alerts)
        snap = {
            "saved_at":      time.time(),
            "watchlist":     wl,
            "sim_positions": positions,
            "sim_trackers":  trackers,
            "sim_closed":    closed,
            "recent_alerts": ra,
            "sent_alerts":   list(sent_alerts),
            "fill_cursor":   dict(fill_cursor),
            "close_episodes": {k: dict(v) for k, v in close_episodes.items()},
            "fc_positions":  [[k[0], k[1], dict(p)] for k, p in fc_positions.items()],
        }
        with strat2_lock:
            snap["rev_open"] = {k: dict(v) for k, v in rev_open.items()}
            snap["follow_open"] = {k: dict(v) for k, v in follow_open.items()}
            snap["follow_last_close"] = dict(follow_last_close)
            snap["vault_cache"] = dict(vault_cache)
        # tmp унікальний на виклик: конкурентні save_state писали в один
        # файл і os.replace інсталював перемішаний JSON (рев'ю v2.2)
        tmp = f"{STATE_FILE}.tmp{threading.get_ident()}"
        with open(tmp, "w") as f:
            json.dump(snap, f)
        os.replace(tmp, STATE_FILE)   # атомарно: або старий файл, або новий
    except Exception as e:
        print(f"  [STATE] save err: {e}")

def load_state():
    try:
        with open(STATE_FILE) as f:
            snap = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        print(f"  [STATE] load err: {e}"); return
    age = time.time() - snap.get("saved_at", 0)
    if age > STATE_MAX_AGE_S:
        print(f"  [STATE] стан старіший за годину ({age/60:.0f} хв), пропускаю")
        return
    try:
        with watchlist_lock:
            watchlist.clear()
            watchlist.update(snap.get("watchlist", {}))
        with sim_lock:
            for a, c, p in snap.get("sim_positions", []):
                sim_positions[(a, c)] = p
            for a, c, t in snap.get("sim_trackers", []):
                t["seen"] = set(t.get("seen", []))
                sim_trackers[(a, c)] = t
            sim_closed.extend(snap.get("sim_closed", [])[:100])
        with alerts_lock:
            recent_alerts.extend(snap.get("recent_alerts", [])[:50])
        sent_alerts.update(snap.get("sent_alerts", []))
        fill_cursor.update(snap.get("fill_cursor", {}))
        close_episodes.update(snap.get("close_episodes", {}))
        with fc_lock:
            for a, c, p in snap.get("fc_positions", []):
                fc_positions[(a, c)] = p
        with strat2_lock:
            rev_open.update(snap.get("rev_open", {}))
            follow_open.update(snap.get("follow_open", {}))
            follow_last_close.update(snap.get("follow_last_close", {}))
            vault_cache.update(snap.get("vault_cache", {}))
        print(f"  [STATE] відновлено (вік {age:.0f}с): watchlist {len(watchlist)} гаманців, "
              f"sim позицій {len(sim_positions)}, трекерів {len(sim_trackers)}")
    except Exception as e:
        print(f"  [STATE] restore err: {e}")

def _prune_leaks():
    """Словники-кеші без TTL ростуть вічно; раз на хвилину чистимо старе."""
    now_ = time.time()
    for k in [k for k, ts in list(fast_last.items()) if ts < now_ - 600]:
        fast_last.pop(k, None)
    for k in [k for k, ts in list(delta_seen.items()) if ts < now_ - 3600]:
        delta_seen.pop(k, None)
    # кулдаун-кеш пріоритетного фетчу: запис старший за кулдаун — зайвий
    # (рев'ю v2.5 п.7: ~14k адрес/добу текли б вічно)
    with _prio_lock:
        for k in [k for k, ts in list(_prio_seen.items())
                  if ts < now_ - PRIO_COOLDOWN_S]:
            _prio_seen.pop(k, None)
    # Курсори АКТИВНИХ пар не чистимо ніколи: після годинного простою
    # монітора видалення курсора відкочувало б пару на 5-хвилинне вікно
    # і губило б хвіст історії, який курсор якраз тримає
    with watchlist_lock:
        _active = {f"{a}:{c}" for a, cs in watchlist.items() for c in cs}
    for k in [k for k, ts in list(fill_cursor.items())
              if ts / 1000 < now_ - 3600 and k not in _active]:
        fill_cursor.pop(k, None)
    for k in [k for k, e in list(close_episodes.items())
              if e.get("last_ts", 0) < now_ - 2 * EPISODE_TTL_S]:
        close_episodes.pop(k, None)
    for k in [k for k, ts in list(scan_tombstones.items()) if ts < now_ - 7200]:
        scan_tombstones.pop(k, None)
    for k in [k for k, ts in list(alerted_txs.items()) if ts < now_ - 3600]:
        alerted_txs.pop(k, None)
    with strat2_lock:
        # 7 днів, не доба: мітка пари тепер і є pair_gap_s для F5 — після
        # чистки «пауза >24 год» і «ніколи не бачили» були б однаковим ""
        # (рев'ю v2.8); один float на пару — пам'ять мізерна
        for k in [k for k, ts in list(follow_last_close.items())
                  if ts < now_ - 7 * 86400]:
            follow_last_close.pop(k, None)
        for k in [k for k, ts in list(profile_retry_at.items()) if ts < now_]:
            profile_retry_at.pop(k, None)
    cut_ms = (now_ - 3600) * 1000
    with fc_lock:
        for k in list(fc_episodes):
            if k not in fc_positions and fc_episodes[k].get("last_ts", 0) < cut_ms:
                del fc_episodes[k]

def run_state_saver():
    last_hb = time.time()
    last_ws_warn = 0.0
    while True:
        time.sleep(STATE_SAVE_S)
        save_state()
        try:
            _prune_leaks()
        except Exception as e:
            print(f"  [STATE] prune err: {e}")
        ws_age = _ws_age_s()
        if ws_age > 90 and time.time() - last_ws_warn >= 600:
            # раз на 10 хв, а не щохвилини: 52k таких рядків у старому лозі
            last_ws_warn = time.time()
            print(f"  [WS] тиша {ws_age:.0f}s: жодного трейда з обох з'єднань")
        # WS-проблема 5+ хв — одне повідомлення в TG; ожив — теж одне.
        # Мертвий детектор не має права мовчати місяць, як минулого разу.
        # _ws_age_s() рахує вік і для "не підключився жодного разу";
        # _ws_dead_labels() ловить смерть ОДНОГО з двох з'єднань, яку
        # глобальний вік не бачить, поки друге живе.
        dead_lbs = _ws_dead_labels()
        if (ws_age > 300 or dead_lbs) and not stats.get("ws_dead_notified"):
            stats["ws_dead_notified"] = True
            if ws_age > 300:
                tg_send("⚠️ <b>WS без трейдів понад 5 хв</b> — детект живе лише "
                        "на резервному обході (алерти працюють, але повільніше). "
                        "Дивись /status і лог.")
            else:
                tg_send(f"⚠️ <b>WS-{'/'.join(dead_lbs)} мертве понад 5 хв</b> — "
                        f"половина монет без миттєвого детекту "
                        f"(резервний обхід прикриває). Дивись /status.")
        elif ws_age < 60 and not dead_lbs and stats.get("ws_dead_notified"):
            stats["ws_dead_notified"] = False
            tg_send("✅ <b>WS знову живий</b> — трейди йдуть по обох з'єднаннях.")
        if time.time() - last_hb >= 3600:
            last_hb = time.time()
            ws_age = _ws_age_s()
            with watchlist_lock:
                wl_n = len(watchlist)
            print(f"  [HEARTBEAT] up {(time.time()-stats['started'])/3600:.1f}h | "
                  f"ws {'OK' if ws_age < 60 else 'МЕРТВИЙ ' + str(round(ws_age)) + 's'} | "
                  f"watchlist {wl_n} | ws_matched {stats['ws_matched']} | "
                  f"checks {stats['checks']} | deltas {stats['delta_events']} | "
                  f"confirmed {stats['fills_confirmed']} | empty {stats['fills_empty']} | "
                  f"alerts {stats['alerts_sent']} | 429 {stats['rate_limited']}")


# ═════════════════════════════════════════════════════════
#  FC: СТРАТЕГІЯ ВІДКАТУ ПІСЛЯ ПОВНОГО ПРОДАЖУ (пункт 6)
#  Кит продав усю позицію маркетом за < 5 хвилин, ціна пішла
#  за його потоком на >= 1%. Тиск скінчився: заходимо у
#  протилежний до його продажу бік (лонг після дампа лонгіста,
#  шорт після відкупу шортиста) і 60 хвилин щохвилини пишемо
#  зміну ціни, щоб знайти статистично найкращу хвилину виходу.
#  Вхід поки ВІРТУАЛЬНИЙ: для реальних ордерів на Binance
#  потрібні API-ключі, місце для них позначено нижче.
# ═════════════════════════════════════════════════════════
FC_ENABLED       = True
FC_MIN_MOVE_PCT  = 0.7      # мінімальний рух ціни за час його продажу
                            # (28.08: 1.0 -> 0.7, більше епізодів у трек;
                            # аналіз показав +30-50% сигналів у зоні 0.7-1.0)
FC_MAX_EPISODE_S = 300      # перша→остання транзакція максимум 5 хв
FC_TRACK_MIN     = 60       # хвилин трекаємо після входу (ТЗ 04.09:
                            # 30 -> 60, старий fc_trades.csv ротується
                            # у .legacy через зміну заголовків)
FC_MAX_OPEN      = 5
FC_WORKSHEET     = "fullclose60"  # v2.9: трек 60 хв — новий аркуш, бо
                                  # старий "fullclose" створений на
                                  # ширину m1..m30 і не ротується
FC_CSV           = os.path.join(DATA_DIR, "fc_trades.csv")
HLP_LIQUIDATOR   = "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14"  # backstop-vault HLP

FC_HEADERS = (["date", "coin", "our_side", "whale_addr", "sum_usd",
               "duration_s", "ratio", "ratio_per_min", "move_pct",
               "entry_px", "pnl_end_pct", "peak_pct"]
              + [f"m{i}" for i in range(1, FC_TRACK_MIN + 1)])

fc_lock      = threading.Lock()
fc_episodes  = {}   # (addr, coin) -> серія його продажів
fc_positions = {}   # (addr, coin) -> наша відкрита позиція відкату

def fc_on_txs(addr, coin, old, mfills):
    """Будує епізод продажу з підтверджених маркет-транзакцій."""
    if not FC_ENABLED or not mfills: return
    key = (addr, coin)
    with fc_lock:
        ep = fc_episodes.get(key)
        seen = ep["seen"] if ep else set()
        new = sorted([t for t in mfills if t.get("hash") not in seen],
                     key=lambda t: t.get("ts", 0))
        if not new: return
        # стара серія видихлась: нова транзакція починає нову
        if ep and (new[0]["ts"] - ep["last_ts"]) / 1000 > FC_MAX_EPISODE_S \
              and key not in fc_positions:
            ep = None
        if ep is None:
            t0 = new[0]
            ep = {"first_ts": t0["ts"], "first_px": t0["px"],
                  "last_ts": t0["ts"], "sum_usd": 0.0, "seen": set(),
                  "side": old.get("side", "?"),
                  "ratio": old.get("ratio", 0), "val": old.get("val", 0)}
            fc_episodes[key] = ep
        for t in new:
            ep["seen"].add(t.get("hash"))
            ep["last_ts"] = max(ep["last_ts"], t.get("ts", 0))
            ep["sum_usd"] += t["px"] * t["sz"]

def fc_on_full_close(addr, coin, old):
    """Кит продав усе: перевіряємо умови і відкриваємо відкат."""
    if not FC_ENABLED: return
    key = (addr, coin)
    with fc_lock:
        ep = fc_episodes.pop(key, None)
        if ep is None or key in fc_positions: return
        if len(fc_positions) >= FC_MAX_OPEN: return
    dur = (ep["last_ts"] - ep["first_ts"]) / 1000.0
    if dur <= 0 or dur > FC_MAX_EPISODE_S:
        print(f"  [FC] {coin}: серія {dur:.0f}с поза вікном 5 хв, пропуск")
        return
    mids = sim_all_mids()
    if not mids or coin not in mids: return
    mid = float(mids[coin])
    fpx = ep["first_px"]
    if fpx <= 0: return
    move = (mid - fpx) / fpx * 100.0   # ціна ДО першого продажу проти зараз
    side = ep["side"]
    ok = (side == "LONG" and move <= -FC_MIN_MOVE_PCT) or \
         (side == "SHORT" and move >= FC_MIN_MOVE_PCT)
    if not ok:
        print(f"  [FC] {coin}: рух {move:+.2f}% менший за поріг {FC_MIN_MOVE_PCT}%, пропуск")
        return
    our_side = side   # проти його продажу: лонг після дампа, шорт після памп-відкупу
    pos = {"addr": addr, "coin": coin, "our_side": our_side,
           "open_ts": time.time(), "entry_mid": mid,
           "sum_usd": ep["sum_usd"], "duration_s": dur,
           "ratio": ep["ratio"], "move_pct": move,
           "depth": _sim_depth(coin, side), "samples": [], "peak": -999.0}
    with fc_lock:
        fc_positions[key] = pos
    # TODO: реальний ордер на Binance піде звідси, коли додамо API-ключі
    print(f"  [FC] ENTER {our_side} {coin} @ {mid:.6g} | кит продав "
          f"${ep['sum_usd']:,.0f} за {dur:.0f}с, рух {move:+.2f}% | "
          f"трекаю {FC_TRACK_MIN} хв")
    threading.Thread(target=save_state, daemon=True).start()

def _fc_finish(key):
    with fc_lock:
        pos = fc_positions.pop(key, None)
    if pos is None: return
    slip  = _sim_slip(pos["depth"]) if pos["depth"] else 0.001
    costs = (2 * SIM_COMMISSION + 2 * slip) * 100.0
    # остання РЕАЛЬНО виміряна хвилина ("" = пропуск після збою)
    last  = next((x for x in reversed(pos["samples"])
                  if x != ""), 0.0) if pos["samples"] else 0.0
    pnl30 = last - costs
    dur_min = pos["duration_s"] / 60.0
    rpm = pos["ratio"] / dur_min if dur_min > 0 else pos["ratio"]
    row = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(pos["open_ts"])),
           pos["coin"], pos["our_side"], pos["addr"],
           round(pos["sum_usd"], 0), round(pos["duration_s"], 1),
           round(pos["ratio"], 2), round(rpm, 3),
           round(pos["move_pct"], 3), round(pos["entry_mid"], 8),
           round(pnl30, 3), round(pos["peak"], 3)] + pos["samples"][:FC_TRACK_MIN]
    # _strat_csv_append: старий файл із заголовком m1..m30 (до ТЗ 04.09
    # «трек 60 хв») ротується у .legacy, а не отримує рядки чужої ширини
    if not _strat_csv_append(FC_CSV, FC_HEADERS, row):
        print(f"  [FC] csv err: рядок {pos['coin']} не записано")
    _sheets_append(FC_WORKSHEET, FC_HEADERS, row)
    print(f"  [FC] DONE {pos['coin']} за {FC_TRACK_MIN} хв | "
          f"pnl_end {pnl30:+.2f}% | peak {pos['peak']:+.2f}%")
    threading.Thread(target=save_state, daemon=True).start()

def run_fc_loop():
    """Кожні 5с: семпли по хвилинах, закриття після FC_TRACK_MIN-ї."""
    while True:
        time.sleep(5)
        with fc_lock:
            keys = list(fc_positions.keys())
        if not keys: continue
        mids = sim_all_mids()
        if not mids: continue
        done = []
        with fc_lock:
            for key in keys:
                pos = fc_positions.get(key)
                if pos is None: continue
                mid = float(mids.get(pos["coin"], 0) or 0)
                if mid <= 0: continue
                gain = (mid - pos["entry_mid"]) / pos["entry_mid"] * 100.0
                if pos["our_side"] == "SHORT": gain = -gain
                if gain > pos["peak"]: pos["peak"] = gain
                # пропущені хвилини (рестарт/збій) -> "" а не пізніша ціна
                _nowt = time.time()
                minute = int((_nowt - pos["open_ts"]) // 60)
                while len(pos["samples"]) < min(minute, FC_TRACK_MIN):
                    _k = len(pos["samples"]) + 1
                    _late = _nowt - (pos["open_ts"] + _k * 60.0)
                    pos["samples"].append(round(gain, 4) if _late <= 30.0 else "")
                if len(pos["samples"]) >= FC_TRACK_MIN:
                    done.append(key)
        for key in done:
            _fc_finish(key)

# ═════════════════════════════════════════════════════════
#  ОБОЛОНКА СТРАТЕГІЙ (paper trading, гроші не чіпає).
#  Мета: на живих сигналах знайти систему, що торгує в плюс,
#  і зібрати максимум даних по кожній позиції.
#  Не заважає основній логіці: окремі потоки, ціни — ОДИН запит
#  allMids раз на 5с на всі монети разом (він же дає BTC-фільтр).
#
#  РЕВЕРС (вхід ПРОТИ продажу кита, коли той закінчив):
#   сигнал: гаманець ПОВНІСТЮ закрив позицію (волт — транзакція
#   >=5% або повністю; позначається) І ціна за останні 3 хв пройшла
#   >=1% у бік його закриття. BTC-фільтр дзеркальний: для дампів
#   лонгістів BTC не впав >0.15%/3хв, для пампів шортокрилів — не
#   виріс. Сигнал пишеться завжди (btc_ok прапорцем), позиції
#   відкриваються лише при btc_ok.
#   R1_загальний — всі монети, рух >=1%, вхід одразу
#   R2_breakout  — рух >=1%, вхід лише після відкату 0.3% (10 хв)
#   R3_великі    — лише ZEC/HYPE, рух >=1%
#   R4_великий   — рух >=2%    R5_дуже — рух >=3%
#   R6_волт      — сигнали від волтів (окремо, як просив користувач)
#   R7_одним     — повне закриття ОДНІЄЮ транзакцією >=$100k (ТЗ 04.09)
#   Кожна позиція трекає ціну ЩОХВИЛИНИ 60 хв (m1..m60) — з кривої
#   видно, на якій хвилині виходити найкраще.
#
#  ВХІД У БІК ТИСКУ (за китом, поки він продає):
#   сигнал: одна транзакція закрила >=5% позиції за раз
#   (одночасні — один вхід на монету на стратегію).
#   F1/F2/F3 — вихід: повне закриття АБО 1/2/3 хв без нових закриттів
#   F6_1хв_перший — як F1, але вхід лише на першому пострілі пари
#   F4_розумний — лише гаманці, що РАНІШЕ зливали позиції >$100k
#   шматками >=5% і до 5 хв (профіль з історії філів, кешується);
#   вихід через 2 середні паузи гаманця (кламп 1..5 хв) або повне
#   закриття.
#   F5_перший — F4 + лише перший постріл пари (пауза >=1 год)
#   F7_без_ратіо — F5, але кваліфікація профілю без ratio (лише $100k)
# ═════════════════════════════════════════════════════════
STRAT2_ENABLED   = True
REV_CSV          = os.path.join(DATA_DIR, "rev_trades.csv")
REV_SIG_CSV      = os.path.join(DATA_DIR, "rev_signals.csv")
FOLLOW_CSV       = os.path.join(DATA_DIR, "follow_trades.csv")
PROFILES_FILE    = os.path.join(DATA_DIR, "wallet_profiles.json")
REV_WINDOW_S     = 180
REV_TRACK_MIN    = 60
REV_BRK_PCT      = 0.3
REV_BRK_WINDOW_S = 600
BTC_VETO_PCT     = 0.15
BIG_COINS        = ("ZEC", "HYPE")
VAULT_PART_PCT   = 0.05
FOLLOW_TX_PCT    = 0.05
FOLLOW_TIMERS    = {"F1_1хв": 60, "F2_2хв": 120, "F3_3хв": 180}
F4_NAME          = "F4_розумний"
F5_NAME          = "F5_перший"   # ті самі швидкі гаманці, але вхід ЛИШЕ
                                 # на першій транзакції пари гаманець:
                                 # монета або після паузи ≥1 год (ТЗ
                                 # 01.09 п.3; дослідження: повтори пари в
                                 # межах години — шум)
# ── ТЗ 04.09 ──
F6_NAME          = "F6_1хв_перший"  # «тиша 1 хв», але вхід лише на
                                    # першому пострілі пари (як F5),
                                    # БЕЗ вимоги профілю гаманця
F6_TIMER_S       = 60.0
F7_NAME          = "F7_без_ратіо"   # швидкі гаманці · перший постріл,
                                    # але кваліфікація профілю БЕЗ
                                    # ratio-гейта: великий епізод =
                                    # лише ≥$100k (nr-гілка профілю)
R7_NAME          = "R7_одним"       # реверс «одним пострілом»: ПОВНЕ
                                    # закриття однією транзакцією
                                    # ≥$100k, рух ≥1% — вхід одразу
R7_MIN_TX_USD    = 100_000.0
# ── ПРОФІЛЬ ШВИДКИХ ГАМАНЦІВ (ТЗ 01.09 п.2) ──
# Вікно — 3 місяці історії філів (кап API: 10k останніх). Великий епізод
# = розвантаження позиції, що на момент ПЕРШОЇ агресивної транзакції
# ≥5% коштувала ≥$100k і мала ratio ≥2 до ПОТОЧНОЇ глибини 1% Binance
# (історичної глибини немає — свідома неточність, узгоджена з
# користувачем). Швидкий = ≥95% позиції закрито за ≤5 хв від тієї
# першої транзакції; інакше повільний. Кваліфікація: ≥5 швидких за
# вікно І ≥70% швидких серед усіх великих.
F4_MIN_EPISODES  = 5        # мінімум ШВИДКИХ великих епізодів
F4_MIN_FAST_PCT  = 70.0     # частка швидких серед великих, %
F4_MIN_NOTIONAL  = 100_000.0
F4_MIN_RATIO     = 2.0      # позиція / глибина 1% сторони (поточна)
F4_MAX_UNLOAD_S  = 300.0    # швидкий: 1-ша tx ≥5% → ≥95% закрито
F4_CHUNK_PCT     = 0.05     # «агресивна транзакція» = одним ордером ≥5%
F4_FULL_PCT      = 0.95     # «закрив повністю» = залишок ≤5% від старту
F4_CLAMP         = (60.0, 300.0)
F5_FIRST_SHOT_S  = 3600.0   # пауза пари, після якої tx знову «перша»
PROFILE_WINDOW_D = 90       # глибина історії, днів
PROFILE_PAGES    = 6        # 5 × 2000 = кап API 10k; 6-та — детект «є ще»
PROFILE_ALGO_V   = 8      # версія алгоритму профілів: старі записи без
                          # цієї позначки перераховуються (аудит v2.1 п.3;
                          # v3: boundary-safe пагінація; v4: епізод =
                          # позиція, malformed fail-closed, композитний
                          # tid-ключ; v5: flat-спліт — нова позиція
                          # будь-якого розміру після зливу в нуль,
                          # структурні поля обов'язкові; v6: hash
                          # обов'язковий непорожній, crossed — строгий
                          # bool, NaN/coin fail-closed; v7: профіль
                          # ШВИДКОСТІ — 90 днів, епізод від 1-ї tx ≥5%,
                          # ratio-гейт, швидкі/повільні, статистика
                          # зливу; 10k-кап API більше не «truncated»;
                          # v8: паралельна nr-гілка БЕЗ ratio-гейта
                          # (лише ≥$100k) — кваліфікація F7, ТЗ 04.09)
DATA_ALGO_V      = "2.9"  # версія логіки збору: трекер отримує її при
                          # СТВОРЕННІ і несе у рядок; API рахує лише
                          # поточну версію (аудит v2.2: рестарт підписував
                          # старі трекери новою версією). При зміні
                          # заголовків старий файл ротується в .legacy
WFAIL_CAP        = 200    # ретраї запису рядка (3с/тик = ~10 хв диска);
                          # рядок заморожений, тому ретраї безкоштовні
PX_AGO_TOL_S     = 90.0   # історична ціна не далі 90с від цілі: інакше
                          # "рух за 3 хв" міг бути рухом за 15 хв (п.5)
REV_OUT_MIN_MAG  = 0.5    # outcome-стрічка пише події вже від 0.5%, щоб
                          # згодом можна було чесно перевірити нижчі пороги
                          # (стратегії відкриваються, як і раніше, від 1%)
PART_OUT_PCT     = 0.30   # часткове закриття >=30% позиції -> тіньова
                          # outcome-стрічка (src=partial, БЕЗ стратегій):
                          # збираємо дані для майбутнього "R7?" замість
                          # гадання (обговорення 30.08)

# ЛЮДСЬКІ назви для UI. Внутрішні ID (R1_… у CSV/стані) СТАБІЛЬНІ —
# перейменування ID зламало б порівнянність зібраних даних
STRAT2_TITLES = {
    "R1_загальний": "Реверс · будь-який дамп ≥1%",
    "R2_breakout":  "Реверс · вхід після відкату",
    "R3_великі":    "Реверс · тільки ZEC і HYPE",
    "R4_великий":   "Реверс · сильний рух ≥2%",
    "R5_дуже":      "Реверс · екстрим ≥3%",
    "R6_волт":      "Реверс · волти",
    "R7_одним":     "Реверс · одним пострілом",
    "F1_1хв":       "За китом · тиша 1 хв",
    "F2_2хв":       "За китом · тиша 2 хв",
    "F3_3хв":       "За китом · тиша 3 хв",
    "F6_1хв_перший": "За китом · тиша 1 хв · перший постріл",
    "F4_розумний":  "За китом · швидкі гаманці",
    "F5_перший":    "За китом · швидкі гаманці · перший постріл",
    "F7_без_ратіо": "За китом · перший постріл · без ratio",
}
STRAT2_DESC = {
    "R1_загальний": "Проти кита: він повністю злив позицію, монета впала "
                    "≥1% за 3 хв — ставимо на відскок, тримаємо 30 хв",
    "R2_breakout":  "Те саме, але чекаємо підтвердження: вхід лише коли "
                    "ціна відбилась на +0.3% від дна (вікно 10 хв)",
    "R3_великі":    "Відскок тільки на ZEC і HYPE (вибір користувача)",
    "R4_великий":   "Відскок тільки після сильного руху ≥2% за 3 хв",
    "R5_дуже":      "Відскок тільки після екстремального руху ≥3% за 3 хв",
    "R6_волт":      "Відскок після зливу ВОЛТОМ (механічний вивід коштів "
                    "вкладників, не рішення трейдера)",
    "R7_одним":     "Проти кита: ВСЯ позиція закрита однією транзакцією "
                    "≥$100k, рух ≥1% за 3 хв — вхід одразу",
    "F1_1хв":       "Разом з китом: він скинув ≥5% позиції — входимо в "
                    "його бік, виходимо після 1 хв без нових продажів",
    "F2_2хв":       "Разом з китом, вихід після 2 хв тиші",
    "F3_3хв":       "Разом з китом, вихід після 3 хв тиші",
    "F4_розумний":  "Разом з китом, але лише за гаманцями, які за 3 місяці "
                    "мали ≥5 великих розвантажень (≥$100k, ratio ≥2) і "
                    "≥70% з них злили повністю за ≤5 хв від першої "
                    "транзакції ≥5%; вихід 2×їхня пауза між шматками",
    "F5_перший":    "Те саме, що швидкі гаманці, але вхід лише на ПЕРШІЙ "
                    "транзакції пари гаманець:монета (або після паузи "
                    "≥1 год) — повтори в межах години пропускаються",
    "F6_1хв_перший": "Як «тиша 1 хв», але вхід лише на ПЕРШІЙ транзакції "
                    "пари гаманець:монета (або після паузи ≥1 год); "
                    "профіль гаманця не вимагається",
    "F7_без_ратіо": "Як «перший постріл», але кваліфікація гаманця БЕЗ "
                    "фільтра ratio: великий епізод = лише ≥$100k "
                    "(≥5 швидких за 3 міс і ≥70% швидких)",
}

REV_SIG_HEADERS = ["sig_id", "date", "coin", "fade_side", "whale_addr", "src",
                   "px", "move_3m_pct", "dur_s", "sum_usd", "usd_s", "ratio",
                   "shtanga", "vault", "hour", "btc_move_pct", "btc_ok",
                   "depth_usd", "opened", "algo_v", "eol"]
REV_HEADERS = (["sig_id", "strategy", "date", "coin", "our_side", "whale_addr",
                "src", "detect_px", "entry_px", "entered", "move_3m_pct",
                "dur_s", "sum_usd", "usd_s", "ratio", "shtanga", "vault",
                "hour", "btc_move_pct", "depth_usd", "costs_pct", "peak_pct",
                "trough_pct", "algo_v"]
               + [f"m{i}" for i in range(1, REV_TRACK_MIN + 1)] + ["eol"])
FOLLOW_HEADERS = ["date_open", "date_close", "strategy", "coin", "our_side",
                  "whale_addr", "entry_px", "exit_px", "exit_reason", "hold_s",
                  "gross_pct", "costs_pct", "net_pct", "peak_pct", "trough_pct",
                  "tx_pct_of_pos", "tx_usd", "ratio", "pos_usd", "vault",
                  "hour", "btc_move_pct", "profile_gap_s", "algo_v",
                  "trade_id",
                  # v2.8: перший постріл пари (0/1) і пауза від попереднього
                  # закриття пари, с ("" = раніше не бачили) — у КОЖНОМУ
                  # follow-рядку, щоб порівнювати F1–F4 і F5 на одних
                  # подіях; далі профіль швидкості гаманця на момент входу
                  # (лише F4/F5, у решти порожньо)
                  "first_shot", "pair_gap_s", "prof_n_fast", "prof_n_slow",
                  "prof_fast_pct", "prof_unload_med_s", "prof_unload_mean_s",
                  "prof_window_d", "eol"]
# Тіньова хвилинна стрічка FOLLOW-входів: m1..m60 у НАШОМУ напрямку від
# ціни входу, незалежно від правил виходу F1-F4 — щоб крива "яка хвилина
# виходу найкраща" існувала й для follow (запит користувача 30.08)
FOLLOW_OUT_CSV = os.path.join(DATA_DIR, "follow_outcomes.csv")
FOLLOW_OUT_HEADERS = (["fo_id", "date", "coin", "our_side", "whale_addr",
                       "entry_px", "tx_pct_of_pos", "tx_usd", "ratio",
                       "pos_usd", "vault", "hour", "btc_move_pct",
                       "depth_usd", "costs_pct", "peak_pct", "trough_pct",
                       "algo_v",
                       # v2.8: щоб крива «хвилина виходу» існувала і для
                       # когорти першого пострілу (F5) окремо
                       "first_shot", "pair_gap_s"]
                      + [f"m{i}" for i in range(1, REV_TRACK_MIN + 1)] + ["eol"])

# OUTCOME-стрічка: шлях ціни КОЖНОГО сигналу від детекту, незалежно від
# BTC-вето/зайнятості/breakout — спільна база для чесного порівняння
# фільтрів на одних і тих самих подіях (аудит 29.08, п.7)
REV_OUT_CSV = os.path.join(DATA_DIR, "rev_outcomes.csv")
REV_OUT_HEADERS = (["sig_id", "date", "coin", "fade_side", "whale_addr",
                    "src", "detect_px", "move_3m_pct", "dur_s", "sum_usd",
                    "usd_s", "ratio", "shtanga", "vault", "hour",
                    "btc_move_pct", "btc_ok", "would_open", "depth_usd",
                    "costs_pct", "peak_pct", "trough_pct", "algo_v"]
                   + [f"m{i}" for i in range(1, REV_TRACK_MIN + 1)] + ["eol"])

strat2_lock       = threading.RLock()   # RLock: захист від self-deadlock
                                        # (аудит 29.08: Lock завис у F4)
rev_open          = {}   # sig_id|strategy -> позиція реверсу (armed/open)
follow_open       = {}   # id -> позиція "у бік"
follow_last_close = {}   # addr:coin -> ts(сек) останнього закриття (тиша)
wallet_profiles   = {}   # addr(lower) -> профіль розвантажень (F4)
profiles_fetching = set()
vault_cache       = {}   # addr -> bool
# Серіалізація персисту: унікальні tmp прибрали колізію файлів, але два
# конкурентні знімки могли завершити os.replace у зворотному порядку і
# СТАРІШИЙ перетирав новіший (аудит v2.2 п.3). Знімок і replace тепер
# атомарні під одним локом на файл — переможець завжди найновіший.
_state_save_lock  = threading.Lock()
_prof_save_lock   = threading.Lock()
# Outbox сигнальних рядків: запис REV_SIG_CSV, що впав, не губиться, а
# чекає ретраю в run_strat2_loop (аудит v2.2: трекери створені, а
# signal-рядок з фільтрами й контекстом зник — paired-аналіз ламався).
# Межа чесності: черга в пам'яті, смерть процесу її втрачає.
_sig_retry_lock   = threading.Lock()
_sig_retry_q      = []   # [path, headers, row, fails]
_sig_seq          = [0]  # лічильник унікальності sig_id: дві події в
                         # одну мс з однаковим hash більше не колізують
                         # (аудит v2.6 №9)

try:
    with open(PROFILES_FILE) as _pf:
        wallet_profiles.update(json.load(_pf))
except FileNotFoundError:
    pass   # перший запуск — файла ще нема, це норма
except Exception as _pe:
    print(f"  [F4] кеш профілів НЕ прочитано ({_pe}) — почнемо з нуля")

def is_vault(addr):
    """Чи адреса є волтом Hyperliquid. Кеш назавжди; збій не кешується."""
    v = vault_cache.get(addr)
    if v is not None: return v
    try:
        r = hl_post({"type": "vaultDetails", "vaultAddress": addr}, retries=1)
        res = isinstance(r, dict) and bool(r.get("name") or r.get("vaultAddress"))
    except Exception:
        return False
    vault_cache[addr] = res
    return res

_csv_lock = threading.Lock()   # ОДИН запис за раз: ротація і append
                               # атомарні між усіма потоками (рев'ю v2.2)

def _strat_csv_append(path, headers, row):
    """Append із звіркою формату. Повертає True лише після успішного
    запису — викликач НЕ видаляє трекер, поки рядок не збережено.
    Під локом і з перечитуванням заголовка щоразу (записи рідкі):
    - файл з ІНШИМ заголовком ротується у .legacy; збій ротації =
      відмова від запису, а не сліпий append під чужий формат;
    - порожній (0-байт) файл після минулого збою отримує заголовок;
    - заголовок+рядок пишуться одним f.write, щоб мінімізувати
      вікно "обірваного рядка" при ENOSPC."""
    try:
        import csv as _csv, io
        with _csv_lock:
            need_header = True
            if os.path.exists(path):
                with open(path, newline="", encoding="utf-8") as f:
                    first = f.readline().strip("\r\n")
                if first == ",".join(headers):
                    need_header = False
                elif first:
                    legacy = f"{path}.legacy-{int(time.time())}.csv"
                    os.replace(path, legacy)
                    print(f"  [STRAT] {os.path.basename(path)}: старий формат "
                          f"-> {os.path.basename(legacy)}")
                # first == "": порожній файл — допишемо заголовок
            # EOL-вартовий: обрив запису ВСЕРЕДИНІ останнього поля
            # лишає рядок з правильною кількістю колонок, але битим хвостом
            # ("trade-123" замість "trade-123456") — підрахунок колонок
            # такого не ловить (аудит v2.5). Константа "^" в останній
            # колонці: обірваний рядок її втрачає, читання карантинить.
            if headers and headers[-1] == "eol" \
               and len(row) == len(headers) - 1:
                row = list(row) + ["^"]
            buf = io.StringIO()
            w = _csv.writer(buf)
            if need_header: w.writerow(headers)
            w.writerow(row)
            with open(path, "a+b") as fb:
                # обірваний минулим збоєм (ENOSPC/short write) хвіст без
                # \n закривається, інакше ретрай приклеївся б до
                # недописаного рядка і зіпсував ОБИДВА (аудит v2.3 п.7);
                # битий короткий рядок потім відкине читання, а дедуп
                # по sig_id залишить повний повторний запис
                fb.seek(0, 2)
                if fb.tell() > 0:
                    fb.seek(-1, 2)
                    if fb.read(1) != b"\n":
                        fb.write(b"\r\n")
                fb.write(buf.getvalue().encode("utf-8"))
        return True
    except Exception as e:
        print(f"  [STRAT] csv err ({os.path.basename(path)}): {e}")
        return False

def _dt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else ""

# ── Ціни: один потік, один запит на 5с, історія ~4 хв на монету ──
px_lock = threading.Lock()
px_hist = {}   # coin -> [(ts, px), ...]

def run_px_poller():
    while True:
        t0 = time.time()
        try:
            mids = sim_all_mids()
        except Exception:
            mids = None
        if mids:
            now = time.time()
            with px_lock:
                for c, v in mids.items():
                    try:
                        px = float(v)
                    except (TypeError, ValueError):
                        continue
                    # NaN проходив крізь "px <= 0" (порівняння з nan =
                    # False) і давав сигнал "+nan%" та отруєні медіани
                    # аж до битого JSON у /strat2 (аудит v2.6 №7)
                    if not math.isfinite(px) or px <= 0: continue
                    h = px_hist.setdefault(c, [])
                    h.append((now, px))
                    if len(h) > 50: del h[:len(h) - 50]
        time.sleep(max(0.5, 5.0 - (time.time() - t0)))

def _px_now(coin, max_age=20.0):
    with px_lock:
        h = px_hist.get(coin)
        if not h: return None
        ts, px = h[-1]
    return px if time.time() - ts <= max_age else None

def _px_ago(coin, sec):
    """Ціна ~sec секунд тому. None, якщо історії немає АБО найближчий
    семпл далі за PX_AGO_TOL_S від цілі: після збою поллера 15-хвилинна
    ціна інакше видавалась за "3 хвилини тому" (аудит v2.1 п.5)."""
    target = time.time() - sec
    with px_lock:
        h = list(px_hist.get(coin) or ())
    best = None
    for ts, px in h:
        if ts <= target: best = (ts, px)
        else: break
    if best is None: return None
    return best[1] if target - best[0] <= PX_AGO_TOL_S else None

# ── РЕВЕРС ──────────────────────────────────────────────
def rev_on_close(addr, coin, old, mfills, full_close):
    """Викликається на кожен підтверджений батч закриттів, ДО того як
    fc_on_full_close зніме епізод (фічі dur/sum ще доступні)."""
    if not STRAT2_ENABLED or not mfills: return
    # хард-фільтр: позиція < $50k — не сигнал і не статистика (кейс MET)
    if (old.get("val") or 0) < MIN_POS_USD: return
    base_sz = old.get("size") or 1e-12
    # шматок волта: >=5% позиції І >= $5k (той самий доларовий поріг,
    # що в алертах і follow — рев'ю v2.5 п.6: 5% від $60k = $3k пил)
    big_any = any(f["sz"] >= base_sz * VAULT_PART_PCT
                  and f["px"] * f["sz"] >= MIN_TX_USD for f in mfills)
    # ЧАСТКОВЕ закриття >=30% позиції: лише ТІНЬОВА outcome-стрічка
    # (src=partial, стратегії не відкриваються) — інакше ми ніколи не
    # дізнаємось, чи працює реверс після великого часткового зливу
    # (обговорення 30.08; досі такі події не збирались взагалі)
    part_shadow = (not full_close
                   and sum(f["sz"] for f in mfills) >= base_sz * PART_OUT_PCT)
    # СВІДОМИЙ виняток (аудит v2.5 №4): ПОВНЕ закриття — сигнал
    # незалежно від розміру окремих транзакцій ($100k, злиті 20x$2.5k,
    # це той самий "кит пішов", що і одним шотом; перевірені 40
    # історичних епізодів рахувались саме так). MIN_TX_USD ріже дрібні
    # ТРИГЕРИ (часткові шматки), а не спосіб нарізки повного зливу;
    # сам розмір події гейтить MIN_POS_USD вище.
    if not (full_close or big_any or part_shadow):
        return   # і не повне, і без шматка >=5% — сигналу точно немає
    vault = is_vault(addr)   # мережа лише тут (кешується назавжди)
    # ВОЛТ = реальна стратегія ЛИШЕ за старим гейтом (повне АБО шматок
    # >=5%); партіал-30% без великого шматка — тінь src=partial і для
    # волта (рев'ю v2.5 п.3: інакше R6 тихо отримував новий клас подій)
    if vault and (full_close or big_any):
        src = "vault"
    elif not vault and full_close:
        src = "wallet"
    elif part_shadow:
        src = "partial"
    else:
        return
    if src == "partial":
        # серійний дедуп (рев'ю v2.5 п.9): поки по парі кит:монета вже
        # трекається активний partial-_OUTCOME, нові батчі того самого
        # розвантаження не породжують нові корельовані рядки
        with strat2_lock:
            if any(p.get("src") == "partial" and p.get("coin") == coin
                   and p.get("addr") == addr and not p.get("done")
                   for p in rev_open.values()):
                return
    side = old.get("side")
    if side not in ("LONG", "SHORT"): return
    px_now = _px_now(coin)
    ago = _px_ago(coin, REV_WINDOW_S)
    if not px_now or not ago:
        return   # історія цін ще не набралась (перші 3 хв після старту)
    move = (px_now / ago - 1.0) * 100.0
    mag = -move if side == "LONG" else move   # рух У БІК закриття кита
    # outcome-стрічка пише від 0.5% (перевірка нижчих порогів у
    # майбутньому); стратегії відкриваються від 1%, як у специфікації
    if mag < REV_OUT_MIN_MAG: return
    below_threshold = mag < 1.0
    b_now = _px_now("BTC"); b_ago = _px_ago("BTC", REV_WINDOW_S)
    if b_now and b_ago:
        btc_move = (b_now / b_ago - 1.0) * 100.0
        btc_ok = (btc_move > -BTC_VETO_PCT) if side == "LONG" \
                 else (btc_move < BTC_VETO_PCT)
    else:
        # немає даних BTC != "BTC не рухався": фільтр не перевірити,
        # позиції не відкриваємо, у CSV btc_move порожній (аудит п.6)
        btc_move = None
        btc_ok = False
    with fc_lock:
        ep = fc_episodes.get((addr, coin))
        dur = (ep["last_ts"] - ep["first_ts"]) / 1000.0 if ep else 0.0
        sum_usd = ep["sum_usd"] if ep else sum(f["px"] * f["sz"] for f in mfills)
    usd_s = sum_usd / dur if dur > 0 else 0.0
    shtanga = int((0 < dur < 30) or (usd_s and usd_s < 5000) or dur == 0)
    hour = time.localtime().tm_hour
    depth = _sim_depth(coin, side)
    ratio = old.get("ratio", 0) or 0
    now = time.time()
    # унікальність: мс + монета + гаманець + hash транзакції-тригера
    _txh = str(mfills[-1].get("hash", ""))[2:10]
    _sig_seq[0] += 1
    sig_id = f"{int(now * 1000)}-{coin}-{addr[2:8]}-{_txh}-{_sig_seq[0]}"
    # «одним пострілом» (ТЗ 04.09 п.2): повне закриття, у якому ОДНА
    # маркет-транзакція (hash-група; батч може нести ще пил) закрила
    # ≥95% позиції і коштувала ≥$100k. mfills уже згруповані по hash у
    # get_recent_market_fills, фліп-розмір клампнутий викликачем.
    # Рев'ю v2.9 S1: «позиція» — це НЕ залишок перед поточним батчем
    # (при живому WS кожна tx серії підтверджується окремим батчем, і
    # остання завжди закривала б «100% залишку»), а позиція на СТАРТІ
    # епізоду продажу: звіряємо нотіонал tx з ep["val"] (вартість на
    # момент першої tx серії; fc_on_txs уже поглинув поточний батч).
    # Свідома межа: пауза >5 хв ресетить епізод — «доїв рештки одним
    # маркетом після довгої паузи» рахується новою позицією.
    # Рев'ю v2.9 S2: ліквідація — не рішення кита, R7 не відкриває
    # (як у профілі швидкості; R1-R6 ліквідації включають, як раніше).
    one_shot = 0
    if full_close and mfills:
        _big_tx = max(mfills, key=lambda f: f["sz"])
        _big_usd = _big_tx["px"] * _big_tx["sz"]
        _ep_val = (ep["val"] if ep and ep.get("val") else 0) \
                  or old.get("val") or 0
        if _big_tx["sz"] >= base_sz * F4_FULL_PCT \
           and _big_usd >= R7_MIN_TX_USD \
           and _big_usd >= F4_FULL_PCT * _ep_val \
           and not _big_tx.get("liq"):
            one_shot = 1
    if below_threshold or src == "partial":
        strats = []   # 0.5-1% або часткове закриття: лише outcome-стрічка
    elif src == "vault":
        strats = ["R6_волт"]
    else:
        strats = ["R1_загальний", "R2_breakout"]
        if coin in BIG_COINS: strats.append("R3_великі")
        if mag >= 2.0: strats.append("R4_великий")
        if mag >= 3.0: strats.append("R5_дуже")
        if one_shot: strats.append(R7_NAME)   # вхід одразу, як R1
    base_pos = {"sig_id": sig_id, "coin": coin,
                "side": side, "addr": addr, "src": src,
                "detect_ts": now, "detect_px": px_now,
                "move": mag, "dur": dur, "sum_usd": sum_usd,
                "usd_s": usd_s, "ratio": ratio, "shtanga": shtanga,
                "vault": int(vault), "hour": hour,
                "btc_move": btc_move, "depth": depth,
                # версія — В МОМЕНТ створення: трекер, відновлений зі
                # старого state.json без версії, пишеться як legacy (""),
                # а не підписується поточною (аудит v2.2)
                "algo_v": DATA_ALGO_V,
                "samples": [], "peak": -999.0, "trough": 999.0}
    opened = []
    with strat2_lock:
        # OUTCOME-СТРІЧКА (аудит п.7): КОЖЕН сигнал — включно з BTC-вето
        # і "монета зайнята" — отримує власний трек m1..m60 від ціни
        # детекту, незалежно від того, чи дозволив хтось paper-вхід.
        # Тільки на цій спільній стрічці можна чесно порівняти
        # "з фільтром проти без фільтра" на ОДНИХ і тих самих подіях.
        pos = dict(base_pos)
        pos["strategy"] = "_OUTCOME"
        pos["state"] = "open"
        pos["entry_ts"] = now
        pos["entry_px"] = px_now
        pos["btc_ok"] = int(btc_ok)
        pos["would_open"] = "+".join(strats)
        rev_open[f"{sig_id}|_OUTCOME"] = pos
        if btc_ok:
            busy = {(p["strategy"], p["coin"]) for p in rev_open.values()
                    if not p["strategy"].startswith("_")
                    and not p.get("done")}
            for st in strats:
                if (st, coin) in busy: continue
                pos = dict(base_pos); pos["strategy"] = st
                pos["samples"] = []
                if st == "R2_breakout":
                    pos["state"] = "armed"
                    pos["trigger_px"] = (px_now * (1 + REV_BRK_PCT / 100.0)
                                         if side == "LONG"
                                         else px_now * (1 - REV_BRK_PCT / 100.0))
                    pos["deadline"] = now + REV_BRK_WINDOW_S
                else:
                    pos["state"] = "open"
                    pos["entry_ts"] = now
                    pos["entry_px"] = px_now
                rev_open[f"{sig_id}|{st}"] = pos
                opened.append(st)
    _sig_row = [sig_id, _dt(now), coin, side, addr, src, round(px_now, 8),
                round(mag, 3), round(dur, 1), round(sum_usd, 0),
                round(usd_s, 0), round(ratio, 2), shtanga, int(vault), hour,
                (round(btc_move, 3) if btc_move is not None else ""),
                int(btc_ok), round(depth or 0, 0), "+".join(opened),
                DATA_ALGO_V]
    if not _strat_csv_append(REV_SIG_CSV, REV_SIG_HEADERS, _sig_row):
        # сигнал — вісь paired-порівнянь: без нього outcome/угоди висять
        # у повітрі. Невдалий запис іде в outbox (ретрай у циклі)
        with _sig_retry_lock:
            _sig_retry_q.append([REV_SIG_CSV, REV_SIG_HEADERS, _sig_row, 0])
    _btc_txt = f"{btc_move:+.2f}%" if btc_move is not None else "н/д"
    print(f"  [REV] сигнал {coin} fade={side} {mag:+.2f}%/3хв src={src} "
          f"btc={_btc_txt}{'' if btc_ok else ' VETO'} -> "
          f"{'+'.join(opened) or '—'}")

def _rev_samples(p, entered):
    """Хвилинні семпли: float або "" (чесний пропуск після збою котирувань)."""
    if not entered:
        return [""] * REV_TRACK_MIN
    out = [("" if x == "" else round(x, 4))
           for x in p["samples"][:REV_TRACK_MIN]]
    return out + [""] * (REV_TRACK_MIN - len(out))

def _rev_row(p, entered):
    slip = _sim_slip(p.get("depth") or 0)
    costs = (2 * SIM_COMMISSION + 2 * slip) * 100.0
    bm = p.get("btc_move")
    return ([p["sig_id"], p["strategy"], _dt(p["detect_ts"]), p["coin"],
             p["side"], p["addr"], p["src"], round(p["detect_px"], 8),
             round(p.get("entry_px") or 0, 8), entered,
             round(p["move"], 3), round(p["dur"], 1),
             round(p["sum_usd"], 0), round(p["usd_s"], 0),
             round(p["ratio"], 2), p["shtanga"], p["vault"], p["hour"],
             (round(bm, 3) if bm is not None else ""),
             round(p.get("depth") or 0, 0),
             round(costs, 4),
             (round(p["peak"], 4) if entered and p["peak"] > -999 else ""),
             (round(p["trough"], 4) if entered and p["trough"] < 999 else ""),
             p.get("algo_v", "")]
            + _rev_samples(p, entered))

def _fol_out_row(p):
    """Рядок тіньової хвилинної стрічки follow-входу (m1..60 у нашому
    напрямку від ціни входу)."""
    slip = _sim_slip(p.get("depth") or 0)
    costs = (2 * SIM_COMMISSION + 2 * slip) * 100.0
    bm = p.get("btc_move")
    return ([p["sig_id"], _dt(p["detect_ts"]), p["coin"], p["side"],
             p["addr"], round(p["entry_px"], 8),
             round(p.get("tx_pct", 0), 3), round(p.get("tx_usd", 0), 0),
             round(p.get("ratio", 0), 2), round(p.get("pos_usd", 0), 0),
             p.get("vault", 0), p.get("hour", ""),
             (round(bm, 3) if bm is not None else ""),
             round(p.get("depth") or 0, 0), round(costs, 4),
             (round(p["peak"], 4) if p["peak"] > -999 else ""),
             (round(p["trough"], 4) if p["trough"] < 999 else ""),
             p.get("algo_v", ""),
             p.get("first_shot", ""), p.get("pair_gap", "")]
            + _rev_samples(p, 1))

def _rev_out_row(p):
    slip = _sim_slip(p.get("depth") or 0)
    costs = (2 * SIM_COMMISSION + 2 * slip) * 100.0
    bm = p.get("btc_move")
    return ([p["sig_id"], _dt(p["detect_ts"]), p["coin"], p["side"],
             p["addr"], p["src"], round(p["detect_px"], 8),
             round(p["move"], 3), round(p["dur"], 1),
             round(p["sum_usd"], 0), round(p["usd_s"], 0),
             round(p["ratio"], 2), p["shtanga"], p["vault"], p["hour"],
             (round(bm, 3) if bm is not None else ""),
             p.get("btc_ok", ""), p.get("would_open", ""),
             round(p.get("depth") or 0, 0), round(costs, 4),
             (round(p["peak"], 4) if p["peak"] > -999 else ""),
             (round(p["trough"], 4) if p["trough"] < 999 else ""),
             p.get("algo_v", "")]
            + _rev_samples(p, 1))

# ── ВХІД У БІК ТИСКУ ────────────────────────────────────
def follow_on_txs(addr, coin, old, mfills, full_close):
    if not STRAT2_ENABLED or not mfills: return
    key = f"{addr}:{coin}"
    now = time.time()
    with strat2_lock:
        # пауза пари ДО оновлення мітки: «перший постріл» (F5) = раніше
        # закриттів цієї пари не бачили АБО минуло ≥1 год (ТЗ 01.09 п.3)
        prev_close = follow_last_close.get(key)
        follow_last_close[key] = now
        if full_close:
            for p in follow_open.values():
                if p["key"] == key:
                    p["force_exit"] = "full_close"
    if full_close: return   # кит уже все закрив — заходити пізно
    pair_gap = (now - prev_close) if prev_close else None
    first_shot = int(pair_gap is None or pair_gap >= F5_FIRST_SHOT_S)
    # хард-фільтри (кейс MET: позиція $17k, шматки по $1k, ratio 0.15 —
    # 12 сміттєвих угод): дрібна позиція/транзакція — не сигнал
    if (old.get("val") or 0) < MIN_POS_USD: return
    base_sz = old.get("size") or 1e-12
    big = [f for f in mfills if f["sz"] >= base_sz * FOLLOW_TX_PCT
           and f["px"] * f["sz"] >= MIN_TX_USD]
    if not big: return
    px = _px_now(coin)
    if not px: return
    tx = max(big, key=lambda f: f["sz"])
    our = "SHORT" if old.get("side") == "LONG" else "LONG"
    vault = is_vault(addr)
    hour = time.localtime().tm_hour
    b_now = _px_now("BTC"); b_ago = _px_ago("BTC", REV_WINDOW_S)
    # немає даних BTC -> None (у CSV порожньо), а не фальшивий 0.0
    btc_move = (b_now / b_ago - 1.0) * 100.0 if b_now and b_ago else None
    depth = _sim_depth(coin, old.get("side"))
    base = {"key": key, "coin": coin, "our_side": our, "addr": addr,
            "open_ts": now, "entry_px": px, "peak": -999.0, "trough": 999.0,
            "tx_pct": tx["sz"] / base_sz * 100.0,
            "tx_usd": tx["px"] * tx["sz"],
            "ratio": old.get("ratio", 0) or 0,
            "pos_usd": old.get("val", 0) or 0,
            "vault": int(vault), "hour": hour, "btc_move": btc_move,
            "depth": depth, "force_exit": None, "profile_gap": 0.0,
            "first_shot": first_shot,
            "pair_gap": (round(pair_gap, 1) if pair_gap is not None else ""),
            # профіль швидкості гаманця НА МОМЕНТ входу (v2.8): у рядок
            # кожної F-угоди, якщо профіль уже є — порівнювати когорти
            "prof": {},
            "algo_v": DATA_ALGO_V}   # версія в момент створення
    prof = wallet_profiles.get(addr.lower())
    # профіль треба (пере)тягнути, якщо його немає, він зі старої версії
    # алгоритму (аудит v2.1 п.3: кеш v2.0 носив виправлені баги) або
    # це збійний запис (TTL вирішує _profile_request)
    prof_valid = (prof is not None
                  and prof.get("v") == PROFILE_ALGO_V
                  and not prof.get("err"))
    need_profile = not prof_valid
    # діра в історії (>2000 філів в одній мс = truncated) НЕ дає права
    # на F4: "поведінковий профіль" з огризка — не профіль (аудит v2.2
    # п.2); 10k-кап API (hist_capped) — навпаки, дає (v2.8: вікно просто
    # коротше). truncated — стан ТИМЧАСОВИЙ (рев'ю v2.3), а
    # ЗДОРОВИЙ профіль просто старіє (аудит v2.3 п.4: кеш 100-денної
    # давності — не "останні 14 днів") — обидва йдуть на рефреш;
    # шторм гасить TTL-гейт у _profile_request. Поточний сигнал при
    # цьому користується кешем (політика "свіжий-вчора краще, ніж
    # нічого"), рефреш доїде фоном до наступного сигналу
    if prof_valid and (prof.get("truncated")
                       or now - prof.get("fetched", 0) >= PROFILE_TTL_S):
        need_profile = True
    # жорстка стеля віку: 24-48г — stale-while-revalidate, старіше —
    # F4 закритий, поки фоновий рефреш не принесе свіжий профіль
    # hist_capped (10k-кап API) — НЕ перешкода: вікно просто коротше і
    # видно у window_d; truncated (>2000 філів в одній мс — діра) — як і
    # раніше, м'який стан без права на вхід
    f4_ok = (prof_valid and prof.get("ok") and not prof.get("truncated")
             and now - prof.get("fetched", 0) < PROFILE_HARD_TTL_S)
    # F7: та сама свіжість/цілісність, але кваліфікація по nr-гілці
    # профілю (епізоди без ratio-гейта, лише ≥$100k — ТЗ 04.09 п.9)
    nr_prof = (prof.get("nr") or {}) if prof_valid else {}
    f7_ok = (prof_valid and nr_prof.get("ok") and not prof.get("truncated")
             and now - prof.get("fetched", 0) < PROFILE_HARD_TTL_S)
    if prof_valid:
        base["prof"] = {"n_fast": prof.get("n_fast", 0),
                        "n_slow": prof.get("n_slow", 0),
                        "fast_pct": prof.get("fast_pct"),
                        "unload_med_s": prof.get("unload_med_s"),
                        "unload_mean_s": prof.get("unload_mean_s"),
                        "window_d": prof.get("window_d")}
    with strat2_lock:
        busy = {(p["strategy"], p["coin"]) for p in follow_open.values()
                if not p.get("done")}
        # F6 — таймер F1 (тиша 1 хв), але ЛИШЕ перший постріл пари; без
        # вимоги профілю (ТЗ 04.09 п.3)
        _timers = list(FOLLOW_TIMERS.items())
        if first_shot:
            _timers.append((F6_NAME, F6_TIMER_S))
        for st, timer in _timers:
            if (st, coin) in busy:
                if st == F6_NAME:
                    # first-shot-когорта рідкісна: мовчазний скіп ховав
                    # би частку вибірки F6 проти F5/F7 (рев'ю v2.9 S3)
                    stats["follow_busy_skips"] = \
                        stats.get("follow_busy_skips", 0) + 1
                    print(f"  [FOLLOW] {st} пропуск: {coin} зайнята "
                          f"({addr[:10]}…)")
                continue
            it = dict(base); it["strategy"] = st; it["timer"] = float(timer)
            follow_open[f"{key}|{st}|{int(now)}"] = it
        # F4 — кожна достатня транзакція швидкого гаманця; F5 — ті
        # самі гаманці, але ЛИШЕ перший постріл пари (або пауза
        # ≥1 год): повтори в межах години — шум (дослідження H080);
        # F7 — як F5, але кваліфікація nr (без ratio-гейта, ТЗ 04.09
        # п.9): свій таймер 2×пауза зі СВОЄЇ популяції епізодів
        _prof_strats = []
        if f4_ok:
            gap2 = 2.0 * float(prof.get("avg_gap_s", 0) or 0)
            timer4 = min(max(gap2, F4_CLAMP[0]), F4_CLAMP[1])
            _prof_strats += [(F4_NAME, timer4, True, None),
                             (F5_NAME, timer4, first_shot, None)]
        if f7_ok:
            gap7 = 2.0 * float(nr_prof.get("avg_gap_s", 0) or 0)
            timer7 = min(max(gap7, F4_CLAMP[0]), F4_CLAMP[1])
            _prof_strats.append((F7_NAME, timer7, first_shot, nr_prof))
        for st_name, timer_x, allowed, prof_override in _prof_strats:
            if not allowed: continue
            if (st_name, coin) in busy:
                # пропуск через «монета зайнята» іншим китом видимий:
                # F5/F7-події рідкісні, мовчазний скіп ховав би частку
                # вибірки (рев'ю v2.8)
                stats["follow_busy_skips"] = stats.get("follow_busy_skips", 0) + 1
                print(f"  [FOLLOW] {st_name} пропуск: {coin} зайнята "
                      f"({addr[:10]}…)")
                continue
            it = dict(base); it["strategy"] = st_name
            it["timer"] = timer_x; it["profile_gap"] = timer_x
            if prof_override is not None:
                # рядок F7 несе ЙОГО кваліфікацію (nr-гілку), а не
                # ratio-гейтнуту — інакше статистика приписувала б F7
                # чужу популяцію епізодів
                it["prof"] = {"n_fast": prof_override.get("n_fast", 0),
                              "n_slow": prof_override.get("n_slow", 0),
                              "fast_pct": prof_override.get("fast_pct"),
                              "unload_med_s": prof_override.get("unload_med_s"),
                              "unload_mean_s": prof_override.get("unload_mean_s"),
                              "window_d": prof.get("window_d")}
            follow_open[f"{key}|{st_name}|{int(now)}"] = it
        # ТІНЬОВА хвилинна стрічка follow-входу: m1..m60 незалежно від
        # правил виходу — одна на активну пару кит:монета
        fo_busy = any(p.get("strategy") == "_FOLLOW_OUT"
                      and p.get("coin") == coin and p.get("addr") == addr
                      and not p.get("done") for p in rev_open.values())
        if not fo_busy:
            fo_id = f"fo-{int(now * 1000)}-{coin}-{addr[2:8]}"
            rev_open[fo_id] = {
                "sig_id": fo_id, "strategy": "_FOLLOW_OUT",
                "state": "open", "coin": coin, "side": our, "addr": addr,
                "detect_ts": now, "detect_px": px,
                "entry_ts": now, "entry_px": px,
                "tx_pct": base["tx_pct"], "tx_usd": base["tx_usd"],
                "ratio": base["ratio"], "pos_usd": base["pos_usd"],
                "vault": base["vault"], "hour": hour,
                "btc_move": btc_move, "depth": depth,
                "algo_v": DATA_ALGO_V,
                "first_shot": first_shot, "pair_gap": base["pair_gap"],
                "samples": [], "peak": -999.0, "trough": 999.0}
    # фоновий підтяг історії — СУВОРО поза strat2_lock (аудит 29.08:
    # виклик зсередини критичної секції давав self-deadlock усього модуля)
    if need_profile:
        _profile_request(addr)
    print(f"  [FOLLOW] {coin} {our} слідом за {addr[:10]}… "
          f"(шматок {base['tx_pct']:.1f}%, ${base['tx_usd']:,.0f})")

# ── F4: профілі гаманців з історії філів ────────────────
PROFILE_ERR_TTL_S = 6 * 3600   # збій запиту = "невідомо", ретрай за 6 год
PROFILE_TTL_S     = 24 * 3600  # і ЗДОРОВИЙ профіль старіє: "останні 90
                               # днів / 10k філів" — ковзне вікно, кеш
                               # 100-денної давності не є актуальною
                               # поведінкою (аудит v2.3 п.4); раз на добу
PROFILE_HARD_TTL_S = 48 * 3600 # жорстка стеля для ВХОДУ F4: до 24г кеш
                               # свіжий, 24-48г — stale-while-revalidate
                               # (користуємось, рефреш їде фоном),
                               # старіше — F4 закритий до рефрешу
                               # (аудит v2.4 п.1: 100-денний ok
                               # відкривав угоди)

profile_retry_at  = {}   # addr -> ts: не перезапитувати історію раніше
                         # (429 / збій рефрешу при ще валідному профілі)
PROFILE_QUEUE_MAX = 100  # стеля черги потоків на семафорі: каскадний день
                         # після бампа версії не має плодити сотні потоків
PROFILE_429_RETRY_S = 300
PROFILE_FAIL_RETRY_S = 1800

def _profile_request(addr):
    a = addr.lower()
    with strat2_lock:
        if a in profiles_fetching: return
        if time.time() < profile_retry_at.get(a, 0): return
        prof = wallet_profiles.get(a)
        if prof is not None:
            stale_version = prof.get("v") != PROFILE_ALGO_V
            soft = prof.get("err") or prof.get("truncated")
            age = time.time() - prof.get("fetched", 0)
            # застаріла версія алгоритму — перерахунок одразу; збійний/
            # обрізаний — ретрай після PROFILE_ERR_TTL_S; ЗДОРОВИЙ
            # (ok і не-ok) — рефреш після PROFILE_TTL_S: 90-денне вікно
            # ковзає, і позитивна, і негативна кваліфікація старіють
            # (аудит v2.3 п.4)
            if not stale_version and \
               age < (PROFILE_ERR_TTL_S if soft else PROFILE_TTL_S):
                return
        if len(profiles_fetching) >= PROFILE_QUEUE_MAX:
            # стеля: адреса не втрачена — наступний сигнал кита попросить
            # знову, коли черга розсмокчеться (рев'ю v2.8)
            if time.time() - stats.get("profile_q_warn", 0) > 60:
                stats["profile_q_warn"] = time.time()
                print(f"  [F4] черга профілів повна ({PROFILE_QUEUE_MAX}) — "
                      f"{a[:10]}… відкладено")
            return
        profiles_fetching.add(a)
    try:
        threading.Thread(target=_fetch_profile, args=(a,), daemon=True).start()
    except Exception as e:
        # збій старту потоку (ліміт потоків ОС): без discard адреса
        # висіла б у profiles_fetching назавжди (рев'ю v2.8)
        with strat2_lock:
            profiles_fetching.discard(a)
        print(f"  [F4] потік профілю {a[:10]}… не стартував: {e}")

def _grade_episode(ep, coin, depth_fn, now_ms, is_last, tail=(),
                   min_ratio=F4_MIN_RATIO):
    """Оцінка ОДНОГО сегмента позиції (між реопенами/flat) за ТЗ 01.09.
    ep: закриття у хронології: (t_ms, px, sz_close, start_pos, aggr, side),
        aggr = тейкер (crossed) і не-TWAP — «агресивна транзакція».
    tail: рядки монети ПІСЛЯ сегмента (наступні сегменти) — кінець
        епізоду шукається і там у межах 5 хв від старту: долив >2% посеред
        зливу ділить СЕГМЕНТ, але не епізод (рев'ю v2.8: «продав 10%,
        докупив 5%, за 40с злив усе» — один швидкий, не slow + fast 0с).
    min_ratio: гейт ratio на старті епізоду; 0 = БЕЗ гейта (nr-гілка
        профілю для F7, ТЗ 04.09 п.9 — лишається тільки поріг $100k;
        глибина тоді не обов'язкова, ratio у результаті інформативний).
    Повертає:
      None                — не великий епізод (жодна агресивна tx ≥5% не
                            пройшла гейти $100k / ratio);
      {"nodepth": 1}      — глибини монети немає: ratio не порахувати
                            (лише при min_ratio > 0);
      {"inprog": 1}       — старт <5 хв тому і ще не закрито: триває;
      {"fast": 0/1, "unload_s": с|None, "gaps": [...], "usd": $,
       "ratio": r, "end_t": ms|None}
    Старт = ПЕРША агресивна tx з часткою ≥5% від позиції НА ТОЙ МОМЕНТ,
    за умови позиції ≥$100k і ratio ≥ min_ratio (до ПОТОЧНОЇ глибини
    сторони). Позиція в сегменті лише зменшується, тому гейти $/ratio
    монотонні: перевіряємо кожен шматок ≥5%, доки один не пройде.
    Кінець = перший філ, після якого залишок (startPosition − закрите —
    це число самої біржі) ≤5% від позиції на старті; рахуються ВСІ
    закриття — мейкер, TWAP, ліквідація: «закрив повністю» не залежить
    від способу. Швидкий = кінець − старт ≤5 хв. Не закрив і старт
    >5 хв тому → повільний."""
    start_i = None
    ratio = 0.0
    usd0 = 0.0
    for i, (t, px, szc, sp, aggr, side) in enumerate(ep):
        if not aggr or sp <= 0: continue
        if szc < sp * F4_CHUNK_PCT: continue
        usd = sp * px
        if usd < F4_MIN_NOTIONAL: continue
        d = depth_fn(coin, side) or 0
        if min_ratio > 0:
            if d <= 0:
                return {"nodepth": 1}
            r = usd / d
            if r < min_ratio: continue
        else:
            r = usd / d if d > 0 else 0.0
        start_i, ratio, usd0 = i, r, usd
        break
    if start_i is None:
        return None
    t0, _px0, _szc0, sp0, _a0, _s0 = ep[start_i]
    done_rem = sp0 * (1.0 - F4_FULL_PCT)
    end_t = None
    aggr_ts = []
    for (t, px, szc, sp, aggr, side) in ep[start_i:]:
        if aggr: aggr_ts.append(t)
        if max(0.0, sp - szc) <= done_rem:
            end_t = t
            break
    if end_t is None:
        # за межами сегмента — лише у вікні «швидкого»: пізніший кінець
        # належить уже іншій позиції (реопен), а для «повільний» точний
        # час не потрібен
        for (t, px, szc, sp, aggr, side) in tail:
            if t - t0 > F4_MAX_UNLOAD_S * 1000: break
            if aggr: aggr_ts.append(t)
            if max(0.0, sp - szc) <= done_rem:
                end_t = t
                break
    gaps = [(aggr_ts[i] - aggr_ts[i - 1]) / 1000.0
            for i in range(1, len(aggr_ts))]
    if end_t is None:
        if now_ms - t0 < F4_MAX_UNLOAD_S * 1000:
            return {"inprog": 1}   # епізод ще триває — не оцінюємо
        return {"fast": 0, "unload_s": None, "gaps": gaps,
                "usd": usd0, "ratio": ratio, "end_t": None}
    unload = (end_t - t0) / 1000.0
    return {"fast": int(unload <= F4_MAX_UNLOAD_S), "unload_s": unload,
            "gaps": gaps, "usd": usd0, "ratio": ratio, "end_t": end_t}

def _build_profile(fills, now_ms=None, depth_fn=None):
    """Історія філів гаманця -> профіль ШВИДКОСТІ розвантажень (ТЗ 01.09).
    (а) філи ГРУПУЮТЬСЯ у транзакції по hash, як у
        get_recent_market_fills: інакше той самий ордер, порізаний
        матчінгом на 10 дрібних філів, валив шматок-тест (аудит п.4);
    (б) агресивні (crossed, не-TWAP) закриття — «транзакції ≥5%», що
        відкривають епізод; пасивні і TWAP-закриття теж читаються, але
        лише як зменшення позиції (для «закрив повністю» і для поділу
        на сегменти) — маркет-тиском вони не є;
    (в) сегменти позиції: реопен/flat ділять, пауза — ні (аудит v2.4).
    Результат: ok, n_ep (=n_fast), n_big, n_fast, n_slow, fast_pct,
    unload_med_s / unload_mean_s (швидкі), unload_all_med_s (усі
    завершені), avg_gap_s (пауза між агресивними tx у швидких —
    таймер виходу F4/F5), n_nodepth, n_inprog, bad_rows."""
    if now_ms is None: now_ms = time.time() * 1000
    if depth_fn is None: depth_fn = _sim_depth
    txs = {}   # (coin, hash) -> [t, cost, sz, start_pos, aggr, side]
    bad = 0    # close-філи, які НЕ вдалося розібрати: історія неповна
    for f in fills:
        # структурні поля перевіряються ДО класифікації: філ без dir чи
        # crossed — це НЕ "не-close" і НЕ "maker", це БИТИЙ запис
        # (аудит v2.4 п.3: раніше він тихо зникав як "пасивний");
        # coin теж обов'язковий (аудит v2.5: без нього філ ліпився
        # у групу "?" і тихо псував чужі епізоди)
        if not all(k in f for k in ("dir", "crossed", "px", "sz",
                                    "time", "startPosition", "coin",
                                    "hash")):
            bad += 1
            continue
        if not f.get("hash"):
            bad += 1   # порожній hash: hashless-філи однієї мс клеїлись
            continue   # у фальшивий "великий шматок" (аудит v2.6 №6)
        _cr = f.get("crossed")
        if not isinstance(_cr, bool):
            bad += 1   # crossed="false" (рядок) — це БИТЕ, не maker
            continue
        d = str(f.get("dir", ""))
        # ліквідація/ADL: dir не «Close…», але позиція ЗМЕНШУЄТЬСЯ — та
        # сама класифікація, що у live-детекторі (рев'ю v2.8: інакше
        # сегмент лишався «незакритим» і клеївся з наступною позицією)
        f_liq = (bool(f.get("liquidation")) or ("Liquidat" in d)
                 or d.startswith("Auto-Delever"))
        if not (d.startswith("Close") or ">" in d or f_liq): continue
        try:
            t = int(f.get("time", 0)); px = float(f.get("px", 0))
            sz = float(f.get("sz", 0))
            sp_signed = float(f.get("startPosition", 0) or 0)
            sp = abs(sp_signed)
        except (TypeError, ValueError):
            bad += 1   # биті значення = невідомий шматок історії, а не
            continue   # "його не було" (аудит v2.3 п.5.2)
        # бік, який ЗАКРИВАЄТЬСЯ: "Close Long" / "Long > Short" — лонг
        # (продаж у bid), "Close Short" / "Short > Long" — шорт (купівля
        # з ask); у ліквідації без слова в dir — знак startPosition;
        # глибина для ratio береться по цій стороні, як у watchlist
        if d.startswith("Close Long") or d.startswith("Long") or "Long" in d:
            side = "LONG"
        elif d.startswith("Close Short") or d.startswith("Short") or "Short" in d:
            side = "SHORT"
        else:
            side = "LONG" if sp_signed > 0 else "SHORT"
        # агресивна = тейкер, не TWAP, не ліквідація; пасивні/TWAP/
        # ліквідаційні закриття лишаються в історії як зменшення позиції
        # (v7), але епізод не відкривають — це не рішення гаманця
        aggr = bool(_cr) and f.get("twapId") is None and not f_liq
        # NaN проходив крізь "px <= 0" (усі порівняння з nan = False) і
        # труїв агрегати — тепер finite обов'язковий (аудит v2.5)
        if not (math.isfinite(px) and math.isfinite(sz)
                and math.isfinite(sp)):
            bad += 1
            continue
        if t <= 0 or px <= 0 or sz <= 0:
            bad += 1
            continue
        h = str(f.get("hash"))
        # системні філи (TWAP-суб-ордери, ліквідації, ADL) несуть нульовий
        # hash 0x000…0: групувати їх по hash = склеїти ВСІ такі закриття
        # монети за 90 днів в одну «транзакцію» (рев'ю v2.8, регресія
        # проти v6, де TWAP відкидались до групування) — ключ по oid/tid
        if h.lower().strip("0x") == "" or f.get("twapId") is not None or f_liq:
            k = (f.get("coin", "?"), "sys",
                 f.get("oid") if f.get("oid") is not None
                 else f.get("tid", t))
        else:
            k = (f.get("coin", "?"), h)
        agg = txs.setdefault(k, [t, 0.0, 0.0, sp, aggr, side])
        agg[0] = min(agg[0], t)
        agg[1] += px * sz
        agg[2] += sz
        agg[3] = max(agg[3], sp)
        agg[4] = agg[4] and aggr   # ордер із TWAP-філом — не агресивний
    closes = {}
    for k, (t, cost, sz, sp, aggr, side) in txs.items():
        coin = k[0]   # ключ — (coin, hash) або (coin, "sys", oid/tid)
        # фліп ("Long > Short") містить і закриття, і відкриття нового
        # боку: закритого не більше, ніж БУЛО позиції — кламп як у live
        # (аудит v2.1 п.4: $50k закриття рахувалось як $150k)
        sz_close = min(sz, sp) if sp > 0 else sz
        closes.setdefault(coin, []).append((t, cost / sz, sz_close, sp,
                                            aggr, side))
    segs_by_coin = {}
    for coin, lst in closes.items():
        lst.sort(key=lambda r: (r[0], -r[3]))
        segs, ep = [], []
        for row in lst:
            if ep:
                prev = ep[-1]
                prev_remaining = max(0.0, prev[3] - prev[2])
                # сегмент ділиться коли позиція ВИРОСЛА між закриттями
                # (перевідкриття) АБО коли попередня була злита В НУЛЬ
                # (flat) — далі будь-який розмір це нова позиція, навіть
                # у 100 разів менша (аудит v2.4: $100k після $10M не
                # проходила поріг 2% від БІЛЬШОЇ і зливалась в один
                # епізод, який валив chunk-тест — губились ОБИДВІ).
                # Пауза сама по собі не ділить: 95% швидко + хвіст за
                # 10 хв = одне повільне розвантаження (аудит v2.3 п.3),
                # воно чесно рахується як ПОВІЛЬНИЙ епізод.
                flat_done = prev_remaining <= 0.02 * prev[3]
                reopened = (row[3] > prev_remaining
                            + 0.02 * max(row[3], prev[3])
                            or (flat_done and row[3] > 0))
                if reopened:
                    segs.append(ep)
                    ep = []
            ep.append(row)
        if ep: segs.append(ep)
        segs_by_coin[coin] = segs

    def _pass(min_ratio):
        """Один прохід оцінки епізодів по СПІЛЬНИХ сегментах. min_ratio
        = F4_MIN_RATIO для основної гілки (F4/F5), 0 — для nr-гілки F7
        (ТЗ 04.09 п.9: старт епізоду можуть відкривати РІЗНІ транзакції,
        тому прохід чесно окремий, а не фільтр по готових епізодах)."""
        n_fast = n_slow = n_nodepth = n_inprog = 0
        unl_fast, unl_all, gaps = [], [], []
        for coin, segs in segs_by_coin.items():
            consumed_until = -1   # рядки до кінця знайденого епізоду вже
                                  # враховані — наступний сегмент не має
                                  # народити з них другий епізод
            for i, seg in enumerate(segs):
                seg_eff = [r for r in seg if r[0] > consumed_until]
                if not seg_eff: continue
                tail = [r for s2 in segs[i + 1:] for r in s2]
                res = _grade_episode(seg_eff, coin, depth_fn, now_ms,
                                     i == len(segs) - 1, tail, min_ratio)
                if res is None: continue
                if res.get("inprog"):
                    n_inprog += 1
                    continue
                if res.get("nodepth"):
                    n_nodepth += 1
                    continue
                if res.get("end_t") is not None:
                    consumed_until = max(consumed_until, res["end_t"])
                if res["fast"]:
                    n_fast += 1
                    unl_fast.append(res["unload_s"])
                    gaps.extend(res["gaps"])
                else:
                    n_slow += 1
                if res["unload_s"] is not None:
                    unl_all.append(res["unload_s"])
        n_big = n_fast + n_slow
        fast_pct = round(100.0 * n_fast / n_big, 1) if n_big else None
        ok = (n_fast >= F4_MIN_EPISODES and fast_pct is not None
              and fast_pct >= F4_MIN_FAST_PCT)
        avg = (sum(gaps) / len(gaps)) if gaps else 30.0   # одним пострілом
                                                          # = мін. кламп
        return {"ok": ok, "n_ep": n_fast, "n_big": n_big, "n_fast": n_fast,
                "n_slow": n_slow, "fast_pct": fast_pct,
                "unload_med_s": (round(_median(unl_fast), 1)
                                 if unl_fast else None),
                "unload_mean_s": (round(sum(unl_fast) / len(unl_fast), 1)
                                  if unl_fast else None),
                "unload_all_med_s": (round(_median(unl_all), 1)
                                     if unl_all else None),
                "avg_gap_s": round(avg, 1),
                "n_nodepth": n_nodepth, "n_inprog": n_inprog}

    out = _pass(F4_MIN_RATIO)
    # nr-гілка (F7): ті самі філи/сегменти, гейт лише ≥$100k. Надмножина
    # основної: кожен ratio-гейтнутий епізод є і тут (можливо, зі
    # старшим стартом), плюс епізоди «$100k у глибокій монеті»
    out["nr"] = _pass(0.0)
    if bad:
        # fail-closed: профіль з дір — "невідомо", не кваліфікація;
        # err=1 -> F4/F7 закриті, ретрай після TTL (аудит v2.3 п.5.2)
        out.update(ok=False, err=1, bad_rows=bad)
        out["nr"]["ok"] = False
    return out

def _fill_key(f):
    """Ідентичність філа для дедуплікації між сторінками. Докстрока HL
    гарантує унікальність трейду по (block_time, coin, tid), НЕ по
    одному tid (аудит v2.3 п.5.1) — ключ композитний; фолбек без tid —
    повний кортеж полів."""
    tid = f.get("tid")
    if tid is not None:
        return ("tid", f.get("time"), f.get("coin"), tid)
    return (f.get("coin"), f.get("hash"), f.get("time"), str(f.get("px")),
            str(f.get("sz")), str(f.get("startPosition")), f.get("dir"))

_profile_sem = threading.Semaphore(1)   # історія — найважчий REST-запит
                                        # (вага 20 + 1 за кожні 20 філів,
                                        # до ~700 на гаманець): один гаманець
                                        # за раз, щоб пачка нових китів у
                                        # каскадний день не з'їла ліміт IP,
                                        # на якій живе детекція

# Бюджет ВАГИ історії за ковзну хвилину по каналах (рев'ю v2.8: семафор
# обмежує лише конкурентність — два гіперактивні гаманці поспіль з'їдали
# ліміт 1200/хв). userFillsByTime = 20 + 1 за кожні 20 філів, повна
# сторінка ≈ 120. Проксі — половина її ліміту, прямий канал — лише
# крихта: там живе детекція (скан + sweep ≈ 860/хв)
PROFILE_W_PER_MIN = {"proxy": 600, "direct": 150}
_profile_w = {"proxy": deque(), "direct": deque()}
_profile_w_lock = threading.Lock()
_profile_proxy = {"dead_until": 0.0}   # проксі мертва → 30 хв напряму

def _profile_budget_wait(via, w_next=120):
    """Спати, доки вага останніх 60с по каналу + очікувана вага сторінки
    не вкладеться у стелю. Максимум ~65с (одне вікно)."""
    cap = PROFILE_W_PER_MIN[via]
    deadline = time.time() + 65
    while True:
        now_ = time.time()
        with _profile_w_lock:
            dq = _profile_w[via]
            while dq and dq[0][0] < now_ - 60:
                dq.popleft()
            used = sum(w for _, w in dq)
            oldest = dq[0][0] if dq else now_
        if used + w_next <= cap or now_ >= deadline:
            return
        time.sleep(min(5.0, max(0.5, oldest + 60 - now_)))

def _profile_budget_add(via, batch):
    w = 20 + (len(batch) // 20 if isinstance(batch, list) else 0)
    with _profile_w_lock:
        _profile_w[via].append((time.time(), w))

def _profile_post(body):
    """Сторінка історії: через REST-проксі (rest_proxy.txt), якщо вона
    є і не визнана мертвою — щоб важкі 90-денні вибірки не їли ліміт
    основної IP. Мережевий збій проксі → 30 хв напряму (не таймаут 10с
    на кожній із 6 сторінок кожного гаманця). 429 на проксі — це
    ВИЧЕРПАНИЙ бюджет, а не мертвий канал: НЕ переносимо вагу на IP
    детекції, віддаємо RateLimited викликачу (рев'ю v2.8)."""
    now_ = time.time()
    via = "direct"
    if REST_PROXY and now_ >= _profile_proxy["dead_until"]:
        ds = _prio_proxy_state.get("dead_since", 0.0)
        if not ds or now_ - ds >= 1800:   # prio-воркер теж не вважає мертвою
            via = "proxy"
    if via == "proxy":
        _profile_budget_wait("proxy")
        try:
            r = hl_post_prio(body, retries=1)
            _profile_budget_add("proxy", r)
            return r
        except RateLimited:
            raise
        except Exception as e:
            _profile_proxy["dead_until"] = time.time() + 1800
            print(f"  [F4] проксі історії: {e} — 30 хв напряму")
    _profile_budget_wait("direct")
    r = hl_post(body, retries=2)
    _profile_budget_add("direct", r)
    return r

def _fetch_profile(addr):
    with _profile_sem:
        _fetch_profile_locked(addr)

def _fetch_profile_locked(addr):
    now_ms = time.time() * 1000
    try:
        cursor = int(now_ms - PROFILE_WINDOW_D * 86400 * 1000)
        fills, seen = [], set()
        truncated = capped = 0
        for i in range(PROFILE_PAGES):
            if i: time.sleep(1.0)   # між сторінками: бюджет ваги
            # endTime фіксований на всю пагінацію: вікно не «їде» під
            # час запитів (визнана межа v2.4 закрита)
            batch = _profile_post({"type": "userFillsByTime", "user": addr,
                                   "startTime": cursor,
                                   "endTime": int(now_ms)})
            if not isinstance(batch, list):
                # відповідь не історія (напр. {"error": ...}): збій, а не
                # "порожня історія". І на початку, і ПОСЕРЕД пагінації це
                # err -> ретрай після TTL: профіль із огризка історії
                # інакше кешувався назавжди (рев'ю v2.2)
                raise ValueError(f"bad history payload: {type(batch).__name__}")
            if not batch: break
            # порядок сторінки — oldest-first, як у докстроці API: інший
            # порядок дав би тихий профіль із 2000 останніх філів без
            # hist_capped (рев'ю v2.8) — fail-closed, як у детекторі
            try:
                _t_first = int(batch[0].get("time", 0) or 0)
                _t_last = int(batch[-1].get("time", 0) or 0)
            except (TypeError, ValueError, AttributeError):
                raise ValueError("bad history row")
            if _t_first > _t_last:
                raise ValueError("history order: descending page")
            new = 0
            for f in batch:
                k = _fill_key(f)
                if k in seen: continue
                seen.add(k); fills.append(f); new += 1
            if len(batch) < 2000: break
            mx = max(f.get("time", 0) for f in batch)
            # курсор ВКЛЮЧНО (без +1): філи тієї ж мс за зрізом сторінки
            # губилися назавжди — 2001-й філ мілісекунди не потрапляв у
            # наступний запит (аудит v2.2 п.2); перекриття знімає дедуп
            if mx <= cursor and new == 0:
                truncated = 1   # >2000 філів в одній мс: далі не пройти
                break
            cursor = mx
            if i == PROFILE_PAGES - 1:
                capped = 1      # сторінки вичерпано, дані ще є
        # API віддає лише 10k ОСТАННІХ філів: у гіперактивного гаманця
        # 90 днів «стискаються» до фактичного вікна — це не збій і не
        # truncated (ТЗ 01.09: «за 3 місяці, до 10 тисяч філів»), а
        # властивість даних; window_d показує реальну глибину історії
        if len(fills) >= 9900: capped = 1
        prof = _build_profile(fills, now_ms=now_ms)
        oldest = min((int(f.get("time", 0) or 0) for f in fills), default=0)
        prof["window_d"] = (round((now_ms - oldest) / 86400000.0, 1)
                            if oldest > 0 else 0.0)
        prof["n_fills"] = len(fills)
        if capped: prof["hist_capped"] = 1
        if truncated: prof["truncated"] = 1
    except RateLimited as e:
        # 429 = вичерпаний бюджет, а не бита історія: профіль НЕ пишемо
        # (валідний старий лишається чинним), ретрай за 5 хв — інакше
        # у каскадний день уся черга діставала err на 6 год і втрачала
        # право на F4/F5 при живому кеші (рев'ю v2.8)
        with strat2_lock:
            profile_retry_at[addr] = time.time() + PROFILE_429_RETRY_S
            profiles_fetching.discard(addr)
        print(f"  [F4] профіль {addr[:10]}…: {e} — ретрай за "
              f"{PROFILE_429_RETRY_S // 60} хв")
        return
    except Exception as e:
        print(f"  [F4] профіль {addr[:10]}…: {e}")
        with strat2_lock:
            old = wallet_profiles.get(addr)
            if (old is not None and old.get("v") == PROFILE_ALGO_V
                    and not old.get("err")
                    and time.time() - old.get("fetched", 0) < PROFILE_HARD_TTL_S):
                # збій РЕФРЕШУ при ще валідному профілі (stale-while-
                # revalidate): не затираємо його err-ом, ретрай за 30 хв
                old["refresh_failed_at"] = int(time.time())
                profile_retry_at[addr] = time.time() + PROFILE_FAIL_RETRY_S
                profiles_fetching.discard(addr)
                return
        # err=1: "невідомо, історія не отрималась" — НЕ вирок гаманцю;
        # _profile_request повторить запит після TTL (аудит п.5)
        prof = {"ok": False, "err": 1, "n_ep": 0, "n_big": 0, "n_fast": 0,
                "n_slow": 0, "fast_pct": None, "avg_gap_s": 0.0}
    prof["v"] = PROFILE_ALGO_V
    prof["fetched"] = int(time.time())
    with strat2_lock:
        wallet_profiles[addr] = prof
        profiles_fetching.discard(addr)
    try:
        # знімок + запис + replace під одним локом: інакше старіший
        # знімок міг перетерти новіший (аудит v2.2 п.3)
        with _prof_save_lock:
            with strat2_lock:
                snapshot = dict(wallet_profiles)
            tmp = f"{PROFILES_FILE}.tmp{threading.get_ident()}"
            with open(tmp, "w") as f:
                json.dump(snapshot, f)
            os.replace(tmp, PROFILES_FILE)
    except Exception as _we:
        print(f"  [F4] кеш профілів НЕ записано: {_we}")
    _um = prof.get("unload_med_s")
    print(f"  [F4] профіль {addr[:10]}…: ok={prof['ok']} "
          f"швидких={prof.get('n_fast', 0)} повільних={prof.get('n_slow', 0)}"
          f" ({prof.get('fast_pct') if prof.get('fast_pct') is not None else '—'}%)"
          f" злив мед.={_um if _um is not None else '—'}с"
          f" пауза={prof['avg_gap_s']:.0f}с вікно={prof.get('window_d', 0)}д"
          f"{' кап10k' if prof.get('hist_capped') else ''}"
          f"{' err' if prof.get('err') else ''}")

# ── Головний цикл оболонки: тик кожні 3с на цінах поллера ──
def run_strat2_loop():
    while True:
        time.sleep(3)
        if not STRAT2_ENABLED: continue
        # outbox сигнальних рядків — ретраїться незалежно від трекерів
        with _sig_retry_lock:
            _pending = _sig_retry_q[:]
            del _sig_retry_q[:]
        if _pending:
            _still = []
            for _it in _pending:
                if _strat_csv_append(_it[0], _it[1], _it[2]): continue
                _it[3] += 1
                if _it[3] > WFAIL_CAP:
                    print(f"  [STRAT] DROP сигнальний рядок після "
                          f"{WFAIL_CAP} спроб (диск?)")
                else:
                    _still.append(_it)
            if _still:
                with _sig_retry_lock:
                    _sig_retry_q.extend(_still)
        with strat2_lock:
            active = bool(rev_open) or bool(follow_open)
        if not active: continue
        now = time.time()
        # Рядки збираємо БЕЗ видалення трекера: видаляємо після успішного
        # запису CSV (аудит v2.1 п.7). Завершений трекер отримує done=1 і
        # ЗАМОРОЖЕНИЙ final_row: ретрай пише ті самі байти, а не
        # перераховує результат новішою ціною — інакше тимчасовий збій
        # диска міняв exit/net завершеної угоди (аудит v2.2 п.1).
        # done не блокує busy; після WFAIL_CAP ретраїв — дроп із логом.
        # Дублі після краху знімає дедуп по sig_id/trade_id у strat2_api.
        rev_rows, fol_rows = [], []
        with strat2_lock:
            for pid, p in list(rev_open.items()):
                if p.get("done"):
                    if p.get("final_row") is not None:
                        rev_rows.append((pid, p.get("row_kind", "rev"),
                                         p["final_row"]))
                    else:
                        # done без final_row: рядок можна відтворити з
                        # заморожених семплів (вони в трекері)
                        if p["strategy"] == "_OUTCOME":
                            p["row_kind"] = "out"
                            p["final_row"] = _rev_out_row(p)
                        elif p["strategy"] == "_FOLLOW_OUT":
                            p["row_kind"] = "fo"
                            p["final_row"] = _fol_out_row(p)
                        else:
                            p["row_kind"] = "rev"
                            p["final_row"] = _rev_row(
                                p, 1 if p.get("entry_px") else 0)
                        rev_rows.append((pid, p["row_kind"], p["final_row"]))
                    continue
                px = _px_now(p["coin"], max_age=30.0)
                if p["state"] == "armed":
                    # СПОЧАТКУ дедлайн (аудит п.3: пізній тик після
                    # закінчення вікна відкривав позицію заднім числом)
                    if now >= p["deadline"]:
                        p["done"] = 1
                        p["row_kind"] = "rev"
                        p["final_row"] = _rev_row(p, 0)
                        rev_rows.append((pid, "rev", p["final_row"]))
                        continue
                    if not px: continue
                    trig = p["trigger_px"]
                    hit = px >= trig if p["side"] == "LONG" else px <= trig
                    if hit:
                        # консервативно: гірша з цін (тригер/поточна)
                        p["entry_px"] = (max(px, trig) if p["side"] == "LONG"
                                         else min(px, trig))
                        p["entry_ts"] = now
                        p["state"] = "open"
                    continue
                # Хвилинні мітки: закриваємо лише ті хвилини, для яких
                # ціна СВІЖА (<=30с від межі хвилини). Пропущені через
                # рестарт/збій пишемо як "" — НЕ підставляємо пізнішу
                # ціну в ранній горизонт (аудит п.2)
                if px:
                    g = (px / p["entry_px"] - 1.0) * 100.0
                    if p["side"] == "SHORT": g = -g
                    p["peak"] = max(p["peak"], g)
                    p["trough"] = min(p["trough"], g)
                else:
                    g = None
                target = min(int((now - p["entry_ts"]) // 60), REV_TRACK_MIN)
                while len(p["samples"]) < target:
                    k = len(p["samples"]) + 1
                    late = now - (p["entry_ts"] + k * 60.0)
                    if g is not None and late <= 30.0:
                        p["samples"].append(round(g, 4))
                    else:
                        p["samples"].append("")
                if len(p["samples"]) >= REV_TRACK_MIN:
                    p["done"] = 1
                    if p["strategy"] == "_OUTCOME":
                        p["row_kind"] = "out"
                        p["final_row"] = _rev_out_row(p)
                    elif p["strategy"] == "_FOLLOW_OUT":
                        p["row_kind"] = "fo"
                        p["final_row"] = _fol_out_row(p)
                    else:
                        p["row_kind"] = "rev"
                        p["final_row"] = _rev_row(p, 1)
                    rev_rows.append((pid, p["row_kind"], p["final_row"]))
            for fid, p in list(follow_open.items()):
                if p.get("done"):
                    if p.get("final_row") is not None:
                        fol_rows.append((fid, p["final_row"]))
                    else:
                        # done без замороженого рядка (відновлений зі
                        # старого state.json): вихідну ціну чесно
                        # відтворити неможливо — дроп із логом
                        follow_open.pop(fid, None)
                        print(f"  [STRAT] DROP {fid}: done без final_row "
                              f"(старий state)")
                    continue
                px = _px_now(p["coin"], max_age=30.0)
                if not px: continue
                g = (px / p["entry_px"] - 1.0) * 100.0
                if p["our_side"] == "SHORT": g = -g
                p["peak"] = max(p["peak"], g)
                p["trough"] = min(p["trough"], g)
                reason = p.get("force_exit")
                if not reason:
                    last = follow_last_close.get(p["key"], p["open_ts"])
                    if now - last >= p["timer"]:
                        reason = "silence"
                        # ціни не було на дедлайні і вихід стався значно
                        # пізніше таймера: маркуємо чесно — "1 хв тиші",
                        # виконана на 5-й хвилині, це ІНША стратегія
                        # (аудит v2.6 №11); аналіз фільтрує за reason
                        if now - (last + p["timer"]) > 60:
                            reason = "silence_late"
                if reason:
                    p["done"] = 1
                    slip = _sim_slip(p.get("depth") or 0)
                    costs = (2 * SIM_COMMISSION + 2 * slip) * 100.0
                    _bm = p.get("btc_move")
                    # заморожений фінальний рядок: exit/hold/net зафіксовані
                    # У МОМЕНТ виходу, ретраї запису їх не перерахують
                    p["final_row"] = [_dt(p["open_ts"]), _dt(now),
                        p["strategy"], p["coin"], p["our_side"], p["addr"],
                        round(p["entry_px"], 8), round(px, 8), reason,
                        round(now - p["open_ts"], 1), round(g, 4),
                        round(costs, 4), round(g - costs, 4),
                        round(p["peak"], 4), round(p["trough"], 4),
                        round(p["tx_pct"], 3), round(p["tx_usd"], 0),
                        round(p["ratio"], 2), round(p["pos_usd"], 0),
                        p["vault"], p["hour"],
                        (round(_bm, 3) if _bm is not None else ""),
                        round(p["profile_gap"], 1), p.get("algo_v", ""), fid,
                        # v2.8: перший постріл/пауза пари і профіль
                        # швидкості на момент входу (трекери зі старого
                        # state.json цих полів не мають -> порожньо)
                        p.get("first_shot", ""), p.get("pair_gap", ""),
                        *(lambda pr: [
                            pr.get("n_fast", ""), pr.get("n_slow", ""),
                            (pr.get("fast_pct") if pr.get("fast_pct") is not None else ""),
                            (pr.get("unload_med_s") if pr.get("unload_med_s") is not None else ""),
                            (pr.get("unload_mean_s") if pr.get("unload_mean_s") is not None else ""),
                            (pr.get("window_d") if pr.get("window_d") is not None else "")]
                          )(p.get("prof") or {})]
                    fol_rows.append((fid, p["final_row"]))
        written_rev, written_fol = [], []
        failed_rev, failed_fol = [], []
        for pid, kind, r in rev_rows:
            if kind == "out":
                ok = _strat_csv_append(REV_OUT_CSV, REV_OUT_HEADERS, r)
            elif kind == "fo":
                ok = _strat_csv_append(FOLLOW_OUT_CSV, FOLLOW_OUT_HEADERS, r)
            else:
                ok = _strat_csv_append(REV_CSV, REV_HEADERS, r)
            (written_rev if ok else failed_rev).append(pid)
        for fid, r in fol_rows:
            ok = _strat_csv_append(FOLLOW_CSV, FOLLOW_HEADERS, r)
            (written_fol if ok else failed_fol).append(fid)
        if rev_rows or fol_rows:
            with strat2_lock:
                for pid in written_rev: rev_open.pop(pid, None)
                for fid in written_fol: follow_open.pop(fid, None)
                # кап ретраїв: після WFAIL_CAP невдалих записів трекер
                # скидається (дані втрачені ЯВНО, з логом), а не висить
                # зомбі, що спамить диск і роздуває стан
                for coll, pid in ([(rev_open, x) for x in failed_rev]
                                  + [(follow_open, x) for x in failed_fol]):
                    it = coll.get(pid)
                    if it is None: continue
                    it["wfail"] = it.get("wfail", 0) + 1
                    if it["wfail"] > WFAIL_CAP:
                        coll.pop(pid, None)
                        print(f"  [STRAT] DROP незаписаний трекер {pid} "
                              f"після {WFAIL_CAP} спроб (диск?)")
        if written_rev or written_fol:
            threading.Thread(target=save_state, daemon=True).start()

# ── API для вкладки "Стратегії" ─────────────────────────
_strat2_cache = {"ts": 0.0, "data": None}

def _median(v):
    if not v: return None
    s = sorted(v); n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0

def strat2_api():
    now = time.time()
    if _strat2_cache["data"] is not None and now - _strat2_cache["ts"] < 15:
        return _strat2_cache["data"]
    import csv as _csv

    quarantined = [0]
    read_errors = [0]

    def read(path):
        # ЧЕСНЕ читання: рядок з іншою кількістю полів, ніж у заголовку
        # (обірваний ENOSPC-огризок, закритий ремонтом хвоста) — у
        # карантин, а не в статистику. csv.DictReader сам такого НЕ
        # відкидає (аудит v2.4 п.4: огризок без trade_id рахувався
        # другою угодою)
        try:
            # errors="replace": битий байт (обірваний мультибайт після
            # ENOSPC) стає U+FFFD і рядок ловиться карантином — а НЕ
            # вбиває декодування цілого чанка з тисячами здорових
            # рядків (рев'ю v2.7 №3: utf-8 декодується буферами, тож
            # виняток бив і по рядках ДО битого байта)
            with open(path, newline="", encoding="utf-8",
                      errors="replace") as f:
                rd = _csv.reader(f)
                hdr = next(rd, None)
                if not hdr:
                    return []
                has_eol = bool(hdr) and hdr[-1] == "eol"
                rows2 = []
                while True:
                    try:
                        rr = next(rd)
                    except StopIteration:
                        break
                    except Exception:
                        # битий байт/декодування ПОСЕРЕД файла: ітератор
                        # мертвий, але НАКОПИЧЕНІ валідні рядки віддаємо
                        # (рев'ю v2.7 №3: раніше один битий хвіст ховав
                        # тисячі здорових рядків як "нуль угод")
                        quarantined[0] += 1
                        read_errors[0] += 1
                        break
                    if len(rr) != len(hdr):
                        quarantined[0] += 1
                        continue
                    if has_eol and rr[-1] != "^":
                        # обрив усередині останнього поля: колонок
                        # стільки ж, але вартовий загублений
                        quarantined[0] += 1
                        continue
                    rows2.append(dict(zip(hdr, rr)))
                return rows2
        except FileNotFoundError:
            return []   # файла ще немає — норма першого запуску
        except Exception as e:
            # битий файл != "нуль угод": рахуємо і логуємо, щоб дашборд
            # не показував тихий нуль замість діагнозу (аудит v2.6)
            read_errors[0] += 1
            print(f"  [STRAT] read {os.path.basename(path)}: {e}")
            return []

    def fnum(x, d=None):
        try:
            v = float(x)
        except (TypeError, ValueError):
            return d
        # non-finite з битого CSV труїть медіани і ламає JSON.parse
        # у браузера (bare NaN — невалідний JSON; аудит v2.6 №7)
        return v if math.isfinite(v) else d

    def dedup(rows, keyf):
        # дублі можливі після краху між записом CSV і save_state
        # (рестарт відновлює вже записаний трекер) — знімаємо на
        # читанні. ОСТАННІЙ запис виграє: після обірваного append
        # перший рядок ключа може бути битим огризком, а повний —
        # повторним записом нижче (аудит v2.3 п.7). Рядок БЕЗ ключа —
        # у карантин: ідентифікувати і дедуплікувати його неможливо
        best, order = {}, []
        for r in rows:
            k = keyf(r)
            if not k:
                quarantined[0] += 1
                continue
            if k not in best: order.append(k)
            best[k] = r
        return [best[k] for k in order]
    rev = dedup(read(REV_CSV),
                lambda r: ((r.get("sig_id"), r.get("strategy"))
                           if r.get("sig_id") and r.get("strategy")
                           else None))
    sigs = dedup(read(REV_SIG_CSV), lambda r: r.get("sig_id"))
    fol = dedup(read(FOLLOW_CSV), lambda r: r.get("trade_id"))
    outs_all = dedup(read(REV_OUT_CSV), lambda r: r.get("sig_id"))
    fouts_all = dedup(read(FOLLOW_OUT_CSV), lambda r: r.get("fo_id"))
    # статистика рахується ЛИШЕ по рядках поточної версії логіки:
    # зміна алгоритму не має тихо змішувати старі й нові вимірювання в
    # одну вибірку (аудит v2.2). Виключене рахуємо у legacy_rows —
    # видно, скільки історії лишилося за бортом (файли не чіпаються)
    def curv(rows):
        return [r for r in rows if (r.get("algo_v") or "") == DATA_ALGO_V]
    n_before = (len(rev) + len(sigs) + len(fol) + len(outs_all)
                + len(fouts_all))
    rev, sigs, fol, outs, fouts = (curv(rev), curv(sigs), curv(fol),
                                   curv(outs_all), curv(fouts_all))
    legacy_rows = n_before - (len(rev) + len(sigs) + len(fol)
                              + len(outs) + len(fouts))
    out = {"updated": now, "strategies": {}, "desc": STRAT2_DESC,
           "titles": STRAT2_TITLES,
           "legacy_rows": legacy_rows, "quarantined": quarantined[0],
           "read_errors": read_errors[0]}

    for st in ("R1_загальний", "R2_breakout", "R3_великі", "R4_великий",
               "R5_дуже", "R6_волт", R7_NAME):
        rows = [r for r in rev if r.get("strategy") == st]
        # половини рахуємо у ХРОНОЛОГІЇ сигналів, а не в порядку
        # завершення записів (аудит п.10)
        rows.sort(key=lambda r: r.get("date") or "")
        nets30, trades = [], []
        n_entered = 0
        curve = [[] for _ in range(REV_TRACK_MIN)]
        complete = []   # угоди з УСІМА 60 хвилинами: спільна когорта
        for r in rows:
            entered = 1 if fnum(r.get("entered"), 0) == 1 else 0
            costs = fnum(r.get("costs_pct"), 0.15) or 0.15
            net30 = None
            if entered:
                n_entered += 1
                row_vals = []
                for i in range(REV_TRACK_MIN):
                    v = fnum(r.get(f"m{i+1}"))
                    row_vals.append(v)
                    if v is not None:
                        curve[i].append(v - costs)
                if all(v is not None for v in row_vals):
                    complete.append([v - costs for v in row_vals])
                n30 = fnum(r.get("m30"))
                net30 = (n30 - costs) if n30 is not None else None
                if net30 is not None: nets30.append(net30)
            # пік руху за перші 3 хв ПІСЛЯ детекту (у напрямку угоди):
            # скільки максимально дало/забрало одразу після закриття
            _m3 = [x for x in (fnum(r.get(f"m{i}")) for i in (1, 2, 3))
                   if x is not None]
            trades.append({"date": r.get("date"), "coin": r.get("coin"),
                "side": r.get("our_side"), "net30": net30,
                "peak": fnum(r.get("peak_pct")),
                "trough": fnum(r.get("trough_pct")),
                "move": fnum(r.get("move_3m_pct")),
                "dur": fnum(r.get("dur_s")), "ratio": fnum(r.get("ratio")),
                "shtanga": fnum(r.get("shtanga"), 0),
                "vault": fnum(r.get("vault"), 0),
                "hour": fnum(r.get("hour")),
                "btc": fnum(r.get("btc_move_pct")),
                "sum_usd": fnum(r.get("sum_usd")),
                "p3u": (max(_m3) if _m3 else None),
                "p3d": (min(_m3) if _m3 else None),
                "entered": entered})
        half = len(nets30) // 2
        cum, acc = [], 0.0
        for v in nets30:
            acc += v; cum.append(round(acc, 3))
        out["strategies"][st] = {
            "kind": "rev", "signals": len(rows), "entered": n_entered,
            "n": len(nets30),
            "median": _median(nets30), "mean": (sum(nets30) / len(nets30))
                if nets30 else None,
            "win": (100.0 * sum(1 for v in nets30 if v > 0) / len(nets30))
                if nets30 else None,
            "pnl_usd": round(sum(nets30) * 10.0, 2) if nets30 else 0.0,
            "half1": _median(nets30[:half]), "half2": _median(nets30[half:]),
            "cum": cum,
            "curve": [(_median(c) if c else None) for c in curve],
            # покриття кожної хвилини: після чесних пропусків хвилини
            # мають РІЗНУ кількість спостережень — без n крива порівнює
            # різні вибірки як одну (аудит v2.1 п.6)
            "curve_n": [len(c) for c in curve],
            # СПІЛЬНА когорта: медіани лише по угодах, що мають УСІ 60
            # хвилин — однакове n НЕ означає однакові події (аудит v2.3
            # п.8: m1 з одних угод проти m2 з інших — не порівняння);
            # тут кожна хвилина рахується з ТИХ САМИХ угод
            "curve_common": ([_median([c[i] for c in complete])
                              for i in range(REV_TRACK_MIN)]
                             if complete else None),
            "curve_common_n": len(complete),
            "trades": trades[-120:],
        }

    for st in list(FOLLOW_TIMERS) + [F6_NAME, F4_NAME, F5_NAME, F7_NAME]:
        rows = [r for r in fol if r.get("strategy") == st]
        rows.sort(key=lambda r: r.get("date_open") or "")
        nets, trades = [], []
        # профіль швидкості ГАМАНЦІВ цієї стратегії (v2.8): один запис
        # на гаманець (останній рядок), щоб агрегат не зважувався
        # кількістю угод одного й того ж кита
        prof_by_wallet = {}
        n_first = 0
        for r in rows:
            net = fnum(r.get("net_pct"))
            if net is not None: nets.append(net)
            fs = fnum(r.get("first_shot"))
            if fs == 1: n_first += 1
            pf = {"n_fast": fnum(r.get("prof_n_fast")),
                  "n_slow": fnum(r.get("prof_n_slow")),
                  "fast_pct": fnum(r.get("prof_fast_pct")),
                  "unload_med": fnum(r.get("prof_unload_med_s")),
                  "unload_mean": fnum(r.get("prof_unload_mean_s")),
                  "window_d": fnum(r.get("prof_window_d"))}
            if pf["n_fast"] is not None and r.get("whale_addr"):
                prof_by_wallet[r.get("whale_addr").lower()] = pf
            trades.append({"date": r.get("date_open"), "coin": r.get("coin"),
                "side": r.get("our_side"), "net30": net,
                "peak": fnum(r.get("peak_pct")),
                "trough": fnum(r.get("trough_pct")),
                "hold": fnum(r.get("hold_s")),
                "reason": r.get("exit_reason"),
                "tx_pct": fnum(r.get("tx_pct_of_pos")),
                "tx_usd": fnum(r.get("tx_usd")),
                "ratio": fnum(r.get("ratio")),
                "pos_usd": fnum(r.get("pos_usd")),
                "vault": fnum(r.get("vault"), 0),
                "hour": fnum(r.get("hour")),
                "btc": fnum(r.get("btc_move_pct")), "entered": 1,
                "first_shot": fs, "pair_gap": fnum(r.get("pair_gap_s")),
                "pf_fast": pf["n_fast"], "pf_slow": pf["n_slow"],
                "pf_pct": pf["fast_pct"], "pf_unload": pf["unload_med"],
                "pf_unload_mean": pf["unload_mean"],
                "wallet": (r.get("whale_addr") or "")[:10]})
        half = len(nets) // 2
        cum, acc = [], 0.0
        for v in nets:
            acc += v; cum.append(round(acc, 3))
        pw = list(prof_by_wallet.values())
        _pcts = [p["fast_pct"] for p in pw if p["fast_pct"] is not None]
        _unl = [p["unload_med"] for p in pw if p["unload_med"] is not None]
        _unm = [p["unload_mean"] for p in pw if p["unload_mean"] is not None]
        out["strategies"][st] = {
            "kind": "follow", "signals": len(rows), "entered": len(rows),
            "n": len(nets),
            "median": _median(nets),
            "mean": (sum(nets) / len(nets)) if nets else None,
            "win": (100.0 * sum(1 for v in nets if v > 0) / len(nets))
                if nets else None,
            "pnl_usd": round(sum(nets) * 10.0, 2) if nets else 0.0,
            "half1": _median(nets[:half]), "half2": _median(nets[half:]),
            "cum": cum, "curve": None,
            "trades": trades[-120:],
            # v2.8: скільки входів були «першим пострілом» пари (у F5 —
            # усі за побудовою; у F1–F4 — частка, порівняння когорт)
            "first_shot_n": n_first,
            # агрегат профілів швидкості по гаманцях цієї стратегії:
            # медіана частки швидких, медіана медіанного зливу і
            # середнього зливу, сума швидких/повільних епізодів
            "prof": {"wallets": len(pw),
                     "fast_pct_med": _median(_pcts),
                     "unload_med_s": _median(_unl),
                     "unload_mean_s": (sum(_unm) / len(_unm)) if _unm else None,
                     "n_fast": sum(int(p["n_fast"] or 0) for p in pw),
                     "n_slow": sum(int(p["n_slow"] or 0) for p in pw)},
        }

    with strat2_lock:
        # "відкрито зараз" = лише РЕАЛЬНІ paper-позиції: без тіней
        # (_OUTCOME), без озброєних R2 (входу ще немає) і без done
        out["open"] = (
            [{"strategy": p["strategy"], "coin": p["coin"], "state": "open",
              "age_s": round(now - p.get("entry_ts", p["detect_ts"]), 0)}
             for p in rev_open.values()
             if not p["strategy"].startswith("_") and not p.get("done")
             and p.get("state", "open") == "open"]
            + [{"strategy": p["strategy"], "coin": p["coin"], "state": "open",
                "age_s": round(now - p["open_ts"], 0)}
               for p in follow_open.values() if not p.get("done")])
        out["armed"] = sum(1 for p in rev_open.values()
                           if p.get("state") == "armed" and not p.get("done"))
        out["shadow_open"] = sum(1 for p in rev_open.values()
                                 if p["strategy"].startswith("_")
                                 and not p.get("done"))
        # ok = справді придатні для F4 ЗАРАЗ: ті самі умови, що й гейт
        # f4_ok (версія, без err/truncated, НЕ старіший за жорстку
        # стелю) — інакше хедер показує більше "придатних", ніж F4
        # реально допускає (аудити v2.3 п.9.5, v2.4 п.1)
        def _prof_ok(v, okf):
            return (isinstance(v, dict) and okf(v)
                    and v.get("v") == PROFILE_ALGO_V
                    and not v.get("err") and not v.get("truncated")
                    and now - v.get("fetched", 0) < PROFILE_HARD_TTL_S)
        out["profiles"] = {"total": len(wallet_profiles),
                           "ok": sum(1 for v in wallet_profiles.values()
                                     if _prof_ok(v, lambda p: p.get("ok"))),
                           # придатні для F7 (nr-гілка без ratio, v2.9)
                           "ok_nr": sum(1 for v in wallet_profiles.values()
                                        if _prof_ok(v, lambda p:
                                            (p.get("nr") or {}).get("ok")))}
    # лічильники сигналів — лише події >=1% (ті, що можуть відкривати
    # стратегії); суб-порогові 0.5-1% окремо (рев'ю v2.2); часткові
    # закриття (src=partial, лише тіньова стрічка) — теж окремо
    sig_ev = [s2 for s2 in sigs if s2.get("src") != "partial"]
    full_sigs = [s2 for s2 in sig_ev
                 if (fnum(s2.get("move_3m_pct")) or 0) >= 1.0]
    out["signals_total"] = len(full_sigs)
    out["signals_sub"] = len(sig_ev) - len(full_sigs)
    out["signals_partial"] = len(sigs) - len(sig_ev)
    # вето = виміряний BTC, що заборонив; відсутні котирування — окремо
    out["btc_veto"] = sum(1 for s2 in full_sigs
                          if fnum(s2.get("btc_move_pct")) is not None
                          and fnum(s2.get("btc_ok"), 1) == 0)
    out["btc_unknown"] = sum(1 for s2 in full_sigs
                             if fnum(s2.get("btc_move_pct")) is None)
    out["outcomes_total"] = len(outs)   # дедуп + поточна версія
    # ── Перші common-cohort порівняння на СПІЛЬНІЙ outcome-стрічці ──
    # (аудит v2.2: дані збирались, але саме порівняння ніхто не рахував).
    # Всі когорти — з ОДНОГО набору подій (m30 gross - costs), тому
    # "BTC ok vs veto" чи "штанга vs ні" порівнюються чесно. Це ще НЕ
    # тест гіпотез (без CI, min-n, holdout) — лише жива зведена таблиця.
    def _net30(r):
        v = fnum(r.get("m30"))
        c = fnum(r.get("costs_pct"), 0.15) or 0.15
        return (v - c) if v is not None else None
    def _cohort(rows2):
        vals = [x for x in (_net30(r) for r in rows2) if x is not None]
        return {"n": len(vals), "median": _median(vals)}
    def _mag(r):
        return fnum(r.get("move_3m_pct")) or 0
    # partial — ІНШИЙ тип події: у спільні когорти не мішаємо, рахуємо
    # окремою когортою (реверс після великого часткового зливу)
    outs_ev = [r for r in outs if r.get("src") != "partial"]
    full_out = [r for r in outs_ev if _mag(r) >= 1.0]
    # "вето" != "BTC не виміряли": btc_ok=0 ставиться і при відсутніх
    # котируваннях (fail-closed для входу), але в дослідженні це ТРИ
    # стани — PASS / VETO / UNKNOWN (аудит v2.3 п.6). Виміряність =
    # непорожній btc_move_pct у рядку.
    def _btc_measured(r):
        return fnum(r.get("btc_move_pct")) is not None
    out["research"] = {
        "btc_on":   _cohort([r for r in full_out if _btc_measured(r)
                             and fnum(r.get("btc_ok")) == 1]),
        "btc_off":  _cohort([r for r in full_out if _btc_measured(r)
                             and fnum(r.get("btc_ok")) == 0]),
        "btc_na":   _cohort([r for r in full_out
                             if not _btc_measured(r)]),
        "mag_05_1": _cohort([r for r in outs_ev if 0.5 <= _mag(r) < 1.0]),
        "mag_1_2":  _cohort([r for r in outs_ev if 1.0 <= _mag(r) < 2.0]),
        "mag_2p":   _cohort([r for r in outs_ev if _mag(r) >= 2.0]),
        "shtanga_1": _cohort([r for r in full_out
                              if fnum(r.get("shtanga"), 0) == 1]),
        "shtanga_0": _cohort([r for r in full_out
                              if fnum(r.get("shtanga"), 0) != 1]),
        "partial":  _cohort([r for r in outs
                             if r.get("src") == "partial"
                             and _mag(r) >= 1.0]),
    }
    # спільна крива FOLLOW-входів із тіньової стрічки: усі F-стратегії
    # входять на тих самих сигналах, тож крива в них одна на всіх
    # v2.8: окремо когорта ПЕРШИХ пострілів пари (first_shot=1) — це
    # власна крива F5 («яка хвилина виходу найкраща саме для неї»)
    fcurve = [[] for _ in range(REV_TRACK_MIN)]
    f1curve = [[] for _ in range(REV_TRACK_MIN)]
    fcomplete, f1complete, n_first = [], [], 0
    for r in fouts:
        fcosts = fnum(r.get("costs_pct"), 0.15) or 0.15
        is_first = fnum(r.get("first_shot")) == 1
        if is_first: n_first += 1
        row_vals = []
        for i in range(REV_TRACK_MIN):
            v = fnum(r.get(f"m{i+1}"))
            row_vals.append(v)
            if v is not None:
                fcurve[i].append(v - fcosts)
                if is_first: f1curve[i].append(v - fcosts)
        if row_vals and all(v is not None for v in row_vals):
            fcomplete.append([v - fcosts for v in row_vals])
            if is_first: f1complete.append([v - fcosts for v in row_vals])
    def _tape(n, curve, complete):
        return {"n": n,
                "curve": [(_median(c) if c else None) for c in curve],
                "curve_n": [len(c) for c in curve],
                "curve_common": ([_median([c[i] for c in complete])
                                  for i in range(REV_TRACK_MIN)]
                                 if complete else None),
                "curve_common_n": len(complete)}
    out["follow_tape"] = _tape(len(fouts), fcurve, fcomplete)
    out["follow_tape_first"] = _tape(n_first, f1curve, f1complete)
    _strat2_cache["data"] = out
    _strat2_cache["ts"] = now
    return out

def run_scan():
    global scan_number
    with cache_lock:
        if cache["scanning"]: return
        cache["scanning"] = True

    scan_number += 1
    scan_start = time.time()   # для merge у update_watchlist
    sn = scan_number
    print(f"\n  [SCAN #{sn}] Starting...")
    t0 = time.time()

    try:
        lb_wallets = load_leaderboard()

        # Додаємо WS-знайдені (яких немає в лідерборді).
        # Раніше тут стояв фільтр pos_count > 0, але pos_count для
        # WS-гаманців ніколи не оновлювався: extra завжди був порожній,
        # і кити поза лідербордом (свіжі гаманці) не сканувались ніколи.
        # Тепер беремо всіх: перший скан їх перевірить, а далі порожні
        # відсіє звичайний smart-skip (empty_streak).
        lb_addrs = {w["addr"].lower() for w in lb_wallets}

        # Хронічно порожні WS-гаманці викидаємо зовсім: якщо адреса
        # знову торгне, WS додасть її назад. Без цього ws_wallets
        # росте вічно і забиває скан мертвими адресами.
        with stats_lock:
            dead_ws = {a for a, s in wallet_stats.items()
                       if s.get("empty_streak", 0) >= SKIP_AFTER * 2}
        with ws_lock:
            for k in list(ws_wallets):
                if k in dead_ws and k not in lb_addrs:
                    del ws_wallets[k]
            extra = [w for k, w in ws_wallets.items() if k not in lb_addrs]
        if len(extra) > WS_EXTRA_MAX:
            # випадкова вибірка, а не "перші N": інакше новачки в кінці
            # черги могли б довго чекати за старими адресами
            print(f"  [SCAN] WS extra {len(extra)} > {WS_EXTRA_MAX}, "
                  f"беремо випадкові {WS_EXTRA_MAX}")
            extra = random.sample(extra, WS_EXTRA_MAX)
        all_wallets = lb_wallets + extra

        # Розділяємо: хто скипається, хто ні. VIP-виняток: топ-N за
        # accountValue (lb_wallets ВЖЕ відсортований за account desc)
        # сканується завжди — капітал важливіший за історію порожніх
        # сканів (аудит покриття 04.09)
        vip = {w["addr"].lower() for w in lb_wallets[:VIP_TOP_N]}
        to_scan   = []
        skipped   = []
        vip_kept  = 0
        for w in all_wallets:
            k = w["addr"].lower()
            if should_skip(k, sn):
                if k in vip:
                    vip_kept += 1
                    to_scan.append(w)
                else:
                    skipped.append(w)
            else:
                to_scan.append(w)

        total = len(to_scan)
        print(f"  [SCAN #{sn}] Total: {len(all_wallets)} | "
              f"Scan: {total} | Skipped (empty): {len(skipped)}"
              + (f" | VIP повернуто зі скіпу: {vip_kept}" if vip_kept else ""))
        print(f"  [SCAN #{sn}] ~{total*DELAY/WORKERS:.0f}s estimated")

        with cache_lock:
            cache["progress"] = {
                "done": 0, "total": total,
                "phase": f"scanning {total} wallets ({len(skipped)} skipped)",
                "skipped": len(skipped)
            }
            cache["scan_number"] = sn

        all_pos = {}
        failed_addrs = set()   # запит впав: стан невідомий, не "порожній"
        fetch_times  = {}      # addr -> коли скан реально зчитав гаманець

        def process(w):
            k = w["addr"].lower()
            # Час знімка — ДО запиту, але ПІСЛЯ fast_hold: біржа формує
            # стан під час обробки, і філ з вікна запит→відповідь не має
            # опинитись позаду курсора (втрачене закриття). Чекати hold
            # тут, а не в t, важливо: інакше при активному fast-path
            # вікно перехлесту розтягувалось на секунди, і вже
            # врахований у базі філ міг роздути епізод.
            # УВАГА: не t0 — зовнішній t0 це старт усього скану (ETA).
            # Стеля 10с: безперервні трейди не мають морозити скан вічно
            _hd = time.time() + 10
            while time.time() < fast_hold[0] and time.time() < _hd:
                time.sleep(0.2)
            _t_fetch = time.time()
            positions = fetch_one(w["addr"])
            if positions is None:
                # Помилка запиту: прогрес рухаємо, статистику не псуємо,
                # а адресу запам'ятовуємо — watchlist її не викине
                with cache_lock:
                    cache["progress"]["done"] += 1
                    failed_addrs.add(k)
                return
            fetch_times[k] = _t_fetch
            update_stats(k, bool(positions), sn)
            with cache_lock:
                if positions:
                    for pos in positions:
                        coin = pos["coin"]
                        if coin not in all_pos: all_pos[coin] = {}
                        all_pos[coin][k] = pos
                cache["progress"]["done"] += 1
                d = cache["progress"]["done"]
                if d % 200 == 0 or d == total:
                    elapsed = time.time() - t0
                    rate = d / elapsed if elapsed > 0 else 1
                    eta = (total - d) / rate if rate > 0 else 0
                    active = sum(1 for c in all_pos.values() for _ in c)
                    print(f"  [SCAN #{sn}] {d}/{total} | "
                          f"{rate:.0f} w/s | ETA {eta:.0f}s | pos: {active}")

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            list(pool.map(process, to_scan))

        result = {
            coin: sorted(m.values(), key=lambda p: p["val"], reverse=True)
            for coin, m in all_pos.items()
        }

        # Збагачуємо позиції ratio (val / depth_max)
        with cache_lock:
            depth_snap = dict(cache["depth"]) or dict(cache.get("depth_prev", {}))

        for coin, positions in result.items():
            d = depth_snap.get(coin)
            for pos in positions:
                ds = depth_for_side(d, pos["side"])
                if ds > 0:
                    pos["depth_max"] = ds
                    pos["ratio"]     = pos["val"] / ds
                else:
                    pos["depth_max"] = 0
                    pos["ratio"]     = 0

        # Оновлюємо real-time watchlist
        update_watchlist(result, depth_snap, scan_start, failed_addrs,
                         fetch_times)

        # Детекція закриття позицій (між сканами)
        alerts = check_position_changes(result, depth_snap)
        for a in alerts:
            alert_queue.put(a)
        if alerts:
            print(f"  [SCAN #{sn}] {len(alerts)} close alerts sent")

        # pos_count per wallet
        addr_count = {}
        for positions in result.values():
            for p in positions:
                addr_count[p["addr"].lower()] = addr_count.get(p["addr"].lower(), 0) + 1

        wallet_list = []
        for w in lb_wallets:
            wc = dict(w)
            wc["pos_count"] = addr_count.get(w["addr"].lower(), 0)
            wallet_list.append(wc)
        wallet_list.sort(key=lambda w: w.get("pos_count", 0), reverse=True)

        elapsed   = time.time() - t0
        total_pos = sum(len(v) for v in result.values())
        active_w  = len([k for k, v in addr_count.items() if v > 0])

        # Статистика скіпання
        with stats_lock:
            skip_stats = {
                "skipped_this_scan": len(skipped),
                "tracked":           len(wallet_stats),
                "chronic_empty":     sum(1 for s in wallet_stats.values()
                                        if s.get("empty_streak", 0) >= SKIP_AFTER),
            }

        print(f"  [SCAN #{sn}] Done in {elapsed:.1f}s | "
              f"{len(result)} coins | {total_pos} positions | "
              f"{active_w} active wallets | "
              f"skipped {skip_stats['chronic_empty']} chronic-empty")

        with cache_lock:
            cache["data"]       = result
            cache["wallets"]    = wallet_list
            cache["updated_at"] = time.time()
            cache["progress"]["phase"] = (
                f"done in {elapsed:.0f}s | "
                f"scanned {total} | skipped {len(skipped)} empty"
            )

    except Exception as e:
        print(f"  [SCAN #{sn}] ERROR: {e}")
        import traceback; traceback.print_exc()
    finally:
        with cache_lock:
            cache["scanning"] = False

    threading.Timer(REFRESH_S, lambda: threading.Thread(
        target=run_scan, daemon=True).start()).start()

# ── HTTP ─────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send_json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/positions":
            # Під локом — ЛИШЕ знімок посилань (аудит покриття 04.09:
            # json.dumps 2+МБ і запис у сокет під cache_lock тримали лок
            # сотні мс на повільному клієнті, а той самий лок бере
            # обробник трейдів — одна відкрита вкладка гальмувала
            # детекцію). cache["data"]/["depth"] замінюються цілком
            # (ніколи не мутуються на місці), тож серіалізація поза
            # локом безпечна; progress мутується — його копіюємо.
            with cache_lock:
                snap = {
                    "ready":      cache["data"] is not None,
                    "scanning":   cache["scanning"],
                    "progress":   dict(cache["progress"]),
                    "updated_at": cache["updated_at"],
                    "data":       cache["data"] or {},
                    "ws_discovered": cache["ws_discovered"],
                    "lb_total":   cache["lb_total"],
                    "scan_number": cache["scan_number"],
                    "depth":       cache["depth"],
                    "watchlist_size": len(watchlist),
                }
            self.send_json(snap)
        elif self.path == "/wallets":
            with cache_lock:
                snap = {
                    "wallets":    cache["wallets"],
                    "scanning":   cache["scanning"],
                    "lb_total":   cache["lb_total"],
                    "ws_discovered": cache["ws_discovered"],
                    "scan_number": cache["scan_number"],
                }
            self.send_json(snap)
        elif self.path == "/watchlist":
            with watchlist_lock:
                wl = []
                for addr, coins in watchlist.items():
                    for coin, p in coins.items():
                        wl.append({
                            "addr": addr, "coin": coin,
                            "val": p["val"], "side": p["side"],
                            "ratio": p["ratio"], "size": p["size"],
                        })
            wl.sort(key=lambda x: x["ratio"], reverse=True)
            with alerts_lock:
                al = list(recent_alerts)
            self.send_json({"watch": wl, "alerts": al})
        elif self.path == "/status":
            with watchlist_lock:
                wl_n = len(watchlist)
                wl_pos = sum(len(c) for c in watchlist.values())
            ws_age = _ws_age_s()
            now_ms = time.time() * 1000
            per_conn = {}
            for lb in ("A", "B"):
                last = stats.get(f"ws_last_{lb}", 0)
                per_conn[f"ws_{lb}_trade_sec_ago"] = round((now_ms - last)/1000, 1) if last else -1
                per_conn[f"ws_{lb}_subs"] = (f"{stats.get(f'ws_subs_{lb}', 0)}"
                                             f"/{stats.get(f'ws_expected_{lb}', 0)}")
                per_conn[f"ws_{lb}_fails"] = stats.get(f"ws_fails_{lb}", 0)
                per_conn[f"ws_{lb}_connects"] = stats.get(f"ws_connects_{lb}", 0)
                # форс-реконекти «протухлих» з'єднань (v2.8) і поточний
                # стрік без трейдів після них (0 = поріг базовий 3 хв)
                per_conn[f"ws_{lb}_stale_reconnects"] = \
                    stats.get(f"ws_stale_reconnects_{lb}", 0)
                per_conn[f"ws_{lb}_stale_streak"] = \
                    stats.get(f"ws_stale_streak_{lb}", 0)
            self.send_json({
                "uptime_min":     round((time.time() - stats["started"]) / 60, 1),
                "ws_alive":       ws_age < 60,   # свіжий ТРЕЙД, не pong
                "ws_trade_sec_ago": round(ws_age, 1),
                "ws_proxy":       bool(WS_PROXY),
                **per_conn,
                "watchlist_wallets": wl_n,
                "watchlist_positions": wl_pos,
                "ws_matched":     stats["ws_matched"],
                "checks":         stats["checks"],
                "delta_events":   stats["delta_events"],
                "fills_confirmed": stats["fills_confirmed"],
                "fills_empty":    stats["fills_empty"],
                "alerts_sent":    stats["alerts_sent"],
                "tg_errors":      stats.get("tg_errors", 0),
                "rate_limited":   stats["rate_limited"],
                "rev_active":     len(rev_open),
                "follow_active":  len(follow_open),
                "follow_busy_skips": stats.get("follow_busy_skips", 0),
                "profiles_queued": len(profiles_fetching),
                "prio_proxy":     bool(REST_PROXY),
                "prio_triggers":  prio_stats["triggers"],
                "prio_added":     prio_stats["added"],
                "prio_dropped":   prio_stats["dropped"],
                "prio_errors":    prio_stats["errors"],
            })
        elif self.path == "/sim":
            with sim_lock:
                open_list = [dict(p) for p in sim_positions.values()]
                closed    = list(sim_closed)
            self.send_json({"open": open_list, "closed": closed})
        elif self.path == "/strat2":
            try:
                self.send_json(strat2_api())
            except Exception as e:
                self.send_json({"error": str(e)})
        elif self.path == "/depth":
            with cache_lock:   # знімок під локом, send поза (як /positions)
                d = cache["depth"]
                snap = {"count": len(d), "coins": list(d.keys())[:20],
                        "sample": {k: d[k] for k in list(d.keys())[:3]}}
            self.send_json(snap)
        elif self.path in ("/", "/index.html"):
            with open(os.path.join(DIR, "hyperliquid-terminal.html"), "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        # /tg-update (webhook) видалено: він дублював polling і дозволяв
        # будь-кому, хто дістався порту, перенаправити алерти на свій chat_id
        if self.path == "/add-wallet":
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length))
            addr   = (body.get("address") or "").strip()
            # саме hex, а не будь-які 42 символи: інакше порт 3000 дозволяв
            # заливати сміттєві "адреси" у чергу сканування
            if __import__("re").fullmatch(r"0x[0-9a-fA-F]{40}", addr):
                CUSTOM_WALLETS.append(addr)
                add_ws_wallet(addr)
                print(f"  [CUSTOM] Added: {addr}")
                self.send_json({"ok": True})
            else:
                self.send_json({"ok": False}, 400)
        else:
            self.send_response(404); self.end_headers()

est = SCAN_TOP * DELAY / WORKERS
print(f"\n  HL TERMINAL  →  http://localhost:{PORT}")
print(f"  Full leaderboard scan (up to {SCAN_TOP}) | {WORKERS} workers | {DELAY}s delay")
print(f"  First scan ETA: ~{est:.0f}s | After warmup: much faster (empty-skip)")
print(f"  Skip logic: {SKIP_AFTER} empty scans → check every {CHECK_EVERY} "
      f"scans | VIP top-{VIP_TOP_N} за екваті — без скіпу\n")

# Depth — запускаємо ПЕРШИМ, паралельно зі скануванням
load_state()   # відновлюємо watchlist і сим-позиції з минулого запуску
threading.Thread(target=run_alert_sender, daemon=True).start()
threading.Thread(target=run_state_saver,  daemon=True).start()
threading.Thread(target=run_depth_loop,   daemon=True).start()
threading.Thread(target=run_scan,         daemon=True).start()
threading.Thread(target=run_websocket,    daemon=True).start()
threading.Thread(target=tg_poll_updates,     daemon=True).start()
threading.Thread(target=run_realtime_monitor, daemon=True).start()
threading.Thread(target=run_sim_loop,        daemon=True).start()
threading.Thread(target=run_fc_loop,         daemon=True).start()
threading.Thread(target=run_px_poller,       daemon=True).start()
threading.Thread(target=run_strat2_loop,     daemon=True).start()
threading.Thread(target=run_prio_fetcher,    daemon=True).start()
threading.Thread(target=_prio_probe,         daemon=True).start()
http.server.ThreadingHTTPServer(("", PORT), Handler).serve_forever()

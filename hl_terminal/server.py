import http.server
import urllib.request
import urllib.error
import urllib.parse
import json
import os
import time
import random
import threading
import socket
import ssl
import struct
import math
import glob
import queue
import calendar
import datetime as _dtmod
import html as _htmlmod
import re
import hashlib
import signal
import atexit
import shutil
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
    # v2.14 (аудит v2.13 №8e): переїжджає ПОВНИЙ набір даних — усі CSV
    # (включно з .legacy-ротаціями і twap_*), state.json та його .bak,
    # профілі, секрети — а не фіксований список, який відставав від коду
    # (twap_trades.csv і legacy лишались у старій папці й «зникали»)
    import glob as _glob
    _mig = {"state.json", "state.json.bak", "wallet_profiles.json",
            "tg_token.txt", "tg_chat.json", "ws_proxy.txt",
            "rest_proxy.txt", "scan_proxy.txt", "creds.json"}
    for _pat in ("*.csv", "state.json.corrupt-*"):
        _mig.update(os.path.basename(_p) for _p in _glob.glob(os.path.join(DIR, _pat)))
    _moved = 0
    for _fn in sorted(_mig):
        try:
            _src, _dst = os.path.join(DIR, _fn), os.path.join(DATA_DIR, _fn)
            if not os.path.exists(_src):
                continue
            if not os.path.exists(_dst):
                os.replace(_src, _dst)
                _moved += 1
                print(f"  [DATA] {_fn} -> {DATA_DIR}")
            elif _fn.endswith(".csv"):
                # однойменний CSV уже є в DATA_DIR (робота в різних папках,
                # аудит v2.14 №8c): рядки з папки коду не ховаємо — той самий
                # заголовок → дописуємо у файл даних, інший → кладемо поруч як
                # .legacy-migrated-<ts>.csv (читач legacy його підхопить);
                # оригінал лишається в папці коду під .migrated-<ts>
                with open(_src, "rb") as _fs:
                    _s_hdr = _fs.readline()
                    _s_rest = _fs.read()
                with open(_dst, "rb") as _fd:
                    _d_hdr = _fd.readline()
                    _fd.seek(0, 2)
                    _d_size = _fd.tell()
                    _d_tail = b""
                    if _d_size:
                        _fd.seek(_d_size - 1)
                        _d_tail = _fd.read(1)
                _ts = int(time.time())
                if _s_hdr.strip() == _d_hdr.strip() and _s_rest.strip():
                    with open(_dst, "ab") as _fd:
                        if _d_tail not in (b"\n", b""):
                            _fd.write(b"\n")
                        _fd.write(_s_rest)
                    _n_rows = _s_rest.count(b"\n")
                    print(f"  [DATA] {_fn}: дописано {_n_rows} рядк. з папки коду")
                elif _s_rest.strip():
                    _leg = os.path.join(DATA_DIR, f"{_fn}.legacy-migrated-{_ts}.csv")
                    shutil.copy2(_src, _leg)
                    print(f"  [DATA] {_fn}: інший заголовок -> {os.path.basename(_leg)}")
                os.replace(_src, f"{_src}.migrated-{_ts}")
                _moved += 1
        except Exception as _e:
            print(f"  [DATA] migrate {_fn}: {_e}")
    if _moved:
        _left = [os.path.basename(_p) for _p in _glob.glob(os.path.join(DIR, "*.csv"))]
        print(f"  [DATA] перенесено {_moved} файл(ів); CSV у старій папці "
              f"лишилось: {len(_left)}")
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

# Проксі для СКАНУ лідерборда (аналіз 08.09): скан — фоновий перепис
# 13-48k гаманців, йому байдужа затримка, а обхіднику (sweep) і
# fast-перевіркам на основній IP — ні. На одній IP скан давав 6 гам/с
# (поступався fast-перевіркам і ділив ліміт з обхідником: 793 запити/хв
# разом), цикл замість 30 хв тягнувся 60 хв — 4.6 год, і нові кити
# з'являлись у watchlist із таким самим лагом. Окрема IP = окремий ліміт
# 1200 ваги/хв: clearinghouseState = 2 → стеля ~10 гам/с; бюджет
# SCAN_W_PER_MIN лишає запас. env SCAN_PROXY або файл scan_proxy.txt.
SCAN_PROXY = _norm_proxy(os.environ.get("SCAN_PROXY", ""))
if not SCAN_PROXY:
    try:
        with open(os.path.join(DATA_DIR, "scan_proxy.txt")) as _f:
            SCAN_PROXY = _norm_proxy(_f.read())
    except Exception:
        pass
SCAN_W_PER_MIN  = 1000   # вага/хв на проксі скану (кап біржі 1200)
SCAN_DEAD_S     = 1800   # мертва проксі: 30 хв напряму зі старим тротлінгом

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
    "KLUNC":  "1000LUNCUSDT",   # v2.16 (рев'ю): не мапилась — без глибини
    "KDOGS":  "DOGSUSDT",       # глибина в USD, префікс одиниць неважливий
}
DEPTH_PCT  = 0.01   # 1% від best price
DEPTH_LIMIT = 1000  # рівнів стакану (v2.16: 500 обрізали 1% для HYPE/ZEC
                    # — глибина занижена ~1.5–2×, ratio роздутий); вага 20

def get_bn_symbol(coin):
    return SYMBOL_MAP.get(coin.upper(), coin.upper() + "USDT")

_bn_backoff = [0.0]        # v2.16 (рев'ю): після 429/418 Binance — пауза до цього часу
_depth_universe = [set()]  # монети, що ЗАРАЗ торгуються на Binance (для чистки кешу)

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
            # аудит v2.16: чи вмістився весь 1%-діапазон у DEPTH_LIMIT рівнів —
            # якщо останній рівень ще всередині діапазону, глибина занижена
            trunc = int(float(asks[-1][0]) <= best_ask * (1 + DEPTH_PCT)
                        or float(bids[-1][0]) >= best_bid * (1 - DEPTH_PCT))
            if trunc:
                stats["depth_trunc"] = stats.get("depth_trunc", 0) + 1
            return {"ask": ask_depth, "bid": bid_depth, "max": max(ask_depth, bid_depth),
                    "trunc": trunc, "ts": time.time()}
        except Exception as e:
            code = getattr(e, "code", None)
            if code in (418, 429):
                # рев'ю v2.16: негайний ретрай після 429 веде до 418-бану IP
                # (хвилини…дні) — пауза за Retry-After (мін. 30 с), решта
                # циклу глибини пропускається, кеш тримає старі значення
                try:
                    ra = float((getattr(e, "headers", None) or {}).get("Retry-After") or 0)
                except (TypeError, ValueError, AttributeError):
                    ra = 0.0
                _bn_backoff[0] = time.time() + max(30.0, ra)
                print(f"  [DEPTH] Binance {code} на {symbol}: пауза {max(30.0, ra):.0f}с")
                return None
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
    # v2.16 (рев'ю): делістнуті монети лишаються в allMids із замороженою
    # ціною, яка виглядає свіжою — тримаємо їх список для поллера цін
    _hl_delisted.clear()
    _hl_delisted.update(u["name"] for u in data[0]["universe"]
                        if u.get("isDelisted") and u.get("name"))
    return [u["name"] for u in data[0]["universe"]
            if "/" not in u.get("name","") and not u.get("isDelisted")]

_hl_delisted = set()   # назви делістнутих perp-монет (оновлює fetch_hl_coins_list)

def fetch_binance_symbols_set():
    req = urllib.request.Request(
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        headers={"User-Agent":"Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        info = json.loads(r.read())
    return {s["symbol"] for s in info.get("symbols",[])
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT"
            # v2.16 (рев'ю): делістнутий символ повертає порожню книгу →
            # None → у кеші назавжди лишалась стара глибина
            and s.get("status") == "TRADING"}

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
    if bn_symbols:
        _depth_universe[0] = set(ok_coins)
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
        if time.time() < _bn_backoff[0]:
            print(f"  [DEPTH] пауза після 429/418 ще "
                  f"{_bn_backoff[0] - time.time():.0f}с — решта монет тримає стару глибину")
            break
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(fetch_one_depth, batch))
        done = min(i+2, total)
        pct  = done/total*100
        if done % 20 == 0 or done == total:
            print(f"  [DEPTH] {done}/{total} ({pct:.0f}%) | got: {len(depth_map)} | errors: {len(errors)}")
        if i+2 < total:
            # v2.16: DEPTH_LIMIT 1000 = вага 20 за запит (було 10 при 500);
            # 2 запити / 1.5 с ≈ 1600 ваги/хв < ліміт 2400 із запасом
            time.sleep(1.5)

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
                # v2.16 (рев'ю): монета, що вибула з Binance-всесвіту
                # (делістинг / статус не TRADING), не тримає стару глибину
                # вічно — інакше ratio рахувався проти мертвої ліквідності
                _uni = _depth_universe[0]
                if len(_uni) >= 10:
                    for _c in [c for c in merged if c not in _uni]:
                        merged.pop(_c, None)
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

def hl_post_prio(body, retries=2, direct=False, max_wait=65.0, w_next=None):
    """hl_post пріоритетного каналу: через REST_PROXY, щоб перевірки
    невідомих китів не їли ліміт основної IP. direct=True (або без
    проксі) — прямий запит; викликач тоді сам тримає жорсткіший кап
    PRIO_DIRECT_PER_MIN. v2.14 (аудит v2.13): СПІЛЬНИЙ ваговий бюджет
    каналу (prio-перевірки, профілі, TWAP) — облік тут, в одному місці;
    max_wait — стеля очікування бюджету (TWAP перед входом чекати не
    може, профіль — може)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "Mozilla/5.0"},
        method="POST")
    via_proxy = not (direct or not _prio_opener)
    opener = _prio_opener.open if via_proxy else urllib.request.urlopen
    via = "proxy" if via_proxy else "direct"
    # резерв до запиту: очікувана вага (профіль передає повну сторінку
    # 120 — рев'ю v2.14: інакше шість сторінок поспіль проходили стелю)
    w_ = w_next if w_next is not None else _hl_weight(body)
    if w_ <= 2:
        max_wait = min(max_wait, 3.0)   # легкий запит prio-воркера: не клінчити
    last_was_429 = False
    last_err = ""
    for attempt in range(retries):
        if not _profile_budget_wait(via, w_, max_wait=max_wait):
            # бюджет вікна вичерпано і за max_wait не звільнився: явна
            # відмова, а не запит понад ліміт (аудит v2.14 №6)
            raise RateLimited(f"prio budget ({via}) exhausted")
        try:
            with opener(req, timeout=10) as r:
                out = json.loads(r.read())
            _profile_budget_add(via, out, body, reserved=w_)
            return out
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            last_was_429 = e.code == 429
            if last_was_429:
                # справжній 429 біржі (IP перевищив ліміт): штраф у півстелі
                # вікна, як у каналі скану — пейсер відступає, а не далі
                # шле «в межах» власного обліку (рев'ю v2.15)
                with _profile_w_lock:
                    _profile_w[via].append((time.time(), PROFILE_W_PER_MIN[via] // 2))
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

# ── Канал СКАНУ: окрема проксі + власний бюджет ваги (v2.11 п.7) ──
_scan_opener = None
if SCAN_PROXY:
    _scan_opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"https": "http://" + SCAN_PROXY,
                                     "http":  "http://" + SCAN_PROXY}))
_scan_state = {"dead_until": 0.0, "streak": 0, "req": 0, "err": 0,
               "rl": 0, "fallback": 0, "last_ok": 0.0}
_scan_w      = deque()          # (ts, вага) за останні 60с на проксі
_scan_w_lock = threading.Lock()

def _scan_via_proxy():
    """Чи йде скан зараз через свою проксі (є і не визнана мертвою)."""
    return bool(_scan_opener) and time.time() >= _scan_state["dead_until"]

def _scan_budget_wait(w=2):
    """Пейсер каналу скану: спати, доки вага останніх 60с + w не
    вкладеться у SCAN_W_PER_MIN. Це і є весь тротлінг скану на проксі —
    DELAY/fast_hold основної IP тут ні до чого. Стеля очікування ~65с
    (одне вікно), далі відпускаємо — краще один 429, ніж клінч."""
    deadline = time.time() + 65
    while True:
        now_ = time.time()
        with _scan_w_lock:
            while _scan_w and _scan_w[0][0] < now_ - 60:
                _scan_w.popleft()
            used = sum(x for _, x in _scan_w)
            if used + w <= SCAN_W_PER_MIN or now_ >= deadline:
                _scan_w.append((now_, w))
                return
            oldest = _scan_w[0][0]
        time.sleep(min(2.0, max(0.05, oldest + 60 - now_)))

def hl_post_scan(body, retries=2):
    """clearinghouseState скану через SCAN_PROXY. Мережевий збій 3
    поспіль → проксі мертва на SCAN_DEAD_S (fetch_one сам повертається
    на основну IP зі старим тротлінгом, потім пробує знову); 429 →
    RateLimited + штраф у бюджет (пів вікна), щоб пейсер відступив."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "Mozilla/5.0"},
        method="POST")
    last_err = ""
    last_was_429 = False
    for attempt in range(retries):
        if attempt:
            _scan_budget_wait(2)   # повтор — теж вага 2 (рев'ю v2.12 №8b)
        try:
            with _scan_opener.open(req, timeout=12) as r:
                out = json.loads(r.read())
            _scan_state["req"] += 1
            _scan_state["streak"] = 0
            _scan_state["last_ok"] = time.time()
            return out
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            last_was_429 = e.code == 429
            if last_was_429:
                _scan_state["rl"] += 1
                with _scan_w_lock:
                    _scan_w.append((time.time(), SCAN_W_PER_MIN // 2))
            if attempt < retries - 1:
                time.sleep(2 ** attempt if last_was_429 else 1)
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            last_was_429 = False
            if attempt < retries - 1:
                time.sleep(1)
    _scan_state["err"] += 1
    if last_was_429:
        raise RateLimited("scan-proxy 429 after retries")
    now_ = time.time()
    if now_ < _scan_state["dead_until"]:
        # воркери, що вже були в польоті, коли проксі визнали мертвою —
        # не накручуємо streak/fallback повторно
        raise APIError(f"scan-proxy dead ({last_err})")
    _scan_state["streak"] += 1
    # смерть — лише коли 3 збої поспіль І понад 20с без жодної успішної
    # відповіді: паралельні воркери падають пачкою за одну мить, і без
    # цієї умови один мережевий чих ховав проксі на пів години
    if (_scan_state["streak"] >= 3
            and now_ - _scan_state["last_ok"] > 20):
        _scan_state["dead_until"] = now_ + SCAN_DEAD_S
        _scan_state["fallback"] += 1
        _scan_state["streak"] = 0
        print(f"  [SCAN] проксі скану мертва ({last_err}) — {SCAN_DEAD_S // 60} хв "
              f"напряму зі старим тротлінгом")
    raise APIError(f"scan-proxy max retries ({last_err})")

def _scan_probe():
    """Разова проба проксі скану на старті — щоб у лозі одразу було
    видно, яким каналом піде перший скан."""
    if not SCAN_PROXY:
        print("  [SCAN] scan_proxy.txt немає — скан іде основною IP "
              f"({WORKERS} воркерів, DELAY {DELAY}s, поступається fast-перевіркам)")
        return
    try:
        r = hl_post_scan({"type": "allMids"}, retries=1)
        ok = isinstance(r, dict) and r
        print(f"  [SCAN] проксі скану {SCAN_PROXY.split('@')[-1]}: "
              f"{'OK' if ok else 'відповідь дивна: ' + str(r)[:80]} | "
              f"бюджет {SCAN_W_PER_MIN} ваги/хв (~{SCAN_W_PER_MIN // 120} гам/с)")
    except Exception as e:
        print(f"  [SCAN] проксі скану НЕ відповідає: {e} — перший скан "
              f"піде основною IP")

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
    # list-відповідь ламала lb.get(...) ДО перевірки isinstance, а
    # порожня — top[0] нижче (аудит v2.10): обидва випадки тепер чесна
    # APIError -> run_scan пропускає цикл, watchlist переживає (carry)
    if isinstance(lb, list):
        rows = lb
    elif isinstance(lb, dict):
        rows = lb.get("leaderboardRows", [])
    else:
        rows = []

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
    if not top:
        raise APIError("порожній лідерборд — пропускаю цикл скану")
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
        if _scan_via_proxy():
            # своя IP: без DELAY і без поступання fast-перевіркам (вони на
            # іншій IP; v2.11 п.7). Пейсер бюджету — у process() ДО
            # знімка часу, інакше _t_fetch випереджав би запит на ~хвилину
            data = hl_post_scan({"type": "clearinghouseState",
                                 "user": addr_str})
        else:
            # Кит щойно торгнув: пропускаємо fast-перевірку вперед. Але
            # зі СТЕЛЕЮ: кожен трейд відсуває hold ще на 3с, і при
            # безперервному потоці скан чекав би необмежено
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

        if streak < SKIP_AFTER:
            return False
        # Перевіряємо раз на CHECK_EVERY сканів, але РОЗПОДІЛЕНО: слот
        # гаманця = хеш адреси mod CHECK_EVERY, тож кожен скан
        # перевіряє свою п'яту частину хронічно порожніх, а не всі
        # 40k разом кожен п'ятий скан (v2.12, з рев'ю CH: той «п'ятий»
        # скан тривав удвічі довше і забивав бюджет проксі)
        slot = int(hashlib.sha256(addr_lower.encode()).hexdigest()[:8], 16) \
            % CHECK_EVERY
        return scan_num % CHECK_EVERY != slot

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
# v2.16 (рев'ю): швидкий шлях побачив дельту, а філів в API ще нема →
# повторна швидка перевірка через FAST_RETRY_S (не чекати sweep 20–30 с);
# кап спроб на пару, лічильник скидається, коли філи знайдено
FAST_RETRY_S   = 1.5
FAST_RETRY_MAX = 3
FAST_SWEEP_CHUNK = 20  # обхід — чанками; між ними обробляються швидкі події
fast_retry     = {}   # (addr, coin) -> ts, коли повторити
fast_retry_cnt = {}   # (addr, coin) -> зроблено повторів
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
    # Грейс (v2.11 п.1): мітка «коли востаннє ratio≥2» живих пар — щоб
    # пара, яка просіла нижче 2 менш як пів години тому, НЕ випадала зі
    # скану, а лишалась у watchlist із успадкованою міткою. Без цього
    # повний скан обнуляв грейс, який sweep/realtime щойно дали.
    _now_wl = time.time()
    with watchlist_lock:
        _live_hi = {(_a, _c): (_p.get("ratio_hi_ts") or 0)
                    for _a, _cs in watchlist.items()
                    for _c, _p in _cs.items()}
        _live_big = {(_a, _c): float(_p.get("big_val") or 0)
                     for _a, _cs in watchlist.items()
                     for _c, _p in _cs.items()}
    new_wl = {}
    for coin, positions in result.items():
        if coin.upper() in COIN_BLACKLIST:
            continue
        d = depth_snap.get(coin)
        if not _depth_ok(d): d = None      # обрізана/стара глибина — ratio 0 (не кваліфікує)
        for pos in positions:
            if not pos.get("size"): continue
            ds = depth_for_side(d, pos["side"])
            ratio = pos["val"] / ds if ds else 0
            addr = pos["addr"].lower()
            if ratio >= 2.0:
                # мітка = момент, коли скан РЕАЛЬНО зчитав гаманець, а не
                # старт скану (він може бути на 20+ хв раніше і з'їдав
                # би грейс наперед)
                hi_ts = (fetch_times or {}).get(addr, scan_start) or _now_wl
                big_val = pos["val"]
            else:
                hi_ts = _live_hi.get((addr, coin), 0)
                if _now_wl - hi_ts >= RATIO_GRACE_S:
                    continue  # watchlist: ratio >= 2 або грейс після нього
                big_val = _live_big.get((addr, coin), 0)
            if addr not in new_wl: new_wl[addr] = {}
            new_wl[addr][coin] = {
                "size":  pos["size"],
                "val":   pos["val"],
                "side":  pos["side"],
                "ratio": ratio,
                "ratio_hi_ts": hi_ts, "big_val": big_val,
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
                # мітка грейсу — свіжіша з двох джерел (realtime міг
                # позначити ratio≥2 уже після знімка скану)
                _hi = max(_p.get("ratio_hi_ts") or 0,
                          lv.get("ratio_hi_ts") or 0)
                _big = max(float(_p.get("big_val") or 0),
                           float(lv.get("big_val") or 0))
                _p["ratio_hi_ts"] = _hi
                _p["big_val"] = _big
                if lv.get("upd", 0) >= _ft and lv.get("upd", 0) >= scan_start > 0:
                    _coins[_c] = dict(lv)
                    _coins[_c]["ratio_hi_ts"] = _hi
                    _coins[_c]["big_val"] = _big
        watchlist.clear()
        watchlist.update(new_wl)
    print(f"  [WATCH] Watchlist updated: {len(new_wl)} wallets, "
          f"{sum(len(v) for v in new_wl.values())} positions with ratio>=2x"
          + (f" | {carried_unscanned} пар carried (не скановані/помилки)"
             if carried_unscanned else ""))

scan_metrics = {"next_scan_at": 0.0, "last_duration_s": 0.0, "new_pairs": 0}
_scan_timer = None
_scan_sched_lock = threading.Lock()

def _discover_pairs(addr, positions, fetched_at):
    """v2.12 (з рев'ю CH): нова велика пара гаманець:монета потрапляє у
    watchlist одразу, як скан зчитав гаманець — раніше вона чекала
    update_watchlist у кінці скану (до 90 хв сліпоти по щойно
    відкритій позиції; prio-fetcher ловить лише тих, хто торгує через
    WS у цей час). Наявних пар не чіпаємо (їх веде realtime/sweep і
    злиття в кінці скану); tombstone після часу читання — не воскрешаємо.
    Курсор філів — на момент знімка, як для нової пари у
    update_watchlist."""
    with cache_lock:
        depth = cache["depth"] or cache.get("depth_prev", {})
        depth = dict(depth) if depth else {}
    if len(depth) < 10:
        return 0
    added = 0
    for p in positions:
        coin = p.get("coin")
        if not coin or not p.get("size") or coin.upper() in COIN_BLACKLIST:
            continue
        _dd = depth.get(coin)
        ds = depth_for_side(_dd, p["side"]) if _depth_ok(_dd) else 0
        ratio = p["val"] / ds if ds else 0
        if ratio < 2.0:
            continue
        k = f"{addr}:{coin}"
        with watchlist_lock:
            if coin in watchlist.get(addr, {}):
                continue
            if scan_tombstones.get(k, 0) >= fetched_at:
                continue
            watchlist.setdefault(addr, {})[coin] = {
                "size": p["size"], "val": p["val"], "side": p["side"],
                "ratio": ratio, "ratio_hi_ts": fetched_at, "big_val": p["val"],
                "entry": p.get("entry", 0), "liq": p.get("liq", 0),
                "upd": fetched_at}
            sent_alerts.discard(k)
            close_episodes.pop(k, None)
            delta_seen.pop(k, None)
            fill_cursor[k] = max(fill_cursor.get(k, 0), int(fetched_at * 1000))
        added += 1
    if added:
        scan_metrics["new_pairs"] += added
    return added

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
            except RateLimited as e:
                # v2.15 (аудит v2.14 №6): бюджет вікна вичерпано (або 429
                # після ретраїв) — запиту понад ліміт НЕ було. Це не «мертва
                # проксі» і не 10-хв глухота: адреса у короткий кулдаун ~75с
                # (вікно рефілу), як і при переповненні прямого каналу
                prio_stats["dropped"] += 1
                with _prio_lock:
                    _prio_seen[addr] = time.time() - PRIO_COOLDOWN_S + 75
                # у журналі розрізняємо власний бюджет і справжній 429 біржі
                _prio_log(addr, coin0, pos_side, notional, depth0, thr,
                          ("429" if "429" in str(e) else "budget"), 0, "", via_proxy)
                continue
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
                        "ratio_hi_ts": time.time(), "big_val": p["val"],
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
        # ADL ("Auto-Deleveraging") класифікується як ліквідація — ЯК у
        # профільному модулі: інакше ADL-закриття було видно в історії
        # гаманця, але не в основному детекторі (аудит v2.10 №5в)
        f_liq = (bool(f.get("liquidation")) or ("Liquidat" in d)
                 or d.startswith("Auto-Delever"))
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
        # СИСТЕМНІ філи (ліквідації/ADL) несуть нульовий hash 0x000…0:
        # групування по ньому склеювало РІЗНІ ліквідації в одну фальшиву
        # «велику транзакцію» (2×3% -> 6% алерт), а глобальний дедуп
        # alerted_txs по цьому hash гасив УСІ наступні ліквідації будь-якої
        # монети на годину (аудит v2.10 №5; дзеркало фікса v2.8 у
        # профільному модулі) — стабільний штучний id по oid/tid+гаманець
        if str(h).lower().strip("0x") == "" or f_liq:
            h = (f"sys:{addr[:10]}:{coin}:"
                 f"{oid if oid is not None else f.get('tid', f.get('time', 0))}")
        px, sz = _vpx, _vsz   # уже валідовані finite > 0
        # startPosition — позиція НА МОМЕНТ цього філа за даними біржі:
        # чесна база для порогів «≥5% позиції» незалежно від нарізки
        # батчів (аудит v2.10 №7); для групи — максимум (позиція перед
        # першим філом ордера)
        try:
            _spf = abs(float(f.get("startPosition") or 0))
        except (TypeError, ValueError):
            _spf = 0.0
        if not math.isfinite(_spf):
            _spf = 0.0
        if h not in txs:
            txs[h] = {
                "hash": h, "sz": 0.0, "cost": 0.0,
                "ts": f.get("time", 0), "dir": f.get("dir", ""),
                "oids": set(), "liq": False, "liq_method": "", "sp": 0.0,
                # аудит v2.16 №6: VWAP ≠ кінець виконання — тримаємо СИРІ
                # ціни першого й останнього філа ордера (у межах однієї мс
                # порядок = прохід по стакану: продаж іде вниз по бідах)
                "px_first": px, "px_last": px,
                "t_first": f.get("time", 0), "t_last": f.get("time", 0),
            }
        t = txs[h]
        t["sz"]   += sz
        t["cost"] += px * sz
        t["sp"]   = max(t.get("sp", 0.0), _spf)
        _ft = f.get("time", 0)
        _sell = d.startswith("Close Long") or d.startswith("Long >") or (
            side == "LONG" and f_liq)
        if _ft < t["t_first"] or (_ft == t["t_first"]
                                  and (px > t["px_first"] if _sell else px < t["px_first"])):
            t["t_first"], t["px_first"] = _ft, px
        if _ft > t["t_last"] or (_ft == t["t_last"]
                                 and (px < t["px_last"] if _sell else px > t["px_last"])):
            t["t_last"], t["px_last"] = _ft, px
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
            "sp":      t.get("sp", 0.0),      # позиція перед транзакцією
            "ts":      t["ts"],               # (біржова, 0 = невідома)
            "dir":     t["dir"],
            "n_orders": len(t["oids"]),
            "liq":     t["liq"],
            "liq_method": t["liq_method"],
            "px_first": t["px_first"],        # сирий перший філ ордера
            "px_last":  t["px_last"],         # сирий останній філ (кінець проходу)
        })
    market_txs.sort(key=lambda x: x["ts"], reverse=True)
    return market_txs

def _depth_ok(d):
    """Глибина придатна як база ratio: є, не старша за 2 цикли і 1%-діапазон
    не обрізаний (аудит-3: 1000 рівнів не вмістили → глибина занижена,
    ratio завищений — не база для відбору)."""
    if not d:
        return False
    if d.get("ts") and time.time() - d["ts"] > 2 * REFRESH_S:
        stats["ratio_stale_depth"] = stats.get("ratio_stale_depth", 0) + 1
        return False
    if d.get("trunc"):
        stats["ratio_trunc_depth"] = stats.get("ratio_trunc_depth", 0) + 1
        return False
    return True

def _fresh_ratio(coin, side, val):
    """ratio позиції за СВІЖОЮ глибиною кешу; None — придатної глибини немає
    (нема / стара / обрізана): тоді ratio пари НЕ оновлюється — лишається
    останній, порахований за придатною глибиною (свідомо: під час збою
    глибини старий ratio кращий за «невідомо», гейт ratio не вимикається).
    Аудит v2.10 №4: часткове закриття оновлювало size/val, а ratio
    лишався від скану — пара, що впала нижче 2, далі проходила гейт
    «ratio на момент алерту» і труїла CSV/когорти."""
    with cache_lock:
        d = cache["depth"].get(coin) or cache.get("depth_prev", {}).get(coin)
    if not _depth_ok(d):
        return None
    ds = depth_for_side(d, side)
    return (val / ds) if ds else None

# ── ГРЕЙС-ВІКНО ratio (ТЗ 08.09 п.1) ──
# Кит із ratio 2.5 закриває половину: залишок має ratio 1.25, і за
# правилом «ratio ≥2 на момент алерту» друга половина зникала б і з
# Telegram, і зі статистики — а це те саме розвантаження. Тому пара,
# що БУЛА великою, ще RATIO_GRACE_S після падіння нижче 2 гейтиться як
# велика (усі закриття ≥5% алертяться/пишуться). Не встиг закрити за
# пів години — пара знову «мала»: тиша, доки ratio не повернеться ≥2
# (свіжа глибина/ціна) — тоді мітка ratio_hi_ts оновиться.
RATIO_GRACE_S = 1800

def _mark_ratio(p, r, now=None):
    """Єдина точка запису ratio у запис пари: ≥2 оновлює мітку
    ratio_hi_ts (востаннє бачили велику) і big_val — вартість позиції в
    той момент («якір» для порогу $50k у грейсі: залишок $30k після
    зливу $200k — та сама подія, не пил; v2.12 з рев'ю CH). Усі місця,
    де ratio пишеться у watchlist, ідуть сюди — інакше грейс не знав би,
    коли пара востаннє була великою."""
    p["ratio"] = r
    if r >= 2.0:
        p["ratio_hi_ts"] = now if now is not None else time.time()
        p["big_val"] = float(p.get("val") or 0)
    return p

def _mark_close(p, ts=None):
    """v2.12 (рішення користувача 08.09): грейс рахується від ОСТАННЬОГО
    великого закриття (≥5% позиції маркетом), а не від останнього
    спостереження ratio≥2. Кожне підтверджене велике закриття пари, що
    на той момент проходила гейт, зсуває мітку вперед: серія «злив 50%
    → ratio 1.3 → через 25 хв ще 10%» лишається живою від останнього
    шматка. Мітка лише рухається вперед."""
    t = ts if ts is not None else time.time()
    if t > (p.get("ratio_hi_ts") or 0):
        p["ratio_hi_ts"] = t
    return p

def _grace_val(p):
    """Розмір події для порогу MIN_POS_USD: у грейсі (ratio<2) — не
    залишок, а найбільше з залишку і «якоря» big_val (вартість, коли
    пара востаннє була ≥2)."""
    v = float(p.get("val") or 0)
    if (p.get("ratio") or 0) < 2.0:
        v = max(v, float(p.get("big_val") or 0))
    return v

def _ratio_ok(p, now=None):
    """Гейт «велика відносно ліквідності»: ratio ≥2 АБО мітка ratio_hi_ts
    (останнє спостереження ≥2 чи останнє велике закриття) молодша за
    RATIO_GRACE_S. Це ЄДИНИЙ гейт для алертів (обидва шляхи), rev і
    follow — раніше в кожному стояв голий `ratio < 2`."""
    if (p.get("ratio") or 0) >= 2.0:
        return True
    return ((now if now is not None else time.time())
            - (p.get("ratio_hi_ts") or 0)) < RATIO_GRACE_S

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
            "ratio_hi_ts": time.time(), "big_val": pos["val"],
            "entry": pos.get("entry", 0),
            "liq":   pos.get("liq", 0),
            "upd":   time.time(),
        }
    # нова позиція = нова історія: дедуп і епізод старого боку скидаємо,
    # курсор — на момент ЗНІМКА нової позиції (не "зараз": філ, що
    # прилетів після знімка, не має опинитись позаду курсора)
    sent_alerts.discard(alert_key)
    close_episodes.pop(alert_key, None)
    with fc_lock:   # аудит v2.16 №7: епізод старого боку не належить новому
        fc_episodes.pop((addr, coin), None)
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
            """Перевіряє один гаманець. Викликається паралельно.
            item = (addr, coins) або (addr, coins, detect_src) — v2.16:
            джерело детекції ("ws" швидкий шлях / "sweep" обхід) їде у
            рядки стратегій."""
            addr, coins = item[0], item[1]
            det_src = item[2] if len(item) > 2 else "sweep"
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
                                    _mark_ratio(_w, new_pos["val"] / _ds)
                                if delta_size < 0:
                                    _w["size"] = new_size
                                    _w["upd"]  = time.time()
                        if delta_size < 0:
                            # Долив обриває епізод розвантаження: інакше
                            # відсотки рахувались би від старої, меншої
                            # бази і поріг 5% спрацьовував би зарано
                            close_episodes.pop(alert_key, None)
                            with fc_lock:   # аудит v2.16 №7: і епізод реверсу
                                fc_episodes.pop((addr, coin), None)
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
                                with fc_lock:   # перевідкриття = нова позиція
                                    fc_episodes.pop((addr, coin), None)
                                # v2.12 (з рев'ю CH): перевідкрита позиція
                                # — нова історія, старий грейс/якір їй не
                                # належать: мітка лише якщо ЗАРАЗ ≥2
                                with watchlist_lock:
                                    _wr = watchlist.get(addr, {}).get(coin)
                                    if _wr is not None:
                                        _wr.pop("ratio_hi_ts", None)
                                        _wr.pop("big_val", None)
                                        _mark_ratio(_wr, _wr.get("ratio") or 0)
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
                        if det_src == "ws":
                            # v2.16 (рев'ю): філи з'являються в API на ~1 с
                            # пізніше за трейд у WS — повторна ШВИДКА
                            # перевірка через FAST_RETRY_S, а не чекання
                            # sweep (20–30 с; це і був «стеарий філ» у
                            # рядках). Кап FAST_RETRY_MAX спроб на пару
                            with fast_lock:
                                _n = fast_retry_cnt.get((addr, coin), 0)
                                if _n < FAST_RETRY_MAX:
                                    fast_retry[(addr, coin)] = time.time() + FAST_RETRY_S
                                    fast_retry_cnt[(addr, coin)] = _n + 1
                                    stats["fast_retries"] = stats.get("fast_retries", 0) + 1
                        continue
                    delta_seen.pop(alert_key, None)
                    with fast_lock:
                        fast_retry_cnt.pop((addr, coin), None)
                    _r9 = (None if full_close else
                           _fresh_ratio(coin, old.get("side"), new_pos["val"]))
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
                            if _r9 is not None:
                                _mark_ratio(watchlist[addr][coin], _r9)
                    if full_close:
                        # тихе повне закриття (лімітками): tombstone проти
                        # воскресіння снапшотом скану + новий бік фліпа
                        scan_tombstones[alert_key] = time.time()
                        close_episodes.pop(alert_key, None)
                        with fc_lock:   # тихе закриття лімітками — епізод мертвий
                            fc_episodes.pop((addr, coin), None)
                        # follow-позиції теж мають вийти: позиція кита
                        # зникла, хай і без нових тейкер-філів (аудит п.8)
                        try:
                            with strat2_lock:
                                for _fp in follow_open.values():
                                    if _fp["key"] == alert_key:
                                        _fp["force_exit"] = "full_close"
                                        # v2.16: час, коли ДІЗНАЛИСЬ (філів
                                        # нема — час події невідомий)
                                        _fp.setdefault("force_exit_ts", time.time())
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
                with fast_lock:
                    fast_retry_cnt.pop((addr, coin), None)   # v2.16: повтори — з нуля на наступну подію
                # аудит-2 №8: первинна подія у журнал — сирі ціни/розміри/час
                # кожної транзакції, знімок позиції, джерело детекції
                _journal("close_batch", addr=addr, coin=coin, side=old.get("side"),
                         full_close=bool(full_close), detect_src=det_src, snap_ms=snap_ms,
                         old_size=old.get("size"), old_val=old.get("val"), ratio=old.get("ratio"),
                         new_size=(new_pos["size"] if new_pos else 0.0),
                         n_txs=len(mfills), truncated=bool(len(mfills) > 500),
                         txs=[{"h": str(f.get("hash"))[:18], "px": f["px"],
                               "pf": f.get("px_first"), "pl": f.get("px_last"),
                               "sz": f["sz"], "sp": f.get("sp"), "ts": f.get("ts"),
                               "dir": f.get("dir"), "liq": int(bool(f.get("liq"))),
                               "n": f.get("n_orders")}
                              for f in mfills[:500]])   # аудит-3: без обрізання на 40
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
                # Розмір ПОДІЇ для алерт-гейта — ДО того, як fc_on_full_close
                # зніме епізод: при повному закритті це позиція на старті
                # серії, а не $5k-хвіст перед фінальним батчем (v2.11 п.1,
                # дзеркало фікса rev v2.10 №1)
                # v2.12: у грейсі — ще й «якір» big_val (вартість, коли
                # пара востаннє була ≥2), а не лише $30k-залишок
                _ev_val = _grace_val(old)
                _ep_ratio = 0
                if full_close:
                    with fc_lock:
                        _ep_a = fc_episodes.get((addr, coin))
                    _ev_val = max(_ev_val, (_ep_a or {}).get("val") or 0)
                    # ratio на СТАРТІ епізоду — в алерт про повне закриття
                    # (рев'ю v2.12 №7a: 50%+50% показувало 1.25 замість 2.5)
                    _ep_ratio = (_ep_a or {}).get("ratio") or 0

                # ── ОБОЛОНКА СТРАТЕГІЙ: реверс + вхід у бік. rev читає
                #    епізод ДО того, як fc_on_full_close його зніме
                try:
                    rev_on_close(addr, coin, old, mfills, full_close,
                                 detect_src=det_src)
                except Exception as _re:
                    print(f"  [REV] hook err: {_re}")
                try:
                    follow_on_txs(addr, coin, old, mfills, full_close,
                                  detect_src=det_src)
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
                # від відсотка (5% позиції на $30k — це $1.5k шуму).
                # База відсотка — startPosition САМОЇ транзакції (число
                # біржі: позиція перед нею), а не спільна база батча:
                # 4 + 4.9 токена з 100 — друга це 4.9/96=5.1%, не 4.9%
                # (аудит v2.10 №7; філ без sp — фолбек стара база)
                big_txs = [f for f in mfills
                           if f["sz"] >= (f.get("sp") or _base) * MIN_CLOSE_PCT
                           and f["px"] * f["sz"] >= MIN_TX_USD]
                # v2.12: велике закриття пари, що проходить гейт, зсуває
                # мітку грейсу на час ОСТАННЬОЇ великої транзакції —
                # наступні пів години пара лишається «великою» навіть
                # якщо після цього шматка ratio впав нижче 2
                if big_txs and _ratio_ok(old) and not full_close:
                    _ts_close = max((f.get("ts") or 0) for f in big_txs) / 1000.0
                    with watchlist_lock:
                        _wc = watchlist.get(addr, {}).get(coin)
                        if _wc is not None:
                            _mark_close(_wc, _ts_close or time.time())

                # Пара могла пережити у watchlist падіння ratio нижче 2
                # (carry живої серії з оновленими метаданими): закриття
                # записуємо (SIM/FC вже отримали, база оновиться), але
                # алерт не шлемо — сигнал це позиція, ВЕЛИКА відносно
                # ліквідності, а ratio 1.09 нею не є.
                # Хард-фільтр $: позиція < MIN_POS_USD — теж не сигнал
                # v2.11: грейс-вікно ratio (_ratio_ok) — пара, що була
                # ≥2 менш як пів години тому, ще алертиться; розмір події
                # при повному закритті — по епізоду (_ev_val вище)
                if big_txs and (not _ratio_ok(old) or _ev_val < MIN_POS_USD):
                    big_txs = []

                if not big_txs:
                    # агресія є, але кожна транзакція дрібна: без алерту,
                    # базу рухаємо (курсор уже пересунутий вище)
                    _r9 = (None if full_close else
                           _fresh_ratio(coin, old.get("side"), new_pos["val"]))
                    with watchlist_lock:
                        if full_close:
                            watchlist.get(addr, {}).pop(coin, None)
                            if not watchlist.get(addr):
                                watchlist.pop(addr, None)
                        elif addr in watchlist and coin in watchlist[addr]:
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                            if _r9 is not None:
                                _mark_ratio(watchlist[addr][coin], _r9)
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
                    _batches = [([f], min(f["sz"] / (f.get("sp") or _base), 1.0),
                                 False)
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
                        "ratio":     (_ep_ratio if (_fc and _ep_ratio) else old["ratio"]),
                        "ratio_src": ("episode" if (_fc and _ep_ratio) else "live"),
                        "live_ratio": old["ratio"],
                        "entry":     old["entry"],
                        "full_close": _fc,
                        "fills":     list(_txs),
                    }
                    alert_queue.put(a)

                _r9 = (None if full_close else
                       _fresh_ratio(coin, old.get("side"), new_pos["val"]))
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
                            if _r9 is not None:
                                _mark_ratio(watchlist[addr][coin], _r9)
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
                        "ratio_hi_ts": time.time(), "big_val": _np["val"],
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

    def _process_fast():
        """Швидкі події (WS-тригери + повторні перевірки, чий час настав):
        один clearinghouseState на адресу, item = (addr, coins, "ws").
        v2.16: викликається і МІЖ чанками обходу — sweep на 300 гаманців
        більше не блокує швидкий шлях на 20–40 с."""
        now_r = time.time()
        with fast_lock:
            due = [k for k, t in fast_retry.items() if t <= now_r]
            for k in due:
                fast_retry.pop(k, None)
                fast_pending.setdefault(k, int(now_r * 1000))
            pend = dict(fast_pending)
            fast_pending.clear()
        if not pend:
            return
        by_addr, gone = {}, []
        with watchlist_lock:
            for a, c in pend:
                if a in watchlist and c in watchlist[a]:
                    by_addr.setdefault(a, {})[c] = dict(watchlist[a][c])
                else:
                    gone.append((a, c))
        if gone:
            with fast_lock:
                for k in gone:
                    fast_retry_cnt.pop(k, None)
        if by_addr:
            t0 = time.time()
            items = [(a, c, "ws") for a, c in by_addr.items()]
            with ThreadPoolExecutor(max_workers=min(10, len(items))) as pool:
                list(pool.map(_safe_worker, items))
            oldest = min(pend.values()) if pend else 0
            chain  = time.time() * 1000 - oldest if oldest else 0
            print(f"  [FAST] checked {len(by_addr)} wallet(s) in "
                  f"{time.time()-t0:.2f}s | блок→готово {chain:.0f}ms"
                  f"{' (+retry)' if due else ''}")

    def _fast_due():
        with fast_lock:
            if fast_pending:
                return True
            if not fast_retry:
                return False
            t = time.time()
            return any(v <= t for v in fast_retry.values())

    # ── Головний цикл: FAST-події миттєво, повний обхід як резерв ──
    last_sweep = 0.0
    while True:
        woke = fast_event.wait(timeout=1.0)
        if woke:
            fast_event.clear()
        if woke or _fast_due():
            _process_fast()

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
                items = [(a, c, "sweep") for a, c in wl_snap.items()]
                with ThreadPoolExecutor(max_workers=10) as pool:
                    for ci in range(0, len(items), FAST_SWEEP_CHUNK):
                        list(pool.map(_safe_worker, items[ci:ci + FAST_SWEEP_CHUNK]))
                        # v2.16: між чанками — швидкі події не чекають кінця
                        # обходу (WS-трейд посеред sweep раніше ждав до 40 с)
                        if fast_event.is_set() or _fast_due():
                            fast_event.clear()
                            _process_fast()
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
    _now = time.time()
    # мітки грейсу з живого watchlist: sweep/realtime бачать ratio≥2
    # частіше за скан, і без них база діфу губила грейс, який watchlist
    # уже дав (рев'ю v2.11)
    with watchlist_lock:
        _wl_hi = {(_a, _c): (_p.get("ratio_hi_ts") or 0)
                  for _a, _cs in watchlist.items()
                  for _c, _p in _cs.items()}
        _wl_big = {(_a, _c): float(_p.get("big_val") or 0)
                   for _a, _cs in watchlist.items()
                   for _c, _p in _cs.items()}
    for coin, positions in new_result.items():
        if coin.upper() in COIN_BLACKLIST: continue
        d = depth_snap.get(coin)
        if not d or not _depth_ok(d): continue   # обрізана/стара глибина — не база ratio
        for pos in positions:
            ds = depth_for_side(d, pos["side"])
            if not ds: continue
            ratio = pos["val"] / ds
            if not pos.get("size"): continue
            addr = pos["addr"].lower()
            # грейс-вікно (v2.11 п.1): пара з ratio<2 лишається у базі
            # діфу, лише якщо БУЛА ≥2 менш як пів години тому (мітка
            # успадковується з попереднього знімка) — інакше база
            # розрослась би на всі 10k позицій із запитами філів на
            # кожну дельту
            _pp = prev.get(addr, {}).get(coin) or {}
            hi_ts = _now if ratio >= 2.0 else max(
                _pp.get("ratio_hi_ts") or 0, _wl_hi.get((addr, coin), 0))
            if ratio < 2.0 and _now - hi_ts >= RATIO_GRACE_S:
                continue
            big_val = pos["val"] if ratio >= 2.0 else max(
                float(_pp.get("big_val") or 0), _wl_big.get((addr, coin), 0))
            if addr not in new_by_addr: new_by_addr[addr] = {}
            new_by_addr[addr][coin] = {
                "size":  pos["size"],
                "val":   pos["val"],
                "side":  pos["side"],
                "ratio": ratio,
                "ratio_hi_ts": hi_ts, "big_val": big_val,
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
            # база відсотка — startPosition транзакції, як у realtime
            # (аудит v2.10 №7)
            big_txs = [f for f in mfills
                       if f["sz"] >= (f.get("sp") or _base) * MIN_CLOSE_PCT
                       and f["px"] * f["sz"] >= MIN_TX_USD]
            # ratio беремо СВІЖИЙ (new_pos, той самий знімок): база,
            # збережена failed_keep через кілька сканів, несла б ratio
            # годинної давнини — v1.3 вимагає "на момент алерту"
            # (рев'ю v2.7 №5б); розмір ПОДІЇ гейтиться старою позицією
            if not big_txs or not _ratio_ok(new_pos, _now) \
               or max(old.get("val") or 0, _grace_val(new_pos)) < MIN_POS_USD:
                continue
            # v2.12: велике закриття зсуває мітку грейсу (і в базі діфу,
            # і в живому watchlist, якщо пара там є)
            _ts_close = max((f.get("ts") or 0) for f in big_txs) / 1000.0 or _now
            _mark_close(new_pos, _ts_close)
            with watchlist_lock:
                _wc = watchlist.get(addr, {}).get(coin)
                if _wc is not None:
                    _mark_close(_wc, _ts_close)

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
                    "ratio_src": "live",   # скан-діф: ratio епізоду тут невідомий
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
        f"📊 <b>Ratio{' на старті епізоду' if a.get('ratio_src') == 'episode' else ''}:</b> {a['ratio']:.2f}x"
        + (f" (зараз {a['live_ratio']:.2f}x)"
           if a.get('live_ratio') is not None and abs(a['live_ratio'] - a['ratio']) > 0.005
           else "") + "\n"
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

def sim_all_mids(retries=4):
    """Поточні mid-ціни всіх монет одним запитом. None при помилці.
    v2.16 (рев'ю): поллер кличе з retries=1 — він і так повторює кожні
    5 с, а 4 ретраї з бекофом давали 16–44 с сліпого вікна саме при 429;
    легасі SIM/FC лишають ретраї (разовий 429 не має губити їхній вхід)."""
    try:
        data = hl_post({"type": "allMids"}, retries=retries)
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

_journal_lock = threading.Lock()
def _journal(_kind, **fields):
    """v2.16 (аудит №8): журнал ПЕРВИННИХ подій для майбутнього replay —
    events-YYYYMMDD.jsonl у DATA_DIR: батчі закриттів кита з сирими
    цінами філів, рішення входу (стакан, мід, референс, причини пропуску),
    вирішені виходи. Без нього старий детектор не можна відтворити;
    з ним — можна перерахувати будь-яке нове правило на минулих подіях.
    Перший аргумент — позиційний _kind (рев'ю: поле події «kind» у
    **fields не має конфліктувати з іменем параметра). Ніколи не кидає."""
    try:
        rec = {"ts": round(time.time(), 3), "kind": _kind}
        rec.update(fields)
        line = json.dumps(rec, ensure_ascii=False, default=str, separators=(",", ":"))
        path = os.path.join(DATA_DIR, time.strftime("events-%Y%m%d.jsonl"))
        with _journal_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        stats["journal_err"] = stats.get("journal_err", 0) + 1
        if stats["journal_err"] in (1, 10, 100):
            print(f"  [JOURNAL] запис не вдався ({stats['journal_err']}): {e}")

def _sim_costs(coin, our_side):
    """v2.16 (рев'ю): витрати за круг, % — комісія ×2 + сліпаж по ОБОХ
    боках стакану Binance: вхід б'є один бік (наш SHORT = SELL у біди),
    вихід — інший (BUY з асків). Раніше обидві ноги рахувались по стороні
    закриття кита. → (costs_pct, depth_entry, depth_exit)."""
    with cache_lock:
        d = cache["depth"].get(coin) or cache.get("depth_prev", {}).get(coin)
    if d and d.get("ts") and time.time() - d["ts"] > 2 * REFRESH_S:
        d = None   # аудит v2.16: стара глибина (2 цикли) — як невідома
    bids, asks = depth_for_side(d, "LONG"), depth_for_side(d, "SHORT")
    d_in, d_out = (bids, asks) if our_side == "SHORT" else (asks, bids)
    return ((2 * SIM_COMMISSION + _sim_slip(d_in or 0) + _sim_slip(d_out or 0)) * 100.0,
            d_in or 0, d_out or 0)

def _p_costs(p):
    """Витрати трекера (ПОВНА модель, колонка costs_pct): збережені при
    вході (v2.16, обидва боки) або, для трекерів зі старого state, стара
    формула по одній глибині."""
    c = p.get("costs")
    if isinstance(c, (int, float)) and math.isfinite(c):
        return float(c)
    slip = _sim_slip(p.get("depth") or 0)
    return (2 * SIM_COMMISSION + 2 * slip) * 100.0

def _leg_costs(costs_full, entry_src, exit_src):
    """Аудит v2.16 №12: нога, виконана по стакану (VWAP по асках/бідах),
    уже містить спред і вплив ордера — модельний сліпаж додається ЛИШЕ до
    ніг, оцінених мідом/семплом. costs_full = комісія×2 + сліпаж×2 (колонка
    costs_pct); повертає ефективні витрати для net."""
    comm = 2 * SIM_COMMISSION * 100.0
    try:
        slip_each = max(0.0, (float(costs_full) - comm) / 2.0)
    except (TypeError, ValueError):
        slip_each = 0.0
    def _book(src):
        return str(src or "").startswith("book")
    return comm + (0.0 if _book(entry_src) else slip_each) \
                + (0.0 if _book(exit_src) else slip_each)

def hl_book_exec(coin, side, usd=None):
    """Свіжий стакан HL (l2Book, вага 2) через prio-канал → ВИКОНУВАНА
    ціна ринкового ордера на usd з нашого боку: BUY іде по асках, SELL по
    бідах, VWAP по рівнях (v2.16, paper-вхід: замість міда з кешу
    5-секундного поллера, який міг передувати філу кита і дарував нам
    частину його зливу). Повертає dict(px, mid, bid, ask, ts_ms, req_ms,
    partial) або None — бюджет/збій/порожній бік; викликач тоді бере
    запасний варіант і ПОЗНАЧАЄ його у рядку."""
    usd = SIM_POSITION_USD if usd is None else usd
    t0 = time.time()
    try:
        data = hl_post_prio({"type": "l2Book", "coin": coin}, retries=1,
                            direct=not _prio_opener, max_wait=3.0)
        lv = data.get("levels") or []
        bids, asks = (lv[0] or []), (lv[1] or [])
        book = asks if side == "BUY" else bids
        if not book:
            raise ValueError("порожній бік стакану")
        best_bid = float(bids[0]["px"]) if bids else None
        best_ask = float(asks[0]["px"]) if asks else None
    except Exception as e:
        stats["book_fail"] = stats.get("book_fail", 0) + 1
        if stats["book_fail"] in (1, 10, 100) or stats["book_fail"] % 1000 == 0:
            print(f"  [BOOK] {coin}: стакан недоступний ({stats['book_fail']}): {e}")
        return None
    left, cost, qty, last_px = float(usd), 0.0, 0.0, None
    for lvl in book:
        try:
            px = float(lvl["px"]); sz = float(lvl["sz"])
        except (KeyError, TypeError, ValueError):
            continue
        take = min(left, px * sz)
        if take <= 0:
            continue
        cost += take; qty += take / px; left -= take; last_px = px
        if left <= 1e-9:
            break
    if qty <= 0:
        return None
    # аудит v2.16 №14: час котирування — біржовий; стакан старший за
    # BOOK_MAX_AGE_S відносно локального годинника = не «зараз»
    try:
        q_ms = int(data.get("time") or 0)
    except (TypeError, ValueError):
        q_ms = 0
    q_age = (time.time() * 1000 - q_ms) / 1000.0 if q_ms else None
    if q_age is not None and abs(q_age) > BOOK_MAX_AGE_S:
        stats["book_stale"] = stats.get("book_stale", 0) + 1
        if stats["book_stale"] in (1, 10, 100, 1000):
            # рев'ю: не мовчати — зсув годинника VPS >5 с від HL обнулив би
            # УСІ paper-входи без жодного рядка логу
            print(f"  [BOOK] {coin}: котирування l2Book старше за {BOOK_MAX_AGE_S:.0f} с "
                  f"(вік {q_age:+.1f} с; {stats['book_stale']}-й раз) — не «зараз»; "
                  f"якщо повторюється — перевір годинник сервера (timedatectl)")
        return None
    stats["book_ok"] = stats.get("book_ok", 0) + 1
    partial = int(left > 1e-9)
    # стакан (20 рівнів) не вміщує $1000: VWAP заповненої частини —
    # оптимістичний; беремо ГІРШИЙ пройдений рівень (рев'ю v2.16)
    px_exec = last_px if (partial and last_px) else cost / qty
    mid = ((best_bid + best_ask) / 2.0 if (best_bid and best_ask)
           else (best_ask or best_bid))
    return {"px": px_exec, "mid": mid,
            "bid": best_bid, "ask": best_ask,
            "ts_ms": (q_ms or int(t0 * 1000)), "q_age_s": q_age,
            "req_ms": int((time.time() - t0) * 1000),
            "partial": partial}

def _px_age_s():
    """Вік найсвіжішого семпла цін (BTC — завжди у allMids), с; None —
    історії ще нема. /status + watchdog (v2.16): поллер, що «живий», але
    мовчить, — сліпа зона paper-угод."""
    with px_lock:
        h = px_hist.get("BTC")
        if not h:
            return None
        ts = h[-1][0]
    return round(time.time() - ts, 1)

def _px_mid_age(coin):
    """Мід з кешу поллера і його вік у мс; (None, None) — історії нема."""
    with px_lock:
        h = px_hist.get(coin)
        if not h:
            return None, None
        ts, px = h[-1]
    return px, int((time.time() - ts) * 1000)

def _paper_px(coin, side, whale_px=None, max_mid_age_ms=20_000, strict=False):
    """Ціна paper-угоди У МОМЕНТ рішення (v2.16): свіжий стакан → src
    'book'. strict=True (ВХОДИ, аудит v2.16 №1): без повного стакану
    угоди немає — (None, 'no_book'); мід/ціна філа кита не є доказом
    виконуваної ціни ПІСЛЯ зливу. strict=False (виходи, тіньова стрічка):
    нема стакану — гірша для нас із кеш-міда і ціни філа кита →
    'mid+whale' (лише мід → 'mid'); нема й міда → (None, 'none'). BUY:
    гірша = вища, SELL: нижча. Стакан, що не вмістив $1000 (partial), —
    не доказ повного виконання: у strict = нема стакану, інакше src
    'book_partial' за гіршим пройденим рівнем. Вік міда міряється ПІСЛЯ
    запиту стакану (запит міг тривати секунди). meta: мід, його вік, ціна
    філа кита, час запиту, біржовий час котирування — усе йде в рядок."""
    meta = {"mid": None, "px_age_ms": None, "whale_px": whale_px,
            "book_ms": None, "partial": 0, "quote_ts_ms": None}
    bx = hl_book_exec(coin, side)
    mid, age_ms = _px_mid_age(coin)   # ПІСЛЯ запиту: вік на момент входу
    meta["mid"], meta["px_age_ms"] = mid, age_ms
    if bx:
        meta["book_ms"] = bx["req_ms"]; meta["partial"] = bx["partial"]
        meta["book_mid"] = bx["mid"]; meta["quote_ts_ms"] = bx["ts_ms"]
        if bx["partial"] and strict:
            stats["book_partial_skips"] = stats.get("book_partial_skips", 0) + 1
            return None, "no_book", meta
        return bx["px"], ("book_partial" if bx["partial"] else "book"), meta
    if strict:
        return None, "no_book", meta
    if mid is None or (age_ms or 0) > max_mid_age_ms:
        return None, "none", meta
    px, src = mid, "mid"
    if whale_px:
        worse = max(mid, whale_px) if side == "BUY" else min(mid, whale_px)
        if worse != mid:
            px, src = worse, "mid+whale"
    return px, src, meta

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
        }
        # під fc_lock: ітерація без нього могла зловити "dict changed
        # size during iteration" від конкурентного fc_on_txs і зірвати
        # ВЕСЬ знімок (аудит v2.10); fc_episodes теж персистяться —
        # рестарт посеред 5-хв зливу втрачав старт епізоду, і R7/повне
        # закриття рахувались від огризка (seen: set -> list для JSON)
        with fc_lock:
            snap["fc_positions"] = [[k[0], k[1], dict(p)]
                                    for k, p in fc_positions.items()]
            _eps = []
            for k, e in fc_episodes.items():
                ee = dict(e); ee["seen"] = list(e.get("seen") or ())
                _eps.append([k[0], k[1], ee])
            snap["fc_episodes"] = _eps
        with strat2_lock:
            snap["rev_open"] = {k: dict(v) for k, v in rev_open.items()}
            snap["follow_open"] = {k: dict(v) for k, v in follow_open.items()}
            snap["follow_last_close"] = dict(follow_last_close)
            snap["vault_cache"] = dict(vault_cache)
        # TWAP-реєстр і останні пости каналів (v2.11 п.5): рестарт не
        # губить твап, що вже йде, і не переобробляє старі пости
        with twap_lock:
            snap["twap_reg"] = {k: dict(v) for k, v in twap_reg.items()}
            snap["twap_last_ids"] = dict(twap_last_ids)
        # незавершені пропуски t.me (v2.15): рестарт під час догортання не
        # губить непрочитані id — last уже піднято вище за них
        snap["twap_gaps"] = {ch: [list(g) for g in (s.get("gaps") or [])]
                             for ch, s in _twap_ch_state.items()}
        snap["strat_activated"] = dict(strat_activated)   # v2.13 (швидкість)
        # резервна копія раз на 10 хв: битий state.json не лишає без
        # відкатного варіанта (рев'ю v2.12 №6a)
        if os.path.exists(STATE_FILE) and time.time() - _state_bak_ts[0] > 600:
            try:
                shutil.copy2(STATE_FILE, STATE_FILE + ".bak")
                _state_bak_ts[0] = time.time()
            except Exception as _be:
                print(f"  [STATE] bak: {_be}")
        # tmp унікальний на виклик: конкурентні save_state писали в один
        # файл і os.replace інсталював перемішаний JSON (рев'ю v2.2)
        tmp = f"{STATE_FILE}.tmp{threading.get_ident()}"
        with open(tmp, "w") as f:
            json.dump(snap, f)
        os.replace(tmp, STATE_FILE)   # атомарно: або старий файл, або новий
    except Exception as e:
        print(f"  [STATE] save err: {e}")

def _csv_written_keys():
    """Ключі вже ЗАПИСАНИХ рядків усіх стрічок трекерів (поточний файл +
    .legacy-ротації) як (стрічка, ключ…). v2.14 (аудит v2.13 №1): трекер,
    чий рядок уже в CSV, при рестарті НЕ відновлюється — інакше аварія
    між записом CSV і save_state закривала ту саму угоду вдруге за
    поточною ціною, а дедуп «останній виграє» підміняв результат.
    Огризки (інша кількість колонок, без eol-вартового) — не доказ."""
    import csv as _csv
    keys = set()
    specs = ((REV_CSV, "rev", ("sig_id", "strategy")),
             (TWAP_CSV, "twap", ("twap_id", "strategy")),
             (TWAP_CURVE_CSV, "twapc", ("twap_id", "strategy")),
             (REV_OUT_CSV, "out", ("sig_id",)),
             (FOLLOW_OUT_CSV, "fo", ("fo_id",)),
             (FOLLOW_CSV, "fol", ("trade_id",)))
    for path, tag, cols in specs:
        for fp in sorted(glob.glob(path + ".legacy-*.csv")) + [path]:
            try:
                with open(fp, newline="", encoding="utf-8", errors="replace") as f:
                    rd = _csv.reader(f)
                    hdr = next(rd, None)
                    if not hdr:
                        continue
                    try:
                        idx = [hdr.index(c) for c in cols]
                    except ValueError:
                        continue
                    has_eol = hdr[-1] == "eol"
                    while True:
                        try:
                            rr = next(rd)
                        except StopIteration:
                            break
                        except Exception:
                            break   # битий хвіст: накопичене лишається
                        if len(rr) != len(hdr) or (has_eol and rr[-1] != "^"):
                            continue
                        k = tuple(rr[i] for i in idx)
                        if all(k):
                            keys.add((tag,) + k)
            except FileNotFoundError:
                continue
            except Exception as e:
                print(f"  [STATE] ключі {os.path.basename(fp)}: {e}")
    return keys

def _tracker_csv_key(coll, pid, p):
    """(стрічка, ключ…) рядка, який цей трекер напише — для звірки з CSV."""
    if coll == "fol":
        return ("fol", str(pid))
    st = p.get("strategy")
    if st == "_OUTCOME":
        return ("out", str(p.get("sig_id")))
    if st == "_FOLLOW_OUT":
        return ("fo", str(p.get("sig_id")))
    if p.get("row_kind") == "twap":
        return ("twap", str(p.get("sig_id")), str(st))
    return ("rev", str(p.get("sig_id")), str(st))

def load_state():
    def _read(path):
        with open(path) as f:
            snap_ = json.load(f)
        if not isinstance(snap_, dict):
            # валідний JSON не тієї структури ([] тощо) — теж битий стан
            # (аудит v2.13 №8d: AttributeError поза захисним блоком)
            raise ValueError(f"state is {type(snap_).__name__}, not dict")
        # структура полів (аудит v2.14 №8a-b): рядок замість saved_at чи
        # список замість rev_open — битий стан, а не TypeError поза
        # захистом; тоді пробуємо .bak
        sa = snap_.get("saved_at", 0)
        if isinstance(sa, bool) or not isinstance(sa, (int, float)):
            raise ValueError(f"saved_at is {type(sa).__name__}")
        for k in ("rev_open", "follow_open", "twap_reg", "twap_last_ids",
                  "strat_activated", "watchlist", "follow_last_close",
                  "vault_cache", "fill_cursor", "close_episodes"):
            if k in snap_ and not isinstance(snap_[k], dict):
                raise ValueError(f"{k} is {type(snap_[k]).__name__}, not dict")
        for k in ("sim_positions", "sim_trackers", "sim_closed", "recent_alerts",
                  "sent_alerts", "fc_positions", "fc_episodes"):
            if k in snap_ and not isinstance(snap_[k], list):
                raise ValueError(f"{k} is {type(snap_[k]).__name__}, not list")
        return snap_
    snap = None
    try:
        snap = _read(STATE_FILE)
    except FileNotFoundError:
        # основного файла нема, а .bak є — відновлюємо з нього (аудит
        # v2.13 №8c: раніше вихід без спроби резервної копії)
        if not os.path.exists(STATE_FILE + ".bak"):
            return
        print("  [STATE] state.json відсутній — пробую .bak")
    except Exception as e:
        # битий файл НЕ перезаписуємо наступним save (рев'ю v2.12 №6a):
        # відкладаємо під .corrupt-<ts> і пробуємо резервну копію
        print(f"  [STATE] load err: {e} — відкладаю битий файл, пробую .bak")
        try:
            os.replace(STATE_FILE, f"{STATE_FILE}.corrupt-{int(time.time())}")
        except Exception as _me:
            print(f"  [STATE] не вдалося відкласти битий state: {_me}")
    if snap is None:
        try:
            snap = _read(STATE_FILE + ".bak")
            print("  [STATE] відновлюю з .bak")
        except Exception as e2:
            print(f"  [STATE] .bak недоступний ({e2}) — старт з порожнім станом")
            return
    age = time.time() - snap.get("saved_at", 0)
    stale = age > STATE_MAX_AGE_S
    if stale:
        # v2.12 (з рев'ю CH): застарілий РИНКОВИЙ знімок (watchlist,
        # курсори, епізоди, сим) не відновлюємо — але відкриті paper-
        # трекери стратегій і TWAP-реєстр повертаємо: цикл трекерів сам
        # пише "" у пропущені хвилини, тож година простою — чесні
        # прогалини в рядку, а не втрачена угода
        print(f"  [STATE] стан старіший за годину ({age/60:.0f} хв): ринковий "
              f"знімок пропускаю, трекери стратегій відновлюю")
    try:
        if not stale:
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
                for a, c, e in snap.get("fc_episodes", []):
                    e["seen"] = set(e.get("seen") or [])
                    fc_episodes[(a, c)] = e
        # v2.14 (аудит v2.13 №1): трекер, чий рядок УЖЕ у CSV (аварія
        # між записом і збереженням стану), не відновлюємо — завершена
        # угода не отримує другий вихід за поточною ціною
        written = _csv_written_keys()
        skipped = []
        with strat2_lock:
            for _cn, _coll, _src in (("rev", rev_open, snap.get("rev_open") or {}),
                                     ("fol", follow_open, snap.get("follow_open") or {})):
                for _pid, _p in _src.items():
                    if not isinstance(_p, dict):
                        continue
                    _key = _tracker_csv_key(_cn, _pid, _p)
                    if _key[0] == "twap":
                        # v2.15: угода записана — трекер відновлюється лише
                        # для кривої (рядок угоди НЕ переписується); крива
                        # теж записана — трекер завершений
                        if ("twapc",) + _key[1:] in written:
                            skipped.append(str(_pid))
                            continue
                        if _key in written:
                            _p["trade_written"] = 1
                            _p["trade_row"] = None
                        _coll[_pid] = _p
                        continue
                    if _key in written:
                        skipped.append(str(_pid))
                        continue
                    _coll[_pid] = _p
            follow_last_close.update(snap.get("follow_last_close", {}))
            vault_cache.update(snap.get("vault_cache", {}))
        if skipped:
            print(f"  [STATE] {len(skipped)} трекер(ів) уже записані у CSV — не "
                  f"відновлюю (аварія між записом і збереженням стану): "
                  f"{', '.join(skipped[:5])}{'…' if len(skipped) > 5 else ''}")
        with twap_lock:
            twap_reg.update(snap.get("twap_reg", {}))
            twap_last_ids.update({k: int(v) for k, v in
                                  snap.get("twap_last_ids", {}).items()})
        _tg = snap.get("twap_gaps")
        if isinstance(_tg, dict):
            for _ch, _gl in _tg.items():
                if _ch in _twap_ch_state and isinstance(_gl, list):
                    _twap_ch_state[_ch]["gaps"] = [
                        [int(g[0]), int(g[1]), float(g[2])]
                        for g in _gl if isinstance(g, (list, tuple)) and len(g) >= 3]
        strat_activated.update(snap.get("strat_activated", {}))
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
    with fast_lock:   # v2.16: лічильники повторів швидкого шляху — лише живі пари
        for k in [k for k in list(fast_retry_cnt) if f"{k[0]}:{k[1]}" not in _active]:
            fast_retry_cnt.pop(k, None)
            fast_retry.pop(k, None)
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
    # TWAP-реєстр: завершені/відкинуті записи — 6 год після кінця твапу
    # (для /strat2 «останні твапи»), далі геть
    with twap_lock:
        for k in [k for k, r in list(twap_reg.items())
                  if r.get("state") in ("dropped", "entered", "ineligible")
                  and (r.get("end") or 0) < now_ - 6 * 3600]:
            twap_reg.pop(k, None)
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
# аудит-3 №2: ЄДИНИЙ алгоритм епізоду — та сама функція, що й у settlement
# (settle.close_episode), застосована до КОЖНОЇ транзакції у хронології;
# результат не залежить від того, як API/sweep порізали транзакції на батчі
try:
    from settle import close_episode as _close_episode
except Exception as _e_ce:   # settle.py має бути поруч із server.py
    _close_episode = None
    print(f"  [FC] settle.close_episode недоступний ({_e_ce!r}) — епізоди лише між батчами")

def _fc_rebuild(ep_txs, old, seen, prev=None):
    """Стан епізоду з його транзакцій (хронологічно): база R7 (start_size,
    max_sz/max_usd/max_liq), перша/остання ціна й час, залишок після
    останньої tx. Перебудовується цілком щоразу — база не залежить від
    пакування батчів (аудит-3 №2). ratio/val — знімок пари на СТАРТІ
    епізоду (розмір події для гейтів, рев'ю v2.12): якщо перша tx та сама,
    що у попереднього стану (prev), вони успадковуються."""
    t0, tl = ep_txs[0], ep_txs[-1]
    # епізод зі старого state (без txs): база та сама, якщо перша tx не змінилась
    # за часом (синтезована _ep0 несе first_ts) — знімок ratio/val не губиться
    same_base = bool(prev and ((prev.get("txs") or [{}])[0].get("hash") == t0.get("hash")
                               or (not prev.get("txs") and prev.get("first_ts") == t0.get("ts"))))
    ep = {"first_ts": t0["ts"], "first_px": (t0.get("px_first") or t0["px"]),
          "last_ts": max(t.get("ts", 0) for t in ep_txs),
          "sum_usd": sum(t["px"] * t["sz"] for t in ep_txs),
          "seen": seen, "side": old.get("side", "?"),
          "ratio": (prev.get("ratio", 0) if same_base else old.get("ratio", 0)),
          "val": (prev.get("val", 0) if same_base else old.get("val", 0)),
          "start_size": (t0.get("sp") or old.get("size", 0)),
          "max_sz": 0.0, "max_usd": 0.0, "max_liq": 0, "pos_after": None,
          "txs": [{k: t.get(k) for k in ("hash", "ts", "px", "px_first", "px_last",
                                          "sz", "sp", "dir", "liq")} for t in ep_txs]}
    for t in ep_txs:
        if t["sz"] > ep["max_sz"]:
            ep["max_sz"], ep["max_usd"] = t["sz"], t["px"] * t["sz"]
            ep["max_liq"] = int(bool(t.get("liq")))
    if tl.get("sp"):
        ep["pos_after"] = max(0.0, float(tl["sp"]) - float(tl["sz"]))
    return ep
fc_positions = {}   # (addr, coin) -> наша відкрита позиція відкату

def fc_on_txs(addr, coin, old, mfills):
    """Будує епізод закриття з підтверджених маркет-транзакцій.
    аудит-3 №2: епізод = settle.close_episode над УСІМА транзакціями
    поточного епізоду + новими, у хронології: розрив ≤300 с між сусідніми
    І позиція між ними не зросла (startPosition наступної ≈ залишок після
    попередньої); долив/перевідкриття ВСЕРЕДИНІ батча теж починає новий
    епізод (раніше перевірялась лише межа між батчами — той самий потік,
    порізаний інакше, давав іншу базу R7 і тривалість)."""
    if not FC_ENABLED or not mfills: return
    key = (addr, coin)
    with fc_lock:
        ep = fc_episodes.get(key)
        seen = ep["seen"] if ep else set()
        new = sorted([t for t in mfills if t.get("hash") not in seen],
                     key=lambda t: t.get("ts", 0))
        if not new: return
        if ep and key in fc_positions:
            # старий FC-трек тримає епізод «як є» (легасі-стрічка fc_trades)
            for t in new:
                ep["seen"].add(t.get("hash"))
                ep["last_ts"] = max(ep["last_ts"], t.get("ts", 0))
                ep["sum_usd"] += t["px"] * t["sz"]
            return
        prev_txs = list((ep or {}).get("txs") or [])
        if ep and not prev_txs:
            # епізод зі старого state без списку транзакцій — його
            # перший/останній філ відомі лише як межі; синтезуємо
            prev_txs = [{"hash": "_ep0", "ts": ep["first_ts"], "px": ep["first_px"],
                         "px_first": ep["first_px"], "px_last": ep["first_px"],
                         "sz": float(ep.get("start_size") or 0) - float(ep.get("pos_after") or 0)
                               if ep.get("pos_after") is not None else 0.0,
                         "sp": ep.get("start_size"), "dir": "Close", "liq": 0}]
        txs_all = sorted(prev_txs + new, key=lambda t: t.get("ts", 0))
        side_hl = "A" if old.get("side") == "LONG" else "B"
        fills = []
        for i, t in enumerate(txs_all):
            fills.append({"coin": coin, "time": t.get("ts", 0), "side": side_hl,
                          "dir": (t.get("dir") or "Close"),
                          "startPosition": (t.get("sp") if t.get("sp") else None),
                          "sz": t["sz"], "px": (t.get("px_first") or t["px"]), "_i": i})
        t_last = max(t.get("ts", 0) for t in txs_all)
        if _close_episode is not None:
            # grow_only: у live лише тейкерські філи — мейкерське закриття
            # між ними не є новим епізодом (рев'ю), розрив лише на зростанні
            _first, _last, ep_f = _close_episode(fills, coin, t_last, True, grow_only=True)
            ep_txs = [txs_all[f["_i"]] for f in ep_f] if ep_f else [txs_all[-1]]
        else:
            # фолбек без settle.py: те саме правило inline (розрив ≤300 с і
            # позиція не зросла між сусідніми tx) — не «склеїти все»
            k = 0
            for i in range(1, len(txs_all)):
                a, b = txs_all[i - 1], txs_all[i]
                grew = bool(a.get("sp") and b.get("sp")
                            and float(b["sp"]) > (float(a["sp"]) - float(a["sz"])) * 1.01 + 1e-9)
                if (b.get("ts", 0) - a.get("ts", 0)) / 1000.0 > FC_MAX_EPISODE_S or grew:
                    k = i
            ep_txs = txs_all[k:]
        seen_all = set(seen) | {t.get("hash") for t in new}
        fc_episodes[key] = _fc_rebuild(ep_txs, old, seen_all, prev=ep)

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
# ── v2.16 ──
BOOK_MAX_AGE_S   = 5.0              # стакан із біржовим часом старшим за це — не «зараз»
R8_NAME          = "R8_тп80"        # реверс з уявним тейк-профітом: лонг
                                    # після падіння, вихід при відновленні
                                    # 80% руху (падіння 1% → TP +0.8%),
                                    # інакше таймер m30
R8_TP_FRAC       = 0.8
REV_HOLD_MIN     = 30               # заголовкова угода реверсу закривається
                                    # на m30; трекер веде криву до
                                    # REV_TRACK_MIN, але монету не тримає
REV_EP_MAX_S     = 300.0            # правило v2.16: епізод закриття кита
                                    # (перший → останній філ) коротший за
                                    # 5 хв…
REV_EP_MIN_MAG   = 1.0              # …і падіння ≥1% між цінами його філів
FOLLOW_MAX_FILL_AGE_S = 20.0        # paper-вхід лише за свіжим філом кита:
                                    # старіший (рестарт, пауза) — не вхід
REV_MAX_FILL_AGE_S    = 60.0
# ── ТЗ 08.09 п.4 ──
F8_NAME          = "F8_ratio35"     # v2.12 (рішення користувача 08.09):
                                    # копія F6 (тиша 1 хв · перший
                                    # постріл, БЕЗ профілю) + ratio пари
                                    # ≥3.5 на момент входу. У v2.11 була
                                    # F5-родина — since піднято до 2.12
F8_MIN_RATIO     = 3.5
F9_NAME          = "F9_без_ратіо_90" # F7 (nr-гілка, лише ≥$100k), але
                                    # ≥90% швидких замість 70%
F9_MIN_FAST_PCT  = 90.0
# ── ТЗ 08.09 п.5: TWAP-реверс ──
T1_NAME          = "T1_твап_відкриття"   # твап ВІДКРИВАЄ нову позицію
T2_NAME          = "T2_твап_скорочення"  # твап СКОРОЧУЄ наявну позицію
# v2.13: когорти ≥1/≥1.5/≥2% — ОКРЕМІ стратегії з власними трекерами
# (рев'ю v2.12: базова займала монету на русі 1.2% і забирала в когорти
# ≥2% пізніший сигнал 2.1%); імена = базове + суфікс
TWAP_COHORT_SUFFIX = {1.0: "", 1.5: "_15", 2.0: "_20"}

# ── ВЕРСІОНУВАННЯ СТАТИСТИКИ (ТЗ 08.09 п.2) ──
# Раніше API показував лише рядки з algo_v == поточна версія: кожен
# патч раз на кілька днів обнуляв ВСІ картки, хоча більшість стратегій
# не мінялись. Тепер у кожної стратегії — своя «since»-версія: перша
# версія DATA_ALGO_V, від якої її логіка (і спільний сигнальний шар,
# що її живить) не мінялась; рядки від since і новіші — порівнянні.
# Правило супроводу: змінив логіку стратегії або спільний шар, що
# міняє її ПОПУЛЯЦІЮ входів — підніми її since до нової DATA_ALGO_V;
# додав нову — since = версія, в якій вона з'явилась. Спільні стрічки
# (сигнали/outcome/follow_outcomes) — TAPE_SINCE.
def _vt(v):
    """'2.10' -> (2, 10); порожнє/бите -> (0,) (legacy без версії)."""
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except (ValueError, AttributeError):
        return (0,)

def _v_ok(row_v, since):
    return _vt(row_v) >= _vt(since) and _vt(row_v) != (0,)

STRAT_SINCE = {
    # v2.16: реверси — нове правило кваліфікації (епізод закриття <5 хв,
    # падіння ≥1% між філами кита), чесний вхід зі свіжого стакану, вихід
    # m30 з ціною стакану, R8 — рядки R з 2.10 не порівнянні (старі рядки
    # доступні через settlement, якщо проходять нове правило)
    # аудит-3 №1: обидві збірки 2.16 писали ту саму версію при різній
    # семантиці (перша — вхід за мідом/ціною філа, епізод між батчами);
    # 2.17 — строгий стакан, єдиний епізод на кожній tx, повне закриття
    # для всіх R. Рядки 2.16 — лише через legacy-допуск за settlement
    # (_rev_adm_legacy: епізод, повне закриття, лаг, ціна зі стрічки)
    "R1_загальний": "2.17", "R2_breakout": "2.17", "R3_великі": "2.17",
    "R4_великий": "2.17", "R5_дуже": "2.17", "R6_волт": "2.17",
    "R7_одним": "2.17", R8_NAME: "2.17",
    "F1_1хв": "2.10", "F2_2хв": "2.10", "F3_3хв": "2.10",
    "F6_1хв_перший": "2.10", "F4_розумний": "2.10", "F5_перший": "2.10",
    "F7_без_ратіо": "2.10",
    # F8: у 2.12 стала F6-копією (інша популяція); T1/T2: у 2.12 вид
    # твапу — зі startPosition першого слайсу (доливи більше не T1),
    # ціни P0/P1 — зі свічок біржі
    # v2.13: вихід при скасуванні, когорти-стратегії, звірка первинності
    # слайсу — рядки T1/T2 з 2.12 не порівнянні
    # v2.14: completed лише за повним виконанням, вихід timer_late при
    # відсутній m60, закриття угоди ≠ спостереження, неокруглений рух
    # когорт, вхід лише зі свіжою успішною звіркою.
    # v2.15: рядок угоди у момент виходу (крива окремо), вихід при
    # скасуванні — коли дізнались (не заднім числом), перший філ за
    # ланцюгом позицій — рядки T1/T2 з 2.14 не порівнянні
    F8_NAME: "2.12", F9_NAME: "2.11", T1_NAME: "2.15", T2_NAME: "2.15",
    T1_NAME + "_15": "2.15", T1_NAME + "_20": "2.15",
    T2_NAME + "_15": "2.15", T2_NAME + "_20": "2.15",
}
TAPE_SINCE = "2.10"
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
PROFILE_ALGO_V   = 9      # версія алгоритму профілів: старі записи без
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
                          # (лише ≥$100k) — кваліфікація F7, ТЗ 04.09;
                          # v9: бік "Short > Long" — SHORT, глибина з
                          # правильної сторони (аудит v2.10 №8))
RESEARCH_SINCE   = "2.17" # аудит-3 №4: спостереження на тіньовій outcome-стрічці —
                          # лише рядки з поточною семантикою (рух епізоду, ціна
                          # детекту після строгого правила); старіші — поза
DATA_ALGO_V      = "2.17" # версія логіки збору: трекер отримує її при
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
    "R8_тп80":      "Реверс · TP 80% відкату",
    "F1_1хв":       "За китом · тиша 1 хв",
    "F2_2хв":       "За китом · тиша 2 хв",
    "F3_3хв":       "За китом · тиша 3 хв",
    "F6_1хв_перший": "За китом · тиша 1 хв · перший постріл",
    "F4_розумний":  "За китом · швидкі гаманці",
    "F5_перший":    "За китом · швидкі гаманці · перший постріл",
    "F8_ratio35":   "За китом · тиша 1 хв · перший постріл · ratio ≥3.5",
    "F7_без_ратіо": "За китом · перший постріл · без ratio",
    "F9_без_ратіо_90": "За китом · перший постріл · без ratio · 90% швидких",
    "T1_твап_відкриття":  "TWAP-реверс · відкриття позиції",
    "T2_твап_скорочення": "TWAP-реверс · скорочення позиції",
    "T1_твап_відкриття_15":  "TWAP-реверс · відкриття · рух ≥1.5%",
    "T1_твап_відкриття_20":  "TWAP-реверс · відкриття · рух ≥2%",
    "T2_твап_скорочення_15": "TWAP-реверс · скорочення · рух ≥1.5%",
    "T2_твап_скорочення_20": "TWAP-реверс · скорочення · рух ≥2%",
}
STRAT2_DESC = {
    "R1_загальний": "Проти кита: він повністю злив позицію за <5 хв, і між "
                    "його першим і останнім філом монета впала ≥1% — "
                    "ставимо на відскок, тримаємо 30 хв",
    "R2_breakout":  "Те саме, але чекаємо підтвердження: вхід лише коли "
                    "ціна відбилась на +0.3% від дна (вікно 10 хв)",
    "R3_великі":    "Відскок тільки на ZEC і HYPE (вибір користувача)",
    "R4_великий":   "Відскок тільки після сильного падіння ≥2% за епізод",
    "R5_дуже":      "Відскок тільки після екстремального падіння ≥3% за епізод",
    "R6_волт":      "Реверс проти волта (механічний вивід коштів вкладників): "
                    "ПОВНЕ закриття, епізод <5 хв, падіння ≥1% (v2.16: шматок "
                    "≥5% без повного закриття — не сигнал)",
    "R7_одним":     "Проти кита: ВСЯ позиція закрита однією транзакцією "
                    "≥$100k, падіння ≥1% — вхід одразу",
    "R8_тп80":      "Проти кита, як R1, але з уявним тейк-профітом: монета "
                    "впала на X% — виходимо, щойно відновила 80% від X "
                    "(падіння 1% → +0.8%); не відновила — таймер 30 хв",
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
    "F8_ratio35":   "Як «тиша 1 хв · перший постріл» (профіль гаманця не "
                    "вимагається), але позиція кита на момент входу має "
                    "ratio ≥3.5 (не 2)",
    "F9_без_ратіо_90": "Як «перший постріл · без ratio», але гаманець "
                    "має ≥90% швидких розвантажень (замість 70%)",
    "T1_твап_відкриття": "TWAP 2–15 хв, яким кит ВІДКРИВАЄ НОВУ позицію "
                    "(до твапу позиції в монеті не було — startPosition "
                    "першого слайсу на біржі = 0; доливи й перевороти не "
                    "рахуються), не скасований, ціна за час TWAP пройшла "
                    "≥1% у його бік (закриття хвилинних свічок біржі перед "
                    "стартом і перед останньою хвилиною) — в останню "
                    "хвилину входимо ПРОТИ, тримаємо 1 год; крива 120 хв; "
                    "когорти ≥1.5% і ≥2%",
    "T2_твап_скорочення": "Те саме, але TWAP СКОРОЧУЄ наявну позицію кита "
                    "(протилежний бік, розмір твапу ≤ позиції)",
    "T1_твап_відкриття_15":  "Як T1, але вхід лише при русі ≥1.5% — окрема "
                    "стратегія зі своїми угодами (не фільтр T1)",
    "T1_твап_відкриття_20":  "Як T1, але вхід лише при русі ≥2%",
    "T2_твап_скорочення_15": "Як T2, але вхід лише при русі ≥1.5%",
    "T2_твап_скорочення_20": "Як T2, але вхід лише при русі ≥2%",
}

REV_SIG_HEADERS = ["sig_id", "date", "coin", "fade_side", "whale_addr", "src",
                   "px", "move_3m_pct", "dur_s", "sum_usd", "usd_s", "ratio",
                   "shtanga", "vault", "hour", "btc_move_pct", "btc_ok",
                   "depth_usd", "opened", "algo_v",
                   # v2.16: епізод закриття (перший→останній філ), вік філа,
                   # грейс, джерело детекції
                   "dump_dur_s", "dump_move_pct", "dump_bucket", "lag_s",
                   "grace", "detect_src", "eol"]
REV_HEADERS = (["sig_id", "strategy", "date", "coin", "our_side", "whale_addr",
                "src", "detect_px", "entry_px", "entered", "move_3m_pct",
                "dur_s", "sum_usd", "usd_s", "ratio", "shtanga", "vault",
                "hour", "btc_move_pct", "depth_usd", "costs_pct", "peak_pct",
                "trough_pct", "algo_v",
                # v2.16: чесний вхід (стакан), вік філа/міда, грейс, епізод
                # закриття, вирішений вихід (m30 зі стакану / TP R8)
                "entry_ts_ms", "fill_ts_ms", "lag_s", "detect_src",
                "entry_src", "entry_px_mid", "px_age_ms", "whale_px", "grace",
                "dump_first_ts_ms", "dump_last_ts_ms", "dump_dur_s",
                "dump_move_pct", "dump_bucket", "exit_min", "exit_reason",
                "exit_ts_ms", "exit_px", "exit_src", "tp_px"]
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
                  "prof_window_d",
                  # v2.16: мс-мітки, вік філа кита і міда, джерела цін входу
                  # й виходу, ціна філа кита, грейс
                  "open_ts_ms", "close_ts_ms", "fill_ts_ms", "lag_s",
                  "detect_src", "entry_src", "entry_px_mid", "px_age_ms",
                  "whale_px", "exit_src", "exit_px_mid", "grace", "eol"]
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
                       "first_shot", "pair_gap_s",
                       # v2.16
                       "fill_ts_ms", "lag_s", "detect_src", "grace", "entry_src"]
                      + [f"m{i}" for i in range(1, REV_TRACK_MIN + 1)] + ["eol"])

# OUTCOME-стрічка: шлях ціни КОЖНОГО сигналу від детекту, незалежно від
# BTC-вето/зайнятості/breakout — спільна база для чесного порівняння
# фільтрів на одних і тих самих подіях (аудит 29.08, п.7)
REV_OUT_CSV = os.path.join(DATA_DIR, "rev_outcomes.csv")
REV_OUT_HEADERS = (["sig_id", "date", "coin", "fade_side", "whale_addr",
                    "src", "detect_px", "move_3m_pct", "dur_s", "sum_usd",
                    "usd_s", "ratio", "shtanga", "vault", "hour",
                    "btc_move_pct", "btc_ok", "would_open", "depth_usd",
                    "costs_pct", "peak_pct", "trough_pct", "algo_v",
                    # v2.16
                    "fill_ts_ms", "lag_s", "detect_src", "grace",
                    "dump_dur_s", "dump_move_pct", "dump_bucket", "entry_src"]
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
_state_bak_ts     = [0.0]
# v2.13: коли стратегію (її since-версію) увімкнули — знаменник швидкості
# «позицій/день» рахується від активації, а не від першої угоди (рев'ю
# v2.12 №7b: дні без сигналів теж дні). st -> {"since": ver, "ts": epoch}
strat_activated   = {}

def _init_strat_activation(now=None):
    now = now if now is not None else time.time()
    for st, ver in STRAT_SINCE.items():
        rec = strat_activated.get(st)
        if not isinstance(rec, dict) or rec.get("since") != ver:
            strat_activated[st] = {"since": ver, "ts": now}
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
            # рядок НЕ тієї ширини — це баг викликача, а не дані: писати
            # його означає карантин на читанні при вже видаленому
            # трекері (аудит v2.10) — відмова голосна, WFAIL-цикл
            # викличе дроп із логом
            if headers and len(row) != len(headers):
                print(f"  [STRAT] {os.path.basename(path)}: рядок "
                      f"{len(row)} колонок при заголовку {len(headers)} — "
                      f"ВІДМОВА запису")
                return False
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
px_min  = {}   # coin -> [(ts, px), ...] — перший семпл кожної хвилини, 4 год (v2.11)
PX_MIN_KEEP = 240

def _px_at(coin, ts, tol=90.0):
    """Ціна монети НА момент ts: найближчий семпл ДО ts (не після — v2.16,
    рев'ю: семпл після точки був підгляданням у майбутнє для P0 TWAP) із
    хвилинної або 5-секундної історії, не далі tol секунд; None — нема."""
    with px_lock:
        pts = list(px_min.get(coin) or ()) + list(px_hist.get(coin) or ())
    best = None
    for t, p in pts:
        if t > ts:
            continue
        d = ts - t
        if best is None or d < best[0]:
            best = (d, p)
    return best[1] if best is not None and best[0] <= tol else None

_px_pending = {}   # coin -> (ts, px): різкий стрибок міда чекає підтвердження

def run_px_poller():
    while True:
        t0 = time.time()
        try:
            mids = sim_all_mids(retries=1)
        except Exception:
            mids = None
        if mids:
            # v2.16 (рев'ю): мітка семпла — час ВІДПРАВКИ запиту (t0), а не
            # відповіді: вік ціни не занижується на RTT; лічильники живості
            # фіду — у /status і вотчдогу
            now = t0
            stats["px_last_ok"] = time.time()
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
                    # делістнута монета висить в allMids із замороженою
                    # ціною — для нас це «ціни немає» (рев'ю v2.15)
                    if c in _hl_delisted: continue
                    h = px_hist.setdefault(c, [])
                    # захист від одиничного викиду (порожній бік стакану
                    # → мід далеко від ринку на один семпл): стрибок >25%
                    # проти попереднього семпла чекає підтвердження
                    # НАСТУПНИМ семплом; реальний рух підтверджується за 5 с
                    if h:
                        _prev = h[-1][1]
                        _pend = _px_pending.get(c)
                        if abs(px / _prev - 1.0) > 0.25:
                            # підтвердження: другий семпл поруч із першим АБО
                            # продовжує рух у той самий бік (швидкий обвал —
                            # не викид; рев'ю v2.16)
                            _same = (_pend is not None
                                     and (px > _prev) == (_pend[1] > _prev))
                            if _pend is not None and (abs(px / _pend[1] - 1.0) <= 0.10 or _same):
                                h.append(_pend)           # підтверджено — обидва
                                _px_pending.pop(c, None)
                            else:
                                _px_pending[c] = (now, px)
                                continue
                        else:
                            _px_pending.pop(c, None)
                    h.append((now, px))
                    if len(h) > 50: del h[:len(h) - 50]
                    # хвилинна історія на 4 год (TWAP-реверс, v2.11 п.5):
                    # ціна «за хвилину до старту» твапу на 15 хв може бути
                    # старша за 4-хв вікно px_hist
                    # рев'ю v2.16: мітка — СПРАВЖНІЙ час семпла (не підлога
                    # хвилини): _px_at «до ts» інакше віддавав ціну першого
                    # семпла хвилини, зробленого ПІСЛЯ філа
                    hm = px_min.setdefault(c, [])
                    if not hm or int(hm[-1][0] // 60) != int(now // 60):
                        hm.append((now, px))
                        if len(hm) > PX_MIN_KEEP: del hm[:len(hm) - PX_MIN_KEEP]
        else:
            stats["px_fail"] = stats.get("px_fail", 0) + 1
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
REV_REF_TOL_S = 12.0   # семпл-референс не старший за це (поллер — кожні 5 с)

def _rev_ref_px(coin, first_ts_ms, first_px):
    """v2.16 п.7: референс «до закриття» для руху епізоду — мід ДО першого
    close-філа кита (5-с семпл поллера, не пізніший за філ). Аудит №6:
    допуск лише REV_REF_TOL_S=12 с — 80-секундний мід давав фальшивий
    «дамп 1% за 2 с» із руху, що стався ДО закриття. Нема свіжого семпла
    — СИРА ціна першого філа (консервативно: удар першого ордера не
    рахується у рух)."""
    if first_ts_ms:
        p = _px_at(coin, first_ts_ms / 1000.0 - 0.001, tol=REV_REF_TOL_S)
        if p:
            return p
    return first_px

def rev_on_close(addr, coin, old, mfills, full_close, detect_src="sweep"):
    """Викликається на кожен підтверджений батч закриттів, ДО того як
    fc_on_full_close зніме епізод (фічі dur/sum ще доступні)."""
    if not STRAT2_ENABLED or not mfills: return
    # ratio-гейт (аудит v2.10 №3): сигнал = позиція, ВЕЛИКА відносно
    # ліквідності — те саме правило, що v1.3 тримає для алертів.
    # v2.11: з грейс-вікном — пара, що була ≥2 менш як пів години тому,
    # ще велика (друга половина зливу не губиться, ТЗ 08.09 п.1)
    if not _ratio_ok(old): return
    # хард-фільтр: позиція < $50k — не сигнал і не статистика (кейс
    # MET). Аудит v2.10 №1: при ПОВНОМУ закритті old["val"] — це
    # залишок перед ФІНАЛЬНИМ батчем ($5k хвоста після зливу $195k),
    # а розмір ПОДІЇ — позиція на старті епізоду продажу (ep["val"]).
    with fc_lock:
        _ep_gate = fc_episodes.get((addr, coin))
        _ep_gate_val = (_ep_gate or {}).get("val") or 0
    _event_val = _grace_val(old)   # v2.12: у грейсі — якір big_val
    if full_close:
        _event_val = max(_event_val, _ep_gate_val)
    if _event_val < MIN_POS_USD: return
    base_sz = old.get("size") or 1e-12
    # шматок волта: >=5% позиції І >= $5k (той самий доларовий поріг,
    # що в алертах і follow — рев'ю v2.5 п.6: 5% від $60k = $3k пил)
    big_any = any(f["sz"] >= (f.get("sp") or base_sz) * VAULT_PART_PCT
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
    # аудит v2.16 №16: R6 — теж лише ПОВНЕ закриття (єдине епізодне
    # правило п.7 для всіх реверсів); шматок волта ≥5% без повного
    # закриття — не сигнал і не тінь
    if vault and full_close:
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
    # ── v2.16 п.7: кваліфікація за ЕПІЗОДОМ закриття кита, а не за 3-хв
    # вікном кешу мідів (аудит: вікно було прив'язане до часу рішення й
    # до застарілого міда). Епізод = серія його close-філів (fc_on_txs,
    # уже викликаний): від ціни ПЕРШОГО філа до ціни ОСТАННЬОГО, за
    # біржовими цінами й часом; коротший за REV_EP_MAX_S і з падінням
    # ≥REV_EP_MIN_MAG у бік закриття. Когорта = ceil(тривалість, хв) 1..5
    with fc_lock:
        _ep0 = fc_episodes.get((addr, coin))
        ep = dict(_ep0) if _ep0 else None
    _txs = sorted(mfills, key=lambda f: f.get("ts") or 0)
    fill_ts_ms = int(_txs[-1].get("ts") or 0)
    # аудит v2.16 №6: кінець зливу — СИРИЙ останній філ (VWAP ордера, що
    # пройшов кілька рівнів, ховає кінець проходу)
    last_px = float(_txs[-1].get("px_last") or _txs[-1]["px"])
    if ep and ep.get("first_ts") and ep.get("first_px"):
        dump_first_ts, dump_first_px = int(ep["first_ts"]), float(ep["first_px"])
    else:
        dump_first_ts = int(_txs[0].get("ts") or fill_ts_ms)
        dump_first_px = float(_txs[0].get("px_first") or _txs[0]["px"])
    dump_last_ts = max(fill_ts_ms, int((ep or {}).get("last_ts") or 0))
    dump_dur = (max(0.0, (dump_last_ts - dump_first_ts) / 1000.0)
                if dump_first_ts and dump_last_ts else 0.0)
    ref_px = _rev_ref_px(coin, dump_first_ts, dump_first_px)
    dump_move = (last_px / ref_px - 1.0) * 100.0 if ref_px else 0.0
    mag = -dump_move if side == "LONG" else dump_move   # падіння У БІК закриття
    dump_bucket = (max(1, int(math.ceil(dump_dur / 60.0)))
                   if dump_dur < REV_EP_MAX_S else 0)
    now = time.time()
    lag_s = (now - fill_ts_ms / 1000.0) if fill_ts_ms else None
    # старий 3-хв рух по кешу — лише як довідкова колонка move_3m_pct
    _pn = _px_now(coin); _pa = _px_ago(coin, REV_WINDOW_S)
    move3 = ((_pn / _pa - 1.0) * 100.0) if (_pn and _pa) else None
    mag3 = ((-move3 if side == "LONG" else move3) if move3 is not None else None)
    # outcome-стрічка пише від 0.5% (перевірка нижчих порогів у
    # майбутньому); стратегії відкриваються від 1% за <5 хв
    if mag + 1e-9 < REV_OUT_MIN_MAG: return
    below_threshold = (mag + 1e-9 < REV_EP_MIN_MAG) or (dump_dur >= REV_EP_MAX_S)
    # ціна входу — зі СВІЖОГО стакану з нашого боку (проти кита: його
    # LONG закрито → ми купуємо), без стакану — гірша з міда й ціни
    # останнього філа кита (v2.16 п.1)
    # аудит v2.16 №1: стратегії відкриваються ЛИШЕ за повним свіжим
    # стаканом; без нього — тільки тіньова outcome-стрічка від міда
    px_entry, entry_src, pmeta = _paper_px(coin, "BUY" if side == "LONG" else "SELL",
                                           whale_px=last_px, strict=True)
    no_book = not px_entry
    if no_book:
        stats["rev_no_book"] = stats.get("rev_no_book", 0) + 1
        _mid0, _age0 = _px_mid_age(coin)
        if _mid0 is None or (_age0 or 0) > 20_000:
            stats["rev_no_px"] = stats.get("rev_no_px", 0) + 1
            return
        px_entry, entry_src = _mid0, "mid"
    t_entry = time.time()   # ціна відома САМЕ зараз (запит стакану міг тривати секунди)
    px_now = pmeta.get("book_mid") or pmeta.get("mid") or px_entry
    # аудит-3 (залишкове): лаг ВИКОНАННЯ (після запиту стакану) — саме він
    # гейтить вхід і пишеться у рядок; лаг рішення — у журнал
    lag_dec = lag_s
    lag_s = (t_entry - fill_ts_ms / 1000.0) if fill_ts_ms else None
    stale = lag_s is not None and lag_s > REV_MAX_FILL_AGE_S
    if stale:
        stats["rev_stale_skips"] = stats.get("rev_stale_skips", 0) + 1
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
    dur = dump_dur
    sum_usd = ep["sum_usd"] if ep else sum(f["px"] * f["sz"] for f in mfills)
    usd_s = sum_usd / dur if dur > 0 else 0.0
    shtanga = int((0 < dur < 30) or (usd_s and usd_s < 5000) or dur == 0)
    hour = time.localtime().tm_hour
    depth = _sim_depth(coin, side)
    # ratio у рядку: при повному закритті — на СТАРТІ епізоду (рев'ю v2.12
    # №7a), інакше — поточний (часткові закриття оновлюють його свіжим)
    ratio = (((_ep_gate or {}).get("ratio") or old.get("ratio", 0) or 0)
             if full_close else (old.get("ratio", 0) or 0))
    grace = int((old.get("ratio") or 0) < 2.0)   # v2.16 п.4
    # унікальність: мс + монета + гаманець + hash транзакції-тригера
    _txh = str(mfills[-1].get("hash", ""))[2:10]
    _sig_seq[0] += 1
    sig_id = f"{int(now * 1000)}-{coin}-{addr[2:8]}-{_txh}-{_sig_seq[0]}"
    # «одним пострілом» (ТЗ 04.09 п.2): повне закриття, у якому ОДНА
    # маркет-транзакція закрила ≥95% позиції і коштувала ≥$100k.
    # Аудит v2.10 №2: звіряємось із ЕПІЗОДОМ, а не з поточним батчем —
    # fc_on_txs (викликаний перед нами) веде start_size (токени на
    # старті серії) і найбільшу tx усього епізоду (max_sz/max_usd):
    # (а) велика tx у попередньому батчі + пил у фінальному — R7 бачить
    #     її через ep, хоч у mfills її вже немає;
    # (б) частка рахується в ТОКЕНАХ (max_sz/start_size) — рух ціни під
    #     час зливу не дає ні хибних спрацювань, ні пропусків;
    # (в) $100k — окремий абсолютний поріг по нотіоналу самої tx.
    # Ліквідація — не рішення кита, R7 не відкриває (рев'ю v2.9 S2).
    # Фолбек без епізоду (рестарт посеред зливу): чесна перевірка по
    # поточному батчу проти old (свідомо гірша, але не мовчання).
    one_shot = 0
    if full_close:
        if ep and ep.get("start_size"):
            one_shot = int(
                ep.get("max_sz", 0) >= ep["start_size"] * F4_FULL_PCT
                and ep.get("max_usd", 0) >= R7_MIN_TX_USD
                and not ep.get("max_liq"))
        elif mfills:
            _big_tx = max(mfills, key=lambda f: f["sz"])
            if _big_tx["sz"] >= base_sz * F4_FULL_PCT \
               and _big_tx["px"] * _big_tx["sz"] >= R7_MIN_TX_USD \
               and not _big_tx.get("liq"):
                one_shot = 1
    if below_threshold or src == "partial" or stale or no_book:
        strats = []   # 0.5-1%, довгий епізод, часткове, старий філ або без стакану: лише outcome
    elif src == "vault":
        strats = ["R6_волт"]
    else:
        strats = ["R1_загальний", "R2_breakout", R8_NAME]
        if coin in BIG_COINS: strats.append("R3_великі")
        if mag + 1e-9 >= 2.0: strats.append("R4_великий")
        if mag + 1e-9 >= 3.0: strats.append("R5_дуже")
        if one_shot: strats.append(R7_NAME)   # вхід одразу, як R1
    _journal("rev_decision", sig_id=sig_id, addr=addr, coin=coin, side=side, src=src,
             detect_src=detect_src, ref_px=ref_px, first_px=dump_first_px, last_px=last_px,
             dump_first_ts=dump_first_ts, dump_last_ts=dump_last_ts, dump_dur=round(dump_dur, 1),
             mag=round(mag, 4), bucket=dump_bucket, lag_s=(round(lag_s, 2) if lag_s is not None else None),
             lag_decision_s=(round(lag_dec, 2) if lag_dec is not None else None),
             stale=bool(stale), no_book=bool(no_book), entry_px=px_entry, entry_src=entry_src,
             mid=pmeta.get("mid"), mid_age_ms=pmeta.get("px_age_ms"),
             quote_ts_ms=pmeta.get("quote_ts_ms"), btc_ok=bool(btc_ok), below=bool(below_threshold),
             strats=list(strats))
    base_pos = {"sig_id": sig_id, "coin": coin,
                "side": side, "addr": addr, "src": src,
                "detect_ts": now, "detect_px": px_now,
                # move — довідковий 3-хв рух по кешу (як у старих рядках),
                # кваліфікація v2.16 — move_ep/dump_* за епізодом
                "move": (mag3 if mag3 is not None else mag), "move_ep": mag,
                "dur": dur, "sum_usd": sum_usd,
                "usd_s": usd_s, "ratio": ratio, "shtanga": shtanga,
                "vault": int(vault), "hour": hour,
                "btc_move": btc_move, "depth": depth,
                "costs": _sim_costs(coin, side)[0],   # v2.16: обидва боки стакану
                # версія — В МОМЕНТ створення: трекер, відновлений зі
                # старого state.json без версії, пишеться як legacy (""),
                # а не підписується поточною (аудит v2.2)
                "algo_v": DATA_ALGO_V,
                # v2.16: епізод, вік філа/міда, джерела, грейс
                "dump_first_ts": dump_first_ts, "dump_last_ts": dump_last_ts,
                "dump_dur": dump_dur, "dump_move": dump_move,
                "dump_bucket": dump_bucket,
                "fill_ts_ms": fill_ts_ms, "lag_s": lag_s, "detect_src": detect_src,
                "grace": grace, "entry_src": entry_src,
                "entry_px_mid": pmeta.get("mid"), "px_age_ms": pmeta.get("px_age_ms"),
                "whale_px": last_px,
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
        pos["entry_ts"] = t_entry
        pos["entry_px"] = px_now
        pos["btc_ok"] = int(btc_ok)
        pos["would_open"] = "+".join(strats)
        rev_open[f"{sig_id}|_OUTCOME"] = pos
        if btc_ok:
            # v2.16 (рев'ю №2): монета зайнята лише поки заголовкова угода
            # (m30 / TP) не закрита — трекер, що веде криву до m60, монету
            # не тримає; пропуски рахуємо
            busy = {(p["strategy"], p["coin"]) for p in rev_open.values()
                    if not p["strategy"].startswith("_")
                    and not p.get("done") and not _rev_trade_closed(p, now)}
            for st in strats:
                if (st, coin) in busy:
                    stats["rev_busy_skips"] = stats.get("rev_busy_skips", 0) + 1
                    continue
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
                    pos["entry_ts"] = t_entry
                    pos["entry_px"] = px_entry
                    if st == R8_NAME:
                        # уявний тейк-профіт: відновлення 80% падіння
                        pos["tp_px"] = (px_entry * (1 + R8_TP_FRAC * mag / 100.0)
                                        if side == "LONG"
                                        else px_entry * (1 - R8_TP_FRAC * mag / 100.0))
                rev_open[f"{sig_id}|{st}"] = pos
                opened.append(st)
    _sig_row = [sig_id, _dt(now), coin, side, addr, src, round(px_now, 8),
                (round(mag3, 3) if mag3 is not None else ""), round(dur, 1),
                round(sum_usd, 0),
                round(usd_s, 0), round(ratio, 2), shtanga, int(vault), hour,
                (round(btc_move, 3) if btc_move is not None else ""),
                int(btc_ok), round(depth or 0, 0), "+".join(opened),
                DATA_ALGO_V,
                round(dump_dur, 1), round(dump_move, 3), dump_bucket,
                (round(lag_s, 1) if lag_s is not None else ""), grace, detect_src]
    if not _strat_csv_append(REV_SIG_CSV, REV_SIG_HEADERS, _sig_row):
        # сигнал — вісь paired-порівнянь: без нього outcome/угоди висять
        # у повітрі. Невдалий запис іде в outbox (ретрай у циклі)
        with _sig_retry_lock:
            _sig_retry_q.append([REV_SIG_CSV, REV_SIG_HEADERS, _sig_row, 0])
    _btc_txt = f"{btc_move:+.2f}%" if btc_move is not None else "н/д"
    print(f"  [REV] сигнал {coin} fade={side} епізод {mag:+.2f}%/{dur:.0f}с "
          f"(к{dump_bucket}) src={src} ціна {entry_src} "
          f"btc={_btc_txt}{'' if btc_ok else ' VETO'}"
          f"{' STALE' if stale else ''} -> {'+'.join(opened) or '—'}")

def _rev_trade_closed(p, now):
    """Заголовкова угода реверсу закрита (v2.16): вирішений вихід (TP або
    таймер m30 зі стакану) або минуло REV_HOLD_MIN хвилин (+35 с допуску
    семпла). Трекер може ще вести криву до REV_TRACK_MIN, але монету для
    нової угоди тієї ж стратегії не тримає."""
    if p.get("exit_reason"):
        return True
    if p.get("state") == "armed":
        return False
    et = p.get("entry_ts") or 0
    if not et:
        return False
    return (len(p.get("samples") or ()) >= REV_HOLD_MIN
            or now - et >= REV_HOLD_MIN * 60.0 + 35.0)

def _rev_samples(p, entered):
    """Хвилинні семпли: float або "" (чесний пропуск після збою котирувань)."""
    if not entered:
        return [""] * REV_TRACK_MIN
    out = [("" if x == "" else round(x, 4))
           for x in p["samples"][:REV_TRACK_MIN]]
    return out + [""] * (REV_TRACK_MIN - len(out))

def _ms(ts):
    """Секунди (float) → цілі мс для CSV; None/0/"" → ""."""
    try:
        if ts is None or ts == "":
            return ""
        ts = float(ts)
        if not math.isfinite(ts) or ts <= 0:
            return ""
        return int(round(ts * 1000.0))
    except Exception:
        return ""

def _rnd(x, n):
    """round або "" для None/нечисла/non-finite (нові v2.16 колонки)."""
    try:
        if x is None or x == "":
            return ""
        x = float(x)
        if not math.isfinite(x):
            return ""
        return round(x, n)
    except Exception:
        return ""

def _rev_extra(p):
    """v2.16: 20 колонок REV_HEADERS після algo_v (перед m1..m60)."""
    return [_ms(p.get("entry_ts")), (p.get("fill_ts_ms") or ""),
            _rnd(p.get("lag_s"), 3), p.get("detect_src", ""),
            p.get("entry_src", ""), _rnd(p.get("entry_px_mid"), 8),
            (int(p["px_age_ms"]) if isinstance(p.get("px_age_ms"), (int, float))
             and math.isfinite(p["px_age_ms"]) else ""),
            _rnd(p.get("whale_px"), 8), p.get("grace", ""),
            # dump_first_ts/dump_last_ts у трекері вже в мс (час біржі)
            (int(p["dump_first_ts"]) if p.get("dump_first_ts") else ""),
            (int(p["dump_last_ts"]) if p.get("dump_last_ts") else ""),
            _rnd(p.get("dump_dur"), 1), _rnd(p.get("dump_move"), 3),
            p.get("dump_bucket", ""),
            p.get("exit_min", ""), p.get("exit_reason", ""),
            (p.get("exit_ts_ms") or ""), _rnd(p.get("exit_px"), 8),
            p.get("exit_src", ""), _rnd(p.get("tp_px"), 8)]

def _rev_row(p, entered):
    costs = _p_costs(p)
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
            + _rev_extra(p)
            + _rev_samples(p, entered))

def _fol_out_row(p):
    """Рядок тіньової хвилинної стрічки follow-входу (m1..60 у нашому
    напрямку від ціни входу)."""
    costs = _p_costs(p)
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
             p.get("first_shot", ""), p.get("pair_gap", ""),
             # v2.16
             (p.get("fill_ts_ms") or ""), _rnd(p.get("lag_s"), 3),
             p.get("detect_src", ""), p.get("grace", ""),
             p.get("entry_src", "")]
            + _rev_samples(p, 1))

def _rev_out_row(p):
    costs = _p_costs(p)
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
             p.get("algo_v", ""),
             # v2.16
             (p.get("fill_ts_ms") or ""), _rnd(p.get("lag_s"), 3),
             p.get("detect_src", ""), p.get("grace", ""),
             _rnd(p.get("dump_dur"), 1), _rnd(p.get("dump_move"), 3),
             p.get("dump_bucket", ""), p.get("entry_src", "")]
            + _rev_samples(p, 1))

def _fol_row(p, fid, close_ts, exit_px, reason, g):
    """Заморожений рядок follow-угоди (FOLLOW_HEADERS без eol). v2.16: ціна
    виходу — зі стакану в момент рішення (exit_src), плюс мс-мітки, вік
    філа кита і міда на вході, джерела цін, ціна філа кита, грейс."""
    costs = _p_costs(p)
    costs_eff = _leg_costs(costs, p.get("entry_src"), p.get("exit_src"))
    _bm = p.get("btc_move")
    pr = p.get("prof") or {}
    return [_dt(p["open_ts"]), _dt(close_ts),
            p["strategy"], p["coin"], p["our_side"], p["addr"],
            round(p["entry_px"], 8), round(exit_px, 8), reason,
            round(close_ts - p["open_ts"], 1), round(g, 4),
            round(costs, 4), round(g - costs_eff, 4),
            round(p["peak"], 4), round(p["trough"], 4),
            round(p["tx_pct"], 3), round(p["tx_usd"], 0),
            round(p["ratio"], 2), round(p["pos_usd"], 0),
            p["vault"], p["hour"],
            (round(_bm, 3) if _bm is not None else ""),
            round(p["profile_gap"], 1), p.get("algo_v", ""), fid,
            # v2.8: перший постріл/пауза пари і профіль швидкості на момент
            # входу (трекери зі старого state.json цих полів не мають)
            p.get("first_shot", ""), p.get("pair_gap", ""),
            pr.get("n_fast", ""), pr.get("n_slow", ""),
            (pr.get("fast_pct") if pr.get("fast_pct") is not None else ""),
            (pr.get("unload_med_s") if pr.get("unload_med_s") is not None else ""),
            (pr.get("unload_mean_s") if pr.get("unload_mean_s") is not None else ""),
            (pr.get("window_d") if pr.get("window_d") is not None else ""),
            # v2.16
            _ms(p["open_ts"]), _ms(close_ts), (p.get("fill_ts_ms") or ""),
            _rnd(p.get("lag_s"), 3), p.get("detect_src", ""),
            p.get("entry_src", ""), _rnd(p.get("entry_px_mid"), 8),
            (int(p["px_age_ms"]) if isinstance(p.get("px_age_ms"), (int, float))
             and math.isfinite(p["px_age_ms"]) else ""),
            _rnd(p.get("whale_px"), 8), p.get("exit_src", ""),
            _rnd(p.get("exit_px_mid"), 8), p.get("grace", "")]

# ── ВХІД У БІК ТИСКУ ────────────────────────────────────
def follow_on_txs(addr, coin, old, mfills, full_close, detect_src="sweep"):
    if not STRAT2_ENABLED or not mfills: return
    key = f"{addr}:{coin}"
    now = time.time()
    with strat2_lock:
        # пауза пари ДО оновлення мітки: «перший постріл» (F5) = раніше
        # закриттів цієї пари не бачили АБО минуло ≥1 год (ТЗ 01.09 п.3)
        prev_close = follow_last_close.get(key)
        # аудит-3 №8: мітка останнього закриття пари — БІРЖОВИЙ час
        # останнього реального філа батча, не час отримання (старий філ,
        # отриманий пізно, зсував таймер тиші відкритої угоди на затримку
        # доставки); без часу у філах — час отримання, як раніше
        _last_fill_ms = max((f.get("ts") or 0) for f in mfills) or None
        _close_t = (_last_fill_ms / 1000.0) if _last_fill_ms else now
        follow_last_close[key] = max(prev_close or 0.0, min(_close_t, now))
        if full_close:
            for p in follow_open.values():
                if p["key"] == key:
                    p["force_exit"] = "full_close"
                    # v2.16 (рев'ю: вихід full_close за мідом ДО філа): час
                    # останнього філа кита і час, коли дізнались — вихід
                    # рахується зі стакану в момент рішення, а спізнення
                    # >60 с маркується full_close_late (поза заголовком)
                    p.setdefault("force_exit_ts", now)
                    if _last_fill_ms:
                        p["force_fill_ts_ms"] = _last_fill_ms
    if full_close: return   # кит уже все закрив — заходити пізно
    pair_gap = (now - prev_close) if prev_close else None
    first_shot = int(pair_gap is None or pair_gap >= F5_FIRST_SHOT_S)
    # хард-фільтри (кейс MET: позиція $17k, шматки по $1k, ratio 0.15 —
    # 12 сміттєвих угод): дрібна позиція/транзакція — не сигнал;
    # v2.12: у грейсі розмір події — по якорю big_val
    if _grace_val(old) < MIN_POS_USD: return
    # ratio-гейт (аудит v2.10 №3): як у rev і в алертах — позиція, що
    # більше не є великою відносно ліквідності, не відкриває F-стратегій;
    # v2.11: з грейс-вікном пів години після падіння нижче 2
    if not _ratio_ok(old): return
    base_sz = old.get("size") or 1e-12
    # база відсотка — startPosition самої tx (аудит v2.10 №7)
    big = [f for f in mfills if f["sz"] >= (f.get("sp") or base_sz) * FOLLOW_TX_PCT
           and f["px"] * f["sz"] >= MIN_TX_USD]
    if not big: return
    # v2.16 (рев'ю v2.15 №1): paper-вхід лише за СВІЖИМ філом кита. Після
    # рестарту/паузи перший прохід підтверджує філи до години давності —
    # вхід «зараз» у рух, що вже відбувся, не є стратегією; алерти й
    # тінь пари від цього не залежать. Вік філа і джерело детекції — у
    # рядок кожної угоди.
    # аудит-3 №9: спершу набір СВІЖИХ придатних транзакцій (вік ≤20 с за
    # біржовим часом; філ без часу — не буває у HL, гейт до нього не
    # застосовується), і лише з них — найбільша (тригер). Раніше найбільша
    # стара tx відкидала весь батч разом зі свіжою придатною
    fresh = [f for f in big if not f.get("ts")
             or now - int(f["ts"]) / 1000.0 <= FOLLOW_MAX_FILL_AGE_S]
    if not fresh:
        _old = max(big, key=lambda f: f["sz"])
        _lag0 = now - int(_old["ts"]) / 1000.0
        stats["follow_stale_skips"] = stats.get("follow_stale_skips", 0) + 1
        print(f"  [FOLLOW] {coin}: філ кита {_lag0:.0f}с тому "
              f"(>{FOLLOW_MAX_FILL_AGE_S:.0f}с, {detect_src}) — paper-вхід пропущено")
        return
    tx = max(fresh, key=lambda f: f["sz"])
    fill_ts_ms = int(tx.get("ts") or 0)
    lag_s = (now - fill_ts_ms / 1000.0) if fill_ts_ms else None
    # «перший постріл» — властивість ТРАНЗАКЦІЇ, не батча (аудит v2.10
    # №6): якщо в цьому ж батчі БУЛА старша транзакція (навіть дрібна,
    # нижче порога), вона вже використала перший постріл пари
    if first_shot and any((f.get("ts") or 0) < (tx.get("ts") or 0)
                          for f in mfills):
        first_shot = 0
    our = "SHORT" if old.get("side") == "LONG" else "LONG"
    # v2.16 (аудит: paper-вхід за застарілим мідом): ціна входу — ЛИШЕ зі
    # СВІЖОГО повного стакану з нашого боку (виконувана на $1000); без
    # нього paper-угоди немає (аудит №1: мід і ціна філа кита не є доказом
    # ціни після зливу) — пропуск рахується
    px, entry_src, pmeta = _paper_px(coin, "SELL" if our == "SHORT" else "BUY",
                                     whale_px=tx["px"], strict=True)
    if not px:
        stats["follow_no_book"] = stats.get("follow_no_book", 0) + 1
        _journal("follow_skip", addr=addr, coin=coin, why="no_book",
                 fill_ts_ms=fill_ts_ms, lag_s=lag_s, detect_src=detect_src)
        return
    t_entry = time.time()   # ціна відома САМЕ зараз — від цього моменту угода
    # аудит-3 (залишкове): лаг ВИКОНАННЯ — після запиту стакану; рішення
    # «свіжий» за 19 с не дає права входити на 25-й секунді
    lag_dec, lag_s = lag_s, ((t_entry - fill_ts_ms / 1000.0) if fill_ts_ms else None)
    if lag_s is not None and lag_s > FOLLOW_MAX_FILL_AGE_S:
        stats["follow_stale_skips"] = stats.get("follow_stale_skips", 0) + 1
        _journal("follow_skip", addr=addr, coin=coin, why="stale_exec",
                 fill_ts_ms=fill_ts_ms, lag_s=round(lag_s, 2), lag_decision_s=round(lag_dec, 2),
                 detect_src=detect_src)
        return
    vault = is_vault(addr)
    hour = time.localtime().tm_hour
    b_now = _px_now("BTC"); b_ago = _px_ago("BTC", REV_WINDOW_S)
    # немає даних BTC -> None (у CSV порожньо), а не фальшивий 0.0
    btc_move = (b_now / b_ago - 1.0) * 100.0 if b_now and b_ago else None
    depth = _sim_depth(coin, old.get("side"))
    # грейс (v2.16 п.4): пара пройшла ratio-гейт лише завдяки 30-хв
    # грейсу після падіння ratio нижче 2 — позначка, щоб вимикати кнопкою
    grace = int((old.get("ratio") or 0) < 2.0)
    _journal("follow_entry", addr=addr, coin=coin, our=our, detect_src=detect_src,
             fill_ts_ms=fill_ts_ms, lag_s=(round(lag_s, 2) if lag_s is not None else None),
             lag_decision_s=(round(lag_dec, 2) if lag_dec is not None else None),
             tx_px=tx["px"], tx_sz=tx["sz"], px=px, src=entry_src, mid=pmeta.get("mid"),
             mid_age_ms=pmeta.get("px_age_ms"), quote_ts_ms=pmeta.get("quote_ts_ms"), grace=grace)
    base = {"key": key, "coin": coin, "our_side": our, "addr": addr,
            "open_ts": t_entry, "entry_px": px, "peak": -999.0, "trough": 999.0,
            "tx_pct": tx["sz"] / (tx.get("sp") or base_sz) * 100.0,
            "tx_usd": tx["px"] * tx["sz"],
            "ratio": old.get("ratio", 0) or 0,
            "pos_usd": old.get("val", 0) or 0,
            "vault": int(vault), "hour": hour, "btc_move": btc_move,
            "depth": depth, "costs": _sim_costs(coin, our)[0],
            "force_exit": None, "profile_gap": 0.0,
            "first_shot": first_shot,
            "pair_gap": (round(pair_gap, 1) if pair_gap is not None else ""),
            # профіль швидкості гаманця НА МОМЕНТ входу (v2.8): у рядок
            # кожної F-угоди, якщо профіль уже є — порівнювати когорти
            "prof": {},
            # v2.16: вік філа й міда, джерела, ціна філа кита, грейс
            "fill_ts_ms": fill_ts_ms, "lag_s": lag_s, "detect_src": detect_src,
            "entry_src": entry_src, "entry_px_mid": pmeta.get("mid"),
            "px_age_ms": pmeta.get("px_age_ms"), "whale_px": tx["px"],
            "grace": grace,
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
            # F8 (v2.12, рішення користувача): копія F6 + ratio пари на
            # момент входу ≥3.5; профіль гаманця не потрібен
            if (old.get("ratio") or 0) >= F8_MIN_RATIO:
                _timers.append((F8_NAME, F6_TIMER_S))
        for st, timer in _timers:
            if (st, coin) in busy:
                # v2.16 (рев'ю): пропуск рахуємо для ВСІХ стратегій; лог —
                # лише для рідкісних first-shot-когорт (рев'ю v2.9 S3)
                stats["follow_busy_skips"] = \
                    stats.get("follow_busy_skips", 0) + 1
                if st in (F6_NAME, F8_NAME):
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
            # F9 (ТЗ 08.09 п.4): та сама nr-гілка, але ≥90% швидких —
            # рахується з nr.n_fast/fast_pct на льоту, бамп профілю не
            # потрібен (обидва поля є з v8)
            # v2.12 (з рев'ю CH): поріг по ТОЧНИХ лічильниках, а не по
            # округленому fast_pct (89.99% у профілі записано як 90.0)
            _nf = int(nr_prof.get("n_fast") or 0)
            _ns = int(nr_prof.get("n_slow") or 0)
            _f9 = (_nf >= F4_MIN_EPISODES
                   and 100.0 * _nf >= F9_MIN_FAST_PCT * (_nf + _ns))
            _prof_strats.append((F9_NAME, timer7, first_shot and _f9, nr_prof))
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
                "detect_ts": t_entry, "detect_px": px,
                "entry_ts": t_entry, "entry_px": px,
                "tx_pct": base["tx_pct"], "tx_usd": base["tx_usd"],
                "ratio": base["ratio"], "pos_usd": base["pos_usd"],
                "vault": base["vault"], "hour": hour,
                "btc_move": btc_move, "depth": depth,
                "algo_v": DATA_ALGO_V,
                "first_shot": first_shot, "pair_gap": base["pair_gap"],
                "fill_ts_ms": fill_ts_ms, "lag_s": lag_s,
                "detect_src": detect_src, "grace": grace, "entry_src": entry_src,
                "samples": [], "peak": -999.0, "trough": 999.0}
    # фоновий підтяг історії — СУВОРО поза strat2_lock (аудит 29.08:
    # виклик зсередини критичної секції давав self-deadlock усього модуля)
    if need_profile:
        _profile_request(addr)
    print(f"  [FOLLOW] {coin} {our} слідом за {addr[:10]}… "
          f"(шматок {base['tx_pct']:.1f}%, ${base['tx_usd']:,.0f}; "
          f"ціна {entry_src}, філ {lag_s:.1f}с тому, {detect_src})"
          if lag_s is not None else
          f"  [FOLLOW] {coin} {our} слідом за {addr[:10]}… "
          f"(шматок {base['tx_pct']:.1f}%, ${base['tx_usd']:,.0f}; ціна {entry_src})")

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
        # "Short > Long" містить слово "Long", і стара перша гілка
        # хапала його як LONG — а закривається ШОРТ, глибина для ratio
        # бралась не з того боку стакану (аудит v2.10 №8). Матчимо
        # точно, як у live-детекторі (Close X / X > Y)
        if d.startswith("Close Long") or d.startswith("Long >"):
            side = "LONG"
        elif d.startswith("Close Short") or d.startswith("Short >"):
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
# v2.14 (аудит v2.13): бюджет СПІЛЬНИЙ для всього, що йде prio-каналом
# (hl_post_prio: перевірки невідомих китів, профілі, TWAP-звірка і
# свічки) — облік у hl_post_prio. Проксі: 800 із 1200/хв її IP (t.me-
# фолбек ваги не має); прямий канал — крихта, там живе детекція
PROFILE_W_PER_MIN = {"proxy": 800, "direct": 150}
_profile_w = {"proxy": deque(), "direct": deque()}
_profile_w_lock = threading.Lock()
_profile_proxy = {"dead_until": 0.0}   # проксі мертва → 30 хв напряму
_HL_LIGHT_TYPES = ("clearinghouseState", "l2Book", "allMids", "orderStatus",
                   "spotClearinghouseState", "exchangeStatus")

def _hl_weight(body, resp=None):
    """Вага info-запиту за докою HL: 2 для легких типів, інакше 20 (+1 за
    кожні 20 елементів списку у відповіді — userFills*/userTwapSliceFills)."""
    t = (body or {}).get("type") if isinstance(body, dict) else None
    if t in _HL_LIGHT_TYPES:
        return 2
    return 20 + (len(resp) // 20 if isinstance(resp, list) else 0)

def _profile_budget_wait(via, w_next=120, max_wait=65.0):
    """Резервує вагу w_next у вікні каналу АТОМАРНО — під локом, у момент
    рішення — і повертає True; якщо за max_wait місце не звільнилось —
    False, і викликач НЕ надсилає запит (аудит v2.14 №6: після стелі
    очікування запит ішов понад ліміт, два паралельні запити бачили ті
    самі вільні одиниці, спроба з 429 не рахувалась)."""
    cap = PROFILE_W_PER_MIN[via]
    deadline = time.time() + max_wait
    while True:
        now_ = time.time()
        with _profile_w_lock:
            dq = _profile_w[via]
            while dq and dq[0][0] < now_ - 60:
                dq.popleft()
            used = sum(w for _, w in dq)
            if used + w_next <= cap:
                dq.append((now_, w_next))   # резерв — ДО запиту, на кожну спробу
                return True
            oldest = dq[0][0] if dq else now_
        if now_ >= deadline:
            return False
        time.sleep(min(5.0, max(0.5, min(oldest + 60 - now_, deadline - now_))))

def _profile_budget_add(via, batch, body=None, reserved=0):
    """Фактична вага відповіді: перевищення над резервом дописується;
    резерв при збої лишається (біржа могла зарахувати запит)."""
    w = _hl_weight(body if body is not None else {"type": "userFillsByTime"}, batch)
    extra = w - (reserved or 0)
    if extra > 0:
        with _profile_w_lock:
            _profile_w[via].append((time.time(), extra))

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
        try:
            # бюджет проксі (очікування + облік) — усередині hl_post_prio;
            # резерв — повна сторінка історії (20 + 2000/20)
            return hl_post_prio(body, retries=1, w_next=120)
        except RateLimited:
            raise
        except Exception as e:
            _profile_proxy["dead_until"] = time.time() + 1800
            print(f"  [F4] проксі історії: {e} — 30 хв напряму")
    if not _profile_budget_wait("direct", 120):
        raise RateLimited("profile budget (direct) exhausted")
    # одна спроба на резерв: внутрішній ретрай hl_post ішов би без резерву
    # (рев'ю v2.15); 429 → RateLimited → ретрай профілю за 5 хв
    r = hl_post(body, retries=1)
    _profile_budget_add("direct", r, reserved=120)
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
        try:
            _strat2_tick()
        except Exception as e:
            # рев'ю аудит-2: один битий тик (несподіване поле, помилка
            # запису) не має вбивати потік стратегій назавжди — трекери
            # лишаються у state, наступний тик продовжує
            stats["strat2_tick_err"] = stats.get("strat2_tick_err", 0) + 1
            if stats["strat2_tick_err"] in (1, 10, 100, 1000):
                import traceback
                print(f"  [STRAT2] тик впав ({stats['strat2_tick_err']}): {e!r}")
                traceback.print_exc()

def _strat2_tick():
    """Один тик циклу стратегій (3 с): outbox сигнальних рядків, семпли
    трекерів, рішення входів/виходів, ціни зі стакану поза локом, запис
    CSV, save_state. Виділено з run_strat2_loop, щоб виняток одного тику
    ловився зовні (рев'ю аудит-2)."""
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
    if not active: return
    now = time.time()
    # Рядки збираємо БЕЗ видалення трекера: видаляємо після успішного
    # запису CSV (аудит v2.1 п.7). Завершений трекер отримує done=1 і
    # ЗАМОРОЖЕНИЙ final_row: ретрай пише ті самі байти, а не
    # перераховує результат новішою ціною — інакше тимчасовий збій
    # диска міняв exit/net завершеної угоди (аудит v2.2 п.1).
    # done не блокує busy; після WFAIL_CAP ретраїв — дроп із логом.
    # Дублі після краху знімає дедуп по sig_id/trade_id у strat2_api.
    rev_rows, fol_rows, tw_rows = [], [], []
    rev_exited = False   # v2.16: вирішений вихід/вхід R — стан треба зберегти одразу
    # v2.16: рішення про вхід/вихід ухвалюються під локом, а ЦІНА для них
    # береться зі свіжого стакану ПОЗА локом (запит ~0.1–3 с; strat2_lock
    # тримають і хуки воркерів) — списки запитів на ціну
    rev_price, fol_exit = [], []
    with strat2_lock:
        for pid, p in list(rev_open.items()):
            if p.get("done"):
                if p.get("row_kind") == "twap":
                    # v2.15 (аудит v2.14 №1): угода TWAP пишеться ОКРЕМИМ
                    # рядком у момент вирішеного виходу (нижче); тут —
                    # лише крива (twap_curves.csv). Рядок угоди ще не
                    # записаний (рестарт зі старого state, збій диска) —
                    # спершу він, крива наступним тиком: трекер не
                    # видаляється, доки не записані обидва
                    if not p.get("trade_written"):
                        if (p.get("trade_row") is not None
                                and len(p["trade_row"]) != len(TWAP_HEADERS) - 1):
                            # заморожений рядок старої схеми (рестарт після
                            # апдейту заголовків) — перебудувати з тих самих
                            # семплів (вихід детермінований), а не відмова
                            # запису щотику назавжди (рев'ю v2.15)
                            p["trade_row"] = None
                        if p.get("trade_row") is None:
                            if not p.get("trade_closed_ts"):
                                # відновлений done-трекер без мітки закриття
                                # (state v2.14): час виходу — хвилина
                                # виходу, не момент рестарту
                                _ex = _twap_exit(p, now)
                                p["trade_closed_ts"] = ((p.get("entry_ts") or now)
                                    + (_ex[0] if _ex else _twap_close_min(p)) * 60.0)
                            p["trade_row"] = _twap_row(p, now)
                            _twap_freeze_exit(p)
                        tw_rows.append((pid, p["trade_row"]))
                        continue
                    if (p.get("final_row") is None
                            or len(p["final_row"]) != len(TWAP_CURVE_HEADERS) - 1):
                        # заморожений рядок старої схеми (рестарт після
                        # апдейту) — перебудувати з тих самих семплів
                        p["final_row"] = _twap_curve_row(p)
                    rev_rows.append((pid, "twapc", p["final_row"]))
                    continue
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
                _ah = p.get("arm_hit")
                if hit and (not _ah or now - (_ah.get("ts") or 0) > 30.0):
                    # v2.16: ціна входу — зі свіжого стакану (запит поза
                    # локом, нижче), консервативно гірша з (стакан,
                    # тригер); застарілий arm_hit (аварія між тиками)
                    # переставляється
                    p["arm_hit"] = {"ts": now, "mid": px, "trig": trig}
                    rev_price.append((pid, "arm"))
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
            # горизонт треку — властивість трекера (v2.11: TWAP-реверс
            # веде 120 хв, решта — REV_TRACK_MIN=60)
            _tm = int(p.get("track_min") or REV_TRACK_MIN)
            target = min(int((now - p["entry_ts"]) // 60), _tm)
            while len(p["samples"]) < target:
                k = len(p["samples"]) + 1
                late = now - (p["entry_ts"] + k * 60.0)
                if g is not None and late <= 30.0:
                    p["samples"].append(round(g, 4))
                elif g is None and late <= 30.0:
                    # ціни нема, але 30-с допуск ще триває: пропуск
                    # НЕ закріплюємо — наступний тик (3с) може
                    # принести свіжу ціну (аудит v2.13 №6: три
                    # секунди без ціни назавжди спустошували m60)
                    break
                else:
                    p["samples"].append("")
            # v2.15 (аудит v2.14 №1/№5): TWAP-угода закривається у момент
            # ВИРІШЕНОГО виходу (_twap_exit) — рядок угоди заморожується
            # і пишеться одразу; далі трекер веде лише криву
            if p.get("row_kind") == "twap" and not p.get("trade_written"):
                if (p.get("trade_row") is not None
                        and len(p["trade_row"]) != len(TWAP_HEADERS) - 1):
                    p["trade_row"] = None   # стара схема — перебудувати (рев'ю v2.15)
                _tx = _twap_exit(p, now) if p.get("trade_row") is None else None
                if _tx is not None:
                    _fresh = now - (p["entry_ts"] + _tx[0] * 60.0) <= 35.0
                    _rt = p.get("exit_retry_at") or 0
                    if _tx[2] is None and _tx[1] != "no_price":
                        # аудит-3 №10: хвилина виходу настала, семпла міда немає —
                        # рішення за ЧАСОМ; ціна ЛИШЕ зі стакану (запит нижче,
                        # повтор через 15 с, поки вікно капу відкрите)
                        if now >= _rt:
                            _er = p.get("exit_req")
                            if _er and now - (_er.get("ts") or 0) > 30.0:
                                _er = None
                            if not _er:
                                p["exit_req"] = {"kind": "twap", "ts": now, "mid": px,
                                                 "ex": (_tx[0], _tx[1])}
                                rev_price.append((pid, "exit"))
                    elif not _fresh:
                        # хвилина виходу давно минула (рестарт/аварія посеред
                        # тику): стакан «зараз» — не та ціна; рядок — з семплів,
                        # детерміновано, як у v2.15 (рев'ю v2.16)
                        p["trade_closed_ts"] = p.get("trade_closed_ts") or now
                        p["trade_row"] = _twap_row(p, now)
                        _twap_freeze_exit(p)
                    elif now >= _rt:
                        # v2.16: ціна виходу — зі свіжого стакану (запит поза
                        # локом, нижче); рядок заморожується там же
                        _er = p.get("exit_req")
                        if _er and now - (_er.get("ts") or 0) > 30.0:
                            _er = None
                        if not _er:
                            p["exit_req"] = {"kind": "twap", "ts": now, "mid": px,
                                             "ex": (_tx[0], _tx[1])}
                            rev_price.append((pid, "exit"))
                if p.get("trade_row") is not None:
                    tw_rows.append((pid, p["trade_row"]))
            # v2.16: ВИРІШЕНИЙ ВИХІД заголовкової угоди реверсу — TP (R8,
            # мід перетнув tp_px) або таймер m30 (перша хвилина з ціною у
            # 30..33, далі — no_price); ціна виходу — зі свіжого стакану,
            # запит поза локом. Трекер далі веде криву до m60.
            if (p.get("row_kind") != "twap"
                    and not p["strategy"].startswith("_")
                    and not p.get("exit_reason")
                    and _vt(p.get("algo_v")) >= (2, 16)):   # старий трекер — стара семантика
                _er = p.get("exit_req")
                if _er and now - (_er.get("ts") or 0) > 30.0:
                    p.pop("exit_req", None); _er = None   # аварія між тиками
                if not _er and now >= (p.get("exit_retry_at") or 0):
                    n_s = len(p["samples"])
                    tp = p.get("tp_px")
                    tp_hit = bool(px and tp and (px >= tp if p["side"] == "LONG"
                                                 else px <= tp))
                    held = now - p["entry_ts"]
                    if tp_hit and held < REV_HOLD_MIN * 60.0:
                        p["exit_req"] = {"kind": "tp", "ts": now, "mid": px,
                                         "min": round(held / 60.0, 2)}
                        rev_price.append((pid, "exit"))
                    elif held >= REV_HOLD_MIN * 60.0:
                        # аудит-3 №10: таймерний вихід вирішується за ЧАСОМ, не за
                        # наявністю хвилинного семпла міда — ціну дає стакан
                        # (запит поза локом); без стакану й міда — повтор через
                        # 15 с у межах вікна 30..33 хв, далі семпл/no_price
                        late = held - REV_HOLD_MIN * 60.0
                        if late <= 3 * 60.0 + 35.0:
                            kind = "timer" if late <= 60.0 else "timer_late"
                            p["exit_req"] = {"kind": kind, "ts": now, "mid": px,
                                             "min": REV_HOLD_MIN + min(3, int(late // 60))}
                            rev_price.append((pid, "exit"))
                        elif n_s > REV_HOLD_MIN + 3:
                            # тик проґавив 30..33 (рестарт/довгий тик): ціну
                            # тієї хвилини бот ЗНАВ — вихід із семпла, а не
                            # no_price (рев'ю v2.16)
                            _k = next((k for k in range(REV_HOLD_MIN, REV_HOLD_MIN + 4)
                                       if p["samples"][k - 1] != ""), None)
                            if _k is not None:
                                _gs = float(p["samples"][_k - 1])
                                _sgn = 1.0 if p["side"] == "LONG" else -1.0
                                p["exit_px"] = p["entry_px"] * (1.0 + _sgn * _gs / 100.0)
                                p["exit_src"] = "sample"
                                p["exit_ts_ms"] = int(round((p["entry_ts"] + _k * 60.0) * 1000))
                                p["exit_min"] = _k
                                p["exit_reason"] = "timer" if _k == REV_HOLD_MIN else "timer_late"
                                rev_exited = True
                            else:
                                p["exit_reason"] = "no_price"
                                p["exit_min"] = ""
            if len(p["samples"]) >= _tm:
                p["done"] = 1
                if p["strategy"] == "_OUTCOME":
                    p["row_kind"] = "out"
                    p["final_row"] = _rev_out_row(p)
                elif p["strategy"] == "_FOLLOW_OUT":
                    p["row_kind"] = "fo"
                    p["final_row"] = _fol_out_row(p)
                elif p.get("row_kind") == "twap":
                    p["final_row"] = _twap_curve_row(p)
                    # криву пишемо лише після записаної угоди (порядок
                    # гарантує done-гілка наступного тику)
                    if p.get("trade_written"):
                        rev_rows.append((pid, "twapc", p["final_row"]))
                    continue
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
            if px:
                g = (px / p["entry_px"] - 1.0) * 100.0
                if p["our_side"] == "SHORT": g = -g
                p["peak"] = max(p["peak"], g)
                p["trough"] = min(p["trough"], g)
            # аудит v2.16 №13: умова виходу (повне закриття / таймер
            # тиші) не залежить від поллера мідів — ціну дає стакан
            if now < (p.get("exit_retry_at") or 0):
                continue
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
                # v2.16: рішення про вихід — тут; ЦІНА виходу — зі
                # свіжого стакану, запит поза локом (нижче); рядок
                # заморожується там же
                p["exit_pending"] = {"reason": reason, "ts": now, "mid": px}
                fol_exit.append(fid)
    # ── v2.16: ціни зі стакану для вирішених входів/виходів — ПОЗА локом ──
    if rev_price or fol_exit:
        priced, fpriced = [], []
        _pxc = {}   # (coin, bside) -> відповідь: 6 R-трекерів одного сигналу
                    # виходять на m30 одним тиком — один запит, не шість (рев'ю)
        _pass_t0 = time.time()
        def _ppx(coin, bside, strict=False):
            # strict (вхід R2 на пробої): ЛИШЕ повний свіжий стакан —
            # без міда/partial (рев'ю аудит-2: armed→open обходив
            # строге правило входу через фолбек міда)
            k = (coin, bside, strict)
            if k not in _pxc:
                if len(_pxc) >= 6 or time.time() - _pass_t0 > 12.0:
                    # кап на прохід: решта — з кеш-міда (тик не має
                    # тривати хвилину, інші трекери втрачають семпли);
                    # для входу мід не годиться → no_book
                    _mid, _age = _px_mid_age(coin)
                    if strict:
                        _pxc[k] = (None, "no_book", {"mid": _mid, "px_age_ms": _age})
                    else:
                        _pxc[k] = ((_mid if (_mid and (_age or 0) <= 35_000) else None),
                                   "mid", {"mid": _mid, "px_age_ms": _age})
                else:
                    _pxc[k] = _paper_px(coin, bside, max_mid_age_ms=35_000, strict=strict)
            return _pxc[k]
        for pid, what in rev_price:
            with strat2_lock:
                p = rev_open.get(pid)
                if p is None: continue
                coin, side = p["coin"], p["side"]
                req = p.get("arm_hit") if what == "arm" else p.get("exit_req")
            if not req: continue
            # відкриття LONG = BUY (аски), закриття LONG = SELL (біди)
            bside = (("BUY" if side == "LONG" else "SELL") if what == "arm"
                     else ("SELL" if side == "LONG" else "BUY"))
            xpx, xsrc, xmeta = _ppx(coin, bside, strict=(what == "arm"))
            priced.append((pid, what, xpx, xsrc, xmeta))
        for fid in fol_exit:
            with strat2_lock:
                p = follow_open.get(fid)
                if p is None or p.get("done") or not p.get("exit_pending"):
                    continue
                coin, side = p["coin"], p["our_side"]
            xpx, xsrc, xmeta = _ppx(coin, "BUY" if side == "SHORT" else "SELL")
            fpriced.append((fid, xpx, xsrc, xmeta))
        _jrn = []   # події журналу — пишуться ПІСЛЯ лока (диск не тримає strat2_lock)
        with strat2_lock:
            for pid, what, xpx, xsrc, xmeta in priced:
                p = rev_open.get(pid)
                if p is None: continue
                now2 = time.time()
                if what == "arm":
                    req = p.pop("arm_hit", None)
                    if not req or p["state"] != "armed": continue
                    if now2 >= p.get("deadline", 0):
                        # аудит v2.16 №14: запит стакану пережив вікно
                        # входу — входу заднім числом немає
                        p["done"] = 1
                        p["row_kind"] = "rev"
                        p["final_row"] = _rev_row(p, 0)
                        rev_rows.append((pid, "rev", p["final_row"]))
                        rev_exited = True
                        continue
                    if not xpx or xsrc != "book":
                        # аудит №1: вхід лише з ПОВНОГО стакану (strict віддає
                        # саме "book"; partial/mid — ні). arm_hit лишається
                        # (з тим самим ts) — повторний запит не раніше ніж
                        # за 30 с (гард переставлення), не щотику
                        # (рев'ю: без стакану — l2Book + save_state кожні 3 с)
                        p["arm_hit"] = req
                        continue
                    trig, mid = req["trig"], req["mid"]
                    base_px = xpx
                    # консервативно: гірша з цін (стакан / тригер)
                    p["entry_px"] = (max(base_px, trig) if p["side"] == "LONG"
                                     else min(base_px, trig))
                    p["entry_ts"] = now2
                    p["entry_src"] = xsrc
                    p["entry_px_mid"] = xmeta.get("mid") or mid
                    p["px_age_ms"] = xmeta.get("px_age_ms")
                    p["state"] = "open"
                    rev_exited = True
                    _jrn.append(("rev_arm_entry", dict(pid=pid, coin=p["coin"], px=p["entry_px"],
                                                       src=xsrc, quote_ts_ms=xmeta.get("quote_ts_ms"))))
                else:
                    req = p.pop("exit_req", None)
                    if not req: continue
                    mid = req.get("mid")
                    # аудит-3 №11: без фолбеку на мід із моменту рішення —
                    # _paper_px (нестрогий) уже віддав свіжий мід (≤35 с), якщо
                    # стакану нема; старіший мід відхилено — ціни немає
                    base_px = xpx
                    if not base_px:
                        # ні стакану, ні свіжого міда: наступна спроба через 15 с,
                        # а не щотику (стакан лежить → 20 запитів/хв на трекер)
                        p["exit_retry_at"] = now2 + 15.0
                    if req["kind"] == "twap":
                        # TWAP: вихід уже вирішений (_twap_exit) — ціна зі
                        # стакану в трекер, рядок угоди заморожується
                        if p.get("trade_row") is not None or p.get("trade_written"):
                            continue
                        if not base_px:
                            continue   # повтор через exit_retry_at; після капу — no_price
                        tw = p.get("twap")
                        if isinstance(tw, dict):
                            tw["exit_px"] = base_px
                            tw["exit_src"] = xsrc
                            tw["exit_ts_ms"] = int(round(now2 * 1000))
                        p["trade_closed_ts"] = p.get("trade_closed_ts") or now2
                        # причина/хвилина — ті, що ВИРІШЕНО у фазі 1 (рев'ю: перерахунок
                        # за now2 після капу давав no_price із ціною)
                        p["trade_row"] = _twap_row(p, now2, ex=req.get("ex"))
                        _twap_freeze_exit(p)
                        tw_rows.append((pid, p["trade_row"]))
                        continue
                    if p.get("exit_reason"): continue
                    if not base_px: continue   # ні стакану, ні свіжого міда — наступний тик
                    if req["kind"] == "tp" and p.get("tp_px"):
                        # лімітка на TP: не краще за TP і не краще за стакан
                        tp = p["tp_px"]
                        base_px = (min(base_px, tp) if p["side"] == "LONG"
                                   else max(base_px, tp))
                    p["exit_px"] = base_px
                    p["exit_src"] = xsrc
                    p["exit_ts_ms"] = int(round(now2 * 1000))
                    p["exit_min"] = req["min"]
                    p["exit_reason"] = req["kind"]
                    p["exit_px_mid"] = xmeta.get("mid") or mid
                    rev_exited = True
                    _gx = (base_px / p["entry_px"] - 1.0) * 100.0
                    if p["side"] == "SHORT": _gx = -_gx
                    _jrn.append(("rev_exit", dict(pid=pid, coin=p["coin"], exit_kind=req["kind"],
                                                  px=base_px, src=p["exit_src"], mid=mid,
                                                  quote_ts_ms=xmeta.get("quote_ts_ms"),
                                                  exit_min=req["min"])))
                    print(f"  [REV] {p['strategy']} {p['coin']} вихід "
                          f"{req['kind']} m{req['min']} @ {base_px:.6g} "
                          f"({p['exit_src']}) gross {_gx:+.2f}%")
            for fid, xpx, xsrc, xmeta in fpriced:
                p = follow_open.get(fid)
                if p is None or p.get("done"): continue
                req = p.pop("exit_pending", None)
                if not req: continue
                now2 = time.time()
                mid = req.get("mid")
                epx = xpx   # аудит-3 №11: лише свіжа ціна (стакан або мід ≤35 с з _paper_px)
                if not epx:
                    p["exit_retry_at"] = now2 + 15.0   # ні стакану, ні свіжого міда
                    continue
                reason = req["reason"]
                if reason == "full_close":
                    # спізнення відносно ФІЛА кита (або часу, коли
                    # дізнались) >60 с — інша угода, поза заголовком
                    _ft = p.get("force_fill_ts_ms")
                    _ref = (_ft / 1000.0) if _ft else p.get("force_exit_ts")
                    if _ref and now2 - _ref > 60.0:
                        reason = "full_close_late"
                g = (epx / p["entry_px"] - 1.0) * 100.0
                if p["our_side"] == "SHORT": g = -g
                p["peak"] = max(p["peak"], g)
                p["trough"] = min(p["trough"], g)
                p["done"] = 1
                p["exit_src"] = xsrc
                p["exit_px_mid"] = xmeta.get("mid") or mid
                p["close_ts"] = now2
                _jrn.append(("follow_exit", dict(fid=fid, coin=p["coin"], reason=reason, px=epx,
                                                 src=p["exit_src"], mid=mid,
                                                 quote_ts_ms=xmeta.get("quote_ts_ms"))))
                # заморожений фінальний рядок: exit/hold/net зафіксовані
                # У МОМЕНТ виходу, ретраї запису їх не перерахують
                p["final_row"] = _fol_row(p, fid, now2, epx, reason, g)
                fol_rows.append((fid, p["final_row"]))
        for _jk, _jf in _jrn:
            _journal(_jk, **_jf)
    written_rev, written_fol = [], []
    failed_rev, failed_fol = [], []
    for pid, kind, r in rev_rows:
        if kind == "out":
            ok = _strat_csv_append(REV_OUT_CSV, REV_OUT_HEADERS, r)
        elif kind == "fo":
            ok = _strat_csv_append(FOLLOW_OUT_CSV, FOLLOW_OUT_HEADERS, r)
        elif kind == "twapc":
            ok = _strat_csv_append(TWAP_CURVE_CSV, TWAP_CURVE_HEADERS, r)
        elif kind == "twap":
            ok = _strat_csv_append(TWAP_CSV, TWAP_HEADERS, r)
        else:
            ok = _strat_csv_append(REV_CSV, REV_HEADERS, r)
        (written_rev if ok else failed_rev).append(pid)
    # рядки TWAP-угод у момент виходу (v2.15): трекер лишається (крива),
    # але результат уже незмінний на диску
    written_tw = []
    for pid, r in tw_rows:
        if _strat_csv_append(TWAP_CSV, TWAP_HEADERS, r):
            written_tw.append(pid)
    if tw_rows:
        with strat2_lock:
            for pid, _ in tw_rows:
                it = rev_open.get(pid)
                if it is None: continue
                if pid in written_tw:
                    it["trade_written"] = 1
                else:
                    it["twfail"] = it.get("twfail", 0) + 1
                    if it["twfail"] in (1, 10, 100):
                        print(f"  [STRAT] TWAP-угода {pid}: запис не вдався "
                              f"({it['twfail']}), повторю")
                    elif it["twfail"] > WFAIL_CAP:
                        # як і решта трекерів: після WFAIL_CAP відмов — дроп
                        # із логом, а не вічний ретрай (рев'ю v2.15)
                        rev_open.pop(pid, None)
                        print(f"  [STRAT] DROP TWAP-угода {pid} без запису "
                              f"після {WFAIL_CAP} спроб (диск?)")
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
    if written_rev or written_fol or written_tw or rev_exited:
        # СИНХРОННО (v2.14, аудит v2.13 №1): вікно «рядок у CSV, а
        # трекер ще у state.json» — мілісекунди, а не «коли потік
        # добереться»; залишок вікна закриває звірка при load_state.
        # v2.16: і після вирішеного виходу R (рядок пишеться лише на
        # m60 — аварія до періодичного save_state переоцінювала вихід)
        save_state()


# ── API для вкладки "Стратегії" ─────────────────────────
_strat2_cache = {"ts": 0.0, "data": None}
_legacy_csv_cache = {}   # path -> ((mtime, size), rows, quarantined, errs)

def _median(v):
    if not v: return None
    s = sorted(v); n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0

def _stat_small(vals):
    """v2.16: зведення по списку net (%): n / медіана / середнє / win% /
    PnL$ при $1000 на угоду — для періодів, когорт і live/tape-зрізів."""
    if not vals:
        return {"n": 0, "median": None, "mean": None, "win": None, "pnl_usd": 0.0}
    return {"n": len(vals), "median": _median(vals),
            "mean": sum(vals) / len(vals),
            "win": 100.0 * sum(1 for v in vals if v > 0) / len(vals),
            "pnl_usd": round(sum(vals) * 10.0, 2)}

def _period_stats(items, now):
    """v2.16 п.5: статистика за сьогодні (від локальної півночі) / 7 днів /
    30 днів / весь час. items = [(ts_сек, net)]."""
    lt = time.localtime(now)
    try:
        midnight = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0,
                                lt.tm_wday, lt.tm_yday, lt.tm_isdst))
    except (OverflowError, ValueError):
        midnight = now - 86400.0
    bounds = (("today", midnight), ("d7", now - 7 * 86400.0),
              ("d30", now - 30 * 86400.0), ("all", 0.0))
    return {k: _stat_small([v for ts, v in items if ts is not None and ts >= b])
            for k, b in bounds}

def _ts_local(s):
    """'YYYY-MM-DD HH:MM:SS' (локальний час рядка) → epoch с або None."""
    try:
        return time.mktime(time.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S"))
    except (TypeError, ValueError, OverflowError):
        return None

_settle_cache = {"key": None, "data": {}}

def _load_settlements():
    """settlements.csv (settle.py) → {ключ угоди: рядок}; кеш по mtime/size."""
    path = os.path.join(DATA_DIR, "settlements.csv")
    try:
        st_ = os.stat(path)
        key = (st_.st_mtime, st_.st_size)
    except OSError:
        return {}
    if _settle_cache["key"] == key:
        return _settle_cache["data"]
    try:
        import settle as _settle
        data = _settle.load_settlements(DATA_DIR)
        # аудит-2 №9/№10: рядки СТАРОЇ версії розрахунку (stale-ціни,
        # переторговані виходи) — не «verified», а pending: воркер їх
        # перераховує (новіші першими), до того угода показується як
        # нерозрахована, а не з хибним офіційним net
        cur = getattr(_settle, "SETTLE_V", None)
        if cur is not None:
            old = [k for k, r in data.items() if (r.get("settle_v") or "").strip() != str(cur)]
            for k in old:
                data.pop(k, None)
            stats["settle_old_v"] = len(old)
    except Exception as e:
        print(f"  [SETTLE] читання settlements.csv: {e}")
        data = {}
    _settle_cache["key"], _settle_cache["data"] = key, data
    return data

def _official_net(net_live, srow, fnum):
    """v2.16 п.3 (аудит-2 №10/№11): офіційний net = ГІРШИЙ з ДОСТУПНИХ
    оцінок — live-запису і стрічки Binance; коли є лише одна — вона
    (записаний результат ніколи не зникає). Статус верифікації:
    verified (обидві оцінки), live_only (стрічки нема / R2 без тригера на
    стрічці), tape_only (live без ціни), none, pending (ще не розраховано).
    → (official, net_tape, settled, flags, status)."""
    if not srow:
        return net_live, None, 0, "", ("live_only" if net_live is not None else "pending")
    nt = fnum(srow.get("net_tape_pct"))
    flags = srow.get("flags") or ""
    if nt is not None and net_live is not None:
        return min(net_live, nt), nt, 1, flags, "verified"
    if nt is not None:
        return nt, nt, 1, flags, "tape_only"
    if net_live is not None:
        return net_live, None, 1, flags, "live_only"
    return None, None, 1, flags, "none"

def _parse_curve(txt, H):
    """curve_tape settlement ("v1;v2;…", "" = нема) → список H float/None."""
    if not txt:
        return None
    out = []
    for x in str(txt).split(";")[:H]:
        try:
            v = float(x)
            out.append(v if math.isfinite(v) else None)
        except (TypeError, ValueError):
            out.append(None)
    if not any(v is not None for v in out):
        return None
    return out + [None] * (H - len(out))

def _curves_of(trs, H):
    """Криві «медіана net за хвилиною виходу» — за ТИМ САМИМ правилом, що
    офіційний net картки (аудит-3 №5): для кожної угоди і хвилини —
    ГІРША з live-семпла й оцінки зі стрічки, коли є обидві, інакше та,
    що є (curve_src=official; раніше — лише стрічка від 3 угод, лише live
    до того — інша методика й підвибірка, знак міг не збігатися з
    офіційною медіаною). Спільна когорта — лише повні траєкторії.
    Окремо повертаються curve_live / curve_tape (той самий склад угод)."""
    def _pad(vals):
        vv = list(vals[:H]) if vals else []
        return vv + [None] * (H - len(vv))
    rows_off, rows_live, rows_tape = [], [], []
    n_both = 0
    for t in trs:
        ct, cl = t.get("_ct"), t.get("_cl")
        if not ct and not cl:
            continue
        pt, pl = (_pad(ct) if ct else None), (_pad(cl) if cl else None)
        if pt and pl:
            n_both += 1
            off = [(min(a, b) if (a is not None and b is not None) else (a if a is not None else b))
                   for a, b in zip(pl, pt)]
        else:
            off = pt or pl
        rows_off.append(off)
        if pl: rows_live.append(pl)
        if pt: rows_tape.append(pt)
    def _agg(rows_):
        curve = [[] for _ in range(H)]
        complete = []
        for vv in rows_:
            for i, v in enumerate(vv):
                if v is not None:
                    curve[i].append(v)
            if all(v is not None for v in vv):
                complete.append(vv)
        return ([(_median(c) if c else None) for c in curve], [len(c) for c in curve],
                ([_median([c[i] for c in complete]) for i in range(H)] if complete else None),
                len(complete))
    cv, cn, cc, ccn = _agg(rows_off)
    lv = _agg(rows_live); tv = _agg(rows_tape)
    return {"curve": cv, "curve_n": cn, "curve_common": cc, "curve_common_n": ccn,
            "curve_src": ("official" if rows_off else "none"),
            "curve_tape_n": len(rows_tape), "curve_live_n": len(rows_live), "curve_both_n": n_both,
            "curve_live": lv[0], "curve_tape": tv[0]}

def _agg_block(trs, now, H, buckets=False):
    """ЄДИНИЙ агрегатор статистики стратегії/зрізу (аудит-2 №4): ті самі
    правила для картки, періодів, когорт, кривих і серверного зрізу.
    Заголовкова вибірка = офіційний net угод БЕЗ пізніх виходів (late) і
    БЕЗ старих F-рядків, що не пройшли б гейт свіжості (stale)."""
    head = [t for t in trs if t.get("entered", 1) != 0 and t.get("net30") is not None
            and not t.get("late") and not t.get("stale")]
    nets = [t["net30"] for t in head]
    items = [(t.get("ts"), t["net30"]) for t in head]
    half = len(nets) // 2
    cum, acc = [], 0.0
    for v in nets:
        acc += v; cum.append(round(acc, 3))
    ver = [t["net30"] for t in head if t.get("status") == "verified"]
    out = {"n": len(nets), "median": _median(nets),
           "mean": (sum(nets) / len(nets)) if nets else None,
           "win": (100.0 * sum(1 for v in nets if v > 0) / len(nets)) if nets else None,
           "pnl_usd": round(sum(nets) * 10.0, 2) if nets else 0.0,
           "half1": _median(nets[:half]), "half2": _median(nets[half:]), "cum": cum,
           "periods": _period_stats(items, now),
           "live": _stat_small([t["net_live"] for t in head if t.get("net_live") is not None]),
           "tape": _stat_small([t["net_tape"] for t in head if t.get("net_tape") is not None]),
           # лише верифіковані (обидві оцінки є; аудит-2 №10)
           "verified": _stat_small(ver),
           "n_late": sum(1 for t in trs if t.get("late")),
           "n_stale": sum(1 for t in trs if t.get("stale")),
           "n_no_price": sum(1 for t in trs if t.get("no_price")),
           "n_settled": sum(1 for t in trs if t.get("settled")),
           "n_unsettled": sum(1 for t in trs if not t.get("settled") and t.get("net30") is not None),
           "n_verified": sum(1 for t in trs if t.get("status") == "verified"),
           "n_live_only": sum(1 for t in trs if t.get("status") == "live_only"),
           "n_tape_only": sum(1 for t in trs if t.get("status") == "tape_only"),
           "n_mismatch": sum(1 for t in trs if t.get("mismatch")),
           # аудит-3 №1/№2: епізод live ≠ епізод settlement (когорта інша)
           "n_ep_mismatch": sum(1 for t in trs if t.get("ep_mismatch")),
           "n_partial_close": sum(1 for t in trs if t.get("full_close") == 0),
           "n_grace": sum(1 for t in trs if t.get("grace") == 1),
           "n_grace_unknown": sum(1 for t in trs if t.get("grace") is None),
           "n_trades_total": len(trs)}
    out.update(_curves_of(head, H))
    if buckets:
        out["buckets"] = {str(b): _stat_small([t["net30"] for t in head if t.get("bucket") == b])
                          for b in (1, 2, 3, 4, 5)}
    return out

_PRIV = ("_ct", "_cl")
def _pub(trs):
    """Публічні копії угод (без внутрішніх кривих)."""
    return [{k: v for k, v in t.items() if k not in _PRIV} for t in trs]

_strat2_full = {}   # стратегія -> {"kind", "H", "trades": [усі угоди з кривими]} (для /strat2_slice)

def strat2_slice(st, grace="all", vault="all", sht="all", bucket="all"):
    """Аудит-2 №4: зріз стратегії на СЕРВЕРІ по ВСІЙ вибірці до агрегації
    (не по останніх 120 у браузері): ті самі правила, що й картка."""
    strat2_api()
    full = _strat2_full.get(st)
    if not full:
        return {"error": "unknown strategy"}
    trs = full["trades"]
    if grace in ("y", "n"):
        trs = [t for t in trs if t.get("grace") == (1 if grace == "y" else 0)]
    if vault in ("y", "n"):
        trs = [t for t in trs if (t.get("vault") or 0) == (1 if vault == "y" else 0)]
    if sht in ("y", "n") and full["kind"] == "rev":
        trs = [t for t in trs if (t.get("shtanga") == 1) == (sht == "y")]
    if bucket not in ("all", "", None) and full["kind"] == "rev":
        try:
            bk = int(float(bucket))
        except (TypeError, ValueError):
            return {"error": "bad bucket"}   # рев'ю: не віддавати НЕфільтрований зріз мовчки
        trs = [t for t in trs if t.get("bucket") == bk]
    blk = _agg_block(trs, time.time(), full["H"], buckets=False)
    blk["strategy"] = st
    blk["filters"] = {"grace": grace, "vault": vault, "sht": sht, "bucket": bucket}
    return blk

def strat2_api():
    now = time.time()
    if _strat2_cache["data"] is not None and now - _strat2_cache["ts"] < 15:
        return _strat2_cache["data"]
    import csv as _csv

    quarantined = [0]
    read_errors = [0]
    # відкриті зараз paper-позиції по стратегіях — входи, яких ще нема в
    # CSV (швидкість «позицій/день» рахує їх теж; рев'ю v2.12 №7b)
    # v2.14 (аудит v2.13 №5, №8a-b): рахуються лише трекери ПОТОЧНОЇ версії
    # логіки стратегії (старий відкритий T1 2.12 не «швидкість» 2.14),
    # унікальні по ключу рядка (той самий вхід у CSV і в state — одна
    # угода), а закрита TWAP-угода (m60/скасування), що ще спостерігає
    # криву до 120 хв, — не «відкрита»: її результат уже є (pending-рядок)
    with strat2_lock:
        _open_keys = {}
        for _k, _p in rev_open.items():
            if _p.get("done") or _p.get("state", "open") != "open":
                continue
            _st = _p.get("strategy") or ""
            if not _v_ok(_p.get("algo_v"), STRAT_SINCE.get(_st, DATA_ALGO_V)):
                continue
            if _p.get("row_kind") == "twap" and _twap_trade_closed(_p, now):
                continue   # угода закрита (рядок уже/от-от у CSV), триває лише крива
            _open_keys.setdefault(_st, set()).add(_tracker_csv_key("rev", _k, _p))
        for _k, _p in follow_open.items():
            if _p.get("done"):
                continue
            _st = _p.get("strategy") or ""
            if not _v_ok(_p.get("algo_v"), STRAT_SINCE.get(_st, DATA_ALGO_V)):
                continue
            _open_keys.setdefault(_st, set()).add(_tracker_csv_key("fol", _k, _p))

    def _read_file(path):
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

    legacy_q, legacy_err, legacy_old = [0], [0], [0]
    # найнижчий поріг версії серед усіх стратегій/стрічок: рядок legacy
    # нижче нього не пройде ЖОДЕН фільтр — не тримаємо його в пам'яті,
    # лише рахуємо (рев'ю v2.11: legacy-файли ростуть роками)
    _min_since = min([_vt(TAPE_SINCE)] + [_vt(v) for v in STRAT_SINCE.values()])

    def read(path):
        """УСІ .legacy-<ts>.csv файла + поточний (v2.11 п.2): зміна
        заголовків ротує файл, але рядки в ньому — та сама статистика
        стратегій, логіка яких не мінялась; фільтр версії нижче вирішує,
        що показувати. Порядок «legacy, потім поточний» — щоб у dedup
        (останній виграє) поточний файл перемагав. Legacy-файли незмінні
        — кешуються по (mtime, size); їхні карантин/збої рахуються
        ОКРЕМО від поточних файлів, щоб не лякати лічильником битих
        рядків, які насправді давно в архіві."""
        rows = []
        present = set()
        for lp in sorted(glob.glob(path + ".legacy-*.csv")):
            present.add(lp)
            try:
                st_ = os.stat(lp)
                key_ = (st_.st_mtime, st_.st_size)
            except OSError:
                continue
            c = _legacy_csv_cache.get(lp)
            if c is None or c[0] != key_:
                q0, e0 = quarantined[0], read_errors[0]
                lrows = _read_file(lp)
                dq, de = quarantined[0] - q0, read_errors[0] - e0
                quarantined[0], read_errors[0] = q0, e0
                keep = [r for r in lrows if _vt(r.get("algo_v")) >= _min_since]
                c = (key_, keep, dq, de, len(lrows) - len(keep))
                _legacy_csv_cache[lp] = c
            legacy_q[0] += c[2]
            legacy_err[0] += c[3]
            legacy_old[0] += c[4]
            rows.extend(c[1])
        for lp in [k for k in _legacy_csv_cache
                   if k.startswith(path + ".legacy-") and k not in present]:
            _legacy_csv_cache.pop(lp, None)   # файл видалили — кеш геть
        rows.extend(_read_file(path))
        return rows

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
        # (рестарт відновлював уже записаний трекер) — знімаємо на
        # читанні. v2.14 (аудит v2.13 №1): ПЕРШИЙ валідний запис виграє
        # — результат завершеної угоди незмінний; огризки (аудит v2.3
        # п.7) сюди не доходять: їх відкидає _read_file за кількістю
        # колонок і eol-вартовим. Рядок БЕЗ ключа — у карантин:
        # ідентифікувати і дедуплікувати його неможливо
        best, order, dups = {}, [], 0
        for r in rows:
            k = keyf(r)
            if not k:
                quarantined[0] += 1
                continue
            if k in best:
                dups += 1
                continue
            order.append(k)
            best[k] = r
        if dups:
            dup_rows[0] += dups
        return [best[k] for k in order]
    dup_rows = [0]
    rev = dedup(read(REV_CSV),
                lambda r: ((r.get("sig_id"), r.get("strategy"))
                           if r.get("sig_id") and r.get("strategy")
                           else None))
    sigs = dedup(read(REV_SIG_CSV), lambda r: r.get("sig_id"))
    fol = dedup(read(FOLLOW_CSV), lambda r: r.get("trade_id"))
    outs_all = dedup(read(REV_OUT_CSV), lambda r: r.get("sig_id"))
    fouts_all = dedup(read(FOLLOW_OUT_CSV), lambda r: r.get("fo_id"))
    tw_all = dedup(read(TWAP_CSV),
                   lambda r: ((r.get("twap_id"), r.get("strategy"))
                              if r.get("twap_id") and r.get("strategy") else None))
    tws_all = dedup(read(TWAP_SIG_CSV), lambda r: r.get("twap_id"))
    # статистика рахується ЛИШЕ по рядках поточної версії логіки:
    # зміна алгоритму не має тихо змішувати старі й нові вимірювання в
    # одну вибірку (аудит v2.2). Виключене рахуємо у legacy_rows —
    # видно, скільки історії лишилося за бортом (файли не чіпаються)
    # v2.11 п.2: версія порівнюється як КОРТЕЖ (2.9 < 2.10), а поріг —
    # per-strategy (STRAT_SINCE: з якої версії логіка стратегії не
    # мінялась) або per-tape (TAPE_SINCE для спільних стрічок). Рядки
    # незмінених стратегій переживають апдейт коду; legacy_rows — лише
    # те, що справді відкинуто версією
    def since_tape(rows):
        return [r for r in rows if _v_ok(r.get("algo_v"), TAPE_SINCE)]
    def since_strat(rows):
        return [r for r in rows
                if _v_ok(r.get("algo_v"),
                         STRAT_SINCE.get(r.get("strategy"), DATA_ALGO_V))]
    n_before = (len(rev) + len(sigs) + len(fol) + len(outs_all)
                + len(fouts_all) + len(tw_all) + len(tws_all))
    rev_all = rev
    rev, sigs, fol, outs, fouts = (since_strat(rev), since_tape(sigs),
                                   since_strat(fol), since_tape(outs_all),
                                   since_tape(fouts_all))
    tw, tws = since_strat(tw_all), since_tape(tws_all)
    legacy_rows = (n_before - (len(rev) + len(sigs) + len(fol)
                               + len(outs) + len(fouts) + len(tw) + len(tws))
                   + legacy_old[0])
    # v2.16: settlement (стрічка Binance) по ключу рядка; офіційний net =
    # гірший з live і стрічки; старі R-рядки (2.10–2.15, інше правило
    # кваліфікації) ДОПУСКАЮТЬСЯ у статистику стратегії, якщо settlement
    # відновив епізод закриття з філів кита і він проходить НОВЕ правило
    # (падіння ≥ поріг стратегії за <5 хв) — 7 днів даних не губляться,
    # а популяція входів стає такою ж, як у нових рядків
    settl = _load_settlements()
    _rev_thr = {"R4_великий": 2.0, "R5_дуже": 3.0}
    def _rev_adm_legacy(st):
        adm = []
        thr = _rev_thr.get(st, 1.0)
        for r in rev_all:
            if r.get("strategy") != st or _v_ok(r.get("algo_v"), STRAT_SINCE.get(st, DATA_ALGO_V)):
                continue
            if _vt(r.get("algo_v")) < (2, 10):
                continue
            s_ = settl.get(f"{r.get('sig_id')}|{st}")
            if not s_:
                continue
            b_ = fnum(s_.get("dump_bucket"))
            mv = fnum(s_.get("dump_move_pct"))   # settlement: додатний = у бік тиску кита
            if b_ is None or not (1 <= b_ <= 5) or mv is None:
                continue
            _lag = fnum(s_.get("lag_s"))
            if _lag is None or _lag > REV_MAX_FILL_AGE_S:
                continue   # live такий сигнал теж не відкриває (старий/невідомий філ)
            # аудит-3 №3: усі реверси — лише ПОВНЕ закриття кита; settlement
            # доводить це залишком після останнього філа епізоду (full_close),
            # partial_close / close_unknown — не допускаються (ціновий
            # перерахунок не підтверджує валідність входу)
            _fl = s_.get("flags") or ""
            if "full_close" not in _fl:
                continue
            if st == R7_NAME:
                # «одним пострілом»: найбільший філ епізоду ≥95% стартової
                # позиції і ≥$100k — за settlement, не за старим live-полем
                _mp, _mu = fnum(s_.get("whale_max_fill_pct")), fnum(s_.get("whale_max_fill_usd"))
                if _mp is None or _mp < F4_FULL_PCT or (_mu or 0) < R7_MIN_TX_USD:
                    continue
            if mv + 1e-9 >= thr:
                adm.append(r)
        return adm
    # v2.15: рядки угод пишуться у момент виходу (pending-синтез не
    # потрібен); криві 120 хв — окремий файл, по ключу угоди
    twc_all = dedup(read(TWAP_CURVE_CSV),
                    lambda r: ((r.get("twap_id"), r.get("strategy"))
                               if r.get("twap_id") and r.get("strategy") else None))
    _tw_curves = {(r.get("twap_id"), r.get("strategy")): r for r in twc_all}
    # відкриті зараз входи — лише ті, яких ще НЕМА у CSV (унікальний
    # облік: аварійний дубль не дає «2 входи/день»; аудит v2.13 №8a)
    _csv_keys = ({("rev", r.get("sig_id"), r.get("strategy")) for r in rev}
                 | {("fol", r.get("trade_id")) for r in fol}
                 | {("twap", r.get("twap_id"), r.get("strategy")) for r in tw})
    _open_by_st = {st_: sum(1 for k_ in ks_ if k_ not in _csv_keys)
                   for st_, ks_ in _open_keys.items()}
    def _rate(rows, key):
        """Швидкість стратегії (v2.11 п.3): угод на день = n / дні від
        ПЕРШОГО рядка стратегії до зараз (мінімум 1 день — інакше 3
        угоди за годину після деплою давали б «72/день»)."""
        ds = [r.get(key) for r in rows if r.get(key)]
        if not ds:
            return None, 0.0
        try:
            first = time.mktime(time.strptime(min(ds), "%Y-%m-%d %H:%M:%S"))
        except (ValueError, OverflowError):
            return None, 0.0
        days = max(1.0, (now - first) / 86400.0)
        return round(len(rows) / days, 2), round(days, 1)
    def _rate2(st, rows, key, n_entries):
        """v2.13 (рев'ю v2.12 №7b): усі унікальні входи, включно з
        відкритими зараз; горизонт — від min(активація since-версії
        стратегії, перший рядок), мінімум 1 день."""
        ds = [r.get(key) for r in rows if r.get(key)]
        first = None
        if ds:
            try:
                first = time.mktime(time.strptime(min(ds), "%Y-%m-%d %H:%M:%S"))
            except (ValueError, OverflowError):
                first = None
        act = (strat_activated.get(st) or {}).get("ts") if isinstance(
            strat_activated.get(st), dict) else None
        cands = [t for t in (first, act) if t]
        if not cands:
            return None, 0.0
        days = max(1.0, (now - min(cands)) / 86400.0)
        return round(n_entries / days, 2), round(days, 1)
    out = {"updated": now, "strategies": {}, "desc": STRAT2_DESC,
           "titles": STRAT2_TITLES,
           "legacy_rows": legacy_rows, "quarantined": quarantined[0],
           "read_errors": read_errors[0],
           # архівні (.legacy) файли — окремо: їхні биті рядки давно
           # в історії і не сигналять про стан диска ЗАРАЗ
           "legacy_quarantined": legacy_q[0], "legacy_read_errors": legacy_err[0],
           "legacy_files": len(_legacy_csv_cache),
           # дублі ключів у CSV (аварійний повтор): перший запис виграє (v2.14)
           "dup_rows": dup_rows[0]}

    for st in ("R1_загальний", "R2_breakout", "R3_великі", "R4_великий",
               "R5_дуже", "R6_волт", R7_NAME, R8_NAME):
        rows = [r for r in rev if r.get("strategy") == st]
        adm_rows = _rev_adm_legacy(st)
        _adm_ids = {id(r) for r in adm_rows}
        rows += adm_rows
        legacy_rows -= len(adm_rows)
        # половини рахуємо у ХРОНОЛОГІЇ сигналів, а не в порядку
        # завершення записів (аудит п.10)
        rows.sort(key=lambda r: r.get("date") or "")
        trades = []
        n_entered = 0
        for r in rows:
            entered = 1 if fnum(r.get("entered"), 0) == 1 else 0
            costs = fnum(r.get("costs_pct"), 0.15) or 0.15
            net_live = None
            late_exit = 0
            exit_reason = r.get("exit_reason") or ""
            live_curve = None
            if entered:
                n_entered += 1
                row_vals = [fnum(r.get(f"m{i+1}")) for i in range(REV_TRACK_MIN)]
                live_curve = [((v - costs) if v is not None else None) for v in row_vals]
                ex_px = fnum(r.get("exit_px")); e_px = fnum(r.get("entry_px"))
                if ex_px and e_px and exit_reason and exit_reason != "no_price":
                    # v2.16: вирішений вихід зі стакану (таймер m30 / TP R8)
                    # — gross від ціни виходу; витрати за ногами (аудит-2 №12)
                    gx = (ex_px / e_px - 1.0) * 100.0
                    if (r.get("our_side") or "") == "SHORT": gx = -gx
                    net_live = gx - _leg_costs(costs, r.get("entry_src"), r.get("exit_src"))
                    late_exit = 1 if exit_reason == "timer_late" else 0
                elif exit_reason == "no_price":
                    net_live = None
                else:
                    n30 = fnum(r.get("m30"))
                    # m30 без ціни: paper-вихід — перша наступна хвилина з ціною
                    # (≤3 хв), з позначкою запізнення (v2.14) — лише рядки ≥2.14
                    if n30 is None and _vt(r.get("algo_v")) >= (2, 14):
                        for _j in (31, 32, 33):
                            n30 = fnum(r.get(f"m{_j}"))
                            if n30 is not None:
                                late_exit = _j - 30
                                break
                    net_live = (n30 - costs) if n30 is not None else None
            srow = settl.get(f"{r.get('sig_id')}|{st}")
            official, net_tape, settled, flags, vstat = _official_net(net_live, srow, fnum)
            late = 1 if late_exit else 0
            ratio_ = fnum(r.get("ratio"))
            grace = fnum(r.get("grace"))
            grace = (int(grace) if grace is not None else None)   # аудит-2 №4: старий рядок — невідомо
            # аудит-3 №1: ознаки епізоду (когорта, тривалість, рух) — з
            # settlement за філами кита (те саме правило, що live), для ВСІХ
            # рядків, де вони є; live-значення лишаються окремо (dump_*_live),
            # розбіжність рахується (n_ep_mismatch). Без settlement — live
            bucket_live = fnum(r.get("dump_bucket"))
            dump_dur_live = fnum(r.get("dump_dur_s"))
            dump_move_live = fnum(r.get("dump_move_pct"))
            bucket, dump_dur, dump_move = bucket_live, dump_dur_live, dump_move_live
            ep_mismatch = 0
            if srow and fnum(srow.get("dump_bucket")) is not None:
                _sb = fnum(srow.get("dump_bucket"))
                _sd = fnum(srow.get("dump_dur_s"))
                _mv = fnum(srow.get("dump_move_pct"))
                _mv_s = ((-_mv) if (r.get("our_side") or "") == "LONG" else _mv) if _mv is not None else None
                if bucket_live is not None and int(bucket_live) != int(_sb):
                    ep_mismatch = 1
                bucket, dump_dur = _sb, (_sd if _sd is not None else dump_dur_live)
                if _mv_s is not None:
                    dump_move = _mv_s
            ts = _ts_local(r.get("date"))
            _m3 = [x for x in (fnum(r.get(f"m{i}")) for i in (1, 2, 3)) if x is not None]
            trades.append({"date": r.get("date"), "coin": r.get("coin"),
                "side": r.get("our_side"), "net30": (official if entered else None),
                "net_live": net_live, "net_tape": net_tape,
                "settled": settled, "flags": flags, "status": (vstat if entered else ""),
                "mismatch": int("no_trigger" in (flags or "")),
                "no_price": int(bool(entered) and (exit_reason == "no_price"
                                                   or (net_live is None and not settled))),
                "peak": fnum(r.get("peak_pct")), "trough": fnum(r.get("trough_pct")),
                "move": fnum(r.get("move_3m_pct")),
                "dur": fnum(r.get("dur_s")), "ratio": ratio_,
                "shtanga": fnum(r.get("shtanga"), 0), "vault": fnum(r.get("vault"), 0),
                "hour": fnum(r.get("hour")), "btc": fnum(r.get("btc_move_pct")),
                "sum_usd": fnum(r.get("sum_usd")),
                "p3u": (max(_m3) if _m3 else None), "p3d": (min(_m3) if _m3 else None),
                "late_exit": late_exit, "late": late, "stale": 0,
                "exit_reason": exit_reason, "exit_min": fnum(r.get("exit_min")),
                "grace": grace, "bucket": (int(bucket) if bucket is not None else None),
                "dump_dur": dump_dur, "dump_move": dump_move,
                "dump_move_live": dump_move_live, "bucket_live": (int(bucket_live) if bucket_live is not None else None),
                "ep_mismatch": ep_mismatch,
                "full_close": (1 if "full_close" in (flags or "") else (0 if "partial_close" in (flags or "") else None)),
                "lag": fnum(r.get("lag_s")),
                "entry_src": r.get("entry_src") or "", "exit_src": r.get("exit_src") or "",
                "detect_src": r.get("detect_src") or "",
                "adm": 1 if id(r) in _adm_ids else 0,
                "entered": entered, "ts": ts,
                "_ct": (_parse_curve(srow.get("curve_tape"), REV_TRACK_MIN) if (srow and entered) else None),
                "_cl": live_curve})
        blk = _agg_block(trades, now, REV_TRACK_MIN, buckets=True)
        _strat2_full[st] = {"kind": "rev", "H": REV_TRACK_MIN, "trades": trades}
        _pd_st, _days_st = _rate2(st, rows, "date", n_entered + _open_by_st.get(st, 0))
        blk.update({
            "kind": "rev", "signals": len(rows), "entered": n_entered,
            "trades": _pub(trades[-120:]),
            "per_day": _pd_st, "days": _days_st,
            "open_now": _open_by_st.get(st, 0),
            "since": STRAT_SINCE.get(st, DATA_ALGO_V),
            "n_adm_legacy": len(adm_rows),
        })
        out["strategies"][st] = blk
    out["legacy_rows"] = legacy_rows   # v2.16: мінус допущені за settlement старі R-рядки

    for st in list(FOLLOW_TIMERS) + [F6_NAME, F4_NAME, F5_NAME, F8_NAME,
                                     F7_NAME, F9_NAME]:
        rows = [r for r in fol if r.get("strategy") == st]
        rows.sort(key=lambda r: r.get("date_open") or "")
        trades = []
        # профіль швидкості ГАМАНЦІВ цієї стратегії (v2.8): один запис
        # на гаманець (останній рядок), щоб агрегат не зважувався
        # кількістю угод одного й того ж кита
        prof_by_wallet = {}
        n_first = 0
        for r in rows:
            net_live = fnum(r.get("net_pct"))
            reason = r.get("exit_reason") or ""
            # пізній вихід (silence_late / full_close_late) — поза заголовком
            late = 1 if reason.endswith("_late") else 0
            srow = settl.get(r.get("trade_id") or "")
            official, net_tape, settled, flags, vstat = _official_net(net_live, srow, fnum)
            grace = fnum(r.get("grace"))
            grace = (int(grace) if grace is not None else None)   # старий рядок — невідомо
            # аудит-2 №8: старий F-рядок (до 2.16) без гейта свіжості — якщо
            # settlement показує лаг філа >20 с, v2.16 такий вхід не відкрила б
            # аудит-3 №1: усі рядки до DATA_ALGO_V (обидві збірки 2.16 теж) —
            # legacy: лаг підтверджує лише settlement; лаг невідомий (філів
            # кита не отримано) — теж поза заголовком, як непідтверджений
            stale = 0
            if _vt(r.get("algo_v")) < _vt(DATA_ALGO_V) and srow:
                _lg = fnum(srow.get("lag_s"))
                if _lg is None or _lg > FOLLOW_MAX_FILL_AGE_S:
                    stale = 1
            ts = _ts_local(r.get("date_open"))
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
                "side": r.get("our_side"), "net30": official,
                "net_live": net_live, "net_tape": net_tape,
                "settled": settled, "flags": flags, "status": vstat,
                "mismatch": 0, "no_price": int(net_live is None and not settled),
                "late": late, "stale": stale,
                "grace": grace, "lag": fnum(r.get("lag_s")),
                "entry_src": r.get("entry_src") or "",
                "exit_src": r.get("exit_src") or "",
                "detect_src": r.get("detect_src") or "",
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
                "wallet": (r.get("whale_addr") or "")[:10], "ts": ts,
                "_ct": (_parse_curve(srow.get("curve_tape"), REV_TRACK_MIN) if srow else None),
                "_cl": None})
        blk = _agg_block(trades, now, REV_TRACK_MIN)
        _strat2_full[st] = {"kind": "follow", "H": REV_TRACK_MIN, "trades": trades}
        if blk["curve_src"] != "official" or (blk.get("curve_tape_n") or 0) < 3:
            # власних live-траєкторій у F немає; <3 траєкторій зі стрічки —
            # не крива: UI бере тіньову стрічку follow (позначену як
            # НЕПЕРЕВІРЕНЕ спостереження, не перевірку стратегії)
            for k_ in ("curve", "curve_n", "curve_common", "curve_live", "curve_tape"):
                blk[k_] = None
            blk["curve_common_n"] = 0
            blk["curve_src"] = "shadow"
        pw = list(prof_by_wallet.values())
        _pcts = [p["fast_pct"] for p in pw if p["fast_pct"] is not None]
        _unl = [p["unload_med"] for p in pw if p["unload_med"] is not None]
        _unm = [p["unload_mean"] for p in pw if p["unload_mean"] is not None]
        blk.update({
            "kind": "follow", "signals": len(rows), "entered": len(rows),
            "trades": _pub(trades[-120:]),
            "per_day": _rate2(st, rows, "date_open", len(rows) + _open_by_st.get(st, 0))[0],
            "days": _rate2(st, rows, "date_open", len(rows) + _open_by_st.get(st, 0))[1],
            "open_now": _open_by_st.get(st, 0),
            "since": STRAT_SINCE.get(st, DATA_ALGO_V),
            "first_shot_n": n_first,
            "prof": {"wallets": len(pw),
                     "fast_pct_med": _median(_pcts),
                     "unload_med_s": _median(_unl),
                     "unload_mean_s": (sum(_unm) / len(_unm)) if _unm else None,
                     "n_fast": sum(int(p["n_fast"] or 0) for p in pw),
                     "n_slow": sum(int(p["n_slow"] or 0) for p in pw)},
        })
        out["strategies"][st] = blk

    # ── TWAP-реверс T1/T2 (v2.11 п.5): headline = m60 net (вихід через
    # годину), крива 120 хв; когорти ≥1/≥1.5/≥2% — ті самі входи,
    # відфільтровані по руху за час твапу (кнопки в UI)
    def _tw_net(r):
        """(official, live, tape, settled, flags, status, live) для рядка TWAP-угоди."""
        nl = fnum(r.get("net60_pct"))
        srow = settl.get(f"{r.get('twap_id')}|{r.get('strategy')}")
        return _official_net(nl, srow, fnum) + (nl,)
    def _tw_trades(rs):
        # v2.14 (аудит v2.13 №4): список угод — ВЛАСНИЙ для кожної
        # когорти-стратегії (це різні входи, а не фільтр базової по руху)
        out_ = []
        for r in rs:
            official, nt, settled, flags, vstat, nl = _tw_net(r)
            srow = settl.get(f"{r.get('twap_id')}|{r.get('strategy')}")
            costs = fnum(r.get("costs_pct"), 0.15) or 0.15
            cr = _tw_curves.get((r.get("twap_id"), r.get("strategy")))
            src_ = cr if cr is not None else r
            vals = [fnum(src_.get(f"m{i + 1}")) for i in range(TWAP_TRACK_MIN)]
            live_curve = [((v - costs) if v is not None else None) for v in vals]
            reason = r.get("exit_reason") or ""
            out_.append({"date": r.get("date_entry"), "coin": r.get("coin"),
                "side": r.get("our_side"), "net30": official,
                "net_live": nl, "net_tape": nt, "settled": settled, "flags": flags,
                "status": vstat, "mismatch": 0,
                "no_price": int(reason == "no_price"),
                "late": int(reason.endswith("_late")), "stale": 0,   # аудит-2 №15
                "grace": None, "vault": 0,
                "entry_src": r.get("entry_src") or "", "exit_src": r.get("exit_src") or "",
                "peak": fnum(r.get("peak_pct")), "trough": fnum(r.get("trough_pct")),
                "move": fnum(r.get("move_pct")), "dur": fnum(r.get("dur_s")),
                "usd": fnum(r.get("usd")), "pos_usd": fnum(r.get("pos_usd")),
                "twap_side": r.get("twap_side"), "src": r.get("src"),
                "kind": r.get("kind"), "hour": fnum(r.get("hour")),
                "btc": fnum(r.get("btc_move_pct")),
                "cancel_after": fnum(r.get("cancel_after_entry"), 0),
                "completed": fnum(r.get("completed"), 0),
                "confirm": r.get("exch_status") or "",
                "exit_reason": reason, "exit_min": fnum(r.get("exit_min")),
                "px_src": f"{r.get('p0_src') or '?'}/{r.get('p1_src') or '?'}",
                "cancel_event_min": fnum(r.get("cancel_event_min")),
                "cancel_seen_min": fnum(r.get("cancel_seen_min")),
                "exit_ts": r.get("exit_ts") or "",
                "wallet": (r.get("whale_addr") or "")[:10], "entered": 1,
                "ts": _ts_local(r.get("date_entry")),
                "_ct": (_parse_curve(srow.get("curve_tape"), TWAP_TRACK_MIN) if srow else None),
                "_cl": live_curve})
        return out_
    def _tw_block(rs, trs=None):
        trs = _tw_trades(rs) if trs is None else trs
        blk = _agg_block(trs, now, TWAP_TRACK_MIN)
        # v2.13 (рев'ю v2.12 №4): головна вибірка — УСІ фактичні paper-
        # входи (скасовані після входу виходять хвилиною скасування);
        # окремо — підтверджено завершені твапи (біржа: finished і
        # виконано повністю, без скасування); лічильники скасованих,
        # частково виконаних (finished, але < sz) і непідтверджених;
        # скільки входів мають обидві ціни зі свічок біржі
        _f = lambda r, k: fnum(r.get(k), 0)
        conf_rows = [r for r in rs
                     if _f(r, "completed") == 1 and _f(r, "cancel_after_entry") == 0
                     and not (r.get("exit_reason") or "").endswith("_late")]
        conf = [x for x in (_tw_net(r)[0] for r in conf_rows) if x is not None]
        n_cancel = sum(1 for r in rs if _f(r, "cancel_after_entry") == 1)
        n_partial = sum(1 for r in rs
                        if _f(r, "completed") == 0 and _f(r, "cancel_after_entry") == 0
                        and r.get("exch_status") == "finished")
        n_unconf = sum(1 for r in rs
                       if _f(r, "completed") == 0 and _f(r, "cancel_after_entry") == 0
                       and r.get("exch_status") != "finished")
        n_candle = sum(1 for r in rs
                       if fnum(r.get("net60_pct")) is not None
                       and r.get("p0_src") == "candle" and r.get("p1_src") == "candle")
        blk.update({"n_confirmed": len(conf), "median_confirmed": _median(conf),
                    "mean_confirmed": (sum(conf) / len(conf)) if conf else None,
                    "win_confirmed": (100.0 * sum(1 for v in conf if v > 0) / len(conf))
                        if conf else None,
                    "n_cancelled": n_cancel, "n_partial": n_partial,
                    "n_unconfirmed": n_unconf, "n_candle": n_candle})
        return blk
    for st, kind_ in ((T1_NAME, "open"), (T2_NAME, "reduce")):
        # когорти ≥1/≥1.5/≥2% — окремі стратегії зі своїми рядками (v2.13)
        fam = [r for r in tw if (r.get("strategy") or "").startswith(st)]
        fam.sort(key=lambda r: r.get("date_entry") or "")
        rows = [r for r in fam if r.get("strategy") == st]
        # v2.16 (рев'ю): сигнали картки — з версії логіки САМОЇ стратегії
        # (STRAT_SINCE), а не спільної стрічки: інакше «сигналів 40, входів
        # 3» після бампа T-стратегій
        sig_rows = [r for r in tws_all if r.get("kind") == kind_
                    and fnum(r.get("eligible"), 0) == 1
                    and _v_ok(r.get("algo_v"), STRAT_SINCE.get(st, DATA_ALGO_V))]
        cohorts = {}
        for thr in TWAP_COHORTS:
            _cn = _twap_cohort_name(st, thr)
            _crows = [r for r in fam if r.get("strategy") == _cn]
            _ctr = _tw_trades(_crows)
            _blk = _tw_block(_crows, _ctr)
            _strat2_full[_cn] = {"kind": "twap", "H": TWAP_TRACK_MIN, "trades": _ctr}
            _blk["entered"] = len(_crows)
            _blk["per_day"], _blk["days"] = _rate2(
                _cn, _crows, "date_entry", len(_crows) + _open_by_st.get(_cn, 0))
            _blk["open_now"] = _open_by_st.get(_cn, 0)
            _blk["trades"] = _pub(_ctr[-120:])
            cohorts[str(thr)] = _blk
        base = cohorts[str(TWAP_COHORTS[0])]
        trades = base["trades"]
        _strat2_full[st] = _strat2_full.get(_twap_cohort_name(st, TWAP_COHORTS[0]))
        out["strategies"][st] = dict(base, **{
            "kind": "twap", "signals": len(sig_rows), "entered": len(rows),
            "track_min": TWAP_TRACK_MIN, "hold_min": TWAP_HOLD_MIN,
            "cohorts": cohorts, "trades": trades,
            "cohort_names": {str(thr): _twap_cohort_name(st, thr) for thr in TWAP_COHORTS},
            "since": STRAT_SINCE.get(st, DATA_ALGO_V),
            # причини пропусків цієї когорти твапів (по сигнальній стрічці)
            "drops": {k: sum(1 for r in sig_rows if r.get("result") == k)
                      for k in ("move_small", "cancelled", "late",
                                "not_new_or_reduce", "dup_twapid",
                                "kind_unproven", "no_verify",
                                "no_price", "no_book", "kind_unknown", "busy")},
        })
    with twap_lock:
        _tw_watch = [{"coin": r["coin"], "side": r["side"], "usd": r["usd"],
                      "dur": r["dur"], "end": r["end"], "kind": r["kind"],
                      "src": r["src"]}
                     for r in twap_reg.values() if r["state"] == "watch"]
        _tw_recent = sorted(
            [{"coin": r["coin"], "side": r["side"], "usd": r["usd"],
              "dur": r["dur"], "end": r["end"], "state": r["state"],
              "reason": r["reason"], "move": r["move"], "strategy": r["strategy"],
              "kind": r["kind"], "src": r["src"]}
             for r in twap_reg.values()
             if r["state"] in ("entered", "dropped")],
            key=lambda x: x["end"] or 0, reverse=True)[:12]
    # v2.16: стан settlement для хедера вкладки
    out["settle"] = {"n": len(settl),
                     "done": stats.get("settle_done", 0),
                     "failed": stats.get("settle_failed", 0),
                     "last_ok_s_ago": (round(now - stats["settle_last_ok"], 0)
                                       if stats.get("settle_last_ok") else None),
                     "err": stats.get("settle_err", "")}
    out["twap"] = dict(twap_stats, watching=_tw_watch, recent=_tw_recent,
                       channels={ch: {"last_id": twap_last_ids.get(ch, 0),
                                      "ok_ago_s": (round(now - s["ok_ts"], 0)
                                                   if s["ok_ts"] else None),
                                      "err": s["err"],
                                      "gaps": len(s.get("gaps") or [])}
                                 for ch, s in _twap_ch_state.items()},
                       signals_total=len(tws),
                       signals_eligible=sum(1 for r in tws
                                            if fnum(r.get("eligible"), 0) == 1))
    with strat2_lock:
        # "відкрито зараз" = лише РЕАЛЬНІ paper-позиції: без тіней
        # (_OUTCOME), без озброєних R2 (входу ще немає) і без done
        out["open"] = (
            [{"strategy": p["strategy"], "coin": p["coin"], "state": "open",
              "age_s": round(now - p.get("entry_ts", p["detect_ts"]), 0)}
             for p in rev_open.values()
             if not p["strategy"].startswith("_") and not p.get("done")
             and p.get("state", "open") == "open"
             # v2.15 (аудит v2.14 №5c): закрита TWAP-угода, що ще веде
             # криву, — не «у ринку»; v2.16: так само R з вирішеним виходом
             and not (p.get("row_kind") == "twap" and _twap_trade_closed(p, now))
             and not p.get("exit_reason")]
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
    # ── Дослідницькі порівняння (аудит-3 №4): ДВА чітко відокремлені блоки.
    # "verified" — на тих самих угодах R1, що й картка: офіційний net (гірший
    # з live/стрічки), без late/stale, ознаки епізоду з settlement (рух
    # епізоду, а не move_3m), та сама популяція (версія/legacy-допуск).
    # "observed" — тіньова outcome-стрічка (m30 міда − повні витрати) ЛИШЕ
    # поточної версії: єдине місце, де є BTC-вето (сигнали без входу);
    # це НЕПЕРЕВІРЕНЕ спостереження — не доказ edge, і так підписується.
    def _cohort_v(rows2):
        vals = [t["net30"] for t in rows2 if t.get("net30") is not None]
        return {"n": len(vals), "median": _median(vals)}
    _r1 = (_strat2_full.get("R1_загальний") or {}).get("trades") or []
    _r1h = [t for t in _r1 if t.get("entered", 1) != 0 and t.get("net30") is not None
            and not t.get("late") and not t.get("stale")]
    def _mag_v(t):
        dm = t.get("dump_move")
        if dm is None:
            return None
        return (-dm) if (t.get("side") or "") == "LONG" else dm
    _r1m = [(t, _mag_v(t)) for t in _r1h]
    research_verified = {
        "mag_1_2":   _cohort_v([t for t, m in _r1m if m is not None and 1.0 <= m < 2.0]),
        "mag_2p":    _cohort_v([t for t, m in _r1m if m is not None and m >= 2.0]),
        "shtanga_1": _cohort_v([t for t in _r1h if t.get("shtanga") == 1]),
        "shtanga_0": _cohort_v([t for t in _r1h if t.get("shtanga") != 1]),
        "bucket_1_2": _cohort_v([t for t in _r1h if t.get("bucket") in (1, 2)]),
        "bucket_3_5": _cohort_v([t for t in _r1h if t.get("bucket") in (3, 4, 5)]),
        "n_base": len(_r1h),
    }
    def _net30(r):
        v = fnum(r.get("m30"))
        c = fnum(r.get("costs_pct"), 0.15) or 0.15
        return (v - c) if v is not None else None
    def _cohort(rows2):
        vals = [x for x in (_net30(r) for r in rows2) if x is not None]
        return {"n": len(vals), "median": _median(vals)}
    def _mag(r):
        # рух ЕПІЗОДУ у бік тиску кита (v2.16+: dump_move_pct), не 3-хв вікно;
        # move_3m_pct — лише для рядків без колонки епізоду (до 2.16, поза
        # RESEARCH_SINCE у проді)
        dm = fnum(r.get("dump_move_pct"))
        if dm is None:
            return fnum(r.get("move_3m_pct"))
        return (-dm) if (r.get("fade_side") or "") == "LONG" else dm
    # лише поточна семантика (RESEARCH_SINCE): старі outcome-рядки (інше
    # правило руху, ціна детекту з міда) у спостереження не змішуються
    outs_cur = [r for r in outs if _v_ok(r.get("algo_v"), RESEARCH_SINCE)]
    outs_ev = [r for r in outs_cur if r.get("src") != "partial"]
    full_out = [r for r in outs_ev if (_mag(r) or 0) >= 1.0]
    def _btc_measured(r):
        return fnum(r.get("btc_move_pct")) is not None
    research_observed = {
        "btc_on":   _cohort([r for r in full_out if _btc_measured(r)
                             and fnum(r.get("btc_ok")) == 1]),
        "btc_off":  _cohort([r for r in full_out if _btc_measured(r)
                             and fnum(r.get("btc_ok")) == 0]),
        "btc_na":   _cohort([r for r in full_out if not _btc_measured(r)]),
        "mag_05_1": _cohort([r for r in outs_ev if _mag(r) is not None and 0.5 <= _mag(r) < 1.0]),
        "mag_1_2":  _cohort([r for r in outs_ev if _mag(r) is not None and 1.0 <= _mag(r) < 2.0]),
        "mag_2p":   _cohort([r for r in outs_ev if (_mag(r) or 0) >= 2.0]),
        "shtanga_1": _cohort([r for r in full_out if fnum(r.get("shtanga"), 0) == 1]),
        "shtanga_0": _cohort([r for r in full_out if fnum(r.get("shtanga"), 0) != 1]),
        "partial":  _cohort([r for r in outs_cur if r.get("src") == "partial"
                             and (_mag(r) or 0) >= 1.0]),
        "n_base": len(outs_cur), "n_excluded_old": len(outs) - len(outs_cur),
    }
    out["research"] = {"verified": research_verified, "observed": research_observed}
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

# ═════════════════════════════════════════════════════════
#  TWAP-РЕВЕРС (ТЗ 08.09 п.5). Джерело — публічні Telegram-канали
#  @HL_TWAP і @TWAPx через веб-прев'ю t.me/s/<канал>: чиста stdlib,
#  без бота-адміна в чужому каналі і без MTProto-акаунта. Беремо ЛИШЕ
#  твапи тривалістю ≤15 хв. В останню хвилину твапу, якщо його не
#  скасували і ціна за час твапу пройшла ≥1% у його бік (ціна за
#  хвилину до старту проти ціни на передостанній хвилині), входимо
#  ПРОТИ твапу, тримаємо 1 год (headline = m60), крива до 120 хв.
#  T1 — твап ВІДКРИВАЄ/нарощує позицію кита, T2 — СКОРОЧУЄ наявну
#  (clearinghouseState на старті, через prio-проксі). Когорти ≥1.5% і
#  ≥2% — підмножини тих самих входів (кнопки в UI), окремих трекерів
#  не потребують.
# ═════════════════════════════════════════════════════════
TWAP_ENABLED     = True
TWAP_CHANNELS    = ("HL_TWAP", "TWAPx")
TWAP_POLL_S      = 30
TWAP_MAX_DUR_S   = 15 * 60
TWAP_MIN_MOVE    = 1.0            # % руху у бік твапу для входу
TWAP_COHORTS     = (1.0, 1.5, 2.0)
TWAP_HOLD_MIN    = 60             # «виходимо через годину» = m60
TWAP_TRACK_MIN   = 120            # крива після входу
TWAP_ENTRY_LEAD  = 60.0           # вхід за хвилину до кінця твапу
TWAP_LATE_S      = 15.0           # вхід ЛИШЕ в останню хвилину (+15с на
                                  # тик); пізніше — пропуск (late). Було 90с
                                  # — рев'ю v2.12 №5a: вхід після кінця
TWAP_BACKLOG_S   = 3600           # старт старший за годину — історія
# v2.12 (з рев'ю CH): звірка з біржею. twapHistory дає twapId, стан
# (activated/finished/terminated/error) і виконання; userTwapSliceFills
# — startPosition ПЕРШОГО слайсу (позиція ДО твапу; слайс з'являється
# через ~1-2с після створення): 0 → нова позиція (T1), протилежний бік
# → скорочення (T2), той самий бік → долив, більше за позицію →
# переворот — обидва НЕ сигнал. Ціни P0/P1 — закриття останньої повної
# 1-хв свічки біржі перед стартом / перед останньою хвилиною
# (відтворювано; фолбек — поллер, позначено p0_src/p1_src).
TWAP_MIN_DUR_S   = 120            # коротший за 2 хв — немає передостанньої хвилини
TWAP_VERIFY_S    = 60             # після кінця: повтор звірки стану (≤5 запитів)
TWAP_CONFIRM_S   = 300            # після кінця: чекати finished/terminated до 5 хв
TWAP_FIRST_SLICE_S = 25.0         # слайс пізніше за це від старту — не «перший»:
                                  # інтервал слайсів 30с, тож філ ≤25с після
                                  # старту не може бути другим (аудит v2.13:
                                  # 35с пропускало другий слайс на 30-й с)
TWAP_VERIFY_MAX_AGE_S = 90.0      # вхід лише зі звіркою, УСПІШНОЮ не пізніше
                                  # ніж за стільки секунд (аудит v2.13 №7)
TWAP_XCH_START_S = 20.0           # той самий твап з іншого каналу: старт ±20с
                                  # (обидва канали дають час біржі; ±120с
                                  # зливало перестворену за 30с заявку)
TWAP_XCH_DUR_S   = 30.0           # …і та сама тривалість (15 і 16 хв — різні)
TWAP_SIG_CSV     = os.path.join(DATA_DIR, "twap_signals.csv")
TWAP_CSV         = os.path.join(DATA_DIR, "twap_trades.csv")
TWAP_SIG_HEADERS = ["twap_id", "date", "src", "posts", "addr", "coin",
                    "twap_side", "usd", "dur_s", "start", "end", "kind",
                    "kind_src", "exch_id", "start_pos",
                    "pos_usd", "eligible", "cancelled", "completed", "p0",
                    "p0_src", "p1", "p1_src", "move_pct", "result", "strategy",
                    "algo_v", "eol"]
# v2.15 (аудит v2.14 №1, №5): рядок УГОДИ пишеться у момент вирішеного
# paper-виходу (ціна хвилини виходу / перша наступна в межах
# TWAP_EXIT_CAP_MIN / кап без ціни) і більше не змінюється; крива до 120 хв
# — ОКРЕМИЙ файл twap_curves.csv, дописується при завершенні трекера.
# cancel_event_min — хвилина самої події скасування (біржа/пост), для
# дослідження; exit_min — хвилина paper-виходу (не раніше, ніж дізнались)
TWAP_EXIT_CAP_MIN = 5             # без ціни на хвилині виходу: до 5 наступних
TWAP_HEADERS     = (["twap_id", "strategy", "date_entry", "coin", "our_side",
                     "twap_side", "whale_addr", "src", "usd", "dur_s", "kind",
                     "kind_src", "exch_id", "start_pos",
                     "pos_usd", "move_pct", "p0", "p0_src", "p1", "p1_src",
                     "entry_px", "net60_pct", "costs_pct", "peak_pct",
                     "trough_pct", "cancel_after_entry", "completed",
                     "exit_min", "exit_reason", "cancel_event_min",
                     "cancel_seen_min", "exit_ts", "exch_status", "exec_pct",
                     "btc_move_pct", "hour", "algo_v",
                     # v2.16: чесний вхід/вихід зі стакану
                     "entry_ts_ms", "entry_src", "entry_px_mid",
                     "exit_ts_ms", "exit_px", "exit_src"]
                    + [f"m{i}" for i in range(1, TWAP_HOLD_MIN + 1)]
                    + ["eol"])
TWAP_CURVE_CSV   = os.path.join(DATA_DIR, "twap_curves.csv")
TWAP_CURVE_HEADERS = (["twap_id", "strategy", "date_entry", "coin", "our_side",
                       "exit_min", "algo_v"]
                      + [f"m{i}" for i in range(1, TWAP_TRACK_MIN + 1)]
                      + ["eol"])

twap_lock     = threading.Lock()
twap_reg      = {}     # twap_id -> запис (див. _twap_register)
twap_last_ids = {}     # канал -> останній оброблений post id
twap_stats    = {"posts": 0, "starts": 0, "eligible": 0, "cancelled": 0,
                 "entered": 0, "dropped": 0, "fetch_err": 0, "parse_err": 0,
                 # v2.15: лічильники ідентичності/догортання — з нуля, щоб у
                 # /status «0» відрізнявся від «ключа нема»
                 "dup_posts": 0, "fin_ambiguous": 0, "fin_unmatched": 0,
                 "fin_nomatch": 0, "pages_extra": 0, "busy_cohorts": 0}
_twap_ch_state = {ch: {"ok_ts": 0.0, "err": 0} for ch in TWAP_CHANNELS}

_TWAP_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
     "nov", "dec"), 1)}

def _twap_usd(num, suf):
    v = float(str(num).replace(",", ""))
    return v * {"k": 1e3, "m": 1e6, "b": 1e9}.get((suf or "").lower(), 1.0)

def _twap_hl_dt(s, off_h):
    """'08 Sep 2026 15:20:45' у зоні UTC+off_h -> epoch UTC. Місяць — по
    словнику, не через %b: strptime залежить від локалі системи."""
    d, mon, y, hms = s.split()
    hh, mm, ss = (int(x) for x in hms.split(":"))
    st = (int(y), _TWAP_MONTHS[mon[:3].lower()], int(d), hh, mm, ss)
    return calendar.timegm(st) - int(off_h) * 3600

_HL_START  = re.compile(r"\$\s*(\d[\d,]*\.?\d*)\s*([KkMmBb])?\s+(selling|buying)"
                        r"\s+([A-Za-z0-9]+)", re.I)
_HL_USER   = re.compile(r"User:\s*(0x[0-9a-fA-F]{40})")
# у reply-цитаті t.me адреса ОБРІЗАНА (~25 символів) — беремо префікс,
# _twap_find звіряє за startswith
_HL_USER_PFX = re.compile(r"User:\s*(0x[0-9a-fA-F]{8,40})")
_HL_PERIOD = re.compile(r"Period:\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})"
                        r"\s*-\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})"
                        r"\s*UTC\s*([+-]\d{1,2})")
_HL_ETA    = re.compile(r"ETA:\s*(?:(\d+)\s*h,?\s*)?(?:(\d+)\s*m)?")
_HL_PRICE  = re.compile(r"Price:\s*\$\s*(\d[\d,]*\.?\d*)")
_HL_CLOSED = re.compile(r"\$\s*(\d[\d,]*\.?\d*)\s*([KkMmBb])?\s+TWAP\s+with\s+"
                        r"([A-Za-z0-9]+)\s+closed", re.I)
_X_START   = re.compile(r"\$\s*(\d[\d,]*\.?\d*)\s*([KkMmBb])?\s+(продажа|покупка)"
                        r"\s+([A-Za-z0-9]+)\s+в\s+течени[ияе]\s+(\d+(?:[.,]\d+)?)"
                        r"\s*(минут|мин|час)", re.I)
_X_ADDR    = re.compile(r"Субъект:\s*(0x[0-9a-fA-F]{40})")
_X_PRICE   = re.compile(r"Цена:\s*\$\s*(\d[\d,]*\.?\d*)")
_X_CREATED = re.compile(r"Создан\s+в:\s*(\d{2}):(\d{2}):(\d{2})\s*\(UTC\)")
_X_SIZE    = re.compile(r"Размер:\s*[\d.,]+\s*/\s*[\d.,]+\s*([A-Za-z0-9]+)")
# v2.14 (аудит v2.13 №2, №3): фінальні пости несуть id заявки на біржі та
# виконання — точна прив'язка і критерій «виконано повністю» без здогадок
_X_TWAPID  = re.compile(r"TwapId:\s*(\d+)")
_X_EXEC    = re.compile(r"Размер:\s*([\d.,]+)\s*/\s*([\d.,]+)")
_X_STATUS  = re.compile(r"Статус:\s*([A-Za-z]+)")
_HL_FILLED = re.compile(r"(?:\[|Filled:\s*)([\d.,]+)\s*/\s*([\d.,]+)")
_HL_TIME   = re.compile(r"Time:\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})\s*GMT")

def _twap_fnum(s):
    try:
        return float(str(s).replace(",", ""))
    except (TypeError, ValueError):
        return None

def twap_parse(channel, text, msg_ts, reply=""):
    """Повідомлення каналу -> dict або None (не про твап).
    kind: start | cancel | done. Поля: addr (у HL «closed by user» —
    лише ПРЕФІКС адреси з reply-цитати), coin (як у пості; канонічну
    назву дає _coin_canon при прийомі), side buy/sell, usd, start, end,
    dur, px_msg. reply — текст цитованого поста (t.me показує його над
    відповіддю): HL-скасування посилається на стартовий пост і саме там
    бік/адреса. Прев'ю t.me інколи «сплющує» пост в один рядок —
    регекси не спираються на переноси. Перевірено на живих постах 08.09."""
    t = " ".join(text.split())
    rq = " ".join((reply or "").split())
    if channel == "HL_TWAP":
        low = t.lower()
        if ("is cancelled" in low or "closed by user" in low
                or t.startswith("⛔")):
            kind = "cancel"
        elif ("successful" in low and "completed" in low) or t.startswith("✅"):
            kind = "done"
        else:
            kind = "start"
        m = _HL_START.search(t)
        if not m:
            mc = _HL_CLOSED.search(t)
            if kind == "cancel" and mc:
                # «$5.10m TWAP with HYPE closed by user» — деталі у цитаті
                mr = _HL_START.search(rq)
                mu = _HL_USER_PFX.search(rq)
                mf = _HL_FILLED.search(t)
                mt = _HL_TIME.search(t)
                return {"kind": "cancel",
                        "addr": mu.group(1).lower() if mu else None,
                        "coin": mc.group(3),
                        "side": (("sell" if mr.group(3).lower() == "selling"
                                  else "buy") if mr else None),
                        "usd": _twap_usd(mc.group(1), mc.group(2)),
                        "start": None, "end": None, "dur": None,
                        "exact": False, "px_msg": None,
                        # v2.14: виконання і час скасування — з поста
                        "twap_id": None, "status": "terminated",
                        "exec_sz": _twap_fnum(mf.group(1)) if mf else None,
                        "target_sz": _twap_fnum(mf.group(2)) if mf else None,
                        "fin_ts": (_twap_hl_dt(mt.group(1), 0) if mt else None)}
            return None
        usd = _twap_usd(m.group(1), m.group(2))
        side = "sell" if m.group(3).lower() == "selling" else "buy"
        coin = m.group(4)
        mu = _HL_USER.search(t)
        addr = mu.group(1).lower() if mu else None
        mp = _HL_PERIOD.search(t)
        if mp:
            start = _twap_hl_dt(mp.group(1), mp.group(3))
            end = _twap_hl_dt(mp.group(2), mp.group(3))
        else:
            me = _HL_ETA.search(t)
            dur = 0
            if me and (me.group(1) or me.group(2)):
                dur = int(me.group(1) or 0) * 3600 + int(me.group(2) or 0) * 60
            start, end = msg_ts, (msg_ts + dur if dur else None)
        mpx = _HL_PRICE.search(t)
        out = {"kind": kind, "addr": addr, "coin": coin, "side": side,
               "usd": usd, "start": start, "end": end,
               "dur": (end - start) if (start and end) else None,
               # exact — старт узятий із Period; без нього (сплющений
               # репост обрізає «Period: 08…») старт = час поста і для
               # звірки cancel/done ним користуватись не можна
               "exact": bool(mp),
               "px_msg": (float(mpx.group(1).replace(",", ""))
                          if mpx else None)}
        if kind != "start":
            # відредагований старт «cancelled [exec/total]» / «successful
            # completed [exec/total]» (v2.14): виконання — з поста
            mf = _HL_FILLED.search(t)
            out.update(twap_id=None, fin_ts=None,
                       status=("terminated" if kind == "cancel" else "finished"),
                       exec_sz=_twap_fnum(mf.group(1)) if mf else None,
                       target_sz=_twap_fnum(mf.group(2)) if mf else None)
        return out
    if channel == "TWAPx":
        low = t.lower()
        fin = None
        if "twap отмен" in low or t.startswith("❌"):
            fin = "cancel"
        elif "twap заверш" in low or t.startswith("✅"):
            fin = "done"
        if fin:
            ma = _X_ADDR.search(t); ms = _X_SIZE.search(t)
            # бік — із цитати стартового поста (у фіналі його немає)
            mr = _X_START.search(rq)
            # v2.14: TwapId біржі, статус і виконання — прямо з поста
            mi = _X_TWAPID.search(t); me = _X_EXEC.search(t); mst = _X_STATUS.search(t)
            return {"kind": fin,
                    "addr": ma.group(1).lower() if ma else None,
                    "coin": ms.group(1) if ms else None,
                    "side": (("sell" if mr.group(3).lower().startswith("прод")
                              else "buy") if mr else None),
                    "usd": None, "start": None, "end": None,
                    "dur": None, "px_msg": None,
                    "twap_id": int(mi.group(1)) if mi else None,
                    "status": (mst.group(1).lower() if mst
                               else ("terminated" if fin == "cancel" else "finished")),
                    "exec_sz": _twap_fnum(me.group(1)) if me else None,
                    "target_sz": _twap_fnum(me.group(2)) if me else None,
                    "fin_ts": None}
        m = _X_START.search(t)
        if not m:
            return None
        usd = _twap_usd(m.group(1), m.group(2))
        side = "sell" if m.group(3).lower().startswith("прод") else "buy"
        coin = m.group(4)
        n = float(m.group(5).replace(",", "."))
        dur = n * (60.0 if m.group(6).lower().startswith("мин") else 3600.0)
        ma = _X_ADDR.search(t)
        mc = _X_CREATED.search(t)
        start = msg_ts
        if mc:
            day = int(msg_ts // 86400) * 86400
            start = (day + int(mc.group(1)) * 3600 + int(mc.group(2)) * 60
                     + int(mc.group(3)))
            if start > msg_ts + 600:      # пост одразу після півночі UTC
                start -= 86400
        mpx = _X_PRICE.search(t)
        return {"kind": "start", "addr": ma.group(1).lower() if ma else None,
                "coin": coin, "side": side, "usd": usd, "start": start,
                "end": start + dur, "dur": dur,
                "px_msg": (float(mpx.group(1).replace(",", ""))
                           if mpx else None)}
    return None

def _iso_ts(s):
    try:
        return _dtmod.datetime.fromisoformat(
            s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time()

def _tme_text(fragment):
    txt = re.sub(r"<br\s*/?>", "\n", fragment)
    return _htmlmod.unescape(re.sub(r"<[^>]+>", "", txt))

def _tme_parse(page):
    """HTML прев'ю t.me/s/<канал> -> [(post_id, ts, text, reply)] за
    зростанням id. text — сам пост (js-message_text), reply — цитата
    поста, на який він відповідає (js-message_reply_text; порожньо,
    якщо не відповідь). Обидва класи починаються з tgme_widget_message_text,
    і цитата стоїть у розмітці ПЕРШОЮ — регекс без js-суфікса брав її
    замість тексту поста (рев'ю v2.11: усі скасування читались як
    старти). <br> -> перенос, теги геть, HTML-сутності розкодовані."""
    out = []
    for b in re.split(r'<div class="tgme_widget_message_wrap', page)[1:]:
        pid = re.search(r'data-post="[^"/]+/(\d+)"', b)
        tx = re.search(r'<div class="tgme_widget_message_text js-message_text[^"]*"'
                       r'[^>]*>(.*?)</div>', b, re.S)
        rp = re.search(r'<div class="tgme_widget_message_text js-message_reply_text[^"]*"'
                       r'[^>]*>(.*?)</div>', b, re.S)
        # v2.12: id поста, на який відповідають (точне зіставлення
        # скасування зі стартом — без здогадок по сумі/монеті)
        rl = re.search(r'<a class="tgme_widget_message_reply[^"]*"\s+href="https://t\.me/'
                       r'[^/"]+/(\d+)"', b)
        tm = re.search(r'<time[^>]*datetime="([^"]+)"', b)
        if not (pid and tx):
            continue
        out.append((int(pid.group(1)), _iso_ts(tm.group(1)) if tm else time.time(),
                    _tme_text(tx.group(1)), _tme_text(rp.group(1)) if rp else "",
                    int(rl.group(1)) if rl else 0))
    out.sort(key=lambda x: x[0])
    return out

def _tme_fetch(channel, before=None):
    """Сторінка каналу. Напряму; після 3 збоїв поспіль — через
    REST-проксі, якщо вона є (t.me може бути закритий на IP сервера).
    Сторінка без жодного поста (заглушка/капча/редирект) — це збій, а
    не «тиша в каналі»: інакше st.err не ріс і фолбек не вмикався.
    before=<pid> — старіша сторінка (догортання після пропуску, v2.14)."""
    req = urllib.request.Request(
        f"https://t.me/s/{channel}" + (f"?before={int(before)}" if before else ""),
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                               "AppleWebKit/537.36 Chrome/124 Safari/537.36",
                 "Accept-Language": "en"})
    st = _twap_ch_state[channel]
    opener = (_prio_opener.open if (st["err"] >= 3 and _prio_opener)
              else urllib.request.urlopen)
    with opener(req, timeout=20) as r:
        page = r.read().decode("utf-8", "replace")
    posts = _tme_parse(page)
    if not posts:
        raise APIError(f"no messages in page ({len(page)} bytes)")
    return posts

def _coin_canon(c):
    """Назва монети з поста -> як у Hyperliquid (kPEPE, а не KPEPE):
    звіряємо без регістру з монетами поллера цін; невідому лишаємо."""
    if not c:
        return c
    cu = c.upper()
    with px_lock:
        for k in px_hist:
            if k.upper() == cu:
                return k
    return c

def _twap_find(addr, coin, side=None, near_ts=None, usd=None,
               states=("watch", "entered")):
    """Запис реєстру по монеті (+гаманець або його ПРЕФІКС, +бік, +старт
    ±2 хв, +сума ±15% для cancel без повної адреси); серед кількох —
    найближчий за сумою, далі найсвіжіший."""
    best, best_k = None, None
    for r in twap_reg.values():
        if r["coin"] != coin or r["state"] not in states:
            continue
        if addr and not (r["addr"] or "").startswith(addr):
            continue
        if side and r["side"] != side:
            continue
        if near_ts is not None and abs(r["start"] - near_ts) > 120:
            continue
        du = (abs(r["usd"] - usd) / r["usd"]
              if (usd is not None and r.get("usd")) else None)
        if du is not None and du > 0.15 and (not addr or len(addr) < 42):
            continue
        k = (-(du if du is not None else 1.0), r["start"])
        if best is None or k > best_k:
            best, best_k = r, k
    return best

def _twap_find_all(addr, coin, side=None, near_ts=None, usd=None,
                   states=("watch", "entered")):
    """УСІ активні записи, що підходять під ті самі фільтри, що й
    _twap_find (v2.14): >1 кандидата — неоднозначність, не вгадуємо."""
    out = []
    for r in twap_reg.values():
        if r["coin"] != coin or r["state"] not in states:
            continue
        if addr and not (r["addr"] or "").startswith(addr):
            continue
        if side and r["side"] != side:
            continue
        if near_ts is not None and abs(r["start"] - near_ts) > 120:
            continue
        du = (abs(r["usd"] - usd) / r["usd"]
              if (usd is not None and r.get("usd")) else None)
        # сума з поста — ЗАВЖДИ обмеження, і за повної адреси (аудит v2.14
        # №4b: «closed by user $2.84m» гасив єдиний відомий $100k-твап
        # гаманця); ±2% покриває округлення сум у постах каналів
        if du is not None and du > 0.02:
            continue
        out.append(r)
    return out

def _twap_register(channel, pid, p, now):
    """Старт твапу -> запис реєстру. Той самий твап з ДРУГОГО каналу
    (обидва дублюють одні події) лише додає пост; новий пост ТОГО Ж
    каналу — завжди новий твап: кит скасував і одразу перестворив
    (HYPE $5.10m/$5.11m за 8с — два різні твапи, рев'ю v2.11)."""
    with twap_lock:
        # той самий твап з ІНШОГО каналу: гаманець+монета+бік, старт ±2 хв,
        # тривалість ±60с І сума ±15% — $100k/10 хв і $1M/15 хв за 60с —
        # різні заявки (рев'ю v2.12 №2c); серед кількох — найближчий за
        # стартом; остаточний дедуп — по twapId з біржі
        # v2.14 (аудит v2.13 №2): старт ±20с і ТА САМА тривалість (±30с)
        # — обидва канали дають час біржі; перестворена за 30с заявка тієї
        # ж суми і 15/16-хв заявки — різні; у вже dropped (скасований)
        # запис не зливаємо: нова заявка отримує власну звірку, а
        # справжній дубль зніме _twap_dedup_by_id по twapId біржі
        ex, ex_d = None, None
        for r_ in twap_reg.values():
            if (r_["coin"] != p["coin"] or r_["addr"] != p["addr"]
                    or r_["side"] != p["side"]
                    or channel in r_["src"].split("+")
                    or abs(r_["start"] - p["start"]) > TWAP_XCH_START_S
                    or abs((r_.get("dur") or 0) - (p.get("dur") or 0)) > TWAP_XCH_DUR_S):
                continue
            if (r_.get("usd") and p.get("usd")
                    and abs(r_["usd"] - p["usd"]) > 0.15 * max(r_["usd"], p["usd"])):
                continue
            # найближчий за стартом; сума, що розходиться понад одиницю
            # третьої значущої цифри (округлення каналів), — гірший кандидат
            d_ = abs(r_["start"] - p["start"])
            if r_.get("usd") and p.get("usd"):
                _u = 10 ** (math.floor(math.log10(max(r_["usd"], p["usd"]))) - 2)
                if abs(r_["usd"] - p["usd"]) > _u:
                    d_ += TWAP_XCH_START_S
            if ex is None or d_ < ex_d:
                ex, ex_d = r_, d_
        if ex is not None and ex["state"] == "dropped":
            # найближча — уже знята/скасована заявка. У dropped НЕ зливаємо
            # (аудит v2.14 №4a: відомий id старої заявки не доводить, що
            # пост про неї — $101k через 10с успадковував скасування), але й
            # у НАСТУПНУ живу заявку кита не зливаємо (рев'ю v2.15: дубль
            # скасованого X зливався у перестворений Y, і reply-скасування X
            # гасило Y). Новий запис; справжній дубль зніме
            # _twap_dedup_by_id по id біржі без другого сигналу
            ex = None
        if ex is not None:
            ex["posts"].append(f"{channel}/{pid}")
            ex["src"] = ex["src"] + "+" + channel
            return ex, False
        tid = f"tw-{int(p['start'])}-{p['coin']}-{(p['addr'] or '0xnone')[2:8]}"
        if tid in twap_reg:
            tid = f"{tid}-{pid}"
        rec = {"id": tid, "src": channel, "posts": [f"{channel}/{pid}"],
               "addr": p["addr"], "coin": p["coin"], "side": p["side"],
               "usd": p["usd"], "start": p["start"], "end": p["end"],
               "dur": p["dur"], "px_msg": p["px_msg"],
               "eligible": int(bool(p["dur"]) and p["dur"] <= TWAP_MAX_DUR_S
                               and p["dur"] >= TWAP_MIN_DUR_S and bool(p["addr"])),
               "kind": None, "kind_src": "", "pos_usd": None, "state": "watch",
               "reason": "", "cancelled": 0, "completed": 0,
               "cancel_after_entry": 0, "p0": None, "p1": None,
               "p0_src": "", "p1_src": "", "move": None, "entry_ts": None,
               "entry_px": None, "strategy": "", "created": now,
               # звірка з біржею (v2.12)
               "twap_id": None, "exch": "", "exec_sz": None, "target_sz": None,
               "sp": None, "verified_ts": 0.0, "verify_n": 0,
               "exch_cancelled": 0, "exch_completed": 0, "confirm_done": 0,
               "trackers": [], "sig_written": 0}
        if not rec["eligible"]:
            rec["state"] = "ineligible"
            rec["reason"] = ("dur>15m" if (p["dur"] or 0) > TWAP_MAX_DUR_S
                             else ("dur<2m" if (p["dur"] or 0) and p["dur"] < TWAP_MIN_DUR_S
                                   else ("no_addr" if not p["addr"] else "no_dur")))
        twap_reg[tid] = rec
    return rec, True

def _twap_by_post(channel, pid):
    """Запис реєстру, що містить пост channel/pid (для reply-зіставлення)."""
    if not pid:
        return None
    tag = f"{channel}/{pid}"
    for r in twap_reg.values():
        if tag in r.get("posts", ()):
            return r
    return None

_twap_api_cache = {}   # json(body) -> (ts, відповідь); лише потік вотчера
_twap_candle_miss = {}   # json(body) -> (n порожніх відповідей, ts першої)
TWAP_CANDLE_TRIES = 3    # порожня свічка: ще 2 повтори на наступних тиках, далі — ні

def _twap_api(body, ttl=10.0):
    """Запит до біржі через prio-канал (проксі) з коротким кешем: кілька
    твапів одного гаманця в одному тику не тягнуть історію повторно."""
    key = json.dumps(body, sort_keys=True)
    now = time.time()
    c = _twap_api_cache.get(key)
    if c is not None and now - c[0] < ttl:
        return c[1]
    # бюджет каналу спільний (v2.14); чекати його понад 5с не можна —
    # вхід у твап має відбутись в останню хвилину або не відбутись
    data = hl_post_prio(body, direct=not _prio_opener, max_wait=5.0)
    _twap_api_cache[key] = (now, data)
    if len(_twap_api_cache) > 400:
        for k in [k for k, v in _twap_api_cache.items() if now - v[0] > 900]:
            _twap_api_cache.pop(k, None)
    return data

def _twap_candle_close(coin, cutoff):
    """Закриття останньої ПОВНІСТЮ закритої 1-хв свічки біржі перед
    cutoff (v2.12, з рев'ю CH): відтворювана межа замість «що бачив
    поллер у ту секунду». None — свічки (ще) немає; порожня відповідь НЕ
    кешується (рев'ю v2.12 №5e: свічка може з'явитись за секунду), але
    після TWAP_CANDLE_TRIES порожніх — більше не запитується (рев'ю v2.13
    №4: монета без свічок тягнула вагу 20 щотику все життя твапу)."""
    bar = (int(cutoff) // 60 - 1) * 60
    body = {"type": "candleSnapshot",
            "req": {"coin": coin, "interval": "1m",
                    "startTime": bar * 1000, "endTime": bar * 1000 + 59_999}}
    key = json.dumps(body, sort_keys=True)
    miss = _twap_candle_miss.get(key)
    if miss and miss[0] >= TWAP_CANDLE_TRIES:
        return None
    try:
        rows = _twap_api(body, ttl=3600)
    except Exception as e:
        print(f"  [TWAP] свічка {coin}: {e}")
        return None
    px = None
    if isinstance(rows, list):
        for r in rows:
            try:
                if int(r.get("t", -1)) == bar * 1000 and int(r.get("T", 0)) < cutoff * 1000:
                    v = float(r.get("c") or 0)
                    if v > 0:
                        px = v
                        break
            except (TypeError, ValueError):
                continue
    if px is None:
        _twap_api_cache.pop(key, None)
        _twap_candle_miss[key] = ((miss[0] + 1) if miss else 1,
                                  (miss[1] if miss else time.time()))
        if len(_twap_candle_miss) > 400:
            _t = time.time()
            for k in [k for k, v in _twap_candle_miss.items() if _t - v[1] > 3600]:
                _twap_candle_miss.pop(k, None)
    else:
        _twap_candle_miss.pop(key, None)
    return px

def _twap_chain_head(mine):
    """Перший філ твапу за ЛАНЦЮГОМ позицій (v2.15, аудит v2.14 №3): для
    кожного філу end = startPosition ± sz за боком (B: +, A: −); голова —
    філ, чий startPosition не дорівнює end жодного іншого. Рівно одна
    голова → вона; нуль або кілька (діра в історії, філ без
    startPosition) → None, і вид твапу лишається недоведеним."""
    items = []
    for f in mine:
        if not isinstance(f, dict):
            continue
        try:
            sp = float(f["startPosition"])
            sz = float(f.get("sz") or 0)
        except (KeyError, TypeError, ValueError):
            return None
        if sz <= 0:
            continue
        sign = 1.0 if f.get("side") == "B" else -1.0
        items.append((f, sp, sp + sign * sz))
    if not items:
        return None
    def _eq(a, b):
        return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))
    heads = [it for it in items
             if not any(o is not it and _eq(it[1], o[2]) for o in items)]
    return heads[0][0] if len(heads) == 1 else None

def _twap_verify(rec, now, slices=True):
    """Звірка запису з біржею (v2.12, з рев'ю CH; уточнено v2.13).
    twapHistory: запис тієї ж монети/боку зі стартом ±10с І тією ж
    тривалістю в хвилинах (±1) → twapId, стан, виконання; два різні
    twapId однаково близькі за стартом — не вгадуємо (рев'ю v2.12 №2b:
    A 10 хв/1000 і B 15 хв/10000 за 8с діставали один id).
    userTwapSliceFills: ПЕРШИЙ слайс цього twapId → startPosition
    (позиція до твапу): 0 → open (T1); протилежний бік і розмір ≤
    |позиції| → reduce (T2); той самий бік → increase; більше за позицію
    → flip (не сигнал). Слайс — «перший» лише якщо не пізніше
    TWAP_FIRST_SLICE_S після старту і має startPosition; інакше вид не
    доведено (kind_src="unproven", рев'ю v2.12 №8a — кап 2000 виконань).
    Повертає True, коли twapId відомий."""
    if not rec.get("addr"):
        return False
    rec["verify_n"] = rec.get("verify_n", 0) + 1
    rec["verified_ts"] = now
    try:
        hist = _twap_api({"type": "twapHistory", "user": rec["addr"]}, ttl=10)
    except Exception as e:
        print(f"  [TWAP] twapHistory {rec['coin']} {rec['addr'][:10]}…: {e}")
        return False
    if not isinstance(hist, list):
        return False
    want = "B" if rec["side"] == "buy" else "A"
    want_min = int(round((rec.get("dur") or 0) / 60.0))
    by_id = {}   # twapId -> (найсвіжіша ревізія, її time, відстань старту)
    for it in hist:
        if not isinstance(it, dict):
            continue
        st = it.get("state") or {}
        if (st.get("coin") or "") != rec["coin"] or st.get("side") != want:
            continue
        tid = it.get("twapId")
        if rec.get("twap_id") is not None:
            if tid != rec["twap_id"]:
                continue
            dist = 0.0
        else:
            try:
                ts0 = float(st.get("timestamp") or 0) / 1000.0
                mins = int(st.get("minutes") or 0)
            except (TypeError, ValueError):
                continue
            dist = abs(ts0 - rec["start"])
            if dist > 10 or (want_min and abs(mins - want_min) > 1):
                continue
        try:
            t_rev = float(it.get("time") or 0)
        except (TypeError, ValueError):
            t_rev = 0.0
        cur = by_id.get(tid)
        if cur is None or t_rev >= cur[1]:
            by_id[tid] = (it, t_rev, dist)
    if not by_id:
        return False
    if rec.get("twap_id") is None and len(by_id) > 1:
        ranked = sorted(by_id.values(), key=lambda x: x[2])
        if ranked[1][2] - ranked[0][2] < 3.0:
            rec["exch"] = "ambiguous"
            return False
        best, best_t = ranked[0][0], ranked[0][1]
    else:
        best, best_t, _ = min(by_id.values(), key=lambda x: x[2])
    st = best.get("state") or {}
    status = best.get("status")
    status = (status.get("status") if isinstance(status, dict) else status) or ""
    rec["twap_id"] = best.get("twapId")
    rec["exch"] = status
    # УСПІШНА звірка (verified_ts — лише спроба; №7): РЕАЛЬНИЙ час — давність
    # перед входом міряється теж реальним годинником, а не часом тику
    rec["verify_ok_ts"] = time.time()
    # час останньої ревізії статусу (поле time — у СЕКУНДАХ, на відміну від
    # state.timestamp у мс): точна хвилина скасування для виходу трекера
    rec["exch_ts"] = best_t if best_t and best_t < 1e11 else (best_t / 1000.0 if best_t else None)
    try:
        rec["exec_sz"] = float(st.get("executedSz") or 0)
        rec["target_sz"] = float(st.get("sz") or 0)
    except (TypeError, ValueError):
        rec["exec_sz"], rec["target_sz"] = None, None
    if status in ("terminated", "error"):
        rec["exch_cancelled"] = 1
    elif (status == "finished" and rec["target_sz"]
          and rec["exec_sz"] is not None
          and rec["exec_sz"] >= rec["target_sz"] * (1 - 1e-6)):
        rec["exch_completed"] = 1
    if not slices or rec.get("kind_src") in ("slice", "unproven"):
        return True
    try:
        fills = _twap_api({"type": "userTwapSliceFills", "user": rec["addr"]}, ttl=10)
    except Exception as e:
        print(f"  [TWAP] slices {rec['coin']} {rec['addr'][:10]}…: {e}")
        return True
    if not isinstance(fills, list):
        return True
    mine = [w.get("fill") or {} for w in fills
            if isinstance(w, dict) and w.get("twapId") == rec["twap_id"]]
    if not mine:
        return True   # перший слайс ще не в історії — наступний тик
    # v2.14 (аудит v2.13): відповідь — ≤2000 ОСТАННІХ виконань; «найраніший
    # наш» доведено перший, лише якщо історія сягає часу старту твапу
    if len(fills) >= 2000:
        try:
            oldest = min(float((w.get("fill") or {}).get("time") or 0)
                         for w in fills if isinstance(w, dict)) / 1000.0
        except (TypeError, ValueError):
            oldest = 0.0
        if oldest > rec["start"]:
            rec["kind_src"] = "unproven"
            return True
    # перший філ — голова ЛАНЦЮГА позицій (startPosition/розміри), а не
    # min(time, tid): tid — хеш, не порядковий номер (аудит v2.14 №3: два
    # філи одного слайсу з однаковим часом — «перший» обирався навмання,
    # open ставав increase, reduce — flip)
    first = _twap_chain_head(mine)
    if first is None:
        rec["kind_src"] = "unproven"
        return True
    try:
        first_t = float(first.get("time") or 0) / 1000.0
        sp = float(first.get("startPosition"))
        px0 = float(first.get("px") or 0)
    except (TypeError, ValueError):
        rec["kind_src"] = "unproven"
        return True
    if first_t - rec["start"] > TWAP_FIRST_SLICE_S:
        rec["kind_src"] = "unproven"
        return True
    qty = rec["target_sz"] or 0
    sign = 1 if rec["side"] == "buy" else -1
    if abs(sp) <= 1e-12:
        kind = "open"
    elif sp * sign < 0 and qty <= abs(sp) + 1e-9:
        kind = "reduce"
    elif sp * sign < 0:
        kind = "flip"
    else:
        kind = "increase"
    rec["kind"], rec["kind_src"], rec["sp"] = kind, "slice", sp
    rec["pos_usd"] = abs(sp) * px0
    return True

def _twap_resolve_kind(rec):
    """Сумісна обгортка: вид твапу лише зі звірки з біржею (v2.12).
    Відомий стан біржі (terminated при читанні поста) застосовується
    одразу (рев'ю v2.12 №1)."""
    _twap_verify(rec, time.time(), slices=True)
    # дубль по id біржі (той самий твап з другого каналу) — знімаємо ДО
    # застосування стану біржі, інакше terminated лічився б і дропався б
    # удруге (рев'ю v2.15: дедуп був недосяжний — звірка з посту одразу
    # давала kind_src=slice, і тик його вже не викликав)
    if _twap_dedup_by_id(rec):
        return None
    _twap_apply_exchange(rec)
    return rec.get("kind") if rec.get("kind_src") == "slice" else None

def _twap_sig_write(rec, result):
    if rec.get("sig_written"):
        return
    def _n(v, nd=None):
        if v is None: return ""
        return round(v, nd) if nd is not None else v
    row = [rec["id"], _dt(time.time()), rec["src"], "+".join(rec["posts"]),
           rec["addr"] or "", rec["coin"], rec["side"] or "",
           _n(rec["usd"], 0), _n(rec["dur"], 0), _dt(rec["start"]),
           (_dt(rec["end"]) if rec["end"] else ""), rec["kind"] or "",
           rec.get("kind_src") or "", _n(rec.get("twap_id")), _n(rec.get("sp")),
           _n(rec["pos_usd"], 0), rec["eligible"], rec["cancelled"],
           rec["completed"], _n(rec["p0"]), rec.get("p0_src") or "",
           _n(rec["p1"]), rec.get("p1_src") or "", _n(rec["move"], 4),
           result, rec["strategy"], DATA_ALGO_V]
    if _strat_csv_append(TWAP_SIG_CSV, TWAP_SIG_HEADERS, row):
        rec["sig_written"] = 1
    else:
        # відмова диска — в outbox (ретрай у циклі трекерів), а не втрата
        # рядка журналу (аудит v2.14 №8d)
        with _sig_retry_lock:
            _sig_retry_q.append([TWAP_SIG_CSV, TWAP_SIG_HEADERS, list(row), 0])
        rec["sig_written"] = 1

def _twap_drop(rec, reason):
    rec["state"] = "dropped"
    rec["reason"] = reason
    twap_stats["dropped"] += 1
    _twap_sig_write(rec, reason)
    print(f"  [TWAP] {rec['coin']} {rec['side']} ${(rec['usd'] or 0):,.0f}: "
          f"пропуск ({reason}"
          f"{', рух ' + format(rec['move'], '+.2f') + '%' if rec.get('move') is not None else ''})")

def _twap_cohort_name(base, thr):
    return base + TWAP_COHORT_SUFFIX[thr]

def _twap_close_min(tr):
    """Планова хвилина закриття paper-угоди: хвилина скасування (≤60,
    коли бот ДІЗНАВСЯ) або таймер m60. Спостереження кривої триває до
    120 хв, але угода закінчується тут (v2.14, аудит v2.13 №5)."""
    xm = int((tr.get("twap") or {}).get("exit_min") or 0)
    return xm if 1 <= xm <= TWAP_HOLD_MIN else TWAP_HOLD_MIN

def _twap_exit(tr, now):
    """ЄДИНИЙ факт виходу (v2.15, аудит v2.14 №1/№5): (exit_min, reason,
    mx) або None, поки вихід не вирішено. Ціна хвилини виходу є → вона;
    нема — перша наступна хвилина з ціною в межах TWAP_EXIT_CAP_MIN
    (timer_late / cancelled_late); нема й після капу — вихід без ціни
    (no_price, результат порожній). Саме цей факт керує записом рядка
    угоди, зайнятістю монети, open_now і списком відкритих — угода не
    вважається закритою, доки її вихід не відомий (нова угода не могла
    відкритись раніше за вихід попередньої)."""
    tw = tr.get("twap") or {}
    s = tr.get("samples") or []
    cm = _twap_close_min(tr)
    xm = int(tw.get("exit_min") or 0)
    base = ((tw.get("exit_reason") or "cancelled") if 1 <= xm <= TWAP_HOLD_MIN
            else "timer_60m")
    if len(s) >= cm and s[cm - 1] != "":
        return cm, base, s[cm - 1]
    for k in range(cm + 1, min(len(s), cm + TWAP_EXIT_CAP_MIN) + 1):
        if s[k - 1] != "":
            return k, ("timer_late" if base == "timer_60m" else "cancelled_late"), s[k - 1]
    et = tr.get("entry_ts") or 0
    if et and now - et >= (cm + TWAP_EXIT_CAP_MIN) * 60.0 + 35.0:
        return cm, "no_price", None
    # аудит-3 №10: хвилина виходу настала, а семпла міда немає (поллер) —
    # вихід усе одно ВИРІШЕНО за часом; ціну дає стакан (mx=None → тик
    # запитує l2Book, повтор до капу; без ціни після капу — no_price вище)
    if et and now - et >= cm * 60.0:
        k = cm + min(TWAP_EXIT_CAP_MIN, int((now - et) // 60) - cm)
        reason = base if k == cm else ("timer_late" if base == "timer_60m" else "cancelled_late")
        return k, reason, None
    return None

def _twap_trade_closed(tr, now):
    """Угода закрита: рядок уже записаний/заморожений або вихід вирішено З
    ЦІНОЮ (семпл хвилини) чи як no_price; вихід «за часом» без семпла
    (аудит-3 №10) чекає ціни стакану — до її отримання монета зайнята."""
    ex = _twap_exit(tr, now)
    return (bool(tr.get("trade_written")) or tr.get("trade_row") is not None
            or (ex is not None and (ex[2] is not None or ex[1] == "no_price")))

def _twap_trackers(rec):
    """Ключі трекерів запису (когорти); старі записи — один ключ."""
    keys = list(rec.get("trackers") or [])
    if not keys and rec.get("strategy"):
        keys = [f"{rec['id']}|{rec['strategy']}"]
    return keys

def _twap_sync_trackers(rec):
    """Стан біржі/скасування -> у КОЖЕН трекер запису. Скасування після
    входу визначає ВИХІД: найближча наступна хвилина (рев'ю v2.12 №4);
    exch_status/exec_pct лишаються в рядку для розділення вибірок."""
    exec_pct = None
    if rec.get("target_sz") and rec.get("exec_sz") is not None:
        exec_pct = round(100.0 * rec["exec_sz"] / rec["target_sz"], 2)
    with strat2_lock:
        for key in _twap_trackers(rec):
            tr = rev_open.get(key)
            if not tr or tr.get("twap") is None:
                continue
            tw = tr["twap"]
            tw["completed"] = rec["completed"]
            tw["cancel_after_entry"] = rec["cancel_after_entry"]
            tw["exch_status"] = rec.get("exch") or ""
            tw["exec_pct"] = exec_pct
            if rec["cancel_after_entry"]:
                # v2.15 (аудит v2.14 №2): paper-вихід — НЕ раніше, ніж бот
                # ДІЗНАВСЯ про скасування: наступна хвилина після поточного
                # семплу в момент отримання сигналу (v2.14 брала час події
                # з біржі/поста і виходила заднім числом за ціною, якої ще
                # не міг знати). Час самої події — окремо, для дослідження
                # (cancel_event_min); закриту угоду (рядок уже записаний)
                # пізнє скасування не змінює
                _et = tr.get("entry_ts") or 0
                _cts = None
                if rec.get("exch_cancelled") and rec.get("exch_ts"):
                    _cts = rec["exch_ts"]            # ревізія біржі
                elif rec.get("tg_fin_ts"):
                    _cts = rec["tg_fin_ts"]          # «Time:» з поста каналу
                if _cts and _et and _cts > _et:
                    _em = max(1, min(int(math.ceil((_cts - _et) / 60.0)), TWAP_TRACK_MIN))
                    if not tw.get("cancel_event_min") or _em < tw["cancel_event_min"]:
                        tw["cancel_event_min"] = _em
                if not tw.get("exit_min") and not tr.get("trade_written") \
                        and tr.get("trade_row") is None:
                    # хвилина, коли дізнались: не раніше поточної хвилини за
                    # годинником (семпли можуть відставати — пропуск ціни у
                    # 30-с допуску ще не закріплений)
                    _now_min = (int(math.ceil((time.time() - _et) / 60.0))
                                if _et else 1)
                    tw["exit_min"] = min(max(len(tr["samples"]) + 1, _now_min, 1),
                                         TWAP_TRACK_MIN)
                    tw["exit_reason"] = "cancelled"
                    tw["cancel_seen_min"] = tw["exit_min"]

def _twap_cancel_after_entry(rec):
    rec["cancel_after_entry"] = 1
    _twap_sync_trackers(rec)

def _twap_enter(rec, now, px):
    """Вхід ПРОТИ твапу: когорти ≥1/≥1.5/≥2% — ОКРЕМІ стратегії з власними
    трекерами (рев'ю v2.12: базова могла зайняти монету на русі 1.2% і
    вкрасти в когорти ≥2% пізніший сигнал 2.1%). «Зайнята» — лише поки
    триває годинний холд угоди тієї ж стратегії на монеті; 120-хв
    спостереження монету не тримає (рев'ю v2.12 №3)."""
    base = T1_NAME if rec["kind"] == "open" else T2_NAME
    our = "SHORT" if rec["side"] == "buy" else "LONG"   # проти твапу
    b_now = _px_now("BTC"); b_ago = _px_ago("BTC", REV_WINDOW_S)
    btc_move = (b_now / b_ago - 1.0) * 100.0 if b_now and b_ago else None
    depth = _sim_depth(rec["coin"], our)
    move = float(rec.get("move") or 0.0)
    # v2.16: ціна входу — зі свіжого стакану (виконувана на $1000 з нашого
    # боку), запит ДО локу; без стакану — жива ціна поллера з позначкою
    epx, esrc, emeta = _paper_px(rec["coin"], "SELL" if our == "SHORT" else "BUY",
                                 strict=True)
    if not epx:
        # аудит v2.16 №1: без повного стакану paper-входу немає. Рев'ю:
        # транзиторний збій l2Book (бюджет prio-каналу, проксі) — як
        # no_verify: поки вікно входу відкрите, наступний тик спробує знову;
        # drop no_book — лише коли вікно вичерпане
        if rec.get("end") is not None and time.time() < rec["end"] + TWAP_LATE_S - TWAP_POLL_S:
            stats["twap_book_retry"] = stats.get("twap_book_retry", 0) + 1
            return
        _twap_drop(rec, "no_book")
        return
    now = time.time()   # ціна відома САМЕ зараз
    if rec.get("end") is not None and now > rec["end"] + TWAP_LATE_S:
        # запит стакану тривав довше за вікно входу — входу заднім числом
        # немає (гарантія v2.13; рев'ю v2.16)
        _twap_drop(rec, "late")
        return
    opened, busy_c = [], []
    with strat2_lock:
        for thr in TWAP_COHORTS:
            if move + 1e-9 < thr:
                continue
            strat = _twap_cohort_name(base, thr)
            # зайнята — лише поки угода тієї ж стратегії на монеті НЕ
            # закрита: скасування на 1-й хвилині звільняє монету одразу,
            # а не через годину (v2.14, аудит v2.13 №5b)
            busy = any(p.get("row_kind") == "twap" and p["strategy"] == strat
                       and p["coin"] == rec["coin"] and not p.get("done")
                       and not _twap_trade_closed(p, now)
                       for p in rev_open.values())
            if busy:
                busy_c.append(strat)
                continue
            tr = {"sig_id": rec["id"], "strategy": strat, "state": "open",
                  "row_kind": "twap", "coin": rec["coin"], "side": our,
                  "addr": rec["addr"], "detect_ts": now, "detect_px": px,
                  "entry_ts": now, "entry_px": epx, "track_min": TWAP_TRACK_MIN,
                  "depth": depth, "btc_move": btc_move,
                  "costs": _sim_costs(rec["coin"], our)[0],   # v2.16: обидва боки
                  "hour": time.localtime().tm_hour, "algo_v": DATA_ALGO_V,
                  "entry_src": esrc, "entry_px_mid": emeta.get("mid") or px,
                  "px_age_ms": emeta.get("px_age_ms"),
                  "twap": {"src": rec["src"], "twap_side": rec["side"],
                           "usd": rec["usd"], "dur": rec["dur"], "kind": rec["kind"],
                           "kind_src": rec.get("kind_src") or "",
                           "twap_id": rec.get("twap_id"), "sp": rec.get("sp"),
                           "pos_usd": rec["pos_usd"], "move": rec["move"],
                           "p0": rec["p0"], "p0_src": rec.get("p0_src") or "",
                           "p1": rec["p1"], "p1_src": rec.get("p1_src") or "",
                           "cohort": thr, "cancel_after_entry": 0, "completed": 0,
                           "exch_status": rec.get("exch") or "", "exec_pct": None,
                           "exit_min": 0, "exit_reason": ""},
                  "samples": [], "peak": -999.0, "trough": 999.0}
            key = f"{rec['id']}|{strat}"
            rev_open[key] = tr
            rec.setdefault("trackers", []).append(key)
            opened.append(strat)
    if busy_c:
        twap_stats["busy_cohorts"] = twap_stats.get("busy_cohorts", 0) + len(busy_c)
    if not opened:
        _twap_drop(rec, "busy")
        return
    rec.update(state="entered", entry_ts=now, entry_px=epx, entry_src=esrc,
               strategy="+".join(opened))
    twap_stats["entered"] += 1
    _twap_sig_write(rec, "entered")
    print(f"  [TWAP] ВХІД {'+'.join(opened)} {our} {rec['coin']} @ {epx:.6g} ({esrc}) | твап "
          f"{rec['side']} ${(rec['usd'] or 0):,.0f} за {(rec['dur'] or 0)/60:.0f} хв, "
          f"рух {rec['move']:+.2f}% | тримаю {TWAP_HOLD_MIN} хв, крива {TWAP_TRACK_MIN}"
          + (f" | зайнято: {', '.join(busy_c)}" if busy_c else ""))
    threading.Thread(target=save_state, daemon=True).start()

def _twap_apply_exchange(rec):
    """Стан з біржі -> запис реєстру (і трекери, якщо вже увійшли).
    Ідемпотентно; викликається на КОЖНОМУ тику й перед входом (рев'ю
    v2.12 №1: відомий terminated не застосовувався, якщо повторна
    звірка «ще свіжа»)."""
    if rec.get("exch_completed") and not rec["completed"]:
        rec["completed"] = 1
    # біржа: finished, але виконано НЕ все — «завершено» з каналу не
    # робить твап підтвердженим (аудит v2.13 №3: 70% потрапляло у
    # n_confirmed); completed лишається лише за фактом повного виконання
    if (rec["completed"] and not rec.get("exch_completed")
            and rec.get("exch") == "finished" and rec.get("target_sz")
            and rec.get("exec_sz") is not None
            and rec["exec_sz"] < rec["target_sz"] * (1 - 1e-6)):
        rec["completed"] = 0
        rec["tg_partial"] = 1
    if rec.get("exch_cancelled") and not rec["cancelled"]:
        rec["cancelled"] = 1
        twap_stats["cancelled"] += 1
        if rec["state"] == "watch":
            _twap_drop(rec, "cancelled")
        elif rec["state"] == "entered":
            _twap_cancel_after_entry(rec)
    if rec["state"] == "entered":
        _twap_sync_trackers(rec)

def _twap_dedup_by_id(rec):
    """Два записи з одним twapId (той самий твап у двох каналах, старт
    розійшовся понад ±2 хв або різні пости) — лишається старший."""
    if rec.get("twap_id") is None:
        return False
    with twap_lock:
        for other in twap_reg.values():
            # v2.14: і dropped (скасований) старший запис — теж той самий
            # твап: другий канал не має рахувати скасування вдруге
            if (other is not rec and other.get("twap_id") == rec["twap_id"]
                    and other.get("created", 0) <= rec.get("created", 0)):
                other["posts"] = list(dict.fromkeys(other["posts"] + rec["posts"]))
                for ch in rec["src"].split("+"):
                    if ch not in other["src"].split("+"):
                        other["src"] += "+" + ch
                break
        else:
            return False
    # дубль поста — не сигнал: без рядка у twap_signals і без другого
    # «старту» у лічильниках (v2.15; у dropped-записи більше не зливаємо)
    rec["sig_written"] = 1
    twap_stats["starts"] = max(0, twap_stats.get("starts", 0) - 1)
    if rec.get("eligible"):
        twap_stats["eligible"] = max(0, twap_stats.get("eligible", 0) - 1)
    twap_stats["dup_posts"] = twap_stats.get("dup_posts", 0) + 1
    _twap_drop(rec, "dup_twapid")
    twap_stats["dropped"] = max(0, twap_stats.get("dropped", 0) - 1)   # дубль ≠ відкинутий сигнал
    return True

def _twap_tick(now):
    """Стан-машина твапів (v2.13): відомий стан біржі застосовується
    щотику; звірка — доки вид невідомий, потім лише перед входом і після
    кінця (рев'ю v2.12 №8b: 29 історій за твап); ціни зі свічок; вхід в
    останню хвилину за СВІЖИМ часом і живою ціною."""
    with twap_lock:
        recs = [r for r in twap_reg.values() if r["state"] == "watch"]
        ent = [r for r in twap_reg.values()
               if r["state"] == "entered" and not r.get("confirm_done")]
    for rec in recs:
        if rec["end"] is None:
            continue
        # 0) дубль по id біржі (незалежно від kind_src) і вже відомий стан
        #    біржі/скасування — завжди (№1)
        if rec.get("twap_id") is not None and _twap_dedup_by_id(rec):
            continue
        _twap_apply_exchange(rec)
        if rec["state"] != "watch":
            continue
        if rec.get("kind_src") == "unproven":
            _twap_drop(rec, "kind_unproven"); continue
        # 1) звірка — лише доки вид невідомий (слайс з'являється ~1–2с
        #    після старту; кеш 10с, тик 30с)
        if rec.get("kind_src") != "slice" and now >= rec["start"] - 5:
            _twap_verify(rec, now, slices=True)
            if _twap_dedup_by_id(rec):
                continue
            _twap_apply_exchange(rec)
            if rec["state"] != "watch":
                continue
            if rec.get("kind_src") == "unproven":
                _twap_drop(rec, "kind_unproven"); continue
        if rec.get("kind") in ("increase", "flip"):
            _twap_drop(rec, "not_new_or_reduce"); continue
        # 2) P0 — свічка, що закрилась перед стартом (раз; після старту)
        if rec.get("p0") is None and now >= rec["start"] + 3:
            _c0 = _twap_candle_close(rec["coin"], rec["start"])
            if _c0:
                rec["p0"], rec["p0_src"] = _c0, "candle"
        t_entry = rec["end"] - TWAP_ENTRY_LEAD
        if now < t_entry:
            continue
        if now > rec["end"] + TWAP_LATE_S:
            _twap_drop(rec, "late"); continue
        # 3) P1 — свічка, що закрилась на початку останньої хвилини
        if rec.get("p1") is None:
            _c1 = _twap_candle_close(rec["coin"], rec["end"] - 60.0)
            if _c1:
                rec["p1"], rec["p1_src"] = _c1, "candle"
        p0 = rec.get("p0")
        if not p0:
            # фолбек — поллер за хвилину до старту; ціну з поста більше
            # не беремо (№5d: інша методика)
            p0 = _px_at(rec["coin"], rec["start"] - 60.0)
            rec["p0_src"] = "hist" if p0 else ""
        p1 = rec.get("p1")
        if not p1:
            p1 = _px_now(rec["coin"])
            rec["p1_src"] = "live" if p1 else ""
        if not p0 or not p1:
            if now < rec["end"] - 20:
                continue   # ще є час дочекатись ціни
            _twap_drop(rec, "no_price"); continue
        move = (p1 / p0 - 1.0) * 100.0
        if rec["side"] == "sell":
            move = -move
        # рух — НЕОКРУГЛЕНИЙ (округлення лише в CSV/лозі): 1.49996% — не
        # когорта ≥1.5% (аудит v2.13)
        rec["p0"], rec["p1"], rec["move"] = p0, p1, move
        if move < TWAP_MIN_MOVE - 1e-9:   # №8c: рівно 1% у float
            _twap_drop(rec, "move_small"); continue
        if rec.get("kind_src") != "slice" or rec.get("kind") not in ("open", "reduce"):
            if now < rec["end"] - 15:
                continue   # слайс/історія ще можуть з'явитись
            _twap_drop(rec, "kind_unknown"); continue
        # 4) перед входом: свіжий стан біржі (лише історія), свіжий час
        #    (запити вище могли тривати), жива ціна — обов'язково (№1, №5)
        ok_v = _twap_verify(rec, now, slices=False)
        _twap_apply_exchange(rec)
        if rec["state"] != "watch":
            continue
        now2 = time.time()
        if now2 > rec["end"] + TWAP_LATE_S:
            _twap_drop(rec, "late"); continue
        if (not ok_v
                or now2 - (rec.get("verify_ok_ts") or 0) > TWAP_VERIFY_MAX_AGE_S):
            # звірка НЕ вдалась (мережа/бюджет/неоднозначність) — входу за
            # старим статусом немає (аудит v2.13 №7): наступний тик
            # спробує ще; коли наступний тик уже не встигає у вікно —
            # пропуск no_verify
            rec["verify_fail"] = rec.get("verify_fail", 0) + 1
            if now2 > rec["end"] + TWAP_LATE_S - TWAP_POLL_S:
                _twap_drop(rec, "no_verify")
            continue
        px = _px_now(rec["coin"])
        if not px:
            _twap_drop(rec, "no_price"); continue
        _twap_enter(rec, now2, px)
    # 5) після кінця: finished (executed ≥ sz) → completed; terminated →
    #    вихід «cancelled»; без відповіді за TWAP_CONFIRM_S — unconfirmed
    for rec in ent:
        if rec["end"] is None or now < rec["end"] + 20:
            continue
        if now > rec["end"] + TWAP_CONFIRM_S:
            rec["confirm_done"] = 1
            if not rec.get("exch_completed") and not rec.get("exch_cancelled"):
                if rec.get("exch") in ("", "activated", "ambiguous"):
                    # канал сказав «завершено», біржа не відповіла — це
                    # окремий статус, не «підтверджено» (v2.14)
                    rec["exch"] = "tg_done" if rec.get("tg_done") else "unconfirmed"
            _twap_sync_trackers(rec)
            continue
        if now - rec.get("verified_ts", 0) < TWAP_VERIFY_S:
            continue
        _twap_verify(rec, now, slices=False)
        _twap_apply_exchange(rec)
        if rec.get("exch_completed") or rec.get("exch_cancelled"):
            rec["confirm_done"] = 1

def _twap_ingest(channel, pid, ts, text, now, reply="", seen=False,
                 reply_pid=0):
    """seen=True — пост уже оброблявся (той самий pid): HL редагує
    стартовий пост у «cancelled»/«successful completed» замість нового
    поста, тому вже бачені пости перечитуємо ЛИШЕ як cancel/done.
    reply_pid — id поста, на який відповідає цей (v2.12): скасування
    зіставляється зі стартом ТОЧНО, а не по сумі/монеті."""
    p = twap_parse(channel, text, ts, reply)
    if p is None:
        return
    p["coin"] = _coin_canon(p["coin"])
    if seen and p["kind"] == "start":
        return
    if not seen:
        twap_stats["posts"] += 1
    if p["kind"] == "start":
        if not p.get("exact", True):
            return   # HL-репост без Period: старт невідомий — не сигнал
        if p["start"] is None or p["start"] < now - TWAP_BACKLOG_S:
            return
        if p["end"] is not None and p["end"] < now - TWAP_LATE_S:
            return   # закінчився до того, як ми його побачили
        rec, new = _twap_register(channel, pid, p, now)
        if not new:
            return
        twap_stats["starts"] += 1
        if rec["eligible"]:
            twap_stats["eligible"] += 1
            print(f"  [TWAP] {channel}: {rec['side']} {rec['coin']} "
                  f"${rec['usd']:,.0f} за {rec['dur']/60:.0f} хв "
                  f"({rec['addr'][:10]}…) — стежу")
            _twap_resolve_kind(rec)
        else:
            _twap_sig_write(rec, "ineligible")
        return
    # cancel / done: ТОЧНА прив'язка — по посту, на який відповідають, або
    # по самому посту (HL редагує старт). Знайдений запис ОСТАТОЧНИЙ:
    # якщо він уже dropped, повідомлення стосується його і нікого іншого
    # (рев'ю v2.12 №2a: перечитуваний пост скасування через евристичний
    # фолбек гасив ДРУГИЙ активний запис). Евристика — лише без прив'язки.
    tid = p.get("twap_id")
    cands = []
    with twap_lock:
        rec = _twap_by_post(channel, reply_pid) if reply_pid else None
        if rec is None and seen:
            rec = _twap_by_post(channel, pid)
        if (rec is not None and tid is not None and rec.get("twap_id") is not None
                and rec.get("twap_id") != tid):
            # пост несе id біржі, а прив'язаний по reply/посту запис має
            # ІНШИЙ id: прив'язка хибна (пост-дубль колись злився у чужу
            # заявку) — шукаємо по id, не гасимо чужий запис (рев'ю v2.15)
            twap_stats["post_id_mismatch"] = twap_stats.get("post_id_mismatch", 0) + 1
            rec = None
        if rec is None and tid is not None:
            # v2.14 (аудит v2.13 №2): id заявки на біржі з поста — точна
            # прив'язка, а не «новіша активна заявка гаманця»
            rec = next((r_ for r_ in twap_reg.values()
                        if r_.get("twap_id") == tid), None)
        exact_hit = rec is not None
        if rec is None and not seen:
            # лише при ПЕРШОМУ читанні поста без прив'язки. Перечитуваний
            # (кожні 30с) пост без прив'язки — старт відредаговано у
            # «cancelled» ще до нашого першого читання, старт старший за
            # годинний зріз, запис уже вичищено — інакше щопоолу
            # зіставлявся б із НОВИМ твапом того ж кита (рев'ю v2.13 №1)
            if tid is not None:
                # id є, але жоден запис його ще не має (звірка не встигла):
                # кандидати — активні записи гаманця/монети без id; їх
                # звіряємо з біржею (поза локом) і зіставляємо ПО ID
                cands = [r_ for r_ in twap_reg.values()
                         if r_["state"] in ("watch", "entered")
                         and r_["coin"] == p["coin"] and r_.get("twap_id") is None
                         and (not p["addr"] or (r_["addr"] or "").startswith(p["addr"]))]
            else:
                # без id (HL_TWAP): евристика лише за ЄДИНОГО кандидата —
                # два схожі активні твапи не вгадуємо (аудит v2.13 №2:
                # «новіша» була хибною), біржа розсудить перед входом
                found = _twap_find_all(
                    p["addr"], p["coin"], p["side"],
                    p["start"] if (p["start"] is not None and p.get("exact", True)) else None,
                    p["usd"])
                if len(found) > 1 and p.get("usd"):
                    # сума в пості — 3 значущі цифри ($5.11m, $1.00m): збіг
                    # = у межах ПІВОДИНИЦІ третьої цифри (5.11m: ±5k;
                    # 1.00m: ±5k, тобто 1.004m теж «збігається»); ЄДИНИЙ
                    # такий запис — це він; два — не вгадуємо
                    _tol = 0.5 * 10 ** (math.floor(math.log10(p["usd"])) - 2)
                    exact_usd = [r_ for r_ in found if r_.get("usd")
                                 and abs(r_["usd"] - p["usd"]) <= _tol]
                    if len(exact_usd) == 1:
                        found = exact_usd
                if len(found) == 1 and p.get("usd") and found[0].get("usd"):
                    # єдиний кандидат — теж лише за збігом суми в межах
                    # ОДИНИЦІ третьої значущої цифри (обидва джерела можуть
                    # бути округлені): $5.10m проти $5,113,000 — не він
                    # (рев'ю v2.15); біржа розсудить перед входом
                    _u = 10 ** (math.floor(math.log10(p["usd"])) - 2)
                    if abs(found[0]["usd"] - p["usd"]) > _u:
                        found = []
                if len(found) == 1:
                    rec = found[0]
                elif len(found) > 1:
                    twap_stats["fin_ambiguous"] = twap_stats.get("fin_ambiguous", 0) + 1
                else:
                    # без id і без жодного кандидата: чужий/старий твап —
                    # лічимо, щоб бачити частку «сліпих» фіналів у /status
                    twap_stats["fin_nomatch"] = twap_stats.get("fin_nomatch", 0) + 1
    if rec is None and cands:
        for r_ in cands:
            try:
                _twap_verify(r_, now, slices=False)
            except Exception as e:
                print(f"  [TWAP] звірка кандидата {r_['id']}: {e}")
            if r_.get("twap_id") == tid:
                rec = r_
                break
    if rec is None:
        if tid is not None and not seen:
            # id біржі з поста нікому не належить — не вгадуємо; біржа
            # сама підтвердить стан перед входом / після кінця
            twap_stats["fin_unmatched"] = twap_stats.get("fin_unmatched", 0) + 1
        return
    if not exact_hit:
        tag = f"{channel}/{pid}"
        if tag not in rec["posts"]:
            rec["posts"].append(tag)   # далі цей пост — точна прив'язка
    elif rec["state"] not in ("watch", "entered"):
        return   # уже оброблено (dropped/ineligible) — нічого не міняємо
    # виконання/статус/id із поста — доки біржа не сказала своє (v2.14)
    if tid is not None and rec.get("twap_id") is None:
        rec["twap_id"] = tid
    if p.get("target_sz") and not rec.get("target_sz"):
        rec["exec_sz"], rec["target_sz"] = (p.get("exec_sz") or 0.0), p["target_sz"]
    if p.get("status"):
        rec["tg_status"] = p["status"]
    if (p.get("status") and (rec.get("exch") or "") in ("", "activated", "ambiguous")
            and p.get("target_sz")):
        # статус із поста — лише разом із цифрами виконання (дані біржі у
        # пості); слово каналу без цифр — tg_status, не exch
        rec["exch"] = p["status"]
    full = bool(p.get("target_sz") and p.get("exec_sz") is not None
                and p["exec_sz"] >= p["target_sz"] * (1 - 1e-6))
    if p["kind"] == "done":
        rec["tg_done"] = 1
        if full and not rec["completed"]:
            # канал показав ПОВНЕ виконання (дані біржі у пості) — інакше
            # «завершено» ≠ «підтверджено» (аудит v2.13 №3: 70% → confirmed)
            rec["completed"] = 1
        elif p.get("target_sz") and not full:
            rec["tg_partial"] = 1
        if rec["state"] == "entered":
            _twap_sync_trackers(rec)
        return
    if rec["cancelled"]:
        return   # повторне читання того самого скасування
    rec["cancelled"] = 1
    if p.get("fin_ts"):
        rec["tg_fin_ts"] = p["fin_ts"]   # час скасування з поста — хвилина виходу
    twap_stats["cancelled"] += 1
    if rec["state"] == "watch":
        _twap_drop(rec, "cancelled")
    elif rec["state"] == "entered":
        _twap_cancel_after_entry(rec)

def _twap_row(p, now=None, ex=None):
    """Рядок twap_trades.csv — пишеться у момент ВИРІШЕНОГО paper-виходу
    (v2.15): m60 (timer_60m), хвилина скасування, коли бот про нього
    дізнався (cancelled), або перша наступна хвилина з ціною
    (timer_late / cancelled_late), чи без ціни після капу (no_price).
    Крива до 120 хв — окремо у twap_curves.csv."""
    costs = _p_costs(p)
    tw = p.get("twap") or {}
    s = p["samples"]
    # v2.15: вихід — з єдиного резолвера (_twap_exit); рядок пишеться у
    # момент вирішеного виходу і більше не змінюється (аудит v2.14 №1)
    if ex is not None and len(ex) == 2:
        # вирішений вихід із фази 1 (k, reason): ціна — зі стакану (tw.exit_px)
        ex = (ex[0], ex[1], None)
    elif ex is None:
        ex = _twap_exit(p, now if now is not None else time.time())
    if ex is None:
        ex = (_twap_close_min(p), "no_price", None)
    exit_min, exit_reason, mx = ex
    # v2.16: ціна виходу зі стакану в момент вирішеного виходу (tw.exit_px)
    # — gross від неї; без стакану — семпл хвилини виходу, як раніше
    ex_px = tw.get("exit_px")
    costs_eff = costs
    if ex_px and p.get("entry_px"):
        # аудит-3 №10: ціна зі стакану дійсна і без семпла хвилини (mx None)
        gx = (ex_px / p["entry_px"] - 1.0) * 100.0
        if p["side"] == "SHORT": gx = -gx
        mx = gx
        costs_eff = _leg_costs(costs, p.get("entry_src"), tw.get("exit_src"))
    bm = p.get("btc_move")
    def _n(v, nd=None):
        if v is None or v == "": return ""
        return round(v, nd) if nd is not None else v
    return ([p["sig_id"], p["strategy"], _dt(p["entry_ts"]), p["coin"],
             p["side"], tw.get("twap_side", ""), p.get("addr") or "",
             tw.get("src", ""), _n(tw.get("usd"), 0), _n(tw.get("dur"), 0),
             tw.get("kind", ""), tw.get("kind_src", ""), _n(tw.get("twap_id")),
             _n(tw.get("sp")),
             _n(tw.get("pos_usd"), 0), _n(tw.get("move"), 4),
             _n(tw.get("p0")), tw.get("p0_src", ""), _n(tw.get("p1")),
             tw.get("p1_src", ""), round(p["entry_px"], 8),
             (round(mx - costs_eff, 4) if mx is not None else ""),
             round(costs, 4),
             (round(p["peak"], 4) if p["peak"] > -999 else ""),
             (round(p["trough"], 4) if p["trough"] < 999 else ""),
             tw.get("cancel_after_entry", 0), tw.get("completed", 0),
             exit_min, exit_reason, _n(tw.get("cancel_event_min")),
             _n(tw.get("cancel_seen_min")),   # хвилина, коли дізнались (≠ exit_min при cancelled_late)
             _dt(p.get("trade_closed_ts")) if p.get("trade_closed_ts") else "",
             tw.get("exch_status", ""),
             _n(tw.get("exec_pct")),
             (round(bm, 3) if bm is not None else ""), p.get("hour", ""),
             p.get("algo_v", ""),
             # v2.16
             _ms(p.get("entry_ts")), p.get("entry_src", ""),
             _rnd(p.get("entry_px_mid"), 8), (tw.get("exit_ts_ms") or ""),
             _rnd(ex_px, 8), tw.get("exit_src", "")]
            + [("" if x == "" else round(x, 4)) for x in s[:TWAP_HOLD_MIN]]
            + [""] * (TWAP_HOLD_MIN - min(len(s), TWAP_HOLD_MIN)))

def _twap_freeze_exit(p):
    """Після заморозки рядка угоди зберегти вирішений вихід окремо у
    трекері (tw["exit_final"] = [exit_min, exit_reason]): крива та API
    беруть його звідти й не залежать від позиції колонки у рядку (v2.15)."""
    tr_row = p.get("trade_row")
    tw = p.get("twap")
    if not tr_row or not isinstance(tw, dict):
        return
    try:
        tw["exit_final"] = [tr_row[TWAP_HEADERS.index("exit_min")],
                            tr_row[TWAP_HEADERS.index("exit_reason")]]
    except (TypeError, IndexError, ValueError):
        pass

def _twap_curve_row(p):
    """Рядок twap_curves.csv (v2.15): крива m1..m120 після завершення
    трекера — дослідницький додаток до вже записаної угоди."""
    s = p["samples"]
    tw = p.get("twap") or {}
    ef = tw.get("exit_final")
    tr_row = p.get("trade_row")
    try:
        if isinstance(ef, (list, tuple)) and len(ef) == 2 and ef[0] != "":
            exit_min = ef[0]
        elif tr_row:
            exit_min = tr_row[TWAP_HEADERS.index("exit_min")]
        else:
            # відновлений трекер без exit_final (аварія між записом рядка і
            # збереженням стану): вирішений вихід детермінований із семплів
            _ex = _twap_exit(p, time.time())
            exit_min = _ex[0] if _ex else _twap_close_min(p)
    except (TypeError, IndexError, ValueError):
        exit_min = _twap_close_min(p)
    return ([p["sig_id"], p["strategy"], _dt(p["entry_ts"]), p["coin"],
             p["side"], exit_min, p.get("algo_v", "")]
            + [("" if x == "" else round(x, 4)) for x in s[:TWAP_TRACK_MIN]]
            + [""] * (TWAP_TRACK_MIN - min(len(s), TWAP_TRACK_MIN)))

def run_twap_watcher():
    if not TWAP_ENABLED:
        return
    time.sleep(20)   # поллер цін має набрати історію
    print(f"  [TWAP] вотчер: {', '.join('t.me/s/' + c for c in TWAP_CHANNELS)} "
          f"кожні {TWAP_POLL_S}с, твапи ≤{TWAP_MAX_DUR_S // 60} хв, вхід за "
          f"{TWAP_ENTRY_LEAD:.0f}с до кінця при русі ≥{TWAP_MIN_MOVE}%")
    while True:
        t0 = time.time()
        for ch in TWAP_CHANNELS:
            st = _twap_ch_state[ch]
            try:
                posts = _tme_fetch(ch)
                st["err"] = 0
                st["ok_ts"] = time.time()
            except Exception as e:
                st["err"] += 1
                twap_stats["fetch_err"] += 1
                if st["err"] in (1, 3, 10) or st["err"] % 120 == 0:
                    print(f"  [TWAP] {ch}: збій читання #{st['err']}: {e}")
                continue
            with twap_lock:
                last = twap_last_ids.get(ch, 0)
            # v2.14 (аудит v2.13): після пропуску (перерва вотчера, сплеск
            # постів) остання сторінка може не сягати останнього баченого
            # id — догортаємо старіші (?before=) до last або годинного
            # зрізу, ≤3 сторінок за цикл. v2.15 (аудит v2.14 №7): якщо
            # сторінок не вистачило, НЕЗАВЕРШЕНИЙ пропуск лишається у черзі
            # st["gaps"] = [[lo, hi, created_ts], …] (id у (lo,hi) ще не
            # читані; новіші першими) і догортається наступними циклами —
            # новий пропуск, що виник під час догортання старого, стає
            # окремим записом, а не губиться. Пости з пропусків — нові,
            # не «бачені»; пропуск старший за годину — історія, знімається
            pages = 0
            now_ = time.time()
            cutoff_ = now_ - TWAP_BACKLOG_S
            gaps = st.setdefault("gaps", [])
            if last and posts and posts[0][0] > last + 1:
                # свіжа сторінка не сягає останнього баченого id: новий
                # пропуск (last, низ сторінки) — завжди вище попередніх,
                # бо last уже піднято до вершини минулої сторінки
                if not gaps or gaps[0][1] <= last:
                    gaps.insert(0, [last, posts[0][0], now_])
            gaps[:] = [g_ for g_ in gaps if g_[2] > cutoff_]
            fresh = []
            # найновіший пропуск першим (його пости ще можуть бути живими
            # заявками); старіші — після його закриття
            while last and gaps and pages < 3:
                lo_, hi_, _c = gaps[0][:3]
                if hi_ <= lo_ + 1:
                    gaps.pop(0)
                    continue
                try:
                    older = _tme_fetch(ch, before=hi_)
                except Exception as e:
                    # збій сторінки — теж збій читання; три поспіль на тому
                    # самому before= (історію нижче вичищено — порожня
                    # сторінка) → пропуск знімаємо, а не тримаємо годину з
                    # мертвим запитом щоциклу (рев'ю v2.15)
                    twap_stats["fetch_err"] += 1
                    _nf = (gaps[0][3] if len(gaps[0]) > 3 else 0) + 1
                    if _nf >= 3:
                        print(f"  [TWAP] {ch}: пропуск ({lo_},{hi_}) знято після "
                              f"{_nf} збоїв: {e}")
                        gaps.pop(0)
                    else:
                        gaps[0][3:] = [_nf]
                        print(f"  [TWAP] {ch}: догортання before={hi_}: {e}")
                    break
                pages += 1
                gaps[0][3:] = []            # успішна сторінка скидає лічильник збоїв
                older = [p_ for p_ in older if lo_ < p_[0] < hi_]
                if not older:
                    gaps.pop(0)         # у проміжку нічого нема — закрито
                    continue
                fresh = older + fresh
                if older[0][1] <= cutoff_ or older[0][0] <= lo_ + 1:
                    gaps.pop(0)         # дійшли до last або до історії
                    continue
                gaps[0][1] = older[0][0]    # ще є що догортати — далі/наступним циклом
            if fresh:
                have = {p_[0] for p_ in posts}
                posts = sorted([p_ for p_ in fresh if p_[0] not in have] + posts,
                               key=lambda p_: p_[0])
            fresh_ids = {p_[0] for p_ in fresh}
            if pages:
                twap_stats["pages_extra"] = twap_stats.get("pages_extra", 0) + pages
            if not last and posts:
                # перший запуск: усе старше за годину — історія, не сигнал
                cutoff = time.time() - TWAP_BACKLOG_S
                last = max((pid for pid, ts, *_ in posts if ts < cutoff),
                           default=0)
            # відповідь (cancel/done) на пост усередині ще не догорнутого
            # пропуску — відкладаємо до його закриття, інакше фінал
            # обробляється раніше за свій старт і губить прив'язку
            # (рев'ю v2.15); відкладені старші за годину — історія
            def _in_gap(rp):
                return any(g_[0] < rp < g_[1] for g_ in gaps)
            dq_ = st.setdefault("deferred", [])
            replay = [d_ for d_ in dq_ if d_[1] > cutoff_ and not _in_gap(d_[4])]
            dq_[:] = [d_ for d_ in dq_ if d_[1] > cutoff_ and _in_gap(d_[4])]
            if replay:
                fresh_ids |= {d_[0] for d_ in replay}
                have = {p_[0] for p_ in posts}
                posts = sorted([d_ for d_ in replay if d_[0] not in have] + posts,
                               key=lambda p_: p_[0])
            _deferred_ids = {d_[0] for d_ in dq_}
            now = time.time()
            last0 = last
            for pid, ts, text, reply, reply_pid in posts:
                if reply_pid and _in_gap(reply_pid):
                    if pid not in _deferred_ids:
                        dq_.append((pid, ts, text, reply, reply_pid))
                        _deferred_ids.add(pid)
                    last = max(last, pid)
                    continue
                try:
                    # pid <= last0 — уже бачений пост: HL редагує старт у
                    # cancelled/completed без нового поста, тож
                    # перечитуємо його лише як cancel/done (ідемпотентно)
                    _twap_ingest(ch, pid, ts, text, now, reply,
                                 seen=(pid <= last0 and pid not in fresh_ids),
                                 reply_pid=reply_pid)
                except Exception as e:
                    twap_stats["parse_err"] += 1
                    print(f"  [TWAP] {ch}/{pid}: помилка обробки: {e}")
                last = max(last, pid)
            with twap_lock:
                twap_last_ids[ch] = last
        try:
            _twap_tick(time.time())
        except Exception as e:
            print(f"  [TWAP] tick err: {e}")
        time.sleep(max(1.0, TWAP_POLL_S - (time.time() - t0)))

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
            # Стеля 10с: безперервні трейди не мають морозити скан вічно.
            # На проксі скану (v2.11) hold не потрібен — інша IP, але
            # пейсер бюджету теж має відпрацювати ДО знімка часу
            if _scan_via_proxy():
                _scan_budget_wait(2)
            else:
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
            # v2.12 (з рев'ю CH): нові великі пари — у watchlist ОДРАЗУ
            # після читання гаманця, а не в кінці 90-хвилинного скану
            if positions:
                try:
                    _discover_pairs(k, positions, _t_fetch)
                except Exception as _de:
                    print(f"  [SCAN #{sn}] discover {k[:10]}…: {_de}")
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
        scan_metrics["last_duration_s"] = round(time.time() - scan_start, 1)

    # v2.12 (з рев'ю CH): наступний скан — через REFRESH_S від СТАРТУ
    # цього, а не від його кінця: 90-хвилинний скан + 30 хв паузи давав
    # 2-годинний цикл; тепер довгий скан переходить у наступний майже
    # одразу (5с), короткий — чекає решту 30 хв
    _schedule_scan(max(5.0, REFRESH_S - (time.time() - scan_start)))

def _schedule_scan(delay):
    """Один таймер на наступний скан; повторний виклик замінює попередній
    (захист від двох паралельних сканів — плюс cache["scanning"])."""
    global _scan_timer
    with _scan_sched_lock:
        if _scan_timer is not None:
            _scan_timer.cancel()
        scan_metrics["next_scan_at"] = time.time() + delay
        _scan_timer = threading.Timer(delay, lambda: threading.Thread(
            target=run_scan, daemon=True).start())
        _scan_timer.daemon = True
        _scan_timer.start()

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
            with twap_lock:
                _twap_active = sum(1 for r in twap_reg.values()
                                   if r.get("state") == "watch")
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
                # канал скану (v2.11 п.7): чи є проксі, чи вона зараз
                # живa, запитів/помилок/429 через неї, скільки разів
                # падали на основну IP
                "scan_proxy":     bool(SCAN_PROXY),
                "scan_proxy_alive": _scan_via_proxy(),
                "scan_proxy_req": _scan_state["req"],
                "scan_proxy_err": _scan_state["err"],
                "scan_proxy_429": _scan_state["rl"],
                "scan_proxy_fallbacks": _scan_state["fallback"],
                # v2.12: планувальник скану і інкрементальне відкриття пар
                "scan_next_in_s": round(max(0.0, scan_metrics["next_scan_at"] - time.time()), 0),
                "scan_last_duration_s": scan_metrics["last_duration_s"],
                "scan_new_pairs_live": scan_metrics["new_pairs"],
                # TWAP-вотчер (v2.11 п.5)
                **{f"twap_{k}": v for k, v in twap_stats.items()},
                "twap_active": _twap_active,
                # v2.16: здоров'я цін і стакану, пропуски, швидкий шлях,
                # settlement
                "px_last_ok_s_ago": (round(time.time() - stats["px_last_ok"], 1)
                                     if stats.get("px_last_ok") else None),
                "px_age_s":       _px_age_s(),
                "px_fail":        stats.get("px_fail", 0),
                "book_ok":        stats.get("book_ok", 0),
                "book_fail":      stats.get("book_fail", 0),
                # рев'ю аудит-2: строгий гейт стакану має бути видимим —
                # book_stale (котирування >5 с: годинник?), partial-стакани,
                # відмови входу без стакану по сім'ях, ретраї TWAP
                "book_stale":     stats.get("book_stale", 0),
                "book_partial_skips": stats.get("book_partial_skips", 0),
                "rev_no_book":    stats.get("rev_no_book", 0),
                "follow_no_book": stats.get("follow_no_book", 0),
                "twap_book_retry": stats.get("twap_book_retry", 0),
                "strat2_tick_err": stats.get("strat2_tick_err", 0),
                "journal_err":    stats.get("journal_err", 0),
                "rev_busy_skips": stats.get("rev_busy_skips", 0),
                "rev_stale_skips": stats.get("rev_stale_skips", 0),
                "follow_stale_skips": stats.get("follow_stale_skips", 0),
                "fast_retries":   stats.get("fast_retries", 0),
                "settle_done":    stats.get("settle_done", 0),
                "settle_failed":  stats.get("settle_failed", 0),
                "settle_pending": stats.get("settle_pending", None),
                "settle_old_v":   stats.get("settle_old_v", 0),   # рядки v1, що чекають перерахунку
                "settle_last_ok_s_ago": (round(time.time() - stats["settle_last_ok"], 0)
                                         if stats.get("settle_last_ok") else None),
                "settle_err":     stats.get("settle_err", ""),
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
        elif self.path.startswith("/strat2_slice"):
            # аудит-2 №4: зріз по ВСІЙ вибірці стратегії на сервері
            try:
                # рев'ю: BaseHTTPRequestHandler декодує рядок запиту як
                # iso-8859-1 — сира кирилиця (curl/скрипти) стала б «R1_Ð·…»
                _path = self.path.encode("latin-1", "replace").decode("utf-8", "replace")
                q = urllib.parse.parse_qs(urllib.parse.urlparse(_path).query)
                g = lambda k, d="all": (q.get(k) or [d])[0]
                self.send_json(strat2_slice(g("st", ""), g("grace"), g("vault"),
                                            g("sht"), g("bucket")))
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

def _on_stop(signum, frame):
    """SIGTERM/SIGINT (systemctl restart/stop): зберегти стан ДО виходу
    (v2.12, з рев'ю CH) — інакше рестарт губив до 60с трекерів/watchlist
    між періодичними записами. daemon-потоки помирають разом із
    процесом; SystemExit запускає atexit."""
    print(f"  [STATE] сигнал {signum}: зберігаю стан і виходжу")
    try:
        save_state()
    finally:
        raise SystemExit(0)

# ═════════════════════════════════════════════════════════
#  SETTLEMENT (v2.16 п.2–3): кожна закрита paper-угода (follow / rev / twap)
#  детерміновано переоцінюється по стрічці aggTrades Binance USDⓈ-M
#  (settle.py: денні zip з data.binance.vision + REST за останні 48 год;
#  ціна = гірша для нас у вікні 3 с → 10 с від моменту рішення); філи кита
#  — з HL userFillsByTime (лаг, епізод дампу для старих R-рядків).
#  Офіційний результат = ГІРШИЙ з live-запису і стрічки (рахує strat2_api).
#  Результати — settlements.csv у DATA_DIR (ключ = ключ рядка угоди).
# ═════════════════════════════════════════════════════════
SETTLE_ENABLED  = True
SETTLE_PERIOD_S = 120     # цикл; рядки угод з'являються при виході, не частіше
SETTLE_BATCH    = 40      # угод за цикл (кожна — до 2 zip-днів + 1 запит HL)
SETTLE_WARMUP_S = 90      # дати боту піднятись (WS/скан/ціни) перед першим прогоном
SETTLE_HL_PER_CYCLE = 12  # запитів userFillsByTime (вага 20) за цикл — ≈120 ваги/хв

def _settle_fetch_hl(body):
    """Філи кита для settlement — через prio-канал (проксі, власний бюджет
    ваги), не з основної IP, де живе детекція. None = збій/бюджет."""
    # кап на цикл: settlement ділить prio-бюджет із paper-входами (l2Book)
    # і звіркою TWAP — не більше SETTLE_HL_PER_CYCLE запитів по 20 ваги
    if stats.get("settle_hl_cycle", 0) >= SETTLE_HL_PER_CYCLE:
        import settle as _settle
        raise _settle.Transient("кап HL-запитів на цикл", "budget")   # без бекофу рядка
    stats["settle_hl_cycle"] = stats.get("settle_hl_cycle", 0) + 1
    try:
        r = hl_post_prio(body, retries=1, direct=not _prio_opener, max_wait=10.0)
        return r if isinstance(r, list) else None
    except Exception as e:
        stats["settle_hl_err"] = stats.get("settle_hl_err", 0) + 1
        if stats["settle_hl_err"] in (1, 10, 100):
            print(f"  [SETTLE] HL fills недоступні ({stats['settle_hl_err']}): {e}")
        import settle as _settle
        raise _settle.Transient(str(e)[:120])   # рядок не фіксується — наступний цикл

def run_settle_worker():
    if not SETTLE_ENABLED:
        return
    try:
        import settle as _settle
    except Exception as e:
        print(f"  [SETTLE] модуль settle.py недоступний: {e} — розрахунок вимкнено")
        stats["settle_err"] = f"import: {e}"
        return
    time.sleep(SETTLE_WARMUP_S)
    # REST-стрічка ділить вагу IP Binance із циклом глибини (≤1600/хв):
    # 1 запит/2 с = ≤600/хв, разом < 2400/хв (рев'ю v2.16)
    fetchers = {"fetch_hl": _settle_fetch_hl, "hl_pace_s": 2.0, "rest_pace_s": 2.0}
    def _log(msg):
        print(f"  [SETTLE] {msg}")
    while True:
        try:
            t0 = time.time()
            stats["settle_hl_cycle"] = 0
            n_done, n_skip, n_fail = _settle.settle_pending(
                DATA_DIR, symbol_map=SYMBOL_MAP, limit=SETTLE_BATCH,
                log=_log, fetchers=fetchers)
            stats["settle_done"] = stats.get("settle_done", 0) + n_done
            stats["settle_failed"] = stats.get("settle_failed", 0) + n_fail
            stats["settle_last_ok"] = time.time()
            stats["settle_err"] = ""
            stats["settle_pending"] = getattr(_settle.settle_pending, "last_pending", None)
            if n_done or n_fail:
                _log(f"цикл {time.time() - t0:.0f}с: розраховано {n_done}, "
                     f"пропущено {n_skip}, збоїв {n_fail}")
                _strat2_cache["ts"] = 0.0   # API підхопить нові офіційні net
        except Exception as e:
            stats["settle_err"] = str(e)[:200]
            print(f"  [SETTLE] цикл впав: {e}")
        time.sleep(SETTLE_PERIOD_S)

def main():
    est = SCAN_TOP * DELAY / WORKERS
    print(f"\n  HL TERMINAL  →  http://localhost:{PORT}")
    print(f"  Full leaderboard scan (up to {SCAN_TOP}) | {WORKERS} workers | {DELAY}s delay")
    print(f"  First scan ETA: ~{est:.0f}s | After warmup: much faster (empty-skip)")
    print(f"  Skip logic: {SKIP_AFTER} empty scans → check every {CHECK_EVERY} "
          f"scans | VIP top-{VIP_TOP_N} за екваті — без скіпу\n")

    # Depth — запускаємо ПЕРШИМ, паралельно зі скануванням
    load_state()   # відновлюємо watchlist і сим-позиції з минулого запуску
    _init_strat_activation()
    signal.signal(signal.SIGTERM, _on_stop)
    signal.signal(signal.SIGINT, _on_stop)
    atexit.register(save_state)
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
    threading.Thread(target=_scan_probe,         daemon=True).start()
    threading.Thread(target=run_twap_watcher,    daemon=True).start()
    threading.Thread(target=run_settle_worker,   daemon=True).start()
    http.server.ThreadingHTTPServer(("", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()

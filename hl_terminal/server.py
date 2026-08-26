import http.server
import urllib.request
import urllib.error
import json
import os
import time
import threading
import socket
import ssl
import struct
from concurrent.futures import ThreadPoolExecutor

PORT      = 3000
DIR       = os.path.dirname(os.path.abspath(__file__))
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

# Скільки WS-знайдених гаманців (поза лідербордом) максимум додаємо
# до одного скану. Запобіжник від сплеску нових адрес у стрімі трейдів.
WS_EXTRA_MAX = 5000

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
        with open(os.path.join(DIR, "tg_token.txt")) as _f:
            TG_TOKEN = _f.read().strip()
    except Exception:
        pass
if not TG_TOKEN:
    print("  [TG] УВАГА: токен не знайдено (env TG_TOKEN або tg_token.txt). "
          "Алерти в Telegram не працюватимуть.")
TG_CHAT_FILE = os.path.join(DIR, "tg_chat.json")
TG_CHAT_ID = None          # заповнюється після /start, зберігається у файл

def _tg_save_chat(cid):
    try:
        with open(TG_CHAT_FILE, "w") as _f:
            json.dump({"chat_id": cid}, _f)
    except Exception:
        pass

try:
    with open(TG_CHAT_FILE) as _f:
        TG_CHAT_ID = json.load(_f).get("chat_id")
    if TG_CHAT_ID:
        print(f"  [TG] chat_id відновлено з файлу: {TG_CHAT_ID}")
except Exception:
    pass
MIN_CLOSE_PCT = 0.05       # мінімум 5% від позиції щоб вважати закриттям
                           # (і хоча б одна маркет-транзакція такого розміру)

tg_chat_lock = threading.Lock()

def tg_send(text):
    if not TG_TOKEN:
        return False
    with tg_chat_lock:
        chat_id = TG_CHAT_ID
    if not chat_id:
        print(f"  [TG] No chat_id yet. Message: {text[:60]}")
        return
    try:
        data = json.dumps({"chat_id": chat_id, "text": text,
                           "parse_mode": "HTML", "disable_web_page_preview": True}).encode()
        req  = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if not resp.get("ok"):
                print(f"  [TG] Error: {resp}")
                return False
            return True
    except Exception as e:
        print(f"  [TG] Send error: {e}")
        return False
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

def fetch_hl_coins_list():
    """Всі perp монети з Hyperliquid."""
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info",
        data=json.dumps({"type":"metaAndAssetCtxs"}).encode(),
        headers={"Content-Type":"application/json","User-Agent":"Mozilla/5.0"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    return [u["name"].upper() for u in data[0]["universe"]
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
                # Новий depth прийшов нормально: старий йде у fallback
                cache["depth_prev"] = dict(cache.get("depth", {}))
                cache["depth"] = depth
                print(f"  [DEPTH] Cache updated: {len(depth)} coins")
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
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                last_was_429 = True
                time.sleep(2 ** attempt)
            else:
                last_was_429 = False
                time.sleep(1)
        except Exception:
            last_was_429 = False
            time.sleep(1)
    # Вичерпали retry — кидаємо конкретний тип помилки
    if last_was_429:
        raise RateLimited("429 after retries")
    raise APIError("max retries")

def hl_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

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
        if k in ws_wallets:
            return False
        ws_wallets[k] = {"addr": addr, "source": "websocket",
                         "account": 0, "pnl_at": 0, "pnl_day": 0,
                         "vol_day": 0, "roi_day": 0, "name": "", "pos_count": 0}
    # Гаманець щойно торгнув: якщо smart-skip списав його як хронічно
    # порожній, скидаємо лічильник — інакше адресу, яку скан-прунінг
    # уже викинув, ніколи б не перевірили знову, навіть з позицією.
    with stats_lock:
        s = wallet_stats.get(k)
        if s and s.get("empty_streak", 0) >= SKIP_AFTER:
            s["empty_streak"] = 0
    return True

def ws_handshake(sock, host, path):
    key = __import__("base64").b64encode(os.urandom(16)).decode()
    sock.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
                  f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                  f"Sec-WebSocket-Version: 13\r\n\r\n").encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(4096)
        if not chunk or len(resp) > 65536:
            # сервер закрив сокет: recv віддає b"" миттєво, без цієї
            # перевірки цикл крутився б вічно впустую
            return False
        resp += chunk
    return b"101" in resp

def ws_recv(sock, send_lock=None):
    """Читає один фрейм. None: з'єднання закрите або помилка. b"": службовий.
    На ping сервера відповідаємо pong, інакше сервер рве з'єднання."""
    try:
        def read_exact(n):
            buf = b""
            while len(buf) < n:
                chunk = sock.recv(min(65536, n - len(buf)))
                if not chunk:
                    # recv повертає b"" на закритому сокеті: без цієї перевірки
                    # цикл читання крутився б вічно впустую
                    raise ConnectionError("closed")
                buf += chunk
            return buf
        h = read_exact(2)
        opcode = h[0] & 0x0F
        length = h[1] & 0x7F
        if length == 126:
            length = struct.unpack(">H", read_exact(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", read_exact(8))[0]
        payload = read_exact(length) if length else b""
        if opcode == 8: return None
        if opcode == 9:   # ping → pong з тим самим payload
            if send_lock is not None:
                with send_lock:
                    ws_send_frame(sock, 0xA, payload)
            else:
                ws_send_frame(sock, 0xA, payload)
            return b""
        return payload if opcode in (1, 2) else b""
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

def _ws_conn(coins, label):
    """
    Одна WS-сесія на свою половину монет. Реконект вічний.
    Hyperliquid рве з'єднання, якщо клієнт ~60с нічого не шле,
    тому окремий потік шле {"method":"ping"} кожні 30с.
    Реконект з експоненційним backoff: шторм реконектів раз на 5с,
    кожен зі своєю пачкою підписок, виглядав для сервера як флуд
    і тримав з'єднання мертвим тижнями (193k реконектів у лозі).
    """
    backoff = 5
    while True:
        sock = None
        alive = threading.Event()
        connected_at = 0.0
        try:
            ctx = ssl.create_default_context()
            raw = socket.create_connection(("api.hyperliquid.xyz", 443), timeout=30)
            sock = ctx.wrap_socket(raw, server_hostname="api.hyperliquid.xyz")
            sock.settimeout(60)
            if not ws_handshake(sock, "api.hyperliquid.xyz", "/ws"):
                raise ConnectionError("handshake failed")
            send_lock = threading.Lock()
            def _send_json(obj):
                with send_lock:
                    ws_send(sock, json.dumps(obj))
            for coin in coins:
                _send_json({"method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}})
                time.sleep(0.05)
            connected_at = time.time()
            alive.set()
            # alive/_send_json фіксуємо через дефолтні аргументи: інакше
            # замикання після реконекту бачило б уже НОВІ alive і сокет,
            # старий пінгер не помирав би, і потоки накопичувались.
            def _pinger(alive=alive, send=_send_json):
                while alive.is_set():
                    time.sleep(30)
                    if not alive.is_set():
                        break
                    try:
                        send({"method": "ping"})
                    except Exception:
                        break
            threading.Thread(target=_pinger, daemon=True).start()
            print(f"  [WS-{label}] connected, {len(coins)} підписок відправлено")
            subs_ok = 0
            while True:
                frame = ws_recv(sock, send_lock)
                if frame is None: break
                if not frame: continue
                stats["ws_last_ms"] = time.time() * 1000
                try:
                    obj = json.loads(frame.decode("utf-8", errors="ignore"))
                except Exception:
                    continue
                ch = obj.get("channel", "")
                if ch == "subscriptionResponse":
                    # Раніше ці відповіді ігнорувались: якщо сервер ріже
                    # підписки, монети глухнуть мовчки. Тепер рахуємо.
                    subs_ok += 1
                    if subs_ok == len(coins):
                        print(f"  [WS-{label}] всі {subs_ok} підписок підтверджені")
                    continue
                if ch == "error":
                    print(f"  [WS-{label}] server error: {str(obj)[:160]}")
                    continue
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
        # Інакше подвоюємо паузу до 5 хв, щоб не потрапити під бан за флуд.
        if connected_at and time.time() - connected_at > 120:
            backoff = 5
        else:
            backoff = min(backoff * 2, 300)
        print(f"  [WS-{label}] reconnect in {backoff}s")
        time.sleep(backoff)

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
    print(f"  [WS] {len(coins)} coins → A:{len(a)} + B:{len(b)} (два з'єднання)")
    threading.Thread(target=_ws_conn, args=(a, "A"), daemon=True).start()
    if b:
        threading.Thread(target=_ws_conn, args=(b, "B"), daemon=True).start()

# ── FETCH ONE WALLET ─────────────────────────────────────
def fetch_one(addr_str):
    try:
        # Кит щойно торгнув: пропускаємо fast-перевірку вперед
        while time.time() < fast_hold[0]:
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
}
delta_seen     = {}      # addr:coin -> коли вперше побачили дельту (анти-рейс)
WATCH_INTERVAL = 20   # резервний обхід. Першу лінію тримає WS,
                      # 20с звільняє ~5 запитів/с постійного навантаження

# Fast path: WS кладе сюди (addr, coin) і будить монітор
fast_pending = {}     # (addr, coin) -> час блоку першого трейда, ms
fast_hold    = [0.0]  # до цього моменту скан-воркери поступаються дорогою
fast_lock    = threading.Lock()
fast_event   = threading.Event()
fast_last    = {}     # (addr, coin) -> ts останнього тригера, дебаунс 2с

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
    except Exception:
        pass

def update_watchlist(result, depth_snap, scan_start=0):
    """Оновлює watchlist після повного скану."""
    new_wl = {}
    for coin, positions in result.items():
        if coin.upper() in COIN_BLACKLIST:
            continue
        d = depth_snap.get(coin)
        depth_max = d["max"] if d and d.get("max") else 0
        for pos in positions:
            if not pos.get("size"): continue
            ratio = pos["val"] / depth_max if depth_max else 0
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
        # Скан іде хвилини. Якщо realtime ПІД ЧАС ЦЬОГО скану бачив свіже
        # закриття, лишаємо його розмір. Але тільки з позначкою часу,
        # свіжішою за старт скану. Старий варіант брав просто менший розмір
        # і плутав "кит закривався" з "кит доливав": розмір у базі міг
        # тільки падати, і закриття ставали невидимими.
        for _a, _coins in new_wl.items():
            live_c = watchlist.get(_a, {})
            for _c, _p in _coins.items():
                lv = live_c.get(_c)
                if lv is None:
                    # Нова пара гаманець-монета: старий ключ дедупу скидаємо,
                    # інакше клоуз нової позиції може бути заблокований назавжди
                    sent_alerts.discard(f"{_a}:{_c}")
                if lv and lv.get("upd", 0) >= scan_start > 0 \
                      and 0 < lv["size"] < _p["size"]:
                    _p["val"]  = _p["val"] * (lv["size"] / _p["size"])
                    _p["size"] = lv["size"]
                    _p["upd"]  = lv["upd"]
        watchlist.clear()
        watchlist.update(new_wl)
    print(f"  [WATCH] Watchlist updated: {len(new_wl)} wallets, "
          f"{sum(len(v) for v in new_wl.values())} positions with ratio>=2x")

def check_one_wallet(addr):
    """
    Запитує поточний стан гаманця.
    Повертає dict{coin->pos} якщо OK (порожній {} = позицій немає).
    Кидає RateLimited / APIError при помилці запиту — НЕ плутати з "закрито".
    """
    data = hl_post({"type": "clearinghouseState", "user": addr})
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

_ot_cache = {}   # addr -> (ts, dict). Кеш на 3с: кит закриває 3 монети
                 # разом, а historicalOrders один на гаманець

def get_order_types(addr):
    """
    Тип кожного ордера через historicalOrders.
    Повертає dict {oid: (orderType, tif)}.
    Важливо: кнопка "маркет" у веб-інтерфейсі Hyperliquid технічно
    записується як лімітка з tif="FrontendMarket", а не orderType="Market".
    Кидає RateLimited / APIError при помилці.
    """
    _c = _ot_cache.get(addr)
    if _c and time.time() - _c[0] < 3:
        return _c[1]
    data = hl_post({"type": "historicalOrders", "user": addr})
    result = {}
    for item in (data or []):
        if not isinstance(item, dict): continue
        order = item.get("order", {})
        if not isinstance(order, dict): continue
        oid = order.get("oid")
        if oid is not None:
            result[oid] = (str(order.get("orderType", "")),
                           str(order.get("tif", "")))
    _ot_cache[addr] = (time.time(), result)
    return result

# Що вважаємо маркетом: справжній Market, кнопка маркет у UI (FrontendMarket)
# і агресивний API-маркет (Ioc). Пасивні лімітки в стакані (Gtc, Alo) відкидаємо.
MARKET_TIFS = {"FrontendMarket", "Ioc"}

def _is_market_order(info):
    """info = (orderType, tif) або None якщо ордер не знайшовся в історії."""
    if info is None:
        # Ордер старіший за вікно historicalOrders: тейкер+клоуз уже
        # відфільтровані, тому не викидаємо
        return True
    otype, tif = info
    if otype == "Market": return True
    if tif in MARKET_TIFS: return True
    return False

def get_recent_market_fills(addr, coin, since_ms):
    """
    Повертає МАРКЕТ-закриття для addr/coin після since_ms, згруповані по ТРАНЗАКЦІЯХ (hash).

    Логіка:
    - Один блок Hyperliquid = один hash = одна транзакція в explorer.
    - Всередині блоку може бути багато fills та ордерів (oid), всі з одним hash.
    - Ми групуємо по hash щоб показувати РЕАЛЬНІ транзакції як в explorer.
    - Тип перевіряємо через historicalOrders: приймаємо Market,
      FrontendMarket (кнопка маркет в UI) і Ioc (маркет через API).
      Пасивні лімітки зі стакану (Gtc, Alo) відкидаємо.

    Кидає RateLimited / APIError при помилці — щоб не плутати з "немає fills".
    """
    fills = hl_post({"type": "userFillsByTime",
                     "user": addr,
                     "startTime": since_ms})
    if fills and len(fills) > 0 and not isinstance(fills[0], dict):
        raise APIError(f"userFillsByTime unexpected format for {addr[:10]}")

    # Спершу збираємо кандидатів (close + taker + не TWAP)
    candidates = []
    oids_needed = set()
    for f in (fills or []):
        if f.get("coin") != coin: continue
        is_taker = f.get("crossed", False)
        d = f.get("dir", "")
        f_liq = bool(f.get("liquidation")) or ("Liquidat" in d)
        is_close = ("Close" in d) or f_liq
        is_twap  = (f.get("twapId") is not None)
        if not ((is_taker or f_liq) and is_close and not is_twap):
            continue
        candidates.append(f)
        oids_needed.add(f.get("oid"))

    if not candidates:
        return []

    # Отримуємо типи ордерів (Market vs Limit) — один запит.
    # Якщо rate limit — order_types буде None, і ми fallback на crossed=True (taker),
    # бо не можемо підтвердити точний тип, але taker вже рухає ціну.
    try:
        order_types = get_order_types(addr)
    except RateLimited:
        order_types = None   # fallback режим
    except APIError:
        order_types = None

    # Групуємо по HASH (транзакція)
    txs = {}  # hash -> {sz, cost, ts, oids, dir}
    rejected = {}
    for f in candidates:
        oid = f.get("oid")
        f_liq = bool(f.get("liquidation")) or ("Liquidat" in f.get("dir", ""))
        if (not f_liq) and order_types is not None:
            info = order_types.get(oid)
            if not _is_market_order(info):
                # Пасивна лімітка: рахуємо для логу і пропускаємо
                k = info[1] or info[0] or "?"
                rejected[k] = rejected.get(k, 0) + 1
                continue
        # else: fallback — order_types недоступні, кандидати вже taker+close, лишаємо як є
        h  = f.get("hash", "")
        px = float(f.get("px", 0))
        sz = float(f.get("sz", 0))
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
    if rejected:
        rj = ", ".join(f"{k}:{v}" for k, v in rejected.items())
        print(f"  [FILLS] {coin} {addr[:10]}: {len(market_txs)} маркет, "
              f"відкинуто пасивних лімiток {sum(rejected.values())} ({rj})")
    return market_txs

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

                if new_pos is None:
                    close_pct = 1.0
                    full_close = True
                    delta_seen.setdefault(alert_key, time.time())
                else:
                    old_size = old["size"]
                    new_size = new_pos["size"]
                    delta_size = old_size - new_size
                    if delta_size < old_size * MIN_CLOSE_PCT:
                        sent_alerts.discard(alert_key)
                        delta_seen.pop(alert_key, None)
                        continue
                    close_pct = delta_size / old_size
                    full_close = False
                    delta_seen.setdefault(alert_key, time.time())

                # ── Підтвердження через fills ──
                stats["delta_events"] += 1
                since_ms = int((time.time() - 300) * 1000)
                try:
                    mfills = get_recent_market_fills(addr, coin, since_ms)
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
                        elif addr in watchlist and coin in watchlist[addr]:
                            # Розмір реально змінився, але лімiткою: алерт
                            # не шлемо, а базу оновлюємо, інакше ця дельта
                            # перевірялась би вічно кожні 10 секунд
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                    continue
                stats["fills_confirmed"] += 1
                delta_seen.pop(alert_key, None)

                # ── Симулятор: кожна підтверджена маркет-транзакція ──
                try:
                    sim_on_market_txs(addr, coin, old, mfills,
                                      (new_pos["size"] if new_pos else 0.0),
                                      full_close)
                except Exception as _se:
                    print(f"  [SIM] hook err: {_se}")

                # ── Стратегія відкату (п.6): годуємо епізоди і повне закриття ──
                try:
                    fc_on_txs(addr, coin, old, mfills)
                    if full_close:
                        fc_on_full_close(addr, coin, old)
                except Exception as _fe:
                    print(f"  [FC] hook err: {_fe}")

                has_liq = any(f.get("liq") for f in mfills)

                # Часткове закриття мусить мати хоча б ОДНУ маркет-транзакцію
                # розміром >= MIN_CLOSE_PCT від позиції. Сума дрібних шматків,
                # що назбирала 5%, сигналом не вважається. Ліквідації — завжди.
                if not full_close and not has_liq:
                    biggest_tx = max((f["sz"] for f in mfills), default=0.0)
                    if biggest_tx < old["size"] * MIN_CLOSE_PCT:
                        with watchlist_lock:
                            if addr in watchlist and coin in watchlist[addr]:
                                watchlist[addr][coin]["size"] = new_pos["size"]
                                watchlist[addr][coin]["val"]  = new_pos["val"]
                                watchlist[addr][coin]["upd"]  = time.time()
                        continue

                # Дедуп тільки для часткових: повне закриття шлеться завжди,
                # навіть якщо перед цим уже був алерт про часткове.
                # При скипі базу все одно оновлюємо, інакше вона застрягає
                # і кожен sweep даремно тягне fills по тій самій дельті.
                if alert_key in sent_alerts and not full_close:
                    with watchlist_lock:
                        if addr in watchlist and coin in watchlist[addr]:
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()
                    continue
                sent_alerts.add(alert_key)

                a = {
                    "addr":      addr,
                    "coin":      coin,
                    "side":      old["side"],
                    "old_val":   old["val"],
                    "old_size":  old["size"],
                    "close_pct": close_pct,
                    "ratio":     old["ratio"],
                    "entry":     old["entry"],
                    "full_close": full_close,
                    "fills":     mfills,
                }
                threading.Thread(target=send_close_alert, args=(a,), daemon=True).start()

                with watchlist_lock:
                    if full_close:
                        watchlist.get(addr, {}).pop(coin, None)
                    else:
                        if addr in watchlist and coin in watchlist[addr]:
                            watchlist[addr][coin]["size"] = new_pos["size"]
                            watchlist[addr][coin]["val"]  = new_pos["val"]
                            watchlist[addr][coin]["upd"]  = time.time()

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
                wl_snap = {addr: dict(coins) for addr, coins in watchlist.items()}
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
    for coin, positions in new_result.items():
        if coin.upper() in COIN_BLACKLIST: continue
        d = depth_snap.get(coin)
        if not d or not d.get("max"): continue
        for pos in positions:
            ratio = pos["val"] / d["max"]
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
                continue
            else:
                delta_size = old["size"] - new_pos["size"]
                if delta_size <= 0: continue
                close_pct = delta_size / old["size"]
                if close_pct < MIN_CLOSE_PCT: continue
                full_close = False

            # ── ПІДТВЕРДЖЕННЯ через fills (обов'язково) ──
            since_ms = int((time.time() - 300) * 1000)
            try:
                mfills = get_recent_market_fills(addr, coin, since_ms)
            except (RateLimited, APIError, Exception):
                # Не можемо підтвердити — пропускаємо без алерту
                continue
            if not mfills:
                # Немає маркет fills — не шлемо алерт
                continue

            # Хоча б одна транзакція >= MIN_CLOSE_PCT (ліквідації — завжди)
            if not any(f.get("liq") for f in mfills):
                biggest_tx = max((f["sz"] for f in mfills), default=0.0)
                if biggest_tx < old["size"] * MIN_CLOSE_PCT:
                    continue

            key_ac = f"{addr}:{coin}"
            if key_ac in sent_alerts:
                continue   # realtime вже відправив цей клоуз
            sent_alerts.add(key_ac)

            alerts.append({
                "addr":      addr,
                "coin":      coin,
                "side":      old["side"],
                "old_val":   old["val"],
                "old_size":  old["size"],
                "close_pct": close_pct,
                "ratio":     old["ratio"],
                "entry":     old["entry"],
                "full_close": full_close,
                "fills":     mfills,
            })

    # Оновлюємо попередні позиції
    with tracking_lock:
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

    fills     = a.get("fills", [])   # тепер це список ТРАНЗАКЦІЙ (по hash)
    fill_info = ""
    if fills:
        avg_px   = sum(f["px"]*f["sz"] for f in fills) / sum(f["sz"] for f in fills)
        total_sz = sum(f["sz"] for f in fills)
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
        n_tx = len(seen_hashes)
        tx_word = "маркет транзакція" if n_tx == 1 else "маркет транзакцій"
        fill_info = f"\n💸 <b>Маркет:</b> {n_tx} {tx_word}, avg ${avg_px:,.4f}, {total_sz:.4f} токенів"
        if tx_links:
            fill_info += f"\n🔗 {tx_links}"
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
        return
    _lat = time.time() * 1000 - max(f["ts"] for f in fills)
    print(f"  [ALERT][{now}] coin={a['coin']} side={a['side']} ratio={a['ratio']:.2f}x "
          f"close_pct={a['close_pct']*100:.1f}% fills={len(fills)} | блок→алерт {_lat:.0f}ms")
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
    tg_result = tg_send(msg)
    tg_status = "ok" if tg_result else "no_chat_id"
    print(f"  [ALERT]  tg_sent={tg_status}")


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
GSPREAD_CREDS_FILE = os.path.join(DIR, "creds.json")  # ключ сервісного акаунта
GSPREAD_SHEET_KEY  = "17NQV-7Ob76XjIUx69K490WvjZv8w6PzR62PPhaTsa3A"   # id таблиці з URL: docs.google.com/spreadsheets/d/<ОЦЕ_ID>/edit
GSPREAD_WORKSHEET  = "trades_imba_bot"  # назва аркуша; якщо такого немає, створиться сам
SIM_CSV = os.path.join(DIR, "sim_trades.csv")

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

def _sim_depth(coin):
    with cache_lock:
        d = cache["depth"].get(coin) or cache.get("depth_prev", {}).get(coin)
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

        # Стара серія видихлась: нова транзакція починає нову серію
        if tr["txs"] and tr["last_tx_ms"] > 0 and new_txs and \
           (new_txs[0]["ts"] - tr["last_tx_ms"]) / 1000 > SIM_TRACKER_TTL_S and \
           key not in sim_positions:
            tr["txs"] = []
            tr["start_size"] = old.get("size", tr["start_size"])
            tr["start_val"]  = old.get("val",  tr["start_val"])

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

    depth = _sim_depth(coin)
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
STATE_FILE   = os.path.join(DIR, "state.json")
STATE_SAVE_S = 60
STATE_MAX_AGE_S = 3600   # старіший за годину стан не відновлюємо

def save_state():
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
            "fc_positions":  [[k[0], k[1], dict(p)] for k, p in fc_positions.items()],
        }
        tmp = STATE_FILE + ".tmp"
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
        with fc_lock:
            for a, c, p in snap.get("fc_positions", []):
                fc_positions[(a, c)] = p
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
    for k in [k for k, v in list(_ot_cache.items()) if v[0] < now_ - 60]:
        _ot_cache.pop(k, None)
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
        ws_age = (time.time()*1000 - stats["ws_last_ms"])/1000 if stats["ws_last_ms"] else -1
        if ws_age > 90 and time.time() - last_ws_warn >= 600:
            # раз на 10 хв, а не щохвилини: 52k таких рядків у старому лозі
            last_ws_warn = time.time()
            print(f"  [WS] тиша {ws_age:.0f}s: жодного повідомлення з обох з'єднань")
        # WS мовчить 5+ хв — одне повідомлення в TG; ожив — теж одне.
        # Мертвий детектор не має право мовчати місяць, як минулого разу.
        if ws_age > 300 and not stats.get("ws_dead_notified"):
            stats["ws_dead_notified"] = True
            tg_send("⚠️ <b>WS мовчить понад 5 хв</b> — детект живе лише "
                    "на резервному обході. Перевір лог.")
        elif 0 <= ws_age < 60 and stats.get("ws_dead_notified"):
            stats["ws_dead_notified"] = False
            tg_send("✅ <b>WS знову живий</b> — трейди йдуть.")
        if time.time() - last_hb >= 3600:
            last_hb = time.time()
            ws_age = (time.time()*1000 - stats["ws_last_ms"])/1000 if stats["ws_last_ms"] else -1
            with watchlist_lock:
                wl_n = len(watchlist)
            print(f"  [HEARTBEAT] up {(time.time()-stats['started'])/3600:.1f}h | "
                  f"ws {'OK' if 0 <= ws_age < 60 else 'МЕРТВИЙ ' + str(round(ws_age)) + 's'} | "
                  f"watchlist {wl_n} | ws_matched {stats['ws_matched']} | "
                  f"checks {stats['checks']} | deltas {stats['delta_events']} | "
                  f"confirmed {stats['fills_confirmed']} | empty {stats['fills_empty']} | "
                  f"alerts {stats['alerts_sent']} | 429 {stats['rate_limited']}")


# ═════════════════════════════════════════════════════════
#  FC: СТРАТЕГІЯ ВІДКАТУ ПІСЛЯ ПОВНОГО ПРОДАЖУ (пункт 6)
#  Кит продав усю позицію маркетом за < 5 хвилин, ціна пішла
#  за його потоком на >= 1%. Тиск скінчився: заходимо у
#  протилежний до його продажу бік (лонг після дампа лонгіста,
#  шорт після відкупу шортиста) і 30 хвилин щохвилини пишемо
#  зміну ціни, щоб знайти статистично найкращу хвилину виходу.
#  Вхід поки ВІРТУАЛЬНИЙ: для реальних ордерів на Binance
#  потрібні API-ключі, місце для них позначено нижче.
# ═════════════════════════════════════════════════════════
FC_ENABLED       = True
FC_MIN_MOVE_PCT  = 1.0      # мінімальний рух ціни за час його продажу
FC_MAX_EPISODE_S = 300      # перша→остання транзакція максимум 5 хв
FC_TRACK_MIN     = 30       # хвилин трекаємо після входу
FC_MAX_OPEN      = 5
FC_WORKSHEET     = "fullclose"
FC_CSV           = os.path.join(DIR, "fc_trades.csv")
HLP_LIQUIDATOR   = "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14"  # backstop-vault HLP

FC_HEADERS = (["date", "coin", "our_side", "whale_addr", "sum_usd",
               "duration_s", "ratio", "ratio_per_min", "move_pct",
               "entry_px", "pnl_30m_pct", "peak_pct"]
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
           "depth": _sim_depth(coin), "samples": [], "peak": -999.0}
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
    last  = pos["samples"][-1] if pos["samples"] else 0.0
    pnl30 = last - costs
    dur_min = pos["duration_s"] / 60.0
    rpm = pos["ratio"] / dur_min if dur_min > 0 else pos["ratio"]
    row = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(pos["open_ts"])),
           pos["coin"], pos["our_side"], pos["addr"],
           round(pos["sum_usd"], 0), round(pos["duration_s"], 1),
           round(pos["ratio"], 2), round(rpm, 3),
           round(pos["move_pct"], 3), round(pos["entry_mid"], 8),
           round(pnl30, 3), round(pos["peak"], 3)] + pos["samples"][:FC_TRACK_MIN]
    try:
        import csv as _csv
        newf = not os.path.exists(FC_CSV)
        with open(FC_CSV, "a", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            if newf: w.writerow(FC_HEADERS)
            w.writerow(row)
    except Exception as e:
        print(f"  [FC] csv err: {e}")
    _sheets_append(FC_WORKSHEET, FC_HEADERS, row)
    print(f"  [FC] DONE {pos['coin']} за {FC_TRACK_MIN} хв | "
          f"pnl30 {pnl30:+.2f}% | peak {pos['peak']:+.2f}%")
    threading.Thread(target=save_state, daemon=True).start()

def run_fc_loop():
    """Кожні 5с: семпли по хвилинах, закриття після 30-ї."""
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
                minute = int((time.time() - pos["open_ts"]) // 60)
                while len(pos["samples"]) < min(minute, FC_TRACK_MIN):
                    pos["samples"].append(round(gain, 4))
                if len(pos["samples"]) >= FC_TRACK_MIN:
                    done.append(key)
        for key in done:
            _fc_finish(key)

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
            print(f"  [SCAN] WS extra {len(extra)} > {WS_EXTRA_MAX}, "
                  f"беремо перші {WS_EXTRA_MAX}")
            extra = extra[:WS_EXTRA_MAX]
        all_wallets = lb_wallets + extra

        # Розділяємо: хто скипається, хто ні
        to_scan   = []
        skipped   = []
        for w in all_wallets:
            k = w["addr"].lower()
            if should_skip(k, sn):
                skipped.append(w)
            else:
                to_scan.append(w)

        total = len(to_scan)
        print(f"  [SCAN #{sn}] Total: {len(all_wallets)} | "
              f"Scan: {total} | Skipped (empty): {len(skipped)}")
        print(f"  [SCAN #{sn}] ~{total*DELAY/WORKERS:.0f}s estimated")

        with cache_lock:
            cache["progress"] = {
                "done": 0, "total": total,
                "phase": f"scanning {total} wallets ({len(skipped)} skipped)",
                "skipped": len(skipped)
            }
            cache["scan_number"] = sn

        all_pos = {}

        def process(w):
            k = w["addr"].lower()
            positions = fetch_one(w["addr"])
            if positions is None:
                # Помилка запиту: прогрес рухаємо, статистику не псуємо
                with cache_lock:
                    cache["progress"]["done"] += 1
                return
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
                if d and d["max"] > 0:
                    pos["depth_max"] = d["max"]
                    pos["ratio"]     = pos["val"] / d["max"]
                else:
                    pos["depth_max"] = 0
                    pos["ratio"]     = 0



        # Оновлюємо real-time watchlist
        update_watchlist(result, depth_snap, scan_start)

        # Детекція закриття позицій (між сканами)
        alerts = check_position_changes(result, depth_snap)
        for a in alerts:
            threading.Thread(target=send_close_alert, args=(a,), daemon=True).start()
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
            with cache_lock:
                self.send_json({
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
                })
        elif self.path == "/wallets":
            with cache_lock:
                self.send_json({
                    "wallets":    cache["wallets"],
                    "scanning":   cache["scanning"],
                    "lb_total":   cache["lb_total"],
                    "ws_discovered": cache["ws_discovered"],
                    "scan_number": cache["scan_number"],
                })
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
            ws_age = (time.time()*1000 - stats["ws_last_ms"])/1000 if stats["ws_last_ms"] else -1
            self.send_json({
                "uptime_min":     round((time.time() - stats["started"]) / 60, 1),
                "ws_alive":       0 <= ws_age < 60,
                "ws_last_sec_ago": round(ws_age, 1),
                "watchlist_wallets": wl_n,
                "watchlist_positions": wl_pos,
                "ws_matched":     stats["ws_matched"],
                "checks":         stats["checks"],
                "delta_events":   stats["delta_events"],
                "fills_confirmed": stats["fills_confirmed"],
                "fills_empty":    stats["fills_empty"],
                "alerts_sent":    stats["alerts_sent"],
                "rate_limited":   stats["rate_limited"],
            })
        elif self.path == "/sim":
            with sim_lock:
                open_list = [dict(p) for p in sim_positions.values()]
                closed    = list(sim_closed)
            self.send_json({"open": open_list, "closed": closed})
        elif self.path == "/depth":
            with cache_lock:
                d = cache["depth"]
                self.send_json({"count": len(d), "coins": list(d.keys())[:20], "sample": {k: d[k] for k in list(d.keys())[:3]}})
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
        if self.path == "/tg-update":
            # Telegram webhook (альтернатива polling)
            length = int(self.headers.get("Content-Length", 0))
            upd    = json.loads(self.rfile.read(length))
            msg    = upd.get("message", {})
            if msg.get("text","").startswith("/start"):
                global TG_CHAT_ID
                TG_CHAT_ID = msg["chat"]["id"]
                _tg_save_chat(TG_CHAT_ID)
                tg_send("✅ Алерти активовані!")
            self.send_json({"ok": True})
        elif self.path == "/add-wallet":
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length))
            addr   = (body.get("address") or "").strip()
            if addr.startswith("0x") and len(addr) == 42:
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
print(f"  Skip logic: {SKIP_AFTER} empty scans → check every {CHECK_EVERY} scans\n")

# Depth — запускаємо ПЕРШИМ, паралельно зі скануванням
load_state()   # відновлюємо watchlist і сим-позиції з минулого запуску
threading.Thread(target=run_state_saver,  daemon=True).start()
threading.Thread(target=run_depth_loop,   daemon=True).start()
threading.Thread(target=run_scan,         daemon=True).start()
threading.Thread(target=run_websocket,    daemon=True).start()
threading.Thread(target=tg_poll_updates,     daemon=True).start()
threading.Thread(target=run_realtime_monitor, daemon=True).start()
threading.Thread(target=run_sim_loop,        daemon=True).start()
threading.Thread(target=run_fc_loop,         daemon=True).start()
http.server.ThreadingHTTPServer(("", PORT), Handler).serve_forever()

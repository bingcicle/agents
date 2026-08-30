#!/usr/bin/env python3
"""Watchdog бота: детермінований, без AI. systemd-таймер кличе його раз
на 5 хв; він опитує /status і шле TG-алерт (тим самим ботом) при
деградації. Стан між запусками — watchdog_state.json у папці даних.
Однократні алерти: про кожну проблему повідомляє раз, і раз — про
відновлення. Роль Claude Code на сервері — діагностика ПІСЛЯ такого
алерту, а не чергування."""
import json, os, shutil, time, urllib.parse, urllib.request

DATA = "/home/hl_data"
ST = os.path.join(DATA, "watchdog_state.json")


def tg(text):
    """True лише коли Telegram РЕАЛЬНО прийняв повідомлення — інакше
    стан 'вже повідомив' не фіксується і наступний тик повторить
    (аудит v2.5: алерт, що впав разом із TG, губився назавжди)."""
    try:
        tok = open(os.path.join(DATA, "tg_token.txt")).read().strip()
        chat = json.load(open(os.path.join(DATA, "tg_chat.json")))["chat_id"]
    except Exception:
        return False   # без токена/чату повідомляти нікуди
    try:
        with urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                data=urllib.parse.urlencode(
                    {"chat_id": chat,
                     "text": "🐕 WATCHDOG: " + text}).encode()),
                timeout=10) as r:
            return r.status == 200
    except Exception:
        return False   # TG ліг — наступний тик спробує знову


prev = {}
try:
    prev = json.load(open(ST))
except Exception:
    pass
cur = dict(prev)
cur["ts"] = time.time()
alerts = []

st = None
try:
    with urllib.request.urlopen("http://localhost:3000/status", timeout=5) as r:
        st = json.load(r)
except Exception:
    # перший збій може бути рестартом — алерт лише з другого поспіль
    if prev.get("down") and not prev.get("down_alerted"):
        alerts.append("бот НЕ відповідає на /status дві перевірки поспіль "
                      "(~10 хв). systemctl status whale-terminal")
        cur["down_alerted"] = 1
    cur["down"] = 1

if st is not None:
    if prev.get("down_alerted"):
        alerts.append("бот знову відповідає ✅")
    cur["down"] = 0
    cur["down_alerted"] = 0

    if not st.get("ws_alive"):
        if prev.get("ws_dead") and not prev.get("ws_alerted"):
            alerts.append("WS мертвий дві перевірки поспіль — детекція "
                          "живе на повільному sweep (~20-30с)")
            cur["ws_alerted"] = 1
        cur["ws_dead"] = 1
    else:
        if prev.get("ws_alerted"):
            alerts.append("WS ожив ✅")
        cur["ws_dead"] = 0
        cur["ws_alerted"] = 0

    restarted = (st.get("uptime_min") or 0) < prev.get("uptime", 0)
    for k, name in (("tg_errors", "помилки Telegram"),
                    ("rate_limited", "429 від Hyperliquid")):
        v = st.get(k, 0) or 0
        pv = None if restarted else prev.get(k)
        if pv is not None and v > pv:
            alerts.append(f"{name}: +{v - pv} за 5 хв (усього {v})")
        cur[k] = v
    cur["uptime"] = st.get("uptime_min") or 0

try:
    du = shutil.disk_usage(DATA)
    pct = du.used / du.total * 100
    if pct > 90 and not prev.get("disk_alerted"):
        alerts.append(f"диск заповнений на {pct:.0f}%")
        cur["disk_alerted"] = 1
    if pct <= 85:
        cur["disk_alerted"] = 0
except Exception:
    pass

sent_ok = True
for a in alerts:
    sent_ok = tg(a) and sent_ok
if alerts and not sent_ok:
    # відправка впала: НЕ фіксуємо новий стан — наступний тик побачить
    # ті самі умови і повторить алерт, замість "запам'ятав і мовчу".
    # Свідомий компроміс: якщо з кількох алертів частина ВСТИГЛА
    # доставитись, вона повториться теж — дубль кращий за втрату
    # (аудит v2.6 №10)
    cur = prev
try:
    json.dump(cur, open(ST, "w"))
except Exception:
    pass

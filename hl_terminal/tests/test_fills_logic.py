import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Тести get_recent_market_fills (новий фільтр: crossed без перевірки
типу ордера, розвороти як закриття) і depth_for_side."""
import math
import re, sys

src = open((_HL + "/server.py")).read()

# depth_for_side
m = re.search(r"(def depth_for_side.*?)(?=\ndef fetch_hl_coins_list)", src, re.S)
ns1 = {}
exec(m.group(1), ns1)
depth_for_side = ns1["depth_for_side"]

# get_recent_market_fills зі стабом hl_post
m = re.search(r"(def get_recent_market_fills.*?)(?=\ndef run_realtime_monitor)", src, re.S)
FAKE_FILLS = []
class RateLimited(Exception): pass
class APIError(Exception): pass
ns2 = {"hl_post": lambda body: FAKE_FILLS, "RateLimited": RateLimited, "OrderedDict": __import__("collections").OrderedDict, "threading": __import__("threading"),
       "APIError": APIError, "math": math}
exec(m.group(1), ns2)
grmf = ns2["get_recent_market_fills"]

def fill(**kw):
    base = {"coin": "SOL", "crossed": True, "dir": "Close Long", "px": "100",
            "sz": "10", "hash": "0xh1", "time": 1000, "oid": 1,
            "twapId": None, "liquidation": None}
    base.update(kw)
    return base

fails = []
def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond: fails.append(name)

# 1. Тейкер-GTC закриття (crossed=True) тепер ПРИЙМАЄТЬСЯ —
#    ніякої перевірки типу ордера більше немає
FAKE_FILLS = [fill()]
r = grmf("0xabc", "SOL", 0)
check("taker close accepted (no order-type filter)", len(r) == 1 and r[0]["sz"] == 10.0)

# 2. Мейкер (crossed=False, пасивна лімітка в стакані) — відкидається
FAKE_FILLS = [fill(crossed=False)]
check("maker close rejected", grmf("0xabc", "SOL", 0) == [])

# 3. Тейкер-ВІДКРИТТЯ — відкидається
FAKE_FILLS = [fill(dir="Open Short")]
check("taker open rejected", grmf("0xabc", "SOL", 0) == [])

# 4. Розворот "Long > Short" для LONG-позиції — закриття
FAKE_FILLS = [fill(dir="Long > Short")]
check("flip closes our LONG", len(grmf("0xabc", "SOL", 0, "LONG")) == 1)

# 4b. "Short > Long" для LONG-позиції — це її ВІДКРИТТЯ, не закриття
FAKE_FILLS = [fill(dir="Short > Long")]
check("opposite flip rejected for LONG", grmf("0xabc", "SOL", 0, "LONG") == [])

# 4c. "Close Short" не підтверджує закриття LONG
FAKE_FILLS = [fill(dir="Close Short")]
check("close of other side rejected", grmf("0xabc", "SOL", 0, "LONG") == [])

# 4d. Для SHORT-позиції: "Short > Long" — закриття, "Close Long" — ні
FAKE_FILLS = [fill(dir="Short > Long")]
check("flip closes our SHORT", len(grmf("0xabc", "SOL", 0, "SHORT")) == 1)
FAKE_FILLS = [fill(dir="Close Long")]
check("long close rejected for SHORT", grmf("0xabc", "SOL", 0, "SHORT") == [])

# 4e. Ліквідація проходить незалежно від боку
FAKE_FILLS = [fill(crossed=False, dir="Close Long", liquidation={"method": "market"})]
check("liq accepted with side param", len(grmf("0xabc", "SOL", 0, "LONG")) == 1)

# 5. TWAP — відкидається
FAKE_FILLS = [fill(twapId=77)]
check("twap rejected", grmf("0xabc", "SOL", 0) == [])

# 6. Ліквідація приймається навіть без crossed
FAKE_FILLS = [fill(crossed=False, dir="Close Long", liquidation={"method": "market"})]
r = grmf("0xabc", "SOL", 0)
check("liquidation accepted", len(r) == 1 and r[0]["liq"] is True)

# 7. Групування по hash: два філи однієї транзакції -> одна tx із сумою
FAKE_FILLS = [fill(sz="10", px="100"), fill(sz="30", px="102", oid=2)]
r = grmf("0xabc", "SOL", 0)
check("grouped by hash", len(r) == 1 and r[0]["sz"] == 40.0 and abs(r[0]["px"] - 101.5) < 1e-9)

# 8. Чужа монета не потрапляє
FAKE_FILLS = [fill(coin="ETH")]
check("other coin filtered", grmf("0xabc", "SOL", 0) == [])

# 9. Пагінація: перша сторінка рівно 2000 філів -> другий запит з
#    ПЕРЕКРИТТЯМ (остання мс, без +1); потрібний філ у другій сторінці
calls = []
def paged_hl_post(body):
    calls.append(body["startTime"])
    if len(calls) == 1:
        return [fill(coin="ETH", time=1000 + i, hash=f"0x{i}", oid=i)
                for i in range(2000)]
    return [fill(coin="SOL", time=99999, hash="0xtarget")]
ns2["hl_post"] = paged_hl_post
r = grmf("0xabc", "SOL", 0, "LONG")
check("pagination fetches second page", len(r) == 1 and r[0]["hash"] == "0xtarget")
check("overlap: second page starts at last ms (no +1)",
      len(calls) == 2 and calls[1] == 2999)

# 9b. Межа сторінок ділить одну мілісекунду: філи не губляться
calls2 = []
def samems_hl_post(body):
    calls2.append(body["startTime"])
    if len(calls2) == 1:
        # 1999 філів о t=5000 + 1 наш о t=5000: сторінка "повна"
        return [fill(coin="ETH", time=5000, hash=f"0xa{i}", oid=i)
                for i in range(2000)]
    # друга сторінка: та сама мс, серед них наш цільовий філ
    return ([fill(coin="ETH", time=5000, hash=f"0xa{i}", oid=i)
             for i in range(1990, 2000)]
            + [fill(coin="SOL", time=5000, hash="0xsame", oid=7777)])
ns2["hl_post"] = samems_hl_post
r = grmf("0xabc", "SOL", 0, "LONG")
check("same-ms boundary fill not lost",
      len(r) == 1 and r[0]["hash"] == "0xsame" and calls2[1] == 5000)
ns2["hl_post"] = lambda body: FAKE_FILLS

# depth_for_side
d = {"bid": 100.0, "ask": 200.0, "max": 200.0}
check("LONG hits bid", depth_for_side(d, "LONG") == 100.0)
check("SHORT hits ask", depth_for_side(d, "SHORT") == 200.0)
check("missing side falls back to max", depth_for_side({"max": 50.0}, "LONG") == 50.0)
check("no depth -> 0", depth_for_side(None, "LONG") == 0)

sys.exit(1 if fails else 0)

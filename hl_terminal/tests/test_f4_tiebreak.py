import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
import re, sys

src = open((_HL + "/server.py")).read()

# extract the two functions verbatim
def grab(name):
    m = re.search(rf"^def {name}\(.*?(?=^def |^# ─)", src, re.M | re.S)
    return m.group(0)

code = "F4_MIN_EPISODES=2\nF4_MIN_NOTIONAL=100_000.0\nF4_MAX_UNLOAD_S=300.0\nF4_CHUNK_PCT=0.05\n"
code += "F4_MIN_FAST_PCT=70.0\nF4_MIN_RATIO=2.0\nF4_FULL_PCT=0.95\n"   # v2.8
code += "PROFILE_ENTRY_LAT_MS=5000\nPROFILE_ZERO_TOL=1e-6\n"   # v2.18
for _fn in ("_wilson_lb", "_profile_txs", "_profile_lifecycles", "_profile_episode", "_build_profile"):
    code += grab(_fn)
import time, math
from v28_shim import _median
ns = {"time": time, "math": math, "_median": _median,
      "_sim_depth": lambda c, s=None: 1e4}   # v2.8: ratio-гейт потребує глибини
exec(code, ns)
_build_profile = ns["_build_profile"]

T = 1_756_000_000_000  # same block time, ms

def fill(h, px, sz, sp, t=T, coin="ZEC"):
    return {"dir": "Close Long", "crossed": True, "twapId": None,
            "time": t, "px": px, "sz": sz, "startPosition": sp,
            "coin": coin, "hash": h}

# Whale: position 100, dumps in one block with two market orders:
# tx_A first (sp=100, closes 50 @ 1200 - higher px),
# tx_B second (sp=50, closes 50 @ 1150 - lower px, price fell).
# Each chunk ~$58-60k, combined $117.5k >= F4_MIN_NOTIONAL.
episode1 = [fill("0xA", 1200.0, 50.0, 100.0), fill("0xB", 1150.0, 50.0, 50.0)]
# Second identical episode an hour later on another coin
T2 = T + 3_600_000
episode2 = [fill("0xC", 1200.0, 50.0, 100.0, t=T2, coin="HYPE"),
            fill("0xD", 1150.0, 50.0, 50.0, t=T2, coin="HYPE")]

prof = _build_profile(episode1 + episode2)
print("same-time two-tx dump, buggy order:", prof)

# Control: force distinct timestamps (1ms apart) -> true chronology preserved
episode1b = [fill("0xA", 1200.0, 50.0, 100.0, t=T),
             fill("0xB", 1150.0, 50.0, 50.0, t=T + 1)]
episode2b = [fill("0xC", 1200.0, 50.0, 100.0, t=T2, coin="HYPE"),
             fill("0xD", 1150.0, 50.0, 50.0, t=T2 + 1, coin="HYPE")]
print("control, distinct ms:", _build_profile(episode1b + episode2b))

# Variant: each half-chunk itself >= $100k (px=4000 -> $200k each)
ep_big = [fill("0xE", 4000.0, 50.0, 100.0), fill("0xF", 3900.0, 50.0, 50.0)]
ep_big2 = [fill("0xG", 4000.0, 50.0, 100.0, t=T2, coin="HYPE"),
           fill("0xH", 3900.0, 50.0, 50.0, t=T2, coin="HYPE")]
print("big chunks (each >=100k), same time:", _build_profile(ep_big + ep_big2))

import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
import json, math, random, re, asyncio
from pathlib import Path

random.seed(7)
BASE = _SPT
html = Path((_HL + "/hyperliquid-terminal.html")).read_text(encoding="utf-8")

def mk_curve():
    out = []
    for m in range(1, 61):
        v = 0.9 * (1 - math.exp(-m / 14)) - 0.35 * math.exp(-((m - 9) ** 2) / 30) - 0.15
        out.append(round(v, 3))
    return out

def mk_trades(n, kind):
    ts = []
    for i in range(n):
        net = round(random.gauss(0.5, 1.4), 3)
        t = {"date": f"2026-08-{25 + i % 4:02d} {8 + i % 14:02d}:1{i % 6}:00",
             "coin": random.choice(["FARTCOIN", "PUMP", "LIT", "ZEC", "HYPE", "NIL"]),
             "side": random.choice(["LONG", "SHORT"]), "net30": net,
             "peak": round(abs(net) + random.random(), 3),
             "trough": round(-random.random() * 1.5, 3),
             "move": round(1 + random.random() * 2, 2),
             "dur": round(random.random() * 200, 0),
             "shtanga": random.randint(0, 1), "vault": 1 if i % 7 == 0 else 0,
             "hour": (6 + i) % 24, "btc": round(random.gauss(0, 0.08), 3),
             "entered": 0 if (kind == "rev" and i % 5 == 4) else 1}
        if kind == "follow":
            t["hold"] = random.choice([60, 120, 180]); t["reason"] = random.choice(["silence", "full_close"])
            t["tx_pct"] = round(5 + random.random() * 20, 1)
            t["first_shot"] = random.randint(0, 1); t["pair_gap"] = None if t["first_shot"] else round(random.random()*3000, 1)
            t["pf_fast"] = random.randint(5, 12); t["pf_slow"] = random.randint(0, 3)
            t["pf_pct"] = round(100 * t["pf_fast"] / (t["pf_fast"] + t["pf_slow"]), 1)
            t["pf_unload"] = round(30 + random.random() * 250, 1); t["pf_unload_mean"] = round(t["pf_unload"] * 1.2, 1)
            t["wallet"] = "0x" + "%08x" % random.randint(0, 2**32)
        if t["entered"] == 0: t["net30"] = None
        ts.append(t)
    return ts

def agg(trades):
    nets = [t["net30"] for t in trades if t["net30"] is not None]
    cum, acc = [], 0.0
    for v in nets: acc += v; cum.append(round(acc, 2))
    half = len(nets) // 2
    med = sorted(nets)[len(nets) // 2] if nets else None
    return nets, cum, med, half

strategies = {}
descs = {"R1_загальний": "Реверс: всі монети, рух ≥1% за 3 хв, вхід одразу",
         "R2_breakout": "Реверс: рух ≥1%, вхід після відкату +0.3% (вікно 10 хв)",
         "R3_великі": "Реверс: лише ZEC і HYPE, рух ≥1%",
         "R4_великий": "Реверс: рух ≥2% за 3 хв", "R5_дуже": "Реверс: рух ≥3% за 3 хв",
         "R6_волт": "Реверс: сигнали від волтів (вивід коштів)",
         "F1_1хв": "У бік кита: транзакція ≥5%, вихід 1 хв тиші/повне",
         "F2_2хв": "У бік кита: транзакція ≥5%, вихід 2 хв тиші/повне",
         "F3_3хв": "У бік кита: транзакція ≥5%, вихід 3 хв тиші/повне",
         "F4_розумний": "У бік кита: лише швидкі гаманці, вихід 2×пауза",
         "F5_перший": "Те саме, але лише перший постріл пари (або пауза ≥1 год)",
         "R7_одним": "Проти кита: вся позиція одним пострілом ≥$100k, рух ≥1%",
         "F6_1хв_перший": "Тиша 1 хв, лише перший постріл пари",
         "F7_без_ратіо": "Перший постріл, кваліфікація без ratio (лише ≥$100k)",
         "F8_ratio35": "Перший постріл + ratio ≥3.5", "F9_без_ратіо_90": "Без ratio, ≥90% швидких",
         "T1_твап_відкриття": "TWAP ≤15 хв відкриває позицію, рух ≥1% у бік твапу — реверс на 1 год",
         "T2_твап_скорочення": "Те саме, TWAP скорочує позицію"}
for name in descs:
    kind = "rev" if name.startswith("R") else ("twap" if name.startswith("T") else "follow")
    n = {"R1_загальний": 24, "R2_breakout": 18, "R3_великі": 3, "R4_великий": 6,
         "R5_дуже": 2, "R6_волт": 7, "R7_одним": 5, "F6_1хв_перший": 8,
         "F7_без_ратіо": 6, "F8_ratio35": 4, "F9_без_ратіо_90": 3,
         "T1_твап_відкриття": 9, "T2_твап_скорочення": 7}.get(name, 15)
    tr = mk_trades(n, kind)
    nets, cum, med, half = agg(tr)
    strategies[name] = {
        "kind": kind, "signals": n + 4, "entered": len(nets), "n": len(nets),
        "median": med, "mean": round(sum(nets) / len(nets), 3) if nets else None,
        "win": round(100 * sum(1 for v in nets if v > 0) / len(nets), 0) if nets else None,
        "pnl_usd": round(sum(nets) * 10, 1) if nets else 0,
        "half1": med, "half2": round((med or 0) + 0.2, 3) if med is not None else None,
        "cum": cum, "curve": mk_curve() if kind == "rev" else None,
        "curve_n": [max(1, n - m // 10) for m in range(60)] if kind == "rev" else None,
        "curve_common": [round(v * 0.9, 3) for v in mk_curve()] if kind == "rev" else None,
        "curve_common_n": max(0, n - 6) if kind == "rev" else 0,
        "trades": tr,
        "first_shot_n": sum(1 for t in tr if t.get("first_shot")),
        "per_day": round(n / 6.5, 2), "days": 6.5, "since": "2.10" if not name.startswith(("F8","F9","T")) else "2.11",
        "prof": ({"wallets": 4, "fast_pct_med": 88.9, "unload_med_s": 74.0, "unload_mean_s": 101.3, "n_fast": 31, "n_slow": 4} if kind == "follow" else None)}

titles = {"R1_загальний": "Реверс · будь-який дамп ≥1%", "R2_breakout": "Реверс · вхід після відкату",
          "R3_великі": "Реверс · тільки ZEC і HYPE", "R4_великий": "Реверс · сильний рух ≥2%",
          "R5_дуже": "Реверс · екстрим ≥3%", "R6_волт": "Реверс · волти",
          "F1_1хв": "За китом · тиша 1 хв", "F2_2хв": "За китом · тиша 2 хв",
          "F3_3хв": "За китом · тиша 3 хв", "F4_розумний": "За китом · швидкі гаманці",
          "F5_перший": "За китом · швидкі гаманці · перший постріл",
          "R7_одним": "Реверс · одним пострілом",
          "F6_1хв_перший": "За китом · тиша 1 хв · перший постріл",
          "F7_без_ратіо": "За китом · перший постріл · без ratio",
          "F8_ratio35": "За китом · перший постріл · ratio ≥3.5",
          "F9_без_ратіо_90": "За китом · перший постріл · без ratio · 90% швидких",
          "T1_твап_відкриття": "TWAP-реверс · відкриття позиції",
          "T2_твап_скорочення": "TWAP-реверс · скорочення позиції"}
for name, st in strategies.items():
    for t in st["trades"]:
        t["ratio"] = 3.1; t["sum_usd"] = 240000; t["p3u"] = 0.42; t["p3d"] = -0.31
        t["tx_usd"] = 61000; t["pos_usd"] = 800000
def mk_curve120():
    return [round(0.8*(1-math.exp(-m/20)) - 0.3*math.exp(-((m-12)**2)/40) - 0.1, 3) for m in range(1, 121)]
for name in ("T1_твап_відкриття", "T2_твап_скорочення"):
    st = strategies[name]
    for i, t in enumerate(st["trades"]):
        t["move"] = round(1.0 + (i % 5) * 0.4, 2); t["dur"] = random.choice([300, 600, 900])
        t["usd"] = random.choice([130000, 165000, 250000]); t["twap_side"] = random.choice(["buy", "sell"])
        t["kind"] = "open" if name.startswith("T1") else "reduce"; t["src"] = random.choice(["TWAPx", "HL_TWAP+TWAPx"])
        t["cancel_after"] = 1 if i % 6 == 0 else 0; t["pos_usd"] = 800000
    def coh(thr):
        rs = [t for t in st["trades"] if t["move"] >= thr]
        nets = [t["net30"] for t in rs if t["net30"] is not None]
        med = sorted(nets)[len(nets)//2] if nets else None
        cum, acc = [], 0.0
        for v in nets: acc += v; cum.append(round(acc, 2))
        return {"n": len(nets), "median": med, "mean": (sum(nets)/len(nets)) if nets else None,
                "win": (100*sum(1 for v in nets if v > 0)/len(nets)) if nets else None,
                "pnl_usd": round(sum(nets)*10, 1), "half1": med, "half2": med, "cum": cum,
                "curve": mk_curve120(), "curve_n": [len(nets)]*120,
                "curve_common": [round(v*0.9, 3) for v in mk_curve120()], "curve_common_n": max(0, len(nets)-2)}
    st["cohorts"] = {"1.0": coh(1.0), "1.5": coh(1.5), "2.0": coh(2.0)}
    st.update(st["cohorts"]["1.0"]); st["track_min"] = 120; st["hold_min"] = 60
    st["drops"] = {"move_small": 14, "cancelled": 3, "late": 1, "no_price": 0, "kind_unknown": 0, "busy": 0}
    st["signals"] = st["n"] + 18
mock = {"strategies": strategies, "desc": descs, "titles": titles,
        "twap": {"posts": 212, "starts": 96, "eligible": 27, "cancelled": 3, "entered": 16, "dropped": 11,
                 "watching": [{"coin": "HYPE", "side": "sell", "usd": 165000, "dur": 600, "end": 0, "kind": "open", "src": "TWAPx"}],
                 "recent": [], "channels": {"HL_TWAP": {"last_id": 1714, "ok_ago_s": 12, "err": 0}, "TWAPx": {"last_id": 44688, "ok_ago_s": 12, "err": 0}}},
        "quarantined": 1, "signals_partial": 4,
        "follow_tape": {"n": 19, "curve": [round(v*0.7,3) for v in mk_curve()],
                        "curve_n": [19]*60,
                        "curve_common": [round(v*0.7,3) for v in mk_curve()],
                        "curve_common_n": 12},
        "follow_tape_first": {"n": 9, "curve": [round(v*0.8,3) for v in mk_curve()],
                        "curve_n": [9]*60,
                        "curve_common": [round(v*0.8,3) for v in mk_curve()],
                        "curve_common_n": 6},
        "signals_total": 61, "btc_veto": 9,
        "signals_sub": 14, "armed": 1, "legacy_rows": 6,
        "open": [{"strategy": "R1_загальний", "coin": "PUMP", "state": "open", "age_s": 420},
                 {"strategy": "R2_breakout", "coin": "PUMP", "state": "armed", "age_s": 120}],
        "profiles": {"total": 14, "ok": 5, "ok_nr": 9}, "shadow_open": 3, "updated": 0, "settle": {"n": 137, "done": 137, "failed": 2, "last_ok_s_ago": 95, "err": ""},
        "research": {"verified": {"mag_1_2": {"n": 30, "median": 0.4}, "mag_2p": {"n": 9, "median": 1.1},
                                  "shtanga_1": {"n": 20, "median": 0.7}, "shtanga_0": {"n": 19, "median": 0.05},
                                  "bucket_1_2": {"n": 24, "median": 0.5}, "bucket_3_5": {"n": 15, "median": 0.2}, "n_base": 39},
                     "observed": {"btc_on": {"n": 38, "median": 0.62},
                                  "btc_off": {"n": 7, "median": -0.31},
                                  "btc_na": {"n": 2, "median": -0.9},
                                  "mag_05_1": {"n": 21, "median": 0.18},
                                  "mag_1_2": {"n": 33, "median": 0.55},
                                  "mag_2p": {"n": 12, "median": 1.4},
                                  "shtanga_1": {"n": 25, "median": 0.9},
                                  "shtanga_0": {"n": 22, "median": 0.1},
                                  "partial": {"n": 8, "median": -0.2}, "n_base": 47, "n_excluded_old": 120}}}


# ── v2.16: R8, грейс/когорти дампу, офіційний/live/stripe net, періоди, settlement ──
descs["R8_тп80"] = "Реверс: падіння ≥1% за <5 хв, вихід при відновленні 80% руху або m30"
titles["R8_тп80"] = "Реверс · TP 80% відкату"
import copy as _copy
strategies["R8_тп80"] = _copy.deepcopy(strategies["R1_загальний"]); strategies["R8_тп80"]["n"] = 11
def _small(vals):
    if not vals: return {"n": 0, "median": None, "mean": None, "win": None, "pnl_usd": 0.0}
    s_ = sorted(vals); n_ = len(s_)
    med = s_[n_//2] if n_ % 2 else (s_[n_//2-1]+s_[n_//2])/2
    return {"n": n_, "median": med, "mean": sum(vals)/n_, "win": 100*sum(1 for v in vals if v > 0)/n_, "pnl_usd": round(sum(vals)*10, 1)}
for name, st in strategies.items():
    kind = st["kind"]
    lives, tapes, offs, items = [], [], [], []
    bks = {str(b): [] for b in (1, 2, 3, 4, 5)}
    n_late = n_grace = n_set = n_unset = n_nopx = 0
    for i, t in enumerate(st["trades"]):
        t["grace"] = 1 if i % 4 == 3 else 0
        t["late"] = 1 if i % 9 == 8 else 0
        t["lag"] = round(0.8 + (i % 6) * 0.9, 1); t["detect_src"] = "ws" if i % 3 else "sweep"
        t["entry_src"] = "book" if i % 5 else "mid"; t["exit_src"] = "book" if i % 7 else "mid"
        if kind == "rev":
            t["bucket"] = 1 + (i % 5); t["dump_move"] = round(-(1.0 + (i % 4) * 0.6), 2); t["dump_dur"] = t["bucket"] * 45
            t["exit_reason"] = "tp" if (name == "R8_тп80" and i % 2) else ("timer_late" if t["late"] else "timer"); t["exit_min"] = 30 if t["exit_reason"] != "tp" else round(3 + i % 20, 1)
            if i % 11 == 10 and t["net30"] is not None: t["exit_reason"] = "no_price"; t["net30"] = None
        if t.get("net30") is None:
            t["net_live"] = None; t["net_tape"] = None; t["settled"] = 0; t["flags"] = ""
            if t.get("entered", 1) != 0: n_nopx += 1
            continue
        live = t["net30"]; tape = round(live - (0.05 + (i % 3) * 0.12), 3) if i % 6 else None
        settled = 0 if i % 8 == 7 else 1
        t["net_live"] = live; t["net_tape"] = tape if settled else None; t["settled"] = settled
        t["flags"] = "" if tape is not None else "no_tape"
        t["net30"] = min(live, tape) if (settled and tape is not None) else live
        if settled: n_set += 1
        else: n_unset += 1
        if t["grace"]: n_grace += 1
        if t["late"]: n_late += 1; continue
        offs.append(t["net30"]); lives.append(live)
        if settled and tape is not None: tapes.append(tape)
        items.append((i, t["net30"]))
        if kind == "rev": bks[str(t["bucket"])].append(t["net30"])
    st.update(_small(offs)); st["n_late"] = n_late; st["n_grace"] = n_grace; st["n_no_price"] = n_nopx
    st["n_settled"] = n_set; st["n_unsettled"] = n_unset; st["live"] = _small(lives); st["tape"] = _small(tapes)
    st["periods"] = {"today": _small(offs[-3:]), "d7": _small(offs[-8:]), "d30": _small(offs), "all": _small(offs)}
    st["n_trades_total"] = len(st["trades"]) + (7 if name == "R1_загальний" else 0)
    if kind == "rev": st["buckets"] = {k: _small(v) for k, v in bks.items()}; st["n_adm_legacy"] = 4 if name == "R1_загальний" else 0
    st["verified"] = _small(offs[:5]); st["n_verified"] = min(5, len(offs)); st["n_live_only"] = 2; st["n_tape_only"] = 1
    st["n_mismatch"] = 1 if kind == "rev" else 0; st["n_stale"] = 1 if kind == "follow" else 0; st["n_grace_unknown"] = 2
    st["curve_src"] = "official" if kind != "follow" else "shadow"; st["curve_tape_n"] = 9 if kind != "follow" else 0
    st["curve_live_n"] = 9 if kind != "follow" else 0; st["curve_both_n"] = 7 if kind != "follow" else 0
    st["n_ep_mismatch"] = 1 if kind == "rev" else 0; st["n_partial_close"] = 0
    for t in st["trades"]:
        t["status"] = "verified" if t.get("settled") and t.get("net_tape") is not None else ("live_only" if t.get("net30") is not None else "none")
        t["mismatch"] = 0; t["stale"] = 0
        if kind == "twap": t["grace"] = None
    if kind == "twap":
        for c in st["cohorts"].values():
            c.update({"verified": st["verified"], "n_verified": st["n_verified"], "n_live_only": 2, "n_tape_only": 1, "n_mismatch": 0, "curve_src": "official", "curve_tape_n": 9, "curve_live_n": 9, "curve_both_n": 7})
    if kind == "twap":
        for c in st["cohorts"].values():
            c.update({"n_settled": c["n"] - 1, "n_unsettled": 1, "live": _small([x for x in [t["net_live"] for t in st["trades"]] if x is not None]),
                      "tape": _small([x for x in [t["net_tape"] for t in st["trades"]] if x is not None]), "periods": st["periods"], "n_trades_total": len(c.get("trades", st["trades"]))})

# ── v2.18: статуси профілів (ok/uncertain/no/pending/err) + три групи міграції, F10, Вілсон/cont/one-shot,
#    рядки з prof_status, невизначені поза заголовком (n_prof_unc/prof_unc), by_wallet/by_day ──
mock["profiles"] = {"total": 14, "ok": 5, "ok_nr": 5, "uncertain": 2, "no": 4, "pending": 2, "err": 1, "fetching": 1,
                    "migration": {"kept": 3, "downgraded": 2, "lost": 1, "gained": 1}, "algo_v": 10}
descs["F10_розумний_60"] = "Як F4 (кожна достатня транзакція швидкого гаманця), але вихід — фіксовані 60 с тиші"
titles["F10_розумний_60"] = "За китом · швидкі гаманці · вихід 60 с"
titles["F5_перший"] = "За китом · швидкі гаманці · перший постріл · вихід 60 с"
strategies["F10_розумний_60"] = _copy.deepcopy(strategies["F4_розумний"]); strategies["F10_розумний_60"]["since"] = "2.18"
for name, st in strategies.items():
    if st["kind"] != "follow": continue
    unc = []
    for i, t in enumerate(st["trades"]):
        t["prof_status"] = "uncertain" if (name in ("F4_розумний", "F10_розумний_60", "F5_перший") and i % 5 == 2) else ("ok" if t.get("pf_fast") else "pending")
        t["unc"] = 1 if t["prof_status"] == "uncertain" else 0
        t["pf_lb95"] = round((t.get("pf_pct") or 80) - 25, 1); t["pf_cont"] = round(40 + (i % 5) * 10, 1); t["pf_oneshot"] = round(10 + (i % 4) * 7, 1)
        if t["unc"] and t.get("net30") is not None and not t.get("late"): unc.append(t["net30"])
    # невизначені — поза заголовком: перерахувати заголовок без них
    head = [t["net30"] for t in st["trades"] if t.get("net30") is not None and not t.get("late") and not t.get("unc")]
    st.update(_small(head)); st["n_prof_unc"] = len(unc); st["prof_unc"] = _small(unc)
    st["periods"] = {"today": _small(head[-3:]), "d7": _small(head[-8:]), "d30": _small(head), "all": _small(head)}
    st["prof"] = {"wallets": 4, "fast_pct_med": 88.9, "unload_med_s": 74.0, "unload_mean_s": 101.3, "n_fast": 31, "n_slow": 4,
                  "n_uncertain": 3, "lb95_med": 61.2, "cont_pct_med": 55.0, "one_shot_pct_med": 20.0}
    st["by_wallet"] = {"n_groups": 4, "median_of_medians": round((st["median"] or 0) - 0.05, 3), "top_share": 45.5}
    st["by_day"] = {"n_groups": 3, "median_of_medians": round((st["median"] or 0) + 0.03, 3), "top_share": 60.0}

# ── v2.19: три факти окремо (сигнал/ціна/вихід), журнал paper, виконання на Binance ──
for name, st in strategies.items():
    if st["kind"] == "twap": continue
    for i, t in enumerate(st["trades"]):
        if t.get("net30") is None: continue
        t["sig_ok"] = 0 if i % 6 == 1 else (None if i % 6 == 2 else 1)
        t["sig_why"] = "partial_close" if i % 6 == 1 else ("pending" if i % 6 == 2 else "")
        t["px_ok"] = 0 if i % 5 == 3 else (None if t["sig_ok"] is None else 1)
        t["exit_partial"] = 1 if i % 7 == 4 else 0; t["fill_frac"] = 0.62 if t["exit_partial"] else 1.0
        t["exit_ok"] = 0 if (st["kind"] == "rev" and i % 8 == 5) else 1; t["exit_why"] = "tp_missed" if t["exit_ok"] == 0 else ""
        t["funding"] = round(((i % 3) - 1) * 0.008, 4); t["exch"] = "bn" if i % 4 else "hl"; t["exec_delay"] = round(0.1 + (i % 5) * 0.2, 2)
        if st["kind"] == "rev": t["net_tp_mkt"] = round(t["net30"] + 0.1, 3) if name == "R8_тп80" else None
    head = [t["net30"] for t in st["trades"] if t.get("net30") is not None and not t.get("late") and not t.get("unc")
            and t.get("sig_ok") == 1 and t.get("px_ok") == 1 and t.get("exit_ok") != 0]
    allp = [t["net30"] for t in st["trades"] if t.get("net30") is not None]
    st.update(_small(head)); st["paper"] = _small(allp); st["n_paper"] = len(allp)
    st["n_sig_invalid"] = sum(1 for t in st["trades"] if t.get("sig_ok") == 0)
    st["n_sig_pending"] = sum(1 for t in st["trades"] if t.get("net30") is not None and t.get("sig_ok") is None)
    st["sig_why"] = {"partial_close": st["n_sig_invalid"]}
    st["n_px_unverified"] = sum(1 for t in st["trades"] if t.get("px_ok") == 0)
    st["n_px_pending"] = sum(1 for t in st["trades"] if t.get("net30") is not None and t.get("px_ok") is None)
    st["n_exit_partial"] = sum(1 for t in st["trades"] if t.get("exit_partial"))
    st["n_exit_invalid"] = sum(1 for t in st["trades"] if t.get("exit_ok") == 0)
    st["n_verified"] = len(head); st["verified"] = _small(head)
    st["periods"] = {"today": _small(head[-3:]), "d7": _small(head[-8:]), "d30": _small(head), "all": _small(head)}

# ── v2.20: статуси tape_thin / neg_lag / trig_unmatched / no_tp, групи по гаманцю-дню-монеті з top_key,
#    частка перевірених серед paper, оцінювальний період EVAL_SINCE ──
for name, st in strategies.items():
    if st["kind"] == "twap": continue
    for i, t in enumerate(st["trades"]):
        if t.get("net30") is None: continue
        if i % 9 == 6: t["status"] = "tape_thin"
        if i % 11 == 7: t["sig_ok"] = 0; t["sig_why"] = "neg_lag"
        if i % 13 == 8: t["sig_ok"] = None; t["sig_why"] = "trig_unmatched"
        if name == "R8_тп80" and i % 8 == 5: t["exit_ok"] = 0; t["exit_why"] = "no_tp"
    head = [t["net30"] for t in st["trades"] if t.get("net30") is not None and not t.get("late") and not t.get("unc")
            and t.get("sig_ok") == 1 and t.get("px_ok") == 1 and t.get("exit_ok") != 0]
    allp = [t["net30"] for t in st["trades"] if t.get("net30") is not None]
    st.update(_small(head)); st["n_verified"] = len(head); st["verified"] = _small(head)
    st["n_sig_invalid"] = sum(1 for t in st["trades"] if t.get("sig_ok") == 0)
    st["sig_why"] = {"partial_close": sum(1 for t in st["trades"] if t.get("sig_why") == "partial_close"),
                     "neg_lag": sum(1 for t in st["trades"] if t.get("sig_why") == "neg_lag")}
    st["pct_verified"] = round(100.0 * len(head) / len(allp), 1) if allp else None
    st["eval_since"] = "2026-09-13"; st["eval"] = _small(head[-4:]); st["eval_paper"] = _small(allp[-6:])
    md = st.get("median") or 0
    st["by_wallet"] = {"n_groups": 4, "median_of_medians": round(md - 0.05, 3), "top_share": 45.5, "top_key": "0x1a2b3c"}
    st["by_day"] = {"n_groups": 3, "median_of_medians": round(md + 0.03, 3), "top_share": 60.0, "top_key": "2026-09-10"}
    st["by_coin"] = {"n_groups": 5, "median_of_medians": round(md - 0.01, 3), "top_share": 38.2, "top_key": "HYPE"}

inject = f"""<script>
const MOCK={json.dumps(mock, ensure_ascii=False)};
// рев'ю аудит-2 (UI №7): зріз — ЛИШЕ поля _agg_block (без trades/buckets/signals),
// крива зі стрічки тільки у R (F: curve_src=live, крива порожня — як віддає сервер);
// режими збою через window.__mode, лог запитів у window.__calls, зсув часу __toff
window.__mode='ok'; window.__calls=[]; window.__toff=0;
const _dn=Date.now; Date.now=()=>_dn()+window.__toff;
function sliceOf(st){{
  const sb=MOCK.strategies[st]||MOCK.strategies['R1_загальний'];
  const isR=st.startsWith('R');
  const H=60, cv=isR?sb.curve:Array(H).fill(null);
  return {{strategy:st, filters:{{}}, n:7, median:0.31, mean:0.2, win:57, pnl_usd:14, half1:0.2, half2:0.4,
    cum:[0.1,0.2,0.31], live:{{n:7,median:0.5}}, tape:{{n:6,median:0.31}}, verified:{{n:6,median:0.3,win:50}},
    n_verified:6, n_live_only:1, n_tape_only:0, n_mismatch:0, n_settled:6, n_unsettled:1, n_late:1, n_stale:0,
    n_no_price:0, n_grace:0, n_grace_unknown:2, n_trades_total:9, periods:sb.periods,
    curve:cv, curve_n:cv.map(v=>v==null?0:7), curve_common:isR?cv:null, curve_common_n:isR?7:0,
    curve_src:isR?'official':'none', curve_tape_n:isR?7:0, curve_live_n:isR?7:0, curve_both_n:isR?7:0,
    n_ep_mismatch:isR?1:0, n_partial_close:0}};
}}
window.fetch=async(u)=>{{
  if(u.startsWith('/strat2_slice')){{
    window.__calls.push(u);
    const st=new URLSearchParams(u.split('?')[1]).get('st');
    if(window.__mode==='netfail') throw new Error('net');
    if(window.__mode==='throw404') return {{ok:false,json:async()=>{{throw new SyntaxError('html 404');}}}};
    if(window.__mode==='error') return {{ok:true,json:async()=>({{error:'unknown strategy'}})}};
    return {{ok:true,json:async()=>sliceOf(st)}};
  }}
  return {{ok:true,json:async()=>{{
    if(u==='/strat2')return MOCK;
    if(u==='/positions')return{{ready:true,data:{{}},depth:{{}},watchlist_size:238}};
    if(u==='/watchlist')return{{watch:[],alerts:[]}};
    return{{}};}}}};
}};
</script>"""
html = re.sub(r"<body[^>]*>", lambda m: m.group(0) + inject, html, count=1)   # v2.12: <body data-pane=…>
Path(f"{BASE}/ui_mock220.html").write_text(html, encoding="utf-8")

async def main():
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        b = await pw.chromium.launch(executable_path="/opt/pw-browsers/chromium")
        pg = await b.new_page(viewport={"width": 1440, "height": 900})
        cons = []
        pg.on("pageerror", lambda e: cons.append(str(e)))
        pg.on("console", lambda m: cons.append(m.text) if m.type == "error" else None)
        await pg.goto(f"file://{BASE}/ui_mock220.html")
        await pg.click("#tabStrat")
        await pg.wait_for_timeout(600)
        await pg.screenshot(path=f"{BASE}/ui220_cards.png", full_page=True)
        _ct = await pg.evaluate("document.getElementById('sCards').textContent")
        assert "перевірено" in _ct and "по монетах: 5" in _ct and "(HYPE)" in _ct and "оцінювальний період з 2026-09-13" in _ct, _ct[:600]
        assert "TP 80%" in await pg.evaluate("document.getElementById('sCards').textContent")
        assert "137 угод" in await pg.evaluate("document.getElementById('svSettle').textContent")
        _rs = await pg.evaluate("document.getElementById('svResearch').textContent")
        assert "ПЕРЕВІРЕНО (офіційний net R1, n=39)" in _rs and "СПОСТЕРЕЖЕННЯ" in _rs and "НЕ доказ edge" in _rs and "старих поза: 120" in _rs, _rs
        await pg.click(".scard:nth-child(1)")   # R1
        await pg.wait_for_timeout(400)
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()")
        await pg.wait_for_timeout(200)
        await pg.screenshot(path=f"{BASE}/ui220_r1.png")
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "сьогодні" in txt and "7 днів" in txt and "весь час" in txt and "Net офіц." in txt, txt[:400]
        # кнопки: без грейсу і когорта дампу ≤2 хв
        await pg.click("text=без грейсу"); await pg.wait_for_timeout(200)
        await pg.click("text=1–2 хв"); await pg.wait_for_timeout(200)
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()")
        await pg.screenshot(path=f"{BASE}/ui220_r1_filters.png")
        await pg.wait_for_timeout(500)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "зріз · усі угоди за фільтрами" in txt and "Верифіковано" in txt, txt[:600]
        # рев'ю UI №5: при активному зрізі блок когорти з /strat2 (без інших фільтрів) не показується
        assert "Когорта дампу" not in txt, txt[:600]
        # рев'ю UI №3: підпис лічильників — зріз vs таблиця
        assert "у зрізі 9 угод, у таблиці" in txt and "з останніх" in txt, txt[:400]
        # рев'ю UI №4: крива зрізу зі стрічки — підпис «зріз за фільтрами» лише коли крива справді зрізу
        assert "ОФІЦІЙНА: за хвилиною гірша" in txt and "зріз за фільтрами · " in txt and "Епізод live ≠ settlement" in txt
        n_rows = await pg.evaluate("document.querySelectorAll('.sd-tbl tbody tr').length")
        ok = await pg.evaluate("[...document.querySelectorAll('.sd-tbl tbody tr')].every(r=>r.innerText.includes('1–2 хв'))")
        assert n_rows > 0 and ok, (n_rows, ok)
        # рев'ю UI №1/№2: один запит на ключ; повторні рендери без зсуву часу — без нових запитів;
        # через 15 с — оновлення зрізу; старий зріз лишається видимим
        n1 = await pg.evaluate("window.__calls.length")
        await pg.evaluate("renderSDetail();renderSDetail();renderSDetail()"); await pg.wait_for_timeout(150)
        assert await pg.evaluate("window.__calls.length") == n1, "шторм запитів"
        await pg.evaluate("window.__toff+=16000;renderSDetail()"); await pg.wait_for_timeout(150)
        assert await pg.evaluate("window.__calls.length") == n1 + 1, "зріз не оновився після 15 с"
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "зріз · усі угоди за фільтрами" in txt
        # збій сервера (старий сервер: 404-HTML, {error}, мережа) → чесний підпис, повтор через 15 с, не назавжди
        for mode in ("throw404", "error", "netfail"):
            await pg.evaluate(f"window.__mode='{mode}';window.__toff+=16000;renderSDetail()"); await pg.wait_for_timeout(200)
            txt = await pg.evaluate("document.getElementById('sDetail').textContent")
            # старий зріз (той самий ключ) лишається видимим при збої оновлення
            assert "зріз · усі угоди за фільтрами" in txt, (mode, txt[:300])
            n2 = await pg.evaluate("window.__calls.length")
            await pg.evaluate("renderSDetail();renderSDetail()"); await pg.wait_for_timeout(100)
            assert await pg.evaluate("window.__calls.length") == n2, (mode, "повтор одразу після збою")
        # нові фільтри при збої: старого зрізу немає → «зріз недоступний», числа по всій вибірці; після 15 с — повтор
        await pg.evaluate("window.__mode='error'"); await pg.click("text=2–3 хв"); await pg.wait_for_timeout(250)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "зріз недоступний" in txt and "Когорта дампу" in txt and "повтор через 15 с" in txt, txt[:500]
        n3 = await pg.evaluate("window.__calls.length")
        await pg.evaluate("renderSDetail()"); await pg.wait_for_timeout(100)
        assert await pg.evaluate("window.__calls.length") == n3
        await pg.evaluate("window.__mode='ok';window.__toff+=16000;renderSDetail()"); await pg.wait_for_timeout(250)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "зріз · усі угоди за фільтрами" in txt and "зріз недоступний" not in txt and "Когорта дампу" not in txt, txt[:500]
        await pg.click("text=1–2 хв"); await pg.wait_for_timeout(250)
        await pg.click("text=дамп: усі"); await pg.click("text=детальніше"); await pg.wait_for_timeout(200)
        await pg.evaluate("window.scrollBy(0, 900)"); await pg.wait_for_timeout(200)
        await pg.screenshot(path=f"{BASE}/ui220_r1_table.png")
        _bt = await pg.evaluate("document.body.textContent")
        assert "стрічка неповна" in _bt and "ціна до філа (причинність)" in _bt and "(тригер?)" in _bt, "v2.20 статуси у таблиці R1"
        # R8 картка і follow
        await pg.click(".scard:nth-child(8)"); await pg.wait_for_timeout(300)
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pg.wait_for_timeout(150)
        assert "TP 80%" in await pg.evaluate("document.getElementById('sDetail').textContent")
        await pg.screenshot(path=f"{BASE}/ui220_r8.png")
        await pg.click(".scard:nth-child(9)"); await pg.wait_for_timeout(300)   # F1
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pg.wait_for_timeout(150)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "без грейсу" in txt and "один трек на активну пару" in txt and "дамп: усі" not in txt
        await pg.screenshot(path=f"{BASE}/ui220_f1.png")
        # рев'ю UI №4: F зі зрізом — крива НЕ зрізу (тіньова стрічка follow), підпис без «зріз за фільтрами»
        await pg.click("text=без грейсу"); await pg.wait_for_timeout(300)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "зріз · усі угоди за фільтрами" in txt and "один трек на активну пару" in txt and "зріз за фільтрами · " not in txt, txt[:700]
        await pg.evaluate("fGrace='all';renderSDetail()"); await pg.wait_for_timeout(200)

        # v2.18: хедер профілів — три статуси / усього, чекають/збій, три групи перерахунку
        _pf = await pg.evaluate("document.getElementById('svProf').textContent")
        assert _pf.startswith("5 · 2 · 4 / 14") and "чекають 2, збій 1" in _pf and "зберіг 3 · невизнач. 2 · втратив 1 · отримав 1" in _pf, _pf
        _cards = await pg.evaluate("document.getElementById('sCards').textContent")
        assert "вихід 60 с" in _cards and "невизн. епізодів 31/4/3" in _cards and "Вілсон LB 61%" in _cards \
               and "продовження після входу 55%" in _cards and "one-shot 20%" in _cards and "невизначений профіль: " in _cards \
               and "поза заголовком" in _cards, _cards[:1500]
        await pg.locator(".scard", has_text="За китом · швидкі гаманці · вихід 60 с").first.click(); await pg.wait_for_timeout(300)   # F10
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pg.wait_for_timeout(150)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "фіксовані 60 с" in txt and "Профіль" in txt and "? невизн." in txt and "✓ підтв." in txt, txt[:900]
        await pg.click("text=детальніше"); await pg.wait_for_timeout(200)
        await pg.evaluate("window.scrollBy(0, 700)"); await pg.wait_for_timeout(200)
        await pg.screenshot(path=f"{BASE}/ui220_f10_table.png")
        _hdr = await pg.evaluate("[...document.querySelectorAll('.sd-tbl thead th')].map(e=>e.textContent)")
        assert "Профіль" in _hdr and "Швидк.%" in _hdr and "Продовж.%" in _hdr, _hdr

        # v2.19: хедер виконання, картка — сигнал/ціна/paper, деталі — блоки, таблиця — колонка «Сигнал»
        assert "Binance · стакан fapi" in await pg.evaluate("document.getElementById('svExec').textContent")
        _cards = await pg.evaluate("document.getElementById('sCards').textContent")
        assert "сигнал: спростовано" in _cards and "чекають перевірки" in _cards and "усі paper-угоди:" in _cards \
               and "частковий вихід:" in _cards and "ціна не підтверджена:" in _cards, _cards[:1200]
        await pg.click(".scard:nth-child(1)"); await pg.wait_for_timeout(300)
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pg.wait_for_timeout(150)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "Сигнал спростовано · чекає · ціна не підтв. · частк. вихід · вихід не за правилом" in txt \
               and "Усі paper-угоди (журнал виконання)" in txt, txt[:900]
        await pg.click("text=детальніше"); await pg.wait_for_timeout(200)
        _hdr = await pg.evaluate("[...document.querySelectorAll('.sd-tbl thead th')].map(e=>e.textContent)")
        assert "Сигнал" in _hdr and "Фандинг" in _hdr, _hdr
        _tbl = await pg.evaluate("document.querySelector('.sd-tbl').textContent")
        assert "✗ частк. закр." in _tbl and "чекає" in _tbl and "частк." in _tbl and "вихід✗" in _tbl and "б.п." in _tbl, _tbl[:600]
        await pg.evaluate("window.scrollBy(0, 700)"); await pg.wait_for_timeout(200)
        await pg.screenshot(path=f"{BASE}/ui220_r1_table.png")
        await pg.click(".scard:nth-child(19)"); await pg.wait_for_timeout(300)   # T1
        await pg.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pg.wait_for_timeout(150)
        txt = await pg.evaluate("document.getElementById('sDetail').textContent")
        assert "Розраховано по стрічці" in txt and "без грейсу" not in txt
        await pg.screenshot(path=f"{BASE}/ui220_t1.png")
        # мобільна
        pm = await b.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True)
        pm.on("pageerror", lambda e: cons.append("M:" + str(e)))
        pm.on("console", lambda m: cons.append("M:" + m.text) if m.type == "error" else None)
        await pm.goto(f"file://{BASE}/ui_mock220.html"); await pm.wait_for_timeout(600)
        await pm.click("#tabStrat"); await pm.wait_for_timeout(600)
        await pm.click(".scard:nth-child(1)"); await pm.wait_for_timeout(400)
        await pm.evaluate("document.getElementById('sDetail').scrollIntoView()"); await pm.wait_for_timeout(200)
        await pm.screenshot(path=f"{BASE}/ui220_m_r1.png", full_page=False)
        hs = await pm.evaluate("[document.documentElement.scrollWidth, document.documentElement.clientWidth]")
        assert hs[0] <= hs[1], hs
        await b.close()
        cons=[c for c in cons if "Failed to load resource" not in c]   # мережа пісочниці (шрифти)
        print("console errors:", cons)
        assert not cons, cons
        print("UI v2.20 OK")

asyncio.run(main())

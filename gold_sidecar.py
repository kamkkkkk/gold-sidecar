"""
gold_sidecar.py  (rule-based, no API key required)
Runs every morning at 07:00 GMT via GitHub Actions.
Fetches ForexFactory economic calendar, applies rule-based logic to
determine XAUUSD bias + news blackout times, writes JSON that
XAU_Breakout_EA.mq5 reads before each trading session.

Rule logic (how gold typically reacts to USD macro events):
  BEARISH for gold  = USD strengthening events
    (strong NFP, hot CPI, hawkish Fed, strong retail sales)
  BULLISH for gold  = USD weakening events
    (weak NFP, low CPI, dovish Fed, poor GDP, geopolitical risk)
  NEUTRAL           = mixed or ambiguous events

  Blackout = ±5 min around any high-impact event release time
  Avoid trading = if 3+ high-impact events in same session window
"""

import requests
import json
import os
import logging
from datetime import datetime, date
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

OUTPUT_PATH  = "claude_output.json"
CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# ─────────────────────────────────────────
# 1. FETCH ECONOMIC CALENDAR
# ─────────────────────────────────────────
def fetch_calendar_today():
    try:
        r = requests.get(CALENDAR_URL, timeout=10)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        log.warning(f"Calendar fetch failed: {e} — using fallback")
        return None

    today_str = date.today().strftime("%m-%d-%Y")
    high_impact = []
    for ev in events:
        if ev.get("date", "")[:10] != today_str:
            continue
        if ev.get("impact") not in ("High", "3"):
            continue
        if ev.get("country") not in ("USD", "US"):
            continue
        high_impact.append({
            "time":     ev.get("time", ""),
            "title":    ev.get("title", ""),
            "forecast": ev.get("forecast", ""),
            "previous": ev.get("previous", ""),
            "actual":   ev.get("actual", ""),
        })

    log.info(f"Calendar: {len(high_impact)} high-impact USD events today")
    return high_impact


# ─────────────────────────────────────────
# 2. RULE-BASED GOLD BIAS ENGINE
# ─────────────────────────────────────────

GOLD_RULES = [
    ("Non-Farm Payroll",      "bearish", "bullish"),
    ("NFP",                   "bearish", "bullish"),
    ("Unemployment",          "bullish", "bearish"),
    ("ADP Employment",        "bearish", "bullish"),
    ("Jobless Claims",        "bullish", "bearish"),
    ("CPI",                   "bearish", "bullish"),
    ("Core CPI",              "bearish", "bullish"),
    ("PCE",                   "bearish", "bullish"),
    ("Core PCE",              "bearish", "bullish"),
    ("PPI",                   "bearish", "bullish"),
    ("Inflation",             "bearish", "bullish"),
    ("GDP",                   "bearish", "bullish"),
    ("Retail Sales",          "bearish", "bullish"),
    ("ISM Manufacturing",     "bearish", "bullish"),
    ("ISM Services",          "bearish", "bullish"),
    ("PMI",                   "bearish", "bullish"),
    ("Industrial Production", "bearish", "bullish"),
    ("Consumer Confidence",   "bearish", "bullish"),
    ("Durable Goods",         "bearish", "bullish"),
    ("Trade Balance",         "bearish", "bullish"),
    ("FOMC",                  "bearish", "bullish"),
    ("Fed",                   "bearish", "bullish"),
    ("Federal Reserve",       "bearish", "bullish"),
    ("Interest Rate",         "bearish", "bullish"),
    ("Powell",                "bearish", "bullish"),
    ("Housing",               "neutral", "neutral"),
    ("Building Permits",      "neutral", "neutral"),
]

AVOID_KEYWORDS = ["FOMC", "Fed Chair", "Powell Speech", "Federal Reserve Statement",
                  "Interest Rate Decision"]

def parse_value(val_str: str) -> Optional[float]:
    if not val_str or val_str.strip() in ("", "N/A", "—", "-"):
        return None
    s = val_str.strip().replace(",", "").replace("%", "").replace("K", "000").replace("M", "000000")
    try:
        return float(s)
    except ValueError:
        return None


def classify_event(event: dict) -> tuple:
    title    = event.get("title", "")
    forecast = parse_value(event.get("forecast", ""))
    actual   = parse_value(event.get("actual", ""))
    previous = parse_value(event.get("previous", ""))

    matched_rule = None
    for keyword, strong_bias, weak_bias in GOLD_RULES:
        if keyword.lower() in title.lower():
            matched_rule = (strong_bias, weak_bias)
            break

    if matched_rule is None:
        return "neutral", f"No rule match for: {title}"

    strong_bias, weak_bias = matched_rule

    if actual is not None and forecast is not None:
        diff_pct = abs(actual - forecast) / max(abs(forecast), 0.001)
        if actual > forecast:
            strength = "strong" if diff_pct > 0.05 else "slight"
            bias = strong_bias if diff_pct > 0.02 else "neutral"
            return bias, f"{title}: actual {actual} > forecast {forecast} ({strength} beat → {bias} gold)"
        elif actual < forecast:
            strength = "weak" if diff_pct > 0.05 else "slight"
            bias = weak_bias if diff_pct > 0.02 else "neutral"
            return bias, f"{title}: actual {actual} < forecast {forecast} ({strength} miss → {bias} gold)"
        else:
            return "neutral", f"{title}: actual = forecast (in-line → neutral)"

    return "neutral", f"{title}: pre-release, no actual yet — blackout only"


def parse_event_time_gmt(time_str: str) -> Optional[str]:
    if not time_str or time_str.strip() in ("", "All Day", "Tentative"):
        return None
    time_str = time_str.strip().upper()
    try:
        fmt = "%I:%M%p" if "AM" in time_str or "PM" in time_str else "%H:%M"
        t   = datetime.strptime(time_str, fmt)
        month = datetime.utcnow().month
        offset = 4 if 3 <= month <= 11 else 5
        gmt_hour = (t.hour + offset) % 24
        return f"{gmt_hour:02d}:{t.minute:02d}"
    except Exception:
        return None


def get_rule_bias(events: list) -> dict:
    if not events:
        return {
            "bias":          "neutral",
            "confidence":    0.5,
            "reason":        "No high-impact USD events today — trade normally",
            "blackout_times": "",
            "avoid_trading": False,
            "notes":         "Clean calendar day",
            "generated_at":  datetime.utcnow().isoformat(),
            "events_count":  0,
            "engine":        "rule-based"
        }

    biases, reasons, blackouts = [], [], []
    avoid_count = 0

    for ev in events:
        title = ev.get("title", "")
        if any(kw.lower() in title.lower() for kw in AVOID_KEYWORDS):
            avoid_count += 1
        gmt_time = parse_event_time_gmt(ev.get("time", ""))
        if gmt_time:
            blackouts.append(gmt_time)
        bias, reason = classify_event(ev)
        biases.append(bias)
        reasons.append(reason)
        log.info(f"  [{ev.get('time','')}] {title} → {bias.upper()} | {reason}")

    bull = biases.count("bullish")
    bear = biases.count("bearish")
    neut = biases.count("neutral")

    if bull > bear and bull > neut:
        final_bias, confidence = "bullish", round(bull / len(biases), 2)
    elif bear > bull and bear > neut:
        final_bias, confidence = "bearish", round(bear / len(biases), 2)
    elif bull > 0 and bear > 0:
        final_bias, confidence = "neutral", 0.4
    else:
        final_bias, confidence = "neutral", 0.5

    avoid = avoid_count >= 1 or len(events) >= 3
    blackout_str = ",".join(sorted(set(blackouts)))
    summary = f"{bull}B/{bear}S/{neut}N signals → {final_bias.upper()}"
    log.info(f"Final: {summary} | Confidence: {confidence:.0%} | Blackouts: {blackout_str} | Avoid: {avoid}")

    return {
        "bias":           final_bias,
        "confidence":     confidence,
        "reason":         summary,
        "blackout_times": blackout_str,
        "avoid_trading":  avoid,
        "notes":          " | ".join(reasons[:3]),
        "generated_at":   datetime.utcnow().isoformat(),
        "events_count":   len(events),
        "engine":         "rule-based"
    }


def _fallback() -> dict:
    return {
        "bias":           "neutral",
        "confidence":     0.0,
        "reason":         "Calendar fetch failed — running without bias",
        "blackout_times": "",
        "avoid_trading":  False,
        "notes":          "Fallback mode — check internet connection",
        "generated_at":   datetime.utcnow().isoformat(),
        "events_count":   0,
        "engine":         "fallback"
    }


# ─────────────────────────────────────────
# 3. WRITE OUTPUT
# ─────────────────────────────────────────
def write_output(data: dict):
    parent = os.path.dirname(OUTPUT_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"Output written to {OUTPUT_PATH}")
    log.info(json.dumps(data, indent=2))


# ─────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 55)
    log.info(f"SIDECAR RUN — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 55)

    events = fetch_calendar_today()
    result = get_rule_bias(events) if events is not None else _fallback()
    write_output(result)

    if result.get("avoid_trading"):
        log.warning("AVOID TRADING today — high-impact event cluster detected")
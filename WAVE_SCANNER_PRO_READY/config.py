from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv() -> None:
    env_path = Path(__file__).resolve().with_name('.env')
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv()


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except ValueError:
        return int(default)


def validate_runtime_config() -> None:
    """Вызывается из main.py после полной загрузки модуля.
    Все ссылки на globals (TELEGRAM_ENABLED и т.д.) разрешаются через globals(),
    потому что функция может быть вызвана до объявления этих констант при
    некорректном порядке импортов.
    """
    g = globals()
    if g.get("TELEGRAM_ENABLED") and (not g.get("TELEGRAM_BOT_TOKEN") or not g.get("TELEGRAM_CHAT_ID")):
        raise ValueError(
            "TELEGRAM_ENABLED=1, but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are not set."
        )

    min_rr = g.get("MIN_RR")
    max_rr = g.get("MAX_RR")
    if min_rr is not None and max_rr is not None and min_rr >= max_rr:
        raise ValueError(f"MIN_RR ({min_rr}) must be < MAX_RR ({max_rr}).")

    fib_low = g.get("ENTRY_FIB_LOW")
    fib_high = g.get("ENTRY_FIB_HIGH")
    if fib_low is not None and fib_high is not None and fib_low >= fib_high:
        raise ValueError(f"ENTRY_FIB_LOW ({fib_low}) must be < ENTRY_FIB_HIGH ({fib_high}).")

    imp_min = g.get("IMPULSE_MIN_BARS")
    imp_max = g.get("IMPULSE_MAX_BARS")
    if imp_min is not None and imp_max is not None and imp_min > imp_max:
        raise ValueError(f"IMPULSE_MIN_BARS ({imp_min}) must be <= IMPULSE_MAX_BARS ({imp_max}).")

    scan = g.get("SCAN_INTERVAL_SEC")
    if scan is not None and scan <= 0:
        raise ValueError(f"SCAN_INTERVAL_SEC must be > 0 (got {scan}).")

    if not g.get("SYMBOLS"):
        raise ValueError("SYMBOLS list is empty.")


TELEGRAM_ENABLED = _env_int("TELEGRAM_ENABLED", 0) == 1
TELEGRAM_BOT_TOKEN = _env_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _env_str("TELEGRAM_CHAT_ID", "")
TELEGRAM_PROXY = _env_str("TELEGRAM_PROXY", "")

DATA_EXCHANGE = _env_str("DATA_EXCHANGE", "bybit")

# Whitelist symbols based on 60-day backtest (see PR description).
# Majors (BTC/ETH/BNB/LINK/etc.) systematically lost on this strategy in the
# tested window — mean_R ranged from -0.55 to -1.00. The list below keeps
# only symbols with non-negative or modestly-negative mean_R, which gave
# WR 46%, PF 1.16 on the same window (vs 33% WR / PF 0.58 for the full list).
# Revisit after walk-forward validation on a second window.
# Full pre-tuning list preserved in SYMBOLS_FULL for reference / rollback.
SYMBOLS = [
    "LDOUSDT", "POLUSDT", "SEIUSDT", "MANAUSDT", "TIAUSDT",
    "OPUSDT", "RUNEUSDT", "ADAUSDT", "GALAUSDT", "STXUSDT",
]

SYMBOLS_FULL = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "ADAUSDT",
    "LTCUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT",
    "OPUSDT", "INJUSDT", "SUIUSDT", "TIAUSDT",
    "POLUSDT", "RENDERUSDT", "LDOUSDT", "AAVEUSDT",
    "SANDUSDT", "MANAUSDT", "GALAUSDT", "AXSUSDT",
]

SYMBOLS_EXTENDED = [
    "WLDUSDT", "JUPUSDT", "WIFUSDT", "PENDLEUSDT",
    "EIGENUSDT", "ENAUSDT",
]

TF_ENTRY = _env_str("TF_ENTRY", "15m")
TF_HTF = _env_str("TF_HTF", "1h")
TF_TREND = _env_str("TF_TREND", "4h")

CANDLES_ENTRY = 320
CANDLES_HTF = 220
CANDLES_TREND = 160

ATR_PERIOD = 14
RSI_PERIOD = 14
EMA_FAST = 20
EMA_SLOW = 50
EMA_TREND = 200
VOLUME_MA_PERIOD = 20
BB_PERIOD = 20
BB_STD = 2.0
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

SCAN_INTERVAL_SEC = _env_int("SCAN_INTERVAL_SEC", 90)
SCAN_WORKERS = _env_int("SCAN_WORKERS", 2)
EXTENDED_SYMBOLS_LIMIT = _env_int("EXTENDED_SYMBOLS_LIMIT", 5)
SIGNAL_COOLDOWN_SEC = _env_int("SIGNAL_COOLDOWN_SEC", 3600)
# MIN_SCORE raised from 66 to 78 — at 78 the backtest keeps only the trades
# whose score band (>=78) had the highest PF on the tested window.
MIN_SCORE = _env_float("MIN_SCORE", 78.0)

RANGING_ATR_RATIO = _env_float("RANGING_ATR_RATIO", 0.007)
RANGING_BB_WIDTH_MIN = _env_float("RANGING_BB_WIDTH_MIN", 0.012)
RANGING_LOOKBACK = _env_int("RANGING_LOOKBACK", 20)
RANGING_MIN_SIGNALS = _env_int("RANGING_MIN_SIGNALS", 3)

WAVE_MIN_IMPULSE_ATR = _env_float("WAVE_MIN_IMPULSE_ATR", 1.5)
WAVE_LOOKBACK = _env_int("WAVE_LOOKBACK", 50)
WAVE_EQUALITY_TOLERANCE = _env_float("WAVE_EQUALITY_TOLERANCE", 0.25)
# Tolerance для условия cur_close vs EMA50 в _trend_from_4h
# 0.0 = строгое (cur_close > ema50), 0.01 = допускает цену до 1% ниже EMA50
TREND_EMA50_TOLERANCE = _env_float("TREND_EMA50_TOLERANCE", 0.006)
TREND_SLOPE_TOLERANCE = _env_float("TREND_SLOPE_TOLERANCE", 0.012)
PIVOT_WINDOW = _env_int("PIVOT_WINDOW", 5)
PIVOT_WINDOW_LIVE = _env_int("PIVOT_WINDOW_LIVE", 3)
STRICT_LAST_PIVOT_BARS = _env_int("STRICT_LAST_PIVOT_BARS", 2)

FIB_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786]
FIB_ZONE_TOLERANCE = _env_float("FIB_ZONE_TOLERANCE", 0.028)
FIB_TARGET_LEVEL = _env_float("FIB_TARGET_LEVEL", 0.618)

SWEEP_LOOKBACK_BARS = _env_int("SWEEP_LOOKBACK_BARS", 20)
SWEEP_MIN_ATR_RATIO = _env_float("SWEEP_MIN_ATR_RATIO", 0.3)
SWEEP_MIN_VOL_SURGE = _env_float("SWEEP_MIN_VOL_SURGE", 1.5)

BRAKING_VOL_SPIKE = _env_float("BRAKING_VOL_SPIKE", 1.4)
BRAKING_ABSORPTION = _env_float("BRAKING_ABSORPTION", 0.30)

IMPULSE_MIN_ATR = _env_float("IMPULSE_MIN_ATR", 0.8)
IMPULSE_EQUALITY_TOL = _env_float("IMPULSE_EQUALITY_TOL", 0.35)
# Fresher impulse requirement (was 16). 6 bars keeps entries within ~30 min
# of the LTF impulse end on 5m TF, avoiding stale setups.
IMPULSE_MAX_AGE_BARS = _env_int("IMPULSE_MAX_AGE_BARS", 6)
IMPULSE_MIN_BARS = _env_int("IMPULSE_MIN_BARS", 2)
IMPULSE_MAX_BARS = _env_int("IMPULSE_MAX_BARS", 4)
IMPULSE_PULLBACK_MAX = _env_float("IMPULSE_PULLBACK_MAX", 0.45)
IMPULSE_BREAKOUT_LOOKBACK = _env_int("IMPULSE_BREAKOUT_LOOKBACK", 12)

ENTRY_FIB_LOW = _env_float("ENTRY_FIB_LOW", 0.382)
ENTRY_FIB_HIGH = _env_float("ENTRY_FIB_HIGH", 0.618)
SL_ATR_MULT = _env_float("SL_ATR_MULT", 1.2)
SL_MIN_ATR_MULT = _env_float("SL_MIN_ATR_MULT", 1.0)
MIN_RR = _env_float("MIN_RR", 1.6)
MAX_RR = _env_float("MAX_RR", 4.0)
MAX_ENTRY_DISTANCE_ATR = _env_float("MAX_ENTRY_DISTANCE_ATR", 1.2)

TP1_WAVE_MULT = _env_float("TP1_WAVE_MULT", 1.0)
TP2_WAVE_MULT = _env_float("TP2_WAVE_MULT", 1.618)
TP3_SPIKE_LEVEL = _env_int("TP3_SPIKE_LEVEL", 1) == 1

BACKTEST_ENTRY_WAIT_BARS = _env_int("BACKTEST_ENTRY_WAIT_BARS", 18)
BACKTEST_TRADE_MAX_BARS = _env_int("BACKTEST_TRADE_MAX_BARS", 220)
BACKTEST_MOVE_SL_TO_BE_AFTER_TP1 = _env_int("BACKTEST_MOVE_SL_TO_BE_AFTER_TP1", 1) == 1
BACKTEST_TIMEOUT_EXIT_ON_CLOSE = _env_int("BACKTEST_TIMEOUT_EXIT_ON_CLOSE", 1) == 1
BACKTEST_CONSERVATIVE_INTRABAR = _env_int("BACKTEST_CONSERVATIVE_INTRABAR", 1) == 1
TP1_CLOSE_PCT = _env_float("TP1_CLOSE_PCT", 0.30)
TP2_CLOSE_PCT = _env_float("TP2_CLOSE_PCT", 0.40)
TP3_CLOSE_PCT = _env_float("TP3_CLOSE_PCT", 0.30)

INITIAL_BALANCE = _env_float("INITIAL_BALANCE", 1000.0)
RISK_PER_TRADE_PCT = _env_float("RISK_PER_TRADE_PCT", 1.0)
MAX_OPEN_TRADES = _env_int("MAX_OPEN_TRADES", 3)
MAX_DAILY_LOSS_PCT = _env_float("MAX_DAILY_LOSS_PCT", 5.0)
MAX_DRAWDOWN_PCT = _env_float("MAX_DRAWDOWN_PCT", 15.0)
LEVERAGE = _env_int("LEVERAGE", 5)

# Session lists re-derived from backtest mean_R per session:
#   london_newyork_overlap: -0.25R  (best liquid session)
#   newyork:                -0.24R
#   rollover:               +0.43R  (small n=5, kept as neutral)
#   london:                 -0.42R  (moved to SKIP)
#   asia:                   -0.55R  (kept in SKIP)
BEST_SESSIONS = ["london_newyork_overlap", "newyork", "rollover"]
SKIP_SESSIONS = ["asia", "london"]

# Volume confirmation now required — adds cheap filter against low-liquidity
# chop entries. Default flipped from 0 to 1.
VOLUME_CONFIRMATION_REQUIRED = _env_int("VOLUME_CONFIRMATION_REQUIRED", 1) == 1
BTC_FILTER_ENABLED = _env_int("BTC_FILTER_ENABLED", 1) == 1
BTC_FILTER_DROP_PCT = _env_float("BTC_FILTER_DROP_PCT", 1.2)
BTC_FILTER_LOOKBACK_BARS = _env_int("BTC_FILTER_LOOKBACK_BARS", 3)
BTC_FILTER_EMA_PERIOD = _env_int("BTC_FILTER_EMA_PERIOD", 20)
BTC_FILTER_REQUIRE_BELOW_EMA = _env_int("BTC_FILTER_REQUIRE_BELOW_EMA", 1) == 1
LOG_REJECT_SUMMARY_EVERY_CYCLE = _env_int("LOG_REJECT_SUMMARY_EVERY_CYCLE", 1) == 1
DATA_CACHE_TTL_SEC = _env_int("DATA_CACHE_TTL_SEC", 45)
DATA_REQUEST_PAUSE_SEC = _env_float("DATA_REQUEST_PAUSE_SEC", 0.15)
DATA_RETRY_SLEEP_SEC = _env_float("DATA_RETRY_SLEEP_SEC", 3.0)

LOG_LEVEL = _env_str("LOG_LEVEL", "INFO")

_LOG_DIR = Path(__file__).resolve().with_name("logs")
LOG_FILE = str(_LOG_DIR / "wave_scanner.log")
SIGNALS_CSV = str(_LOG_DIR / "signals.csv")
TRADES_CSV = str(_LOG_DIR / "trades.csv")

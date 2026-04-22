# ============================================================
#  data_fetcher.py — Загрузка данных с биржи
# ============================================================
from __future__ import annotations

import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Any, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import ccxt

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg

logger = logging.getLogger(__name__)

_exchange: Optional[Any] = None
_market_cache: dict[str, str] = {}
_thread_local = __import__('threading').local()
_data_cache: dict[tuple[str, str, int], tuple[float, pd.DataFrame]] = {}


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if "/" in symbol:
        return symbol
    m = re.fullmatch(r"([A-Z0-9]+)(USDT|USDC)", symbol)
    if m:
        base, quote = m.groups()
        return f"{base}/{quote}:{quote}"
    return symbol


def _get_exchange() -> Any:
    """Возвращает thread-local экземпляр exchange (thread-safe)."""
    if not hasattr(_thread_local, "exchange"):
        import ccxt
        _thread_local.exchange = ccxt.bybit({
            "enableRateLimit": True,
            "options": {"defaultType": "linear"},
        })
    return _thread_local.exchange


def _validate_df(df: pd.DataFrame, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    """Валидация и очистка OHLCV данных."""
    df = df.dropna()
    df = df[~df.index.duplicated(keep='first')]
    if len(df) > 0:
        invalid = (df["high"] < df["low"]) | (df["close"] < 0) | (df["volume"] < 0)
        if invalid.any():
            logger.warning(f"{symbol} {timeframe}: {invalid.sum()} invalid bars removed")
            df = df[~invalid]
    if len(df) < limit * 0.8:
        logger.warning(f"{symbol} {timeframe}: insufficient valid data ({len(df)}/{limit})")
        return None
    return df


def fetch_ohlcv(symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    cache_key = (symbol, timeframe, limit)
    cached = _data_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < cfg.DATA_CACHE_TTL_SEC:
        return cached[1].copy()
    try:
        ex = _get_exchange()
        ex_symbol = _market_cache.get(symbol) or normalize_symbol(symbol)
        _market_cache[symbol] = ex_symbol
        if cfg.DATA_REQUEST_PAUSE_SEC > 0:
            time.sleep(cfg.DATA_REQUEST_PAUSE_SEC)
        raw = ex.fetch_ohlcv(ex_symbol, timeframe, limit=limit)
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.set_index("timestamp", inplace=True)
        df = df.astype(float)
        df = _validate_df(df, symbol, timeframe, limit)
        if df is not None:
            _data_cache[cache_key] = (time.time(), df.copy())
        return df
    except Exception as e:
        logger.error(f"fetch_ohlcv {symbol} ({_market_cache.get(symbol, normalize_symbol(symbol))}) {timeframe}: {e}")
        time.sleep(cfg.DATA_RETRY_SLEEP_SEC)
        return None


def fetch_multi_tf(symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Загружает 3 таймфрейма последовательно (ccxt не thread-safe для одного exchange)."""
    specs = [
        (cfg.TF_ENTRY,  cfg.CANDLES_ENTRY),
        (cfg.TF_HTF,    cfg.CANDLES_HTF),
        (cfg.TF_TREND,  cfg.CANDLES_TREND),
    ]
    data = {}
    for tf, limit in specs:
        df = fetch_ohlcv(symbol, tf, limit)
        if df is None or len(df) < 30:
            logger.warning(f"{symbol} {tf}: insufficient data")
            return None
        data[tf] = df
    return data

# ============================================================
#  data_fetcher.py — Загрузка данных с биржи
# ============================================================
from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import ccxt

sys.path.insert(0, os.path.dirname(__file__))
import config as cfg

logger = logging.getLogger(__name__)

_thread_local = threading.local()

_cache_lock = threading.Lock()
_market_cache: Dict[str, str] = {}
_data_cache: Dict[tuple, tuple] = {}


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


def _cache_get(key: tuple) -> Optional[pd.DataFrame]:
    with _cache_lock:
        entry = _data_cache.get(key)
        if entry is None:
            return None
        ts, df = entry
    if (time.time() - ts) >= cfg.DATA_CACHE_TTL_SEC:
        return None
    return df.copy()


def _cache_set(key: tuple, df: pd.DataFrame) -> None:
    with _cache_lock:
        _data_cache[key] = (time.time(), df.copy())


def _market_get(symbol: str) -> Optional[str]:
    with _cache_lock:
        return _market_cache.get(symbol)


def _market_set(symbol: str, ex_symbol: str) -> None:
    with _cache_lock:
        _market_cache[symbol] = ex_symbol


def fetch_ohlcv(symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    cache_key = (symbol, timeframe, limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        ex = _get_exchange()
        ex_symbol = _market_get(symbol) or normalize_symbol(symbol)
        _market_set(symbol, ex_symbol)
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
            _cache_set(cache_key, df)
        return df
    except Exception as e:
        logger.error(f"fetch_ohlcv {symbol} ({_market_get(symbol) or normalize_symbol(symbol)}) {timeframe}: {e}")
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

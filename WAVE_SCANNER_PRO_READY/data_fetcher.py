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
from collections import OrderedDict
from typing import Any, Dict, Optional, TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    import ccxt

sys.path.insert(0, os.path.dirname(__file__))
import config as cfg

logger = logging.getLogger(__name__)

_thread_local = threading.local()

_cache_lock = threading.Lock()
_market_cache: Dict[str, str] = {}
# Bounded LRU; eviction is FIFO by insertion order. Without this the cache
# only grew over time because TTL was a read-side check — entries that were
# never read again sat in memory forever. Each OHLCV frame is small (~50 KB
# at float32 / ~100 KB at float64) but with multiple (symbol,tf,limit) keys
# accumulating across long-running sessions it adds up.
_data_cache: "OrderedDict[tuple, tuple]" = OrderedDict()


def _ohlcv_dtype() -> Any:
    return np.float32 if getattr(cfg, "OHLCV_DTYPE", "float64") == "float32" else np.float64


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


def _purge_expired_locked(now: float) -> None:
    """Drop entries past their TTL. Caller must hold _cache_lock."""
    ttl = cfg.DATA_CACHE_TTL_SEC
    if ttl <= 0:
        return
    expired = [k for k, (ts, _) in _data_cache.items() if (now - ts) >= ttl]
    for k in expired:
        _data_cache.pop(k, None)


def _cache_get(key: tuple) -> Optional[pd.DataFrame]:
    now = time.time()
    with _cache_lock:
        _purge_expired_locked(now)
        entry = _data_cache.get(key)
        if entry is None:
            return None
        ts, df = entry
        if (now - ts) >= cfg.DATA_CACHE_TTL_SEC:
            _data_cache.pop(key, None)
            return None
        # Mark MRU.
        _data_cache.move_to_end(key)
    # Cached frames are read-only by contract (every consumer in
    # wave_analyzer / impulse_detector / signal_engine slices, calls
    # .ewm/.rolling/.shift, or does column reads — none mutate). Returning
    # the original avoids two full DataFrame copies per scan/per timeframe.
    return df


def _cache_set(key: tuple, df: pd.DataFrame) -> None:
    max_entries = max(1, int(getattr(cfg, "DATA_CACHE_MAX_ENTRIES", 96)))
    now = time.time()
    with _cache_lock:
        _purge_expired_locked(now)
        _data_cache[key] = (now, df)
        _data_cache.move_to_end(key)
        while len(_data_cache) > max_entries:
            _data_cache.popitem(last=False)


def cache_stats() -> Dict[str, Any]:
    """Inspector for the dashboard / debug endpoints."""
    with _cache_lock:
        sizes = []
        for _, df in _data_cache.values():
            try:
                sizes.append(int(df.memory_usage(index=True, deep=False).sum()))
            except Exception:
                sizes.append(0)
        return {
            "entries": len(_data_cache),
            "max_entries": int(getattr(cfg, "DATA_CACHE_MAX_ENTRIES", 96)),
            "ttl_sec": int(cfg.DATA_CACHE_TTL_SEC),
            "approx_bytes": int(sum(sizes)),
        }


def clear_cache() -> int:
    with _cache_lock:
        n = len(_data_cache)
        _data_cache.clear()
        return n


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
        df = df.astype(_ohlcv_dtype(), copy=False)
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

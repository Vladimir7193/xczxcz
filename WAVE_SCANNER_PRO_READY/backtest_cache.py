"""OHLCV cache layer for deterministic backtest runs.

Why this exists
---------------
Bybit's REST endpoint occasionally returns partial data on a single call
(rate-limit trims, transient timeouts) and the live backtest loop has no
retry-until-complete logic. Empirically this means two runs of
``backtest.py`` on the same symbols/parameters produce noticeably different
trade counts (e.g. ADAUSDT 1 signal vs 10 signals across two back-to-back
runs), which makes any parameter comparison meaningless.

This module adds a sticky, on-disk cache so that:
  * The FIRST run per symbol/timeframe fetches fully, validates completeness
    with retries, and persists a ``.csv`` snapshot.
  * Every SUBSEQUENT run reads that snapshot verbatim — exact same bars,
    byte-for-byte identical trade list.
  * ``BACKTEST_REFRESH_CACHE=1`` forces a re-fetch + overwrite of the cache.

Completeness rule
-----------------
For a ``days``-day history at timeframe ``tf``, expected bars
= ``days * 86400 / tf_seconds``. We require ``>=90%`` of that and ``<=105%``
(5% over is allowed for boundary effects: the request may cross an
exchange's `since` boundary slightly). If the fetch is incomplete, we retry
up to ``BACKTEST_CACHE_FETCH_RETRIES`` times with backoff before giving up.
When giving up, we return whatever we got for the current run but do NOT
persist it to disk — so the next run will try again instead of locking in
the partial snapshot.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("backtest.cache")

_TF_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
}


def _tf_seconds(timeframe: str) -> int:
    if timeframe not in _TF_SECONDS:
        raise ValueError(f"Unknown timeframe: {timeframe}")
    return _TF_SECONDS[timeframe]


def _cache_path(cache_dir: Path, symbol: str, timeframe: str, days: int) -> Path:
    safe = symbol.replace("/", "_").replace(":", "_")
    return cache_dir / f"{safe}_{timeframe}_{days}d.csv.gz"


def _expected_bar_count(days: int, timeframe: str) -> int:
    return int(days * 86400 / _tf_seconds(timeframe))


def _is_complete(df: pd.DataFrame, days: int, timeframe: str) -> bool:
    if df is None or df.empty:
        return False
    expected = _expected_bar_count(days, timeframe)
    if expected <= 0:
        return True
    # Allow 10% shortfall (exchange maintenance, boundary truncation) and up
    # to 5% over-fetch (ccxt's since-alignment can return one extra batch).
    return 0.90 * expected <= len(df) <= 1.05 * expected


def _load_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, compression="gzip")
    except (OSError, ValueError, pd.errors.ParserError) as e:
        logger.warning("Corrupt cache file %s: %s — ignoring", path.name, e)
        return None
    if "timestamp" not in df.columns:
        logger.warning("Cache file %s missing timestamp column — ignoring", path.name)
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.drop_duplicates(subset=["timestamp"]).set_index("timestamp").sort_index()
    numeric_cols = ["open", "high", "low", "close", "volume"]
    missing = [c for c in numeric_cols if c not in df.columns]
    if missing:
        logger.warning("Cache file %s missing OHLCV columns %s — ignoring", path.name, missing)
        return None
    return df[numeric_cols].astype(float)


def _save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.reset_index().rename(columns={"index": "timestamp"})
    tmp = path.with_suffix(path.suffix + ".tmp")
    out.to_csv(tmp, index=False, compression="gzip")
    # Atomic replace so an interrupted write never leaves a half-file that
    # the next run would treat as "cached".
    os.replace(tmp, path)


def load_or_fetch_ohlcv(
    symbol: str,
    timeframe: str,
    days: int,
    fetch_fn,
    cache_dir: Path,
    use_cache: bool = True,
    refresh: bool = False,
    retries: int = 3,
    retry_sleep_sec: float = 2.0,
) -> Optional[pd.DataFrame]:
    """Return OHLCV for ``symbol`` on ``timeframe`` over ``days`` days.

    ``fetch_fn(symbol, timeframe, days)`` must return a ``DataFrame`` indexed
    by UTC timestamps with ``open/high/low/close/volume`` columns (matching
    ``backtest.fetch_full_history`` signature).

    Behavior:
      * If ``use_cache`` and not ``refresh`` and a valid cache file exists:
        return the cached frame.
      * Otherwise fetch; retry until complete (or ``retries`` reached); on
        completeness, persist to cache and return.
      * If never complete, return the last fetched frame without saving so
        the user can still see a result, but on the next run we'll try
        fetching again.
    """
    path = _cache_path(cache_dir, symbol, timeframe, days)

    if use_cache and not refresh:
        cached = _load_csv(path)
        if cached is not None and _is_complete(cached, days, timeframe):
            first = cached.index[0].strftime("%Y-%m-%d")
            last = cached.index[-1].strftime("%Y-%m-%d")
            logger.info(
                "  cache hit %s %s: %d bars %s..%s",
                symbol, timeframe, len(cached), first, last,
            )
            return cached
        elif cached is not None:
            logger.info(
                "  cache stale %s %s: %d bars (expected ~%d) — refetching",
                symbol, timeframe, len(cached), _expected_bar_count(days, timeframe),
            )

    expected = _expected_bar_count(days, timeframe)
    last_df: Optional[pd.DataFrame] = None
    for attempt in range(1, retries + 1):
        df = fetch_fn(symbol, timeframe, days)
        if df is None:
            logger.warning("  fetch %s %s attempt %d/%d returned None", symbol, timeframe, attempt, retries)
        elif _is_complete(df, days, timeframe):
            last_df = df
            if use_cache:
                _save_csv(df, path)
                logger.info(
                    "  cache save %s %s: %d bars (exp ~%d) -> %s",
                    symbol, timeframe, len(df), expected, path.name,
                )
            return df
        else:
            last_df = df
            logger.warning(
                "  fetch %s %s attempt %d/%d incomplete: %d bars (expected ~%d)",
                symbol, timeframe, attempt, retries, len(df), expected,
            )
        if attempt < retries:
            time.sleep(retry_sleep_sec * attempt)

    if last_df is not None:
        logger.warning(
            "  fetch %s %s INCOMPLETE after %d retries — using partial data, NOT caching",
            symbol, timeframe, retries,
        )
        return last_df
    logger.error("  fetch %s %s failed on all %d retries", symbol, timeframe, retries)
    return None


def clear_cache(cache_dir: Path) -> int:
    """Delete all cache files. Returns number of files removed."""
    if not cache_dir.exists():
        return 0
    count = 0
    for f in cache_dir.glob("*.csv.gz"):
        try:
            f.unlink()
            count += 1
        except OSError:
            pass
    return count


def describe_cache(cache_dir: Path) -> str:
    """Return a short multiline summary of cached files for logging."""
    if not cache_dir.exists():
        return "cache dir missing"
    files = sorted(cache_dir.glob("*.csv.gz"))
    if not files:
        return "cache empty"
    lines = [f"cache dir: {cache_dir} ({len(files)} files)"]
    for f in files:
        try:
            size_kb = f.stat().st_size / 1024
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            age_h = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
            lines.append(f"  {f.name:<40} {size_kb:>6.1f} KiB  age={age_h:>5.1f}h")
        except OSError:
            continue
    return "\n".join(lines)

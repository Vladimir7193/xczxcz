"""Deterministic OHLCV fetcher backed by Bybit's public tick archive.

Motivation
----------
The live ccxt REST endpoint (api.bybit.com / fapi.binance.com) has proved
unreliable for backtesting:

  * Cross-fetch determinism is poor — partial responses under rate-limit
    or transient timeouts yield different OHLCV bars for the same period.
    This was the root cause of the PR #10 → PR #11 cache-variance anomaly
    (combined PF 2.08 vs 0.82 on the same 120-day window).
  * Geo-blocking. CloudFront routinely returns 403 Forbidden for api.bybit.com
    from several regions including Devin VMs, and Binance REST returns
    451 Restricted Location. This makes the whole backtest unrunnable on
    affected machines.

Bybit publishes raw tick-level trade data at
``https://public.bybit.com/trading/<SYMBOL>/<SYMBOL><YYYY-MM-DD>.csv.gz``.
Those files are static (never rewritten), served from an origin that does
not enforce the same geo-block as the REST API, and each day contains
every single executed trade. Aggregating them to OHLCV gives perfectly
reproducible bars — the same input files always yield the same bars,
across machines and across runs.

Design
------
The module is a drop-in replacement for the ``fetch_fn`` callback used by
``backtest_cache.load_or_fetch_ohlcv``. It:

  1. Downloads each daily tick file to a local on-disk cache (raw ticks,
     kept verbatim so the aggregation step is reproducible).
  2. Resamples the concatenated ticks to the requested timeframe using
     pandas ``Grouper`` (label=left, closed=left — standard OHLCV).
  3. Returns a DataFrame indexed by UTC timestamps with
     ``open/high/low/close/volume`` columns, identical in shape to what
     ``fetch_full_history`` produces.

The upstream OHLCV cache (``backtest_cache``) layers on top unchanged:
this fetcher just removes the non-determinism source.

Rate-limit courtesy: one sequential HTTP download per file with a short
sleep between calls; tick files are a few MB each and public.bybit.com
is not anycasted aggressively, so we stay polite.
"""
from __future__ import annotations

import gzip
import io
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import urllib.request
import urllib.error

logger = logging.getLogger("backtest.public_fetch")

PUBLIC_BYBIT_BASE = os.environ.get(
    "BACKTEST_PUBLIC_BYBIT_BASE",
    "https://public.bybit.com/trading",
)
TICK_CACHE_SUBDIR = "ticks"
HTTP_TIMEOUT_SEC = int(os.environ.get("BACKTEST_PUBLIC_HTTP_TIMEOUT", "60"))
HTTP_RETRIES = int(os.environ.get("BACKTEST_PUBLIC_HTTP_RETRIES", "3"))
INTER_REQUEST_SLEEP = float(os.environ.get("BACKTEST_PUBLIC_SLEEP_SEC", "0.2"))


def _tick_cache_dir(root: Path, symbol: str) -> Path:
    return root / TICK_CACHE_SUBDIR / symbol


def _tick_file(root: Path, symbol: str, day: datetime) -> Path:
    return _tick_cache_dir(root, symbol) / f"{symbol}{day.strftime('%Y-%m-%d')}.csv.gz"


def _download_tick_day(symbol: str, day: datetime, dest: Path) -> bool:
    """Download one day of tick data. Returns True on success."""
    url = f"{PUBLIC_BYBIT_BASE}/{symbol}/{symbol}{day.strftime('%Y-%m-%d')}.csv.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "wave-scanner-bt/1.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
                data = resp.read()
            # Write atomically — a partial file would silently corrupt the
            # cache and the next run would reuse it without fetching again.
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            tmp.write_bytes(data)
            os.replace(tmp, dest)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug("  tick 404 %s %s (day likely not published yet)", symbol, day.date())
                return False
            logger.warning(
                "  tick HTTP %d %s %s attempt %d/%d",
                e.code, symbol, day.date(), attempt, HTTP_RETRIES,
            )
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            logger.warning(
                "  tick fetch err %s %s attempt %d/%d: %s",
                symbol, day.date(), attempt, HTTP_RETRIES, e,
            )
        if attempt < HTTP_RETRIES:
            time.sleep(1.0 * attempt)
    return False


def _load_tick_day(path: Path) -> Optional[pd.DataFrame]:
    """Load one cached tick file. Returns None if unreadable."""
    try:
        with gzip.open(path, "rt") as f:
            df = pd.read_csv(f, usecols=["timestamp", "size", "price"])
    except (OSError, ValueError, pd.errors.ParserError) as e:
        logger.warning("  tick file unreadable %s: %s", path.name, e)
        return None
    if df.empty:
        return None
    # ``timestamp`` is unix seconds with fractional part.
    df["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["size"] = pd.to_numeric(df["size"], errors="coerce")
    df = df.dropna(subset=["ts", "price", "size"])
    return df[["ts", "price", "size"]]


def _aggregate_ohlcv(ticks: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample trade ticks to OHLCV at ``timeframe``.

    Uses pandas Grouper with label=left/closed=left — identical convention
    to ccxt OHLCV (bar stamped with its opening time, closes include the
    last tick strictly before the next open).
    """
    freq = {
        "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min",
        "30m": "30min", "1h": "1h", "2h": "2h", "4h": "4h",
        "6h": "6h", "12h": "12h", "1d": "1D",
    }.get(timeframe)
    if freq is None:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    g = ticks.set_index("ts").groupby(pd.Grouper(freq=freq, label="left", closed="left"))
    bars = g.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
    ).dropna(subset=["open"])
    bars.index.name = "timestamp"
    return bars


def fetch_ohlcv_from_public(
    symbol: str,
    timeframe: str,
    days: int,
    tick_cache_root: Path,
) -> Optional[pd.DataFrame]:
    """Return deterministic OHLCV for ``symbol`` covering the last ``days`` days.

    ``tick_cache_root`` is the parent cache directory; raw daily tick files
    are stored under ``<root>/ticks/<symbol>/``. The function is safe to
    call repeatedly — already-downloaded days are reused verbatim.

    Return value matches ``backtest.fetch_full_history``: DataFrame indexed
    by UTC timestamps with ``open/high/low/close/volume`` float columns, or
    ``None`` on total failure.
    """
    # Build the day list — Bybit publishes one file per UTC day. We request
    # ``days`` full days ending today (today's file is typically not yet
    # published, so callers should treat today as best-effort).
    now = datetime.now(timezone.utc)
    end_day = now.date()
    day_list: List[datetime] = []
    for i in range(days, -1, -1):
        d = end_day - timedelta(days=i)
        day_list.append(datetime(d.year, d.month, d.day, tzinfo=timezone.utc))

    downloaded = 0
    cached = 0
    missing = 0
    frames: List[pd.DataFrame] = []
    for day in day_list:
        path = _tick_file(tick_cache_root, symbol, day)
        if not path.exists():
            ok = _download_tick_day(symbol, day, path)
            if ok:
                downloaded += 1
                time.sleep(INTER_REQUEST_SLEEP)
            else:
                missing += 1
                continue
        else:
            cached += 1
        df_day = _load_tick_day(path)
        if df_day is not None and not df_day.empty:
            frames.append(df_day)

    if not frames:
        logger.error(
            "  public-fetch %s %s: no usable tick data (downloaded=%d cached=%d missing=%d)",
            symbol, timeframe, downloaded, cached, missing,
        )
        return None

    logger.info(
        "  public-fetch %s %s: %d days (downloaded=%d cached=%d missing=%d)",
        symbol, timeframe, len(frames), downloaded, cached, missing,
    )
    ticks = pd.concat(frames, ignore_index=True).sort_values("ts")
    bars = _aggregate_ohlcv(ticks, timeframe)
    # Trim bars older than the requested window (we fetched one extra day
    # on the leading edge to guarantee full coverage across DST-like shifts).
    cutoff = pd.Timestamp(now - timedelta(days=days))
    bars = bars[bars.index >= cutoff].astype(float)
    return bars

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import ccxt
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from backtest_cache import load_or_fetch_ohlcv, describe_cache
from backtest_public_fetch import fetch_ohlcv_from_public
from wave_analyzer import is_ranging, analyze_wave_structure, check_correction_complete
from impulse_detector import detect_first_impulse, calculate_entry
from signal_engine import WaveSignalEngine, score_to_label, volume_confirming

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("backtest")

BACKTEST_SYMBOLS = cfg.SYMBOLS + cfg.SYMBOLS_EXTENDED[:4]
# DAYS_BACK (2026-04-22): 60 → 120.
# Reason: every filter PR (#7 MAX_SCORE=85, #8 SL=1.5, #9 MIN_SCORE=73)
# was derived and measured on the SAME 60-day deterministic cache
# (Feb 21 – Apr 22 2026). That is in-sample tuning by construction; even
# walk-forward W1/W2 shares the same generating regime. Extending the
# period to 120 days (Dec 22 2025 – Apr 22 2026) gives ~60 previously-
# unseen days (Dec-Jan) as genuine out-of-sample data, while preserving
# the existing cached Feb-Apr slice as the inner test.
# Combined with BACKTEST_WALK_FORWARD_WINDOWS=4 this produces four
# non-overlapping 30-day walk-forward windows; edge is only credible if
# PF holds in the oldest windows (which were never used to derive any
# filter default).
# Env-overridable so a user can bisect (`DAYS_BACK=60` reproduces the
# prior snapshot exactly, provided the cache dir is wiped or a separate
# cache dir is used).
DAYS_BACK = int(os.environ.get("BACKTEST_DAYS_BACK", "120"))
WINDOW_5M = 300
WINDOW_1H = 180
WINDOW_4H = 140
STEP_BARS_5M = 12
MIN_SCORE_BT = cfg.MIN_SCORE
MAX_SCORE_BT = cfg.MAX_SCORE
RISK_PER_TRADE = cfg.RISK_PER_TRADE_PCT / 100.0
INITIAL_BALANCE = cfg.INITIAL_BALANCE

# Realistic trading costs (Bybit linear futures, April 2026 schedule).
FEE_TAKER_PCT = 0.055 / 100.0
FEE_MAKER_PCT = 0.02 / 100.0
SLIPPAGE_PCT = 0.02 / 100.0


@dataclass
class TradeResult:
    symbol: str
    direction: str
    signal_time: pd.Timestamp
    entry_time: Optional[pd.Timestamp]
    entry_price: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    rr_ratio: float
    score: float
    session: str
    fib_level: float
    fib_reached: bool
    liquidity_swept: bool
    a_equals_c: bool
    braking_volume: bool
    correction_type: str
    outcome: str = ""
    exit_price: float = 0.0
    pnl_r: float = 0.0
    pnl_pct: float = 0.0
    bars_held: int = 0
    fill_bars: int = 0


def get_exchange() -> ccxt.Exchange:
    return ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})


def tf_to_ms(tf: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    return int(tf[:-1]) * units[tf[-1]]


def fetch_full_history(symbol: str, timeframe: str, days: int, exchange: ccxt.Exchange) -> Optional[pd.DataFrame]:
    limit = 1000
    tf_ms = tf_to_ms(timeframe)
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    rows = []
    while True:
        try:
            batch = exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
        except Exception as e:
            logger.error("fetch error %s %s: %s", symbol, timeframe, e)
            return None
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        since_ms = batch[-1][0] + tf_ms
        time.sleep(0.25)
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.drop_duplicates(subset=["timestamp"]).set_index("timestamp").sort_index().astype(float)


def get_session(ts: pd.Timestamp) -> str:
    hour = ts.hour
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 13:
        return "london"
    if 13 <= hour < 16:
        return "london_newyork_overlap"
    if 16 <= hour < 22:
        return "newyork"
    return "rollover"


def simulate_trade(
    df_future: pd.DataFrame,
    direction: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
    entry_wait_bars: int = cfg.BACKTEST_ENTRY_WAIT_BARS,
    max_bars: int = cfg.BACKTEST_TRADE_MAX_BARS,
) -> Tuple[str, float, int, int, Optional[pd.Timestamp]]:
    if len(df_future) == 0:
        return "no_data", entry, 0, 0, None

    fill_idx = None
    fill_time = None
    for i, (ts, row) in enumerate(df_future.head(entry_wait_bars).iterrows()):
        high = float(row["high"])
        low = float(row["low"])
        if low <= entry <= high:
            fill_idx = i
            fill_time = ts
            break
    if fill_idx is None:
        return "not_filled", entry, 0, entry_wait_bars, None

    trade_bars = df_future.iloc[fill_idx:fill_idx + max_bars]
    remaining = 1.0
    weighted_exit = 0.0
    tp1_hit = False
    tp2_hit = False
    active_sl = sl

    for i, (_, row) in enumerate(trade_bars.iterrows()):
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if direction == "long":
            if cfg.BACKTEST_CONSERVATIVE_INTRABAR and low <= active_sl:
                weighted_exit += remaining * active_sl
                return "sl", weighted_exit, i + 1, fill_idx + 1, fill_time
            if (not tp1_hit) and high >= tp1:
                weighted_exit += cfg.TP1_CLOSE_PCT * tp1
                remaining -= cfg.TP1_CLOSE_PCT
                tp1_hit = True
                if cfg.BACKTEST_MOVE_SL_TO_BE_AFTER_TP1:
                    risk_abs = abs(entry - sl)
                    be_lock = entry + cfg.BACKTEST_BE_LOCK_IN_R * risk_abs
                    active_sl = max(active_sl, be_lock)
            if tp1_hit and (not tp2_hit) and high >= tp2:
                weighted_exit += cfg.TP2_CLOSE_PCT * tp2
                remaining -= cfg.TP2_CLOSE_PCT
                tp2_hit = True
            if tp2_hit and high >= tp3:
                weighted_exit += remaining * tp3
                return "tp3", weighted_exit, i + 1, fill_idx + 1, fill_time
            if (not cfg.BACKTEST_CONSERVATIVE_INTRABAR) and low <= active_sl:
                weighted_exit += remaining * active_sl
                return "sl", weighted_exit, i + 1, fill_idx + 1, fill_time
        else:
            if cfg.BACKTEST_CONSERVATIVE_INTRABAR and high >= active_sl:
                weighted_exit += remaining * active_sl
                return "sl", weighted_exit, i + 1, fill_idx + 1, fill_time
            if (not tp1_hit) and low <= tp1:
                weighted_exit += cfg.TP1_CLOSE_PCT * tp1
                remaining -= cfg.TP1_CLOSE_PCT
                tp1_hit = True
                if cfg.BACKTEST_MOVE_SL_TO_BE_AFTER_TP1:
                    risk_abs = abs(sl - entry)
                    be_lock = entry - cfg.BACKTEST_BE_LOCK_IN_R * risk_abs
                    active_sl = min(active_sl, be_lock)
            if tp1_hit and (not tp2_hit) and low <= tp2:
                weighted_exit += cfg.TP2_CLOSE_PCT * tp2
                remaining -= cfg.TP2_CLOSE_PCT
                tp2_hit = True
            if tp2_hit and low <= tp3:
                weighted_exit += remaining * tp3
                return "tp3", weighted_exit, i + 1, fill_idx + 1, fill_time
            if (not cfg.BACKTEST_CONSERVATIVE_INTRABAR) and high >= active_sl:
                weighted_exit += remaining * active_sl
                return "sl", weighted_exit, i + 1, fill_idx + 1, fill_time

        timeout_close = close

    weighted_exit += remaining * timeout_close
    if tp2_hit:
        return "tp2", weighted_exit, len(trade_bars), fill_idx + 1, fill_time
    if tp1_hit:
        return "tp1", weighted_exit, len(trade_bars), fill_idx + 1, fill_time
    return "timeout", weighted_exit, len(trade_bars), fill_idx + 1, fill_time


def btc_is_falling_at(btc_1h: Optional[pd.DataFrame], ts_now: pd.Timestamp) -> bool:
    """Historical mirror of ``signal_engine.btc_is_falling``.

    Looks at BTC 1h closes up to and including the bar at ``ts_now`` and
    returns True iff the live filter would have blocked a long signal at
    that moment.

    Identical parameterisation to the live code (drop %, candle losses,
    EMA bias) so that backtest-reported PF matches what the scanner will
    actually execute post-merge. Returns False on any data shortfall —
    consistent with live behaviour where a missing BTC cache simply
    disables the filter rather than forcing a rejection.
    """
    if btc_1h is None or len(btc_1h) == 0:
        return False
    try:
        closes = btc_1h.loc[:ts_now, "close"].astype(float)
        lookback = max(2, int(cfg.BTC_FILTER_LOOKBACK_BARS))
        if len(closes) < lookback + 2:
            return False
        last_close = float(closes.iloc[-1])
        base_close = float(closes.iloc[-1 - lookback])
        if base_close <= 0:
            return False
        drop_pct = ((last_close - base_close) / base_close) * 100.0
        candle_losses = int((closes.diff().iloc[-lookback:] < 0).sum())
        below_ema = True
        if cfg.BTC_FILTER_REQUIRE_BELOW_EMA:
            ema = closes.ewm(span=cfg.BTC_FILTER_EMA_PERIOD, adjust=False).mean()
            below_ema = last_close < float(ema.iloc[-1])
        return (
            drop_pct <= -abs(cfg.BTC_FILTER_DROP_PCT)
            and candle_losses >= max(2, lookback - 1)
            and below_ema
        )
    except Exception:
        return False


def run_backtest_symbol(
    symbol: str,
    df_5m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    btc_1h: Optional[pd.DataFrame] = None,
) -> List[TradeResult]:
    results: List[TradeResult] = []
    seen: set = set()
    engine = WaveSignalEngine()

    for step in range(0, len(df_5m) - WINDOW_5M, STEP_BARS_5M):
        end_5m = step + WINDOW_5M
        ts_now = df_5m.index[end_5m - 1]
        df_5m_w = df_5m.iloc[step:end_5m]
        df_1h_w = df_1h[df_1h.index <= ts_now].tail(WINDOW_1H)
        df_4h_w = df_4h[df_4h.index <= ts_now].tail(WINDOW_4H)
        if min(len(df_5m_w), len(df_1h_w), len(df_4h_w)) < 60:
            continue

        ranging, _ = is_ranging(df_1h_w)
        if ranging:
            continue

        structure = analyze_wave_structure(df_1h_w, df_4h_w)
        if structure.trend not in ("up", "down") or structure.impulse_start is None:
            continue

        correction = check_correction_complete(df_5m_w, structure)
        if not correction.complete or structure.correction_pct > 0.786:
            continue

        impulse = detect_first_impulse(df_5m_w, structure)
        if not impulse.found:
            continue

        bars_since = len(df_5m_w) - 1 - impulse.bar_index
        if bars_since > cfg.IMPULSE_MAX_AGE_BARS:
            continue

        entry_setup = calculate_entry(df_5m_w, structure, impulse)
        if not entry_setup.valid:
            continue

        risk = abs(entry_setup.entry_price - entry_setup.stop_loss)
        if risk < entry_setup.atr * cfg.SL_MIN_ATR_MULT:
            continue
        if entry_setup.rr_ratio < cfg.MIN_RR or entry_setup.rr_ratio > cfg.MAX_RR:
            continue
        cur_price = float(df_5m_w["close"].iloc[-1])
        if structure.trend == "up" and cur_price < entry_setup.cancel_level:
            continue
        if structure.trend == "down" and cur_price > entry_setup.cancel_level:
            continue
        entry_distance_atr = abs(cur_price - entry_setup.entry_price) / entry_setup.atr if entry_setup.atr > 0 else 0.0
        if entry_distance_atr > cfg.MAX_ENTRY_DISTANCE_ATR:
            continue
        if cfg.VOLUME_CONFIRMATION_REQUIRED and not volume_confirming(df_5m_w):
            continue
        if structure.correction_type == "unknown" and not structure.has_5waves:
            continue

        score = engine.calculate_final_score(correction, impulse, entry_setup, structure, df_5m_w)
        if score < MIN_SCORE_BT or score > MAX_SCORE_BT or score_to_label(score) == "SKIP":
            continue

        direction = "long" if structure.trend == "up" else "short"
        # Mirror of signal_engine.py: LONG-only BTC-falling filter. Keeps
        # backtest semantics identical to live scanner so reported PF
        # reflects what the bot will actually execute.
        if (
            cfg.BTC_FILTER_ENABLED
            and symbol != "BTCUSDT"
            and direction == "long"
            and btc_is_falling_at(btc_1h, ts_now)
        ):
            continue
        dedup_key = f"{symbol}:{direction}:{ts_now.floor('1h')}:{round(entry_setup.entry_price, 4)}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        df_future = df_5m.iloc[end_5m:end_5m + max(cfg.BACKTEST_ENTRY_WAIT_BARS + cfg.BACKTEST_TRADE_MAX_BARS + 5, 260)]
        outcome, exit_price, bars_held, fill_bars, entry_time = simulate_trade(
            df_future,
            direction,
            entry_setup.entry_price,
            entry_setup.stop_loss,
            entry_setup.tp1,
            entry_setup.tp2,
            entry_setup.tp3,
        )
        if outcome == "not_filled":
            continue

        # Realistic costs: limit-style entry (maker) + taker exit + 2x slippage.
        entry_cost = entry_setup.entry_price * (FEE_MAKER_PCT + SLIPPAGE_PCT)
        exit_cost = exit_price * (FEE_TAKER_PCT + SLIPPAGE_PCT)
        total_fee = entry_cost + exit_cost
        pnl_r = 0.0
        if risk > 0:
            if direction == "long":
                pnl_r = (exit_price - entry_setup.entry_price - total_fee) / risk
            else:
                pnl_r = (entry_setup.entry_price - exit_price - total_fee) / risk
        pnl_pct = pnl_r * RISK_PER_TRADE * 100

        results.append(TradeResult(
            symbol=symbol,
            direction=direction,
            signal_time=ts_now,
            entry_time=entry_time,
            entry_price=entry_setup.entry_price,
            stop_loss=entry_setup.stop_loss,
            tp1=entry_setup.tp1,
            tp2=entry_setup.tp2,
            tp3=entry_setup.tp3,
            rr_ratio=entry_setup.rr_ratio,
            score=score,
            session=get_session(ts_now),
            fib_level=correction.fib_level,
            fib_reached=correction.fib_reached,
            liquidity_swept=correction.liquidity_swept,
            a_equals_c=correction.a_equals_c,
            braking_volume=correction.braking_volume,
            correction_type=structure.correction_type,
            outcome=outcome,
            exit_price=round(exit_price, 6),
            pnl_r=round(pnl_r, 3),
            pnl_pct=round(pnl_pct, 4),
            bars_held=bars_held,
            fill_bars=fill_bars,
        ))
    return results


def _slice_5m_windows(df_5m: pd.DataFrame, n_windows: int) -> List[Tuple[str, pd.DataFrame]]:
    """Split the 5m frame into ``n_windows`` roughly equal contiguous slices.

    Each slice starts at a bar boundary; HTF frames are NOT sliced because the
    inner loop pulls lookback history via ``tail(WINDOW_1H/4H)`` anyway.
    Returns a list of ``(label, df_5m_slice)`` pairs.
    """
    n = max(1, int(n_windows))
    if n == 1 or len(df_5m) <= WINDOW_5M + STEP_BARS_5M:
        return [("all", df_5m)]

    bars = len(df_5m)
    # We need each window to contain at least WINDOW_5M + STEP_BARS_5M bars
    # so that run_backtest_symbol has at least one iteration.
    min_bars = WINDOW_5M + STEP_BARS_5M * 2
    if bars // n < min_bars:
        logger.warning(
            "Walk-forward: not enough 5m bars (%d) to split into %d windows (min %d each) — falling back to 1 window",
            bars, n, min_bars,
        )
        return [("all", df_5m)]

    chunk = bars // n
    slices: List[Tuple[str, pd.DataFrame]] = []
    for i in range(n):
        start = i * chunk
        end = (i + 1) * chunk if i < n - 1 else bars
        sub = df_5m.iloc[start:end]
        t0 = sub.index[0].strftime("%Y-%m-%d")
        t1 = sub.index[-1].strftime("%Y-%m-%d")
        slices.append((f"W{i + 1}({t0}..{t1})", sub))
    return slices


def run_backtest_windows(
    symbol: str,
    df_5m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    n_windows: int,
    btc_1h: Optional[pd.DataFrame] = None,
) -> List[Tuple[str, List[TradeResult]]]:
    """Run ``run_backtest_symbol`` independently on each walk-forward window.

    Returns ``[(window_label, trades), ...]`` so the caller can report per-window
    stats. HTF frames are passed in full because the inner loop bounds lookback
    by ``df_1h.index <= ts_now`` already.

    ``btc_1h`` is forwarded to ``run_backtest_symbol`` so the LONG-only
    BTC-falling filter (mirror of signal_engine) can be evaluated with
    point-in-time BTC data.
    """
    windows = _slice_5m_windows(df_5m, n_windows)
    out: List[Tuple[str, List[TradeResult]]] = []
    for label, sub_5m in windows:
        trades = run_backtest_symbol(symbol, sub_5m, df_1h, df_4h, btc_1h=btc_1h)
        out.append((label, trades))
    return out


def _stats_block(results: List[TradeResult]) -> dict:
    """Compute summary stats for a list of trades."""
    if not results:
        return {
            "total": 0, "win_rate": 0.0, "total_r": 0.0,
            "avg_win_r": 0.0, "avg_loss_r": 0.0,
            "profit_factor": 0.0, "outcomes": {},
        }
    df = pd.DataFrame([vars(r) for r in results])
    wins = df[df["pnl_r"] > 0]
    losses = df[df["pnl_r"] <= 0]
    total = len(df)
    win_rate = (len(wins) / total * 100) if total else 0.0
    total_r = float(df["pnl_r"].sum())
    avg_win_r = float(wins["pnl_r"].mean()) if len(wins) else 0.0
    avg_loss_r = float(losses["pnl_r"].mean()) if len(losses) else 0.0
    loss_sum = float(losses["pnl_r"].sum())
    profit_factor = (float(wins["pnl_r"].sum()) / abs(loss_sum)) if loss_sum != 0 else float("inf")
    outcomes = df["outcome"].value_counts().to_dict()
    return {
        "total": total,
        "win_rate": win_rate,
        "total_r": total_r,
        "avg_win_r": avg_win_r,
        "avg_loss_r": avg_loss_r,
        "profit_factor": profit_factor,
        "outcomes": outcomes,
    }


def print_stats(label: str, results: List[TradeResult]) -> None:
    """Print a single stats block for a window or the combined run."""
    s = _stats_block(results)
    print("\n" + "=" * 60)
    print(f"           BACKTEST RESULTS — {label}")
    print("=" * 60)
    if s["total"] == 0:
        print("  (no trades)")
        print("=" * 60)
        return
    print(f"  Total trades:    {s['total']}")
    print(f"  Win rate:        {s['win_rate']:.1f}%")
    print(f"  Total R:         {s['total_r']:+.2f}R")
    print(f"  Avg win:         {s['avg_win_r']:+.2f}R")
    print(f"  Avg loss:        {s['avg_loss_r']:+.2f}R")
    pf = s["profit_factor"]
    print(f"  Profit factor:   {pf:.2f}" if pf != float("inf") else "  Profit factor:   inf")
    print(f"  Outcomes:        {s['outcomes']}")
    if results:
        df = pd.DataFrame([vars(r) for r in results])
        by_session = df.groupby("session")["pnl_r"].agg(["count", "sum", "mean"]).round(3)
        top_syms = (
            df.groupby("symbol")["pnl_r"].agg(["count", "sum"])
            .sort_values("sum", ascending=False).head(10)
        )
        print("\n  By session:")
        print(by_session.to_string())
        print("\n  Top symbols by PnL (R):")
        print(top_syms.to_string())
    print("=" * 60)


def _save_results_csv(results: List[TradeResult], path: str) -> None:
    if not results:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pd.DataFrame([vars(r) for r in results]).to_csv(path, index=False)
    logger.info("Saved %d trades to %s", len(results), path)


def main() -> None:
    os.makedirs("logs", exist_ok=True)
    n_windows = max(1, int(cfg.BACKTEST_WALK_FORWARD_WINDOWS))

    # Data source selection. ``public`` pulls raw tick CSVs from
    # public.bybit.com and aggregates them locally to OHLCV — fully
    # deterministic (static files) and not geoblocked by CloudFront.
    # ``rest`` uses the ccxt/Bybit REST API (live signal_engine path,
    # the legacy default). ``auto`` prefers public when the PUBLIC base
    # is reachable, falling back to REST otherwise.
    data_source = os.environ.get("BACKTEST_DATA_SOURCE", "public").lower()
    if data_source not in {"public", "rest"}:
        logger.warning("Unknown BACKTEST_DATA_SOURCE=%s; falling back to 'public'", data_source)
        data_source = "public"

    exchange = get_exchange() if data_source == "rest" else None

    # Cache setup — paths resolved relative to this file so running from
    # any cwd still lands the snapshots in WAVE_SCANNER_PRO_READY/cache/.
    cache_dir = Path(__file__).resolve().parent / cfg.BACKTEST_CACHE_DIR
    use_cache = cfg.BACKTEST_USE_CACHE
    refresh_cache = cfg.BACKTEST_REFRESH_CACHE

    logger.info(
        "Starting backtest: %d symbols, %d days back, walk-forward windows=%d, data_source=%s",
        len(BACKTEST_SYMBOLS), DAYS_BACK, n_windows, data_source,
    )
    logger.info(
        "Cache: use=%s refresh=%s dir=%s",
        use_cache, refresh_cache, cache_dir,
    )
    if use_cache and cache_dir.exists():
        for line in describe_cache(cache_dir).splitlines():
            logger.info("  %s", line)

    def _fetch(sym: str, tf: str, d: int) -> Optional[pd.DataFrame]:
        if data_source == "public":
            return fetch_ohlcv_from_public(sym, tf, d, cache_dir)
        return fetch_full_history(sym, tf, d, exchange)

    # Pre-fetch BTC 1h once for the LONG-only BTC-falling filter (mirror of
    # signal_engine.py live behaviour). We use DAYS_BACK+10 to match the
    # existing 1h caches so validation against the existing cache file
    # succeeds when no refresh is requested. If BTC fetch fails the filter
    # silently degrades (same as live behaviour when the BTC cache is cold).
    btc_1h: Optional[pd.DataFrame] = None
    if cfg.BTC_FILTER_ENABLED:
        try:
            btc_1h = load_or_fetch_ohlcv(
                "BTCUSDT", "1h", DAYS_BACK + 10, _fetch, cache_dir,
                use_cache=use_cache, refresh=refresh_cache,
                retries=cfg.BACKTEST_CACHE_FETCH_RETRIES,
                retry_sleep_sec=cfg.BACKTEST_CACHE_RETRY_SLEEP_SEC,
            )
            if btc_1h is None or len(btc_1h) == 0:
                logger.warning("BTC filter enabled but BTC 1h data missing — filter will be a no-op")
                btc_1h = None
            else:
                logger.info("BTC filter active: %d BTC 1h bars loaded", len(btc_1h))
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("BTC filter fetch failed (%s); continuing without filter", exc)
            btc_1h = None

    # results per window label, preserving insertion order via index
    per_window: List[Tuple[str, List[TradeResult]]] = []

    for i, symbol in enumerate(BACKTEST_SYMBOLS):
        logger.info("[%d/%d] Loading %s ...", i + 1, len(BACKTEST_SYMBOLS), symbol)
        df_5m = load_or_fetch_ohlcv(
            symbol, "5m", DAYS_BACK, _fetch, cache_dir,
            use_cache=use_cache, refresh=refresh_cache,
            retries=cfg.BACKTEST_CACHE_FETCH_RETRIES,
            retry_sleep_sec=cfg.BACKTEST_CACHE_RETRY_SLEEP_SEC,
        )
        df_1h = load_or_fetch_ohlcv(
            symbol, "1h", DAYS_BACK + 10, _fetch, cache_dir,
            use_cache=use_cache, refresh=refresh_cache,
            retries=cfg.BACKTEST_CACHE_FETCH_RETRIES,
            retry_sleep_sec=cfg.BACKTEST_CACHE_RETRY_SLEEP_SEC,
        )
        df_4h = load_or_fetch_ohlcv(
            symbol, "4h", DAYS_BACK + 30, _fetch, cache_dir,
            use_cache=use_cache, refresh=refresh_cache,
            retries=cfg.BACKTEST_CACHE_FETCH_RETRIES,
            retry_sleep_sec=cfg.BACKTEST_CACHE_RETRY_SLEEP_SEC,
        )

        if df_5m is None or df_1h is None or df_4h is None:
            logger.warning("Skipping %s — missing data", symbol)
            continue
        if len(df_5m) < WINDOW_5M + 50:
            logger.warning("Skipping %s — not enough 5m bars (%d)", symbol, len(df_5m))
            continue

        symbol_windows = run_backtest_windows(symbol, df_5m, df_1h, df_4h, n_windows, btc_1h=btc_1h)
        for window_idx, (label, trades) in enumerate(symbol_windows):
            # align windows across symbols by index
            if window_idx >= len(per_window):
                per_window.append((label, []))
            # Keep the first encountered label; dates may differ slightly across symbols
            # but index alignment is what matters for reporting.
            per_window[window_idx][1].extend(trades)
        logger.info("  %s: %d signals across %d windows",
                    symbol, sum(len(t) for _, t in symbol_windows), len(symbol_windows))

    combined: List[TradeResult] = [t for _, trades in per_window for t in trades]

    if n_windows > 1:
        for label, trades in per_window:
            print_stats(label, trades)
        print_stats("COMBINED (all windows)", combined)
        # OOS sanity summary
        print("\n" + "-" * 60)
        print("  WALK-FORWARD SUMMARY")
        print("-" * 60)
        for label, trades in per_window:
            s = _stats_block(trades)
            pf = s["profit_factor"]
            pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
            print(
                f"  {label:<40} n={s['total']:>4}  WR={s['win_rate']:>5.1f}%  "
                f"R={s['total_r']:+7.2f}  PF={pf_str}"
            )
        print("-" * 60)
    else:
        print_stats("ALL", combined)

    _save_results_csv(combined, "logs/backtest_results.csv")


if __name__ == "__main__":
    main()

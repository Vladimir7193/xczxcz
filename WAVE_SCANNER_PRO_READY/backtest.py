from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import ccxt
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from wave_analyzer import is_ranging, analyze_wave_structure, check_correction_complete
from impulse_detector import detect_first_impulse, calculate_entry
from signal_engine import WaveSignalEngine, score_to_label, volume_confirming

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("backtest")

BACKTEST_SYMBOLS = cfg.SYMBOLS + cfg.SYMBOLS_EXTENDED[:4]
DAYS_BACK = 60
WINDOW_5M = 300
WINDOW_1H = 180
WINDOW_4H = 140
STEP_BARS_5M = 12
MIN_SCORE_BT = cfg.MIN_SCORE
RISK_PER_TRADE = cfg.RISK_PER_TRADE_PCT / 100.0
INITIAL_BALANCE = cfg.INITIAL_BALANCE


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
                    active_sl = max(active_sl, entry)
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
                    active_sl = min(active_sl, entry)
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


def run_backtest_symbol(symbol: str, df_5m: pd.DataFrame, df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> List[TradeResult]:
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
        if score < MIN_SCORE_BT or score_to_label(score) == "SKIP":
            continue

        direction = "long" if structure.trend == "up" else "short"
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

        pnl_r = 0.0
        if risk > 0:
            if direction == "long":
                pnl_r = (exit_price - entry_setup.entry_price) / risk
            else:
                pnl_r = (entry_setup.entry_price - exit_price) / risk
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

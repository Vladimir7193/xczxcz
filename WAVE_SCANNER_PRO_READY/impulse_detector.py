from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from wave_analyzer import WaveStructure

logger = logging.getLogger(__name__)


@dataclass
class ImpulseSignal:
    found: bool = False
    impulse_start: float = 0.0
    impulse_end: float = 0.0
    impulse_size: float = 0.0
    comparable: bool = False
    bar_index: int = 0
    start_index: int = 0
    bars_used: int = 0
    breakout: bool = False


@dataclass
class EntrySetup:
    valid: bool = False
    entry_price: float = 0.0
    stop_loss: float = 0.0
    cancel_level: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    rr_ratio: float = 0.0
    direction: str = ""
    atr: float = 0.0
    entry_zone_low: float = 0.0
    entry_zone_high: float = 0.0


def calc_atr(df: pd.DataFrame) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=cfg.ATR_PERIOD, adjust=False).mean()


def check_impulse_comparable(
    df: pd.DataFrame,
    bar_index: int,
    current_size: float,
    direction: str,
    atr: pd.Series,
    lookback: int = 24,
) -> bool:
    start = max(1, bar_index - lookback)
    prev_impulses = []
    for i in range(start, bar_index):
        o = float(df["open"].iloc[i])
        c = float(df["close"].iloc[i])
        h = float(df["high"].iloc[i])
        l = float(df["low"].iloc[i])
        rng = h - l
        if rng <= 0:
            continue
        body = abs(c - o)
        cur_atr = float(atr.iloc[i]) if not pd.isna(atr.iloc[i]) else 0.0
        if cur_atr <= 0:
            continue
        move_atr = body / cur_atr
        body_ratio = body / rng
        if direction == "up" and c > o and body_ratio >= 0.6 and move_atr >= cfg.IMPULSE_MIN_ATR * 0.8:
            prev_impulses.append(move_atr)
        elif direction == "down" and c < o and body_ratio >= 0.6 and move_atr >= cfg.IMPULSE_MIN_ATR * 0.8:
            prev_impulses.append(move_atr)
    if not prev_impulses:
        return True
    avg_prev = float(np.mean(prev_impulses))
    if avg_prev <= 0:
        return True
    ratio = current_size / avg_prev
    return (0.7 - cfg.IMPULSE_EQUALITY_TOL * 0.25) <= ratio <= (1.3 + cfg.IMPULSE_EQUALITY_TOL)


def detect_first_impulse(df_entry: pd.DataFrame, structure: WaveStructure) -> ImpulseSignal:
    result = ImpulseSignal()
    if len(df_entry) < 40:
        return result

    atr = calc_atr(df_entry)
    cur_atr = float(atr.iloc[-1])
    if cur_atr <= 0:
        return result

    direction = structure.trend
    start_scan = max(1, len(df_entry) - (cfg.IMPULSE_MAX_AGE_BARS + cfg.IMPULSE_MAX_BARS + 4))
    if structure.wave_c is not None:
        later = df_entry.index.searchsorted(structure.wave_c.timestamp)
        start_scan = max(start_scan, int(later))

    best = None
    highs = df_entry["high"].to_numpy(dtype=float)
    lows = df_entry["low"].to_numpy(dtype=float)
    opens = df_entry["open"].to_numpy(dtype=float)
    closes = df_entry["close"].to_numpy(dtype=float)

    search_from = max(start_scan + 1, len(df_entry) - cfg.IMPULSE_MAX_AGE_BARS - 1)
    for end in range(search_from, len(df_entry)):
        for bars_used in range(cfg.IMPULSE_MIN_BARS, cfg.IMPULSE_MAX_BARS + 1):
            start = end - bars_used + 1
            if start < start_scan:
                continue

            start_price = float(opens[start])
            end_price = float(closes[end])
            hi = float(highs[start:end + 1].max())
            lo = float(lows[start:end + 1].min())
            move = end_price - start_price
            move_abs = abs(move)
            if move_abs <= 0:
                continue
            if direction == "up" and move <= 0:
                continue
            if direction == "down" and move >= 0:
                continue

            window_atr = float(np.nanmean(atr.iloc[start:end + 1]))
            if window_atr <= 0:
                continue
            move_atr = move_abs / window_atr
            if move_atr < cfg.IMPULSE_MIN_ATR:
                continue

            range_total = hi - lo
            if range_total <= 0:
                continue
            bodies = np.abs(closes[start:end + 1] - opens[start:end + 1])
            body_strength = float(bodies.sum() / range_total)
            if body_strength < 0.65:
                continue

            if direction == "up":
                worst_pullback = max(0.0, (hi - end_price) / move_abs)
                breakout_ref = float(df_entry["high"].iloc[max(0, start - cfg.IMPULSE_BREAKOUT_LOOKBACK):start].max()) if start > 0 else hi
                breakout = hi > breakout_ref
            else:
                worst_pullback = max(0.0, (end_price - lo) / move_abs)
                breakout_ref = float(df_entry["low"].iloc[max(0, start - cfg.IMPULSE_BREAKOUT_LOOKBACK):start].min()) if start > 0 else lo
                breakout = lo < breakout_ref

            if worst_pullback > cfg.IMPULSE_PULLBACK_MAX:
                continue
            # breakout — бонус к score, не hard requirement
            # (для shallow correction ltf_reversal уже подтвердил разворот)

            comparable = check_impulse_comparable(df_entry, end, move_atr, direction, atr)
            score = move_atr + body_strength + (0.35 if comparable else 0.0) + (0.35 if breakout else 0.0)
            candidate = (score, start_price, end_price, move_atr, comparable, end, start, bars_used, breakout)
            if best is None or score > best[0]:
                best = candidate

    if best is None:
        return result

    _, start_price, end_price, move_atr, comparable, end, start, bars_used, breakout = best
    result.found = True
    result.impulse_start = round(start_price, 6)
    result.impulse_end = round(end_price, 6)
    result.impulse_size = round(move_atr, 3)
    result.comparable = comparable
    result.bar_index = end
    result.start_index = start
    result.bars_used = bars_used
    result.breakout = breakout
    return result


def calculate_entry(df_entry: pd.DataFrame, structure: WaveStructure, impulse: ImpulseSignal) -> EntrySetup:
    setup = EntrySetup()
    if not impulse.found:
        return setup
    if structure.impulse_start is None or structure.impulse_end is None:
        return setup

    atr = calc_atr(df_entry)
    cur_atr = float(atr.iloc[-1])
    if cur_atr <= 0:
        return setup

    direction = structure.trend
    imp_start = impulse.impulse_start
    imp_end = impulse.impulse_end
    imp_size = abs(imp_end - imp_start)
    if imp_size <= 0:
        return setup

    if direction == "up":
        zone_high = imp_end - imp_size * cfg.ENTRY_FIB_LOW
        zone_low = imp_end - imp_size * cfg.ENTRY_FIB_HIGH
        entry_price = (zone_low + zone_high) / 2
        corr_low = structure.wave_c.price if structure.wave_c is not None else float(df_entry["low"].iloc[-20:].min())
        raw_stop = corr_low - cur_atr * 0.35 * cfg.SL_ATR_MULT
        min_stop = entry_price - cur_atr * cfg.SL_MIN_ATR_MULT
        stop_loss = min(raw_stop, min_stop)
        cancel_level = min(structure.impulse_start.price, imp_start)
        # wave1_size: кэп сверху (HTF волна не больше MAX_RR*ATR),
        # минимум снизу (достаточно для MIN_RR при TP2_WAVE_MULT)
        htf_wave = abs(structure.impulse_end.price - structure.impulse_start.price)
        min_wave = cur_atr * cfg.SL_MIN_ATR_MULT * cfg.MIN_RR / cfg.TP2_WAVE_MULT
        wave1_size = max(min(htf_wave, cur_atr * cfg.MAX_RR), imp_size, min_wave)
        tp1 = entry_price + wave1_size * cfg.TP1_WAVE_MULT
        tp2 = entry_price + wave1_size * cfg.TP2_WAVE_MULT
        tp3 = structure.spike_level if cfg.TP3_SPIKE_LEVEL and structure.spike_level > tp2 else tp2 * 1.08
    else:
        zone_low = imp_end + imp_size * cfg.ENTRY_FIB_LOW
        zone_high = imp_end + imp_size * cfg.ENTRY_FIB_HIGH
        entry_price = (zone_low + zone_high) / 2
        corr_high = structure.wave_c.price if structure.wave_c is not None else float(df_entry["high"].iloc[-20:].max())
        raw_stop = corr_high + cur_atr * 0.35 * cfg.SL_ATR_MULT
        min_stop = entry_price + cur_atr * cfg.SL_MIN_ATR_MULT
        stop_loss = max(raw_stop, min_stop)
        cancel_level = max(structure.impulse_start.price, imp_start)
        htf_wave = abs(structure.impulse_start.price - structure.impulse_end.price)
        min_wave = cur_atr * cfg.SL_MIN_ATR_MULT * cfg.MIN_RR / cfg.TP2_WAVE_MULT
        wave1_size = max(min(htf_wave, cur_atr * cfg.MAX_RR), imp_size, min_wave)
        tp1 = entry_price - wave1_size * cfg.TP1_WAVE_MULT
        tp2 = entry_price - wave1_size * cfg.TP2_WAVE_MULT
        tp3 = structure.spike_level if cfg.TP3_SPIKE_LEVEL and 0 < structure.spike_level < tp2 else tp2 * 0.92

    risk = abs(entry_price - stop_loss)
    reward = abs(tp2 - entry_price)
    rr = reward / risk if risk > 0 else 0.0
    if rr < cfg.MIN_RR or rr > cfg.MAX_RR:
        return setup

    setup.valid = True
    setup.entry_price = round(entry_price, 6)
    setup.stop_loss = round(stop_loss, 6)
    setup.cancel_level = round(cancel_level, 6)
    setup.tp1 = round(tp1, 6)
    setup.tp2 = round(tp2, 6)
    setup.tp3 = round(tp3, 6)
    setup.rr_ratio = round(rr, 2)
    setup.direction = direction
    setup.atr = round(cur_atr, 6)
    setup.entry_zone_low = round(min(zone_low, zone_high), 6)
    setup.entry_zone_high = round(max(zone_low, zone_high), 6)
    return setup

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg

logger = logging.getLogger(__name__)


@dataclass
class WavePoint:
    index: int
    price: float
    timestamp: pd.Timestamp
    is_high: bool


@dataclass
class WaveStructure:
    trend: str = "ranging"
    wave1_start: Optional[WavePoint] = None
    wave1_end: Optional[WavePoint] = None
    wave2_end: Optional[WavePoint] = None
    wave3_end: Optional[WavePoint] = None
    wave4_end: Optional[WavePoint] = None
    wave5_end: Optional[WavePoint] = None
    wave_a: Optional[WavePoint] = None
    wave_b: Optional[WavePoint] = None
    wave_c: Optional[WavePoint] = None
    impulse_size: float = 0.0
    wave3_size: float = 0.0
    correction_pct: float = 0.0
    fib_level: float = 0.0
    a_equals_c: bool = False
    has_expanding: bool = False
    spike_level: float = 0.0
    correction_type: str = "unknown"
    wave4_valid: bool = False
    has_5waves: bool = False

    @property
    def impulse_start(self) -> Optional[WavePoint]:
        return self.wave1_start

    @property
    def impulse_end(self) -> Optional[WavePoint]:
        return self.wave5_end if self.wave5_end else self.wave3_end


@dataclass
class CorrectionComplete:
    complete: bool = False
    fib_reached: bool = False
    fib_level: float = 0.0
    liquidity_swept: bool = False
    a_equals_c: bool = False
    braking_volume: bool = False
    braking_strength: float = 0.0
    score: float = 0.0
    details: dict = field(default_factory=dict)


def is_ranging(df: pd.DataFrame) -> Tuple[bool, str]:
    if len(df) < cfg.RANGING_LOOKBACK + 5:
        return True, "insufficient_data"
    close = df["close"]
    high = df["high"]
    low = df["low"]
    tr = pd.concat([(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=cfg.ATR_PERIOD, adjust=False).mean()
    cur_atr = float(atr.iloc[-1])
    cur_price = float(close.iloc[-1])
    if cur_price <= 0:
        return True, "invalid_price"
    atr_ratio = cur_atr / cur_price
    low_volatility = atr_ratio < cfg.RANGING_ATR_RATIO
    bb_mid = close.rolling(cfg.BB_PERIOD).mean()
    bb_std = close.rolling(cfg.BB_PERIOD).std()
    bb_upper = bb_mid + cfg.BB_STD * bb_std
    bb_lower = bb_mid - cfg.BB_STD * bb_std
    bb_width = ((bb_upper - bb_lower) / bb_mid).iloc[-1]
    bb_squeeze = bb_width < cfg.RANGING_BB_WIDTH_MIN
    lookback_high = high.iloc[-cfg.RANGING_LOOKBACK:].max()
    lookback_low = low.iloc[-cfg.RANGING_LOOKBACK:].min()
    range_size = (lookback_high - lookback_low) / cur_price
    tight_range = range_size < cfg.RANGING_ATR_RATIO * 3
    ema_fast = close.ewm(span=cfg.EMA_FAST, adjust=False).mean()
    ema_slope = abs(float(ema_fast.iloc[-1]) - float(ema_fast.iloc[-cfg.RANGING_LOOKBACK])) / cur_price
    flat_ema = ema_slope < 0.005
    ranging_signals = sum([low_volatility, bb_squeeze, tight_range, flat_ema])
    if ranging_signals >= cfg.RANGING_MIN_SIGNALS:
        return True, f"signals={ranging_signals} atr={atr_ratio:.3f} bb_width={bb_width:.3f} range={range_size:.3f}"
    return False, "trending"


def find_pivots(df: pd.DataFrame, window: int | None = None) -> Tuple[List[WavePoint], List[WavePoint]]:
    w = cfg.PIVOT_WINDOW if window is None else window
    highs: List[WavePoint] = []
    lows: List[WavePoint] = []
    last_idx = len(df) - 1
    for i in range(w, len(df) - w):
        if df["high"].iloc[i] == df["high"].iloc[i - w:i + w + 1].max():
            highs.append(WavePoint(i, float(df["high"].iloc[i]), df.index[i], True))
        if df["low"].iloc[i] == df["low"].iloc[i - w:i + w + 1].min():
            lows.append(WavePoint(i, float(df["low"].iloc[i]), df.index[i], False))
    if len(df) >= w + 2:
        recent_high = float(df["high"].iloc[-(cfg.STRICT_LAST_PIVOT_BARS + 1):].max())
        recent_low = float(df["low"].iloc[-(cfg.STRICT_LAST_PIVOT_BARS + 1):].min())
        last_high_already = highs and highs[-1].index == last_idx
        last_low_already = lows and lows[-1].index == last_idx
        if not last_high_already and float(df["high"].iloc[last_idx]) >= recent_high:
            highs.append(WavePoint(last_idx, float(df["high"].iloc[last_idx]), df.index[last_idx], True))
        if not last_low_already and float(df["low"].iloc[last_idx]) <= recent_low:
            lows.append(WavePoint(last_idx, float(df["low"].iloc[last_idx]), df.index[last_idx], False))
    return highs, lows


def check_fibonacci(start: float, end: float, current: float, direction: str) -> Tuple[float, bool]:
    impulse = abs(end - start)
    if impulse <= 0:
        return 0.0, False
    retracement = (end - current) / impulse if direction == "up" else (current - end) / impulse
    if retracement < 0 or retracement > 1.0:
        return round(retracement, 3), False
    closest = min(cfg.FIB_LEVELS, key=lambda x: abs(x - retracement))
    reached = abs(retracement - closest) <= cfg.FIB_ZONE_TOLERANCE
    return round(closest, 3), reached


def classify_correction(df: pd.DataFrame, start_idx: int, end_idx: int) -> str:
    if end_idx <= start_idx or end_idx >= len(df):
        return "unknown"
    segment = df.iloc[start_idx:end_idx + 1]
    if len(segment) < 3:
        return "unknown"
    price_range = abs(float(segment["high"].max()) - float(segment["low"].min()))
    tr = pd.concat([
        segment["high"] - segment["low"],
        (segment["high"] - segment["close"].shift()).abs(),
        (segment["low"] - segment["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    avg_atr = float(tr.mean()) if len(tr) else 0.0
    if avg_atr <= 0:
        return "unknown"
    return "sharp" if price_range / avg_atr >= 3.0 and len(segment) <= 8 else "flat"


def find_spike_level(df: pd.DataFrame, direction: str) -> float:
    if len(df) < 20:
        return 0.0
    if direction == "up":
        return float(df["high"].iloc[-20:-1].max())
    return float(df["low"].iloc[-20:-1].min())


def _trend_from_4h(df_trend: pd.DataFrame) -> str:
    if len(df_trend) < 30:
        return "neutral"
    ema50 = df_trend["close"].ewm(span=50, adjust=False).mean()
    ema200 = df_trend["close"].ewm(span=200, adjust=False).mean()
    cur_close = float(df_trend["close"].iloc[-1])
    ema50_now = float(ema50.iloc[-1])
    ema200_now = float(ema200.iloc[-1])
    ema50_prev = float(ema50.iloc[-10])
    slope = (ema50_now - ema50_prev) / ema50_prev if ema50_prev else 0.0
    tol = ema50_now * cfg.TREND_EMA50_TOLERANCE
    slope_tol = cfg.TREND_SLOPE_TOLERANCE
    if cur_close >= ema50_now - tol and ema50_now > ema200_now and slope > -slope_tol:
        return "up"
    if cur_close <= ema50_now + tol and ema50_now < ema200_now and slope < slope_tol:
        return "down"
    # softer fallback when price and fast EMA are on the same side of EMA200
    if cur_close >= ema200_now and ema50_now >= ema200_now and slope > -(slope_tol * 1.5):
        return "up"
    if cur_close <= ema200_now and ema50_now <= ema200_now and slope < (slope_tol * 1.5):
        return "down"
    return "neutral"


def _simple_directional_structure(df_htf: pd.DataFrame, direction: str, atr_now: float) -> Optional[WaveStructure]:
    if len(df_htf) < 30 or atr_now <= 0:
        return None
    window = min(cfg.WAVE_LOOKBACK, len(df_htf) - 1)
    seg = df_htf.iloc[-window:]
    if direction == "up":
        start_ts = seg["low"].idxmin()
        start_idx = int(seg.index.get_loc(start_ts))
        after = seg.iloc[start_idx:]
        if len(after) < 10:
            return None
        peak_ts = after["high"].idxmax()
        peak_local = int(after.index.get_loc(peak_ts))
        peak_idx = start_idx + peak_local
        if peak_idx <= start_idx + 3:
            return None
        start_price = float(seg["low"].iloc[start_idx])
        peak_price = float(seg["high"].iloc[peak_idx])
        impulse = peak_price - start_price
        if impulse <= 0 or impulse / atr_now < cfg.WAVE_MIN_IMPULSE_ATR:
            return None
        corr_seg = seg.iloc[peak_idx:]
        corr_price = float(corr_seg["low"].min())
        corr_low_local = int(corr_seg["low"].values.argmin())
        corr_pct = (peak_price - corr_price) / impulse if impulse else 0.0
        if not 0.2 <= corr_pct <= 0.9:
            return None
        abs_start = len(df_htf) - len(seg) + start_idx
        abs_peak = len(df_htf) - len(seg) + peak_idx
        abs_corr = len(df_htf) - len(seg) + peak_idx + corr_low_local
        w1 = WavePoint(abs_start, start_price, seg.index[start_idx], False)
        w5 = WavePoint(abs_peak, peak_price, seg.index[peak_idx], True)
        wc = WavePoint(abs_corr, corr_price, corr_seg.index[corr_low_local], False)
        # ABC: A = peak→corr_low, B = corr_low→bounce_high, C = bounce_high→current
        bounce_high = float(corr_seg.iloc[corr_low_local:]["high"].max()) if corr_low_local < len(corr_seg) - 1 else peak_price
        wave_a_size = peak_price - corr_price
        wave_c_size = bounce_high - float(df_htf["close"].iloc[-1])
        a_equals_c = wave_a_size > 0 and wave_c_size > 0 and abs(wave_c_size - wave_a_size) / wave_a_size <= cfg.WAVE_EQUALITY_TOLERANCE
    else:
        start_ts = seg["high"].idxmax()
        start_idx = int(seg.index.get_loc(start_ts))
        after = seg.iloc[start_idx:]
        if len(after) < 10:
            return None
        trough_ts = after["low"].idxmin()
        trough_local = int(after.index.get_loc(trough_ts))
        peak_idx = start_idx + trough_local
        if peak_idx <= start_idx + 3:
            return None
        start_price = float(seg["high"].iloc[start_idx])
        peak_price = float(seg["low"].iloc[peak_idx])
        impulse = start_price - peak_price
        if impulse <= 0 or impulse / atr_now < cfg.WAVE_MIN_IMPULSE_ATR:
            return None
        corr_seg = seg.iloc[peak_idx:]
        corr_price = float(corr_seg["high"].max())
        corr_high_local = int(corr_seg["high"].values.argmax())
        corr_pct = (corr_price - peak_price) / impulse if impulse else 0.0
        if not 0.2 <= corr_pct <= 0.9:
            return None
        abs_start = len(df_htf) - len(seg) + start_idx
        abs_peak = len(df_htf) - len(seg) + peak_idx
        abs_corr = len(df_htf) - len(seg) + peak_idx + corr_high_local
        w1 = WavePoint(abs_start, start_price, seg.index[start_idx], True)
        w5 = WavePoint(abs_peak, peak_price, seg.index[peak_idx], False)
        wc = WavePoint(abs_corr, corr_price, corr_seg.index[corr_high_local], True)
        # ABC: A = trough→corr_high, B = corr_high→bounce_low, C = bounce_low→current
        bounce_low = float(corr_seg.iloc[corr_high_local:]["low"].min()) if corr_high_local < len(corr_seg) - 1 else peak_price
        wave_a_size = corr_price - peak_price
        wave_c_size = float(df_htf["close"].iloc[-1]) - bounce_low
        a_equals_c = wave_a_size > 0 and wave_c_size > 0 and abs(wave_c_size - wave_a_size) / wave_a_size <= cfg.WAVE_EQUALITY_TOLERANCE

    fib_level, _ = check_fibonacci(w1.price, w5.price, wc.price, direction)
    return WaveStructure(
        trend=direction,
        wave1_start=w1,
        wave5_end=w5,
        wave_c=wc,
        impulse_size=round(impulse / atr_now, 3),
        wave3_size=round(impulse / atr_now, 3),
        correction_pct=round(corr_pct, 3),
        fib_level=fib_level,
        a_equals_c=a_equals_c,
        has_expanding=False,
        spike_level=find_spike_level(df_htf, direction),
        correction_type=classify_correction(df_htf, w5.index, len(df_htf) - 1),
        wave4_valid=True,
        has_5waves=False,
    )


def analyze_wave_structure(df_htf: pd.DataFrame, df_trend: pd.DataFrame) -> WaveStructure:
    structure = WaveStructure(trend="ranging")
    if len(df_htf) < 50:
        return structure
    tr = pd.concat([
        df_htf["high"] - df_htf["low"],
        (df_htf["high"] - df_htf["close"].shift()).abs(),
        (df_htf["low"] - df_htf["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=cfg.ATR_PERIOD, adjust=False).mean()
    cur_atr = float(atr.iloc[-1])
    ematrend = _trend_from_4h(df_trend)
    if ematrend not in ("up", "down"):
        return structure
    candidate = _simple_directional_structure(df_htf, ematrend, cur_atr)
    return candidate if candidate is not None else structure


def _check_braking_volume(df: pd.DataFrame, direction: str, vol_ma: pd.Series, atr: pd.Series, lookback: int = 5) -> Tuple[bool, float]:
    if len(df) < lookback + 3:
        return False, 0.0
    for i in range(len(df) - 1, max(len(df) - lookback - 1, 0), -1):
        cur_vol = float(df["volume"].iloc[i])
        cur_vol_ma = float(vol_ma.iloc[i]) if not pd.isna(vol_ma.iloc[i]) else 0.0
        if cur_vol_ma <= 0:
            continue
        vol_spike = cur_vol / cur_vol_ma
        if vol_spike < cfg.BRAKING_VOL_SPIKE:
            continue
        cur_low = float(df["low"].iloc[i])
        cur_high = float(df["high"].iloc[i])
        cur_close = float(df["close"].iloc[i])
        bar_range = cur_high - cur_low
        if bar_range <= 0:
            continue
        absorption = (cur_close - cur_low) / bar_range if direction == "up" else (cur_high - cur_close) / bar_range
        if absorption < cfg.BRAKING_ABSORPTION:
            continue
        hold_count = 0
        for j in range(i + 1, min(i + 4, len(df))):
            if direction == "up":
                if float(df["low"].iloc[j]) >= cur_low * 0.999:
                    hold_count += 1
            else:
                if float(df["high"].iloc[j]) <= cur_high * 1.001:
                    hold_count += 1
        if hold_count < 1:
            continue
        strength = min(1.0, (vol_spike / 5.0) * 0.4 + absorption * 0.4 + (hold_count / 3.0) * 0.2)
        return True, round(strength, 2)
    return False, 0.0


def check_correction_complete(df_entry: pd.DataFrame, structure: WaveStructure) -> CorrectionComplete:
    result = CorrectionComplete()
    impstart = structure.impulse_start
    impend = structure.impulse_end
    if impstart is None or impend is None:
        return result
    curprice = float(df_entry["close"].iloc[-1])
    impulsesize = abs(impend.price - impstart.price)
    if impulsesize <= 0:
        return result
    lookback_entry = min(30, len(df_entry))
    if structure.trend == "up":
        correction_price = float(df_entry["low"].iloc[-lookback_entry:].min())
    else:
        correction_price = float(df_entry["high"].iloc[-lookback_entry:].max())
    if structure.wave_c is not None:
        if structure.trend == "up":
            correction_price = min(correction_price, structure.wave_c.price)
        else:
            correction_price = max(correction_price, structure.wave_c.price)
    retracement = (impend.price - correction_price) / impulsesize if structure.trend == "up" else (correction_price - impend.price) / impulsesize
    fibvalid = 0.2 <= retracement <= 0.9

    # Синхронизировано с cfg.FIB_LEVELS — нет захардкоженных уровней
    closest_level = min(cfg.FIB_LEVELS, key=lambda x: abs(x - retracement))
    fibreached = fibvalid and abs(retracement - closest_level) <= cfg.FIB_ZONE_TOLERANCE
    fiblevel = closest_level if fibreached else round(retracement, 3)

    # Скоринг по уровням (глубина коррекции)
    if fibreached:
        if closest_level >= 0.618:
            fib_score = 35.0
        elif closest_level >= 0.500:
            fib_score = 25.0
        elif closest_level >= 0.382:
            fib_score = 20.0   # было 15 — shallow continuation тоже торгуем
        else:  # 0.236
            fib_score = 15.0   # было 8 — shallow retracement в тренде
    else:
        fib_score = 0.0

    result.fib_reached = fibreached
    result.fib_level = round(fiblevel, 3)
    lookback = min(cfg.SWEEP_LOOKBACK_BARS, len(df_entry) - 5)
    if lookback > 5:
        recentlow = float(df_entry["low"].iloc[-lookback - 5:-5].min())
        recenthigh = float(df_entry["high"].iloc[-lookback - 5:-5].max())
    else:
        recentlow = float(df_entry["low"].iloc[:-1].min())
        recenthigh = float(df_entry["high"].iloc[:-1].max())
    last5low = float(df_entry["low"].iloc[-5:].min())
    last5high = float(df_entry["high"].iloc[-5:].max())
    swept = last5low <= recentlow if structure.trend == "up" else last5high >= recenthigh
    result.liquidity_swept = swept
    result.a_equals_c = structure.a_equals_c
    vol_ma = df_entry["volume"].rolling(cfg.VOLUME_MA_PERIOD).mean()
    tr = pd.concat([
        df_entry["high"] - df_entry["low"],
        (df_entry["high"] - df_entry["close"].shift()).abs(),
        (df_entry["low"] - df_entry["close"].shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=cfg.ATR_PERIOD, adjust=False).mean()
    cur_atr_entry = float(atr.iloc[-1]) if len(atr) > 0 else 0.0
    braking, strength = _check_braking_volume(df_entry, structure.trend, vol_ma, atr)
    result.braking_volume = braking
    result.braking_strength = strength
    score = 0.0
    score += fib_score
    if swept:
        score += 25.0
    if structure.a_equals_c:
        score += 20.0
    if braking:
        score += strength * 20.0
    result.score = min(100.0, round(score, 1))
    other = sum([swept, structure.a_equals_c, braking])
    standard_complete = (fibreached and other >= 1) or (other >= 3)
    lenient_complete = fibreached and closest_level >= 0.5
    result.complete = standard_complete  # уточним после ltf

    # --- LTF reversal check (softened) ---
    ltf_window = 5
    if len(df_entry) >= ltf_window + 1:
        ltf_bars   = df_entry.iloc[-ltf_window:]
        opens_ltf  = ltf_bars["open"].values
        closes_ltf = ltf_bars["close"].values
        highs_ltf  = ltf_bars["high"].values
        lows_ltf   = ltf_bars["low"].values
        if structure.trend == "up":
            bars_in_dir = int((closes_ltf > opens_ltf).sum())
            net_move    = float(closes_ltf[-1] - opens_ltf[0])
        else:
            bars_in_dir = int((closes_ltf < opens_ltf).sum())
            net_move    = float(opens_ltf[0] - closes_ltf[-1])
        bars_ratio      = bars_in_dir / ltf_window

        # Check for strong single reversal candle (engulfing / hammer)
        last_body = abs(float(closes_ltf[-1]) - float(opens_ltf[-1]))
        last_range = float(highs_ltf[-1]) - float(lows_ltf[-1])
        strong_candle = last_range > 0 and last_body / last_range >= 0.6
        if structure.trend == "up":
            strong_candle = strong_candle and closes_ltf[-1] > opens_ltf[-1]
        else:
            strong_candle = strong_candle and closes_ltf[-1] < opens_ltf[-1]

        # Accept: 3/5 bars + net OR 2/5 bars + net + strong last candle OR strong candle alone
        ltf_reversal_ok = (
            (net_move > 0 and bars_in_dir >= 3)
            or (net_move > 0 and bars_in_dir >= 2 and strong_candle)
            or (strong_candle and last_body > 0 and cur_atr_entry > 0 and last_body / cur_atr_entry >= 0.5)
        )
    else:
        bars_in_dir     = 0
        net_move        = 0.0
        bars_ratio      = 0.0
        ltf_reversal_ok = False

    # Standard path: fibreached + confirmation + LTF reversal
    # Aggressive path: shallow (0.236/0.382) + sweep + LTF reversal
    shallow_complete = (
        fibvalid
        and closest_level <= 0.382
        and swept
        and ltf_reversal_ok
    )
    strong_completion = standard_complete or lenient_complete
    result.complete = (strong_completion and (ltf_reversal_ok or strength >= 0.6 or swept)) or shallow_complete

    result.details = {
        "retracement": round(retracement, 3),
        "fib_level": fiblevel,
        "fib_valid": fibvalid,
        "liquidity_swept": swept,
        "a_equals_c": structure.a_equals_c,
        "braking_volume": braking,
        "braking_strength": round(strength, 2),
        "score": result.score,
        "current_price": curprice,
        "ltf_reversal_ok": ltf_reversal_ok,
        "ltf_bars_in_dir": bars_in_dir,
        "ltf_bars_ratio": round(bars_ratio, 2),
        "ltf_net_move": round(net_move, 6),
    }
    return result

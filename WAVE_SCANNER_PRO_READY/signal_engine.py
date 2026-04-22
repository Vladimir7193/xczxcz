from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List

import pandas as pd

import sys, os
from pathlib import Path
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from wave_analyzer import WaveStructure, CorrectionComplete, is_ranging, analyze_wave_structure, check_correction_complete
from impulse_detector import ImpulseSignal, EntrySetup, detect_first_impulse, calculate_entry

_MODULE_DIR = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


@dataclass
class WaveSignal:
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    cancel_level: float
    tp1: float
    tp2: float
    tp3: float
    rr_ratio: float
    score: float
    label: str
    session: str
    timestamp: pd.Timestamp
    atr: float
    structure: WaveStructure = field(default_factory=WaveStructure)
    correction: CorrectionComplete = field(default_factory=CorrectionComplete)
    impulse: ImpulseSignal = field(default_factory=ImpulseSignal)
    correction_type_next: str = ""

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "cancel_level": self.cancel_level,
            "tp1": self.tp1,
            "tp2": self.tp2,
            "tp3": self.tp3,
            "rr_ratio": self.rr_ratio,
            "score": self.score,
            "label": self.label,
            "session": self.session,
            "timestamp": str(self.timestamp),
            "atr": self.atr,
            "fib_level": self.correction.fib_level,
            "liquidity_swept": self.correction.liquidity_swept,
            "a_equals_c": self.correction.a_equals_c,
            "braking_volume": self.correction.braking_volume,
            "has_expanding": self.structure.has_expanding,
            "correction_type": self.structure.correction_type,
            "correction_type_next": self.correction_type_next,
        }


def score_to_label(score: float) -> str:
    if score >= 85:
        return "STRONG"
    if score >= 70:
        return "GOOD"
    if score >= 55:
        return "WEAK"
    return "SKIP"


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


def predict_next_correction(current_type: str) -> str:
    if current_type == "sharp":
        return "flat"
    if current_type == "flat":
        return "sharp"
    return "unknown"


_btc_cache: Dict[str, object] = {"df": None, "ts": 0.0}
_btc_cache_lock = Lock()


def btc_is_falling() -> bool:
    try:
        with _btc_cache_lock:
            df_1h = _btc_cache.get("df")
            ts = float(_btc_cache.get("ts", 0.0) or 0.0)
        if df_1h is None or (time.time() - ts) > 300:
            return False
        lookback = max(2, int(cfg.BTC_FILTER_LOOKBACK_BARS))
        if len(df_1h) < lookback + 2:
            return False
        closes = df_1h["close"].astype(float)
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
        return drop_pct <= -abs(cfg.BTC_FILTER_DROP_PCT) and candle_losses >= max(2, lookback - 1) and below_ema
    except Exception:
        return False


def update_btc_cache(data: Dict[str, pd.DataFrame]) -> None:
    with _btc_cache_lock:
        _btc_cache["df"] = data.get(cfg.TF_HTF)
        _btc_cache["ts"] = time.time()


class SignalCooldown:
    FILE = str(_MODULE_DIR / "logs" / "cooldown.json")

    def __init__(self):
        self.history: Dict[str, float] = {}
        self.structures: Dict[str, float] = {}
        self._lock = Lock()
        self.load()

    def can_fire(self, symbol: str, direction: str, impulse_start: float = 0.0) -> bool:
        key = f"{symbol}:{direction}"
        with self._lock:
            time_ok = (time.time() - self.history.get(key, 0.0)) >= cfg.SIGNAL_COOLDOWN_SEC
            if not time_ok:
                return False
            if impulse_start > 0:
                prev = self.structures.get(key, 0.0)
                if prev > 0 and abs(impulse_start - prev) / impulse_start < 0.001:
                    return False
        return True

    def record(self, symbol: str, direction: str, impulse_start: float = 0.0) -> None:
        key = f"{symbol}:{direction}"
        with self._lock:
            self.history[key] = time.time()
            if impulse_start > 0:
                self.structures[key] = impulse_start
        self.save()

    def save(self) -> None:
        import json
        try:
            os.makedirs(os.path.dirname(self.FILE), exist_ok=True)
            with self._lock:
                payload = {"history": dict(self.history), "structures": dict(self.structures)}
            tmp_path = self.FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp_path, self.FILE)
        except OSError as e:
            logger.warning(f"SignalCooldown.save failed: {e}")
        except Exception as e:
            logger.error(f"SignalCooldown.save unexpected error: {e}", exc_info=True)

    def load(self) -> None:
        import json
        if not os.path.exists(self.FILE):
            return
        try:
            with open(self.FILE, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "history" in data:
                self.history = data.get("history", {}) or {}
                self.structures = data.get("structures", {}) or {}
            elif isinstance(data, dict):
                # legacy flat format: {key: ts}
                self.history = data
                self.structures = {}
            else:
                logger.warning("SignalCooldown.load: unexpected format, resetting")
                self.history = {}
                self.structures = {}
        except (OSError, ValueError) as e:
            logger.warning(f"SignalCooldown.load failed ({e}), starting with empty state")
            self.history = {}
            self.structures = {}


class WaveSignalEngine:
    def __init__(self):
        self.cooldown = SignalCooldown()
        self._reject_stats: Counter[str] = Counter()
        self._stats_lock = Lock()

    def _bump(self, reason: str) -> None:
        with self._stats_lock:
            self._reject_stats[reason] += 1

    def consume_reject_stats(self) -> Dict[str, int]:
        with self._stats_lock:
            data = dict(self._reject_stats)
            self._reject_stats.clear()
        return data

    def calculate_final_score(
        self,
        correction: CorrectionComplete,
        impulse: ImpulseSignal,
        entry: EntrySetup,
        structure: WaveStructure,
        df_entry: pd.DataFrame,
    ) -> float:
        score = correction.score
        if impulse.comparable:
            score += 5.0
        if impulse.breakout:
            score += 5.0
        session = get_session(df_entry.index[-1])
        if session in cfg.BEST_SESSIONS:
            score += 10.0
        elif session in cfg.SKIP_SESSIONS:
            score *= 0.6
        if entry.rr_ratio >= 2.5:
            score += 5.0
        if impulse.impulse_size >= 1.5:
            score += 5.0
        if structure.has_expanding:
            score -= 15.0
        if structure.correction_pct > 0.618:
            score *= 0.85
        if "rsi" in df_entry.columns:
            rsi = float(df_entry["rsi"].iloc[-1])
            if structure.trend == "up" and rsi <= 40:
                score += 5.0
            elif structure.trend == "down" and rsi >= 60:
                score += 5.0
        return min(100.0, max(0.0, round(score, 1)))

    def process(self, symbol: str, data: Dict[str, pd.DataFrame]) -> List[WaveSignal]:
        df_entry = data.get(cfg.TF_ENTRY)
        df_htf = data.get(cfg.TF_HTF)
        df_trend = data.get(cfg.TF_TREND)
        if df_entry is None or df_htf is None or df_trend is None:
            self._bump("missing_tf")
            return []
        if len(df_entry) < 50 or len(df_htf) < 50:
            self._bump("not_enough_bars")
            return []

        if symbol == "BTCUSDT":
            update_btc_cache(data)

        ranging, _ = is_ranging(df_htf)
        if ranging:
            self._bump("ranging")
            return []

        structure = analyze_wave_structure(df_htf, df_trend)
        if structure.trend not in ("up", "down"):
            self._bump("no_trend")
            return []
        if structure.impulse_start is None:
            self._bump("no_htf_impulse")
            return []
        correction = check_correction_complete(df_entry, structure)
        if not correction.complete:
            self._bump("correction_incomplete")
            return []
        if structure.correction_pct > 0.786:
            self._bump("correction_too_deep")
            return []
        impulse = detect_first_impulse(df_entry, structure)
        if not impulse.found:
            self._bump("no_ltf_impulse")
            return []
        bars_since = len(df_entry) - 1 - impulse.bar_index
        if bars_since > cfg.IMPULSE_MAX_AGE_BARS:
            self._bump("impulse_too_old")
            return []
        entry = calculate_entry(df_entry, structure, impulse)
        if not entry.valid:
            self._bump("entry_invalid")
            return []
        risk = abs(entry.entry_price - entry.stop_loss)
        if risk < entry.atr * cfg.SL_MIN_ATR_MULT:
            self._bump("risk_too_small")
            return []
        if entry.rr_ratio < cfg.MIN_RR or entry.rr_ratio > cfg.MAX_RR:
            self._bump("rr_out_of_range")
            return []
        direction_str = "long" if structure.trend == "up" else "short"
        imp_start_price = structure.impulse_start.price if structure.impulse_start else 0.0
        if not self.cooldown.can_fire(symbol, direction_str, imp_start_price):
            self._bump("cooldown")
            return []
        score = self.calculate_final_score(correction, impulse, entry, structure, df_entry)
        if score < cfg.MIN_SCORE:
            self._bump("score_too_low")
            return []
        if score > cfg.MAX_SCORE:
            # Top-end scores empirically under-perform (score >= 86 in
            # the 103-trade deterministic snapshot hit WR 4.8%).
            self._bump("score_too_high")
            return []
        label = score_to_label(score)
        if label == "SKIP":
            self._bump("label_skip")
            return []
        cur_price = float(df_entry["close"].iloc[-1])
        if direction_str == "long" and cur_price < entry.cancel_level:
            self._bump("below_cancel")
            return []
        if direction_str == "short" and cur_price > entry.cancel_level:
            self._bump("above_cancel")
            return []
        entry_distance_atr = abs(cur_price - entry.entry_price) / entry.atr if entry.atr > 0 else 0.0
        if entry_distance_atr > cfg.MAX_ENTRY_DISTANCE_ATR:
            self._bump("entry_too_far")
            return []
        if cfg.BTC_FILTER_ENABLED and symbol != "BTCUSDT" and direction_str == "long" and btc_is_falling():
            self._bump("btc_filter")
            return []
        if cfg.VOLUME_CONFIRMATION_REQUIRED and not volume_confirming(df_entry):
            self._bump("volume_fail")
            return []
        if structure.correction_type == "unknown" and not structure.has_5waves:
            self._bump("unknown_structure")
            return []
        sig = WaveSignal(
            symbol=symbol,
            direction=direction_str,
            entry_price=entry.entry_price,
            stop_loss=entry.stop_loss,
            cancel_level=entry.cancel_level,
            tp1=entry.tp1,
            tp2=entry.tp2,
            tp3=entry.tp3,
            rr_ratio=entry.rr_ratio,
            score=score,
            label=label,
            session=get_session(df_entry.index[-1]),
            timestamp=df_entry.index[-1],
            atr=entry.atr,
            structure=structure,
            correction=correction,
            impulse=impulse,
            correction_type_next=predict_next_correction(structure.correction_type),
        )
        self.cooldown.record(symbol, direction_str, imp_start_price)
        self._bump("accepted")
        return [sig]


def volume_confirming(df_entry: pd.DataFrame) -> bool:
    if len(df_entry) < 20:
        return True
    vol = df_entry["volume"].iloc[-3:].values
    vol_ma = float(df_entry["volume"].iloc[-20:].mean())
    if vol_ma <= 0:
        return True
    return sum(1 for v in vol if v >= vol_ma * 0.8) >= 2

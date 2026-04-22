# ============================================================
#  logger.py — Логирование сигналов в CSV
# ============================================================
from __future__ import annotations

import csv
import json
import logging
import logging.handlers
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import sys
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from signal_engine import WaveSignal

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    Path("logs").mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.LOG_LEVEL, logging.INFO))

    if getattr(setup_logging, "_configured", False):
        return

    # Консоль
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Файл с ротацией
    fh = logging.handlers.RotatingFileHandler(
        cfg.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)
    setup_logging._configured = True


def log_signal(sig: WaveSignal) -> None:
    Path("logs").mkdir(exist_ok=True)
    file_exists = Path(cfg.SIGNALS_CSV).exists()

    row = {
        "timestamp":      str(sig.timestamp),
        "symbol":         sig.symbol,
        "direction":      sig.direction,
        "score":          sig.score,
        "label":          sig.label,
        "entry_price":    sig.entry_price,
        "stop_loss":      sig.stop_loss,
        "cancel_level":   sig.cancel_level,
        "tp1":            sig.tp1,
        "tp2":            sig.tp2,
        "tp3":            sig.tp3,
        "rr_ratio":       sig.rr_ratio,
        "session":        sig.session,
        "fib_level":      sig.correction.fib_level,
        "fib_reached":    sig.correction.fib_reached,
        "liquidity_swept": sig.correction.liquidity_swept,
        "a_equals_c":     sig.correction.a_equals_c,
        "braking_volume": sig.correction.braking_volume,
        "braking_strength": sig.correction.braking_strength,
        "has_expanding":  sig.structure.has_expanding,
        "correction_type": sig.structure.correction_type,
        "next_correction": sig.correction_type_next,
        "atr":            sig.atr,
    }

    with open(cfg.SIGNALS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

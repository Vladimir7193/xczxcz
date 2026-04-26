#!/usr/bin/env python3
# ============================================================
#  WAVE_SCANNER_PRO — main.py
#  Сканер по волновой стратегии Игоря
#
#  Шаг 0: Фильтр боковика
#  Шаг 1: Структура 1h/4h (импульс + ABC)
#  Шаг 2: Завершение коррекции (Фибо + ликвидность + A=C + объём)
#  Шаг 3: Первый импульс на 5m
#  Шаг 4: Вход 50-61.8%, стоп, отмена
#  Шаг 5: Цели (TP1/TP2/TP3 + шип)
# ============================================================
from __future__ import annotations

import gc
import logging
import os
import signal as signal_module
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List

sys.path.insert(0, os.path.dirname(__file__))

import config as cfg
cfg.validate_runtime_config()
from logger import setup_logging, log_signal
from data_fetcher import fetch_multi_tf, cache_stats
from signal_engine import WaveSignalEngine, WaveSignal, update_btc_cache
from telegram_notify import send_wave_signal, send_status, send_error, send_daily_report

try:
    import psutil  # type: ignore
    _PROC = psutil.Process(os.getpid())
except ImportError:
    psutil = None  # type: ignore
    _PROC = None


def _log_memory(cycle: int) -> None:
    every = int(getattr(cfg, "MEM_LOG_EVERY_N_CYCLES", 0) or 0)
    if every <= 0 or cycle % every != 0:
        return
    stats = cache_stats()
    rss_mb: float = -1.0
    if _PROC is not None:
        try:
            rss_mb = _PROC.memory_info().rss / (1024 * 1024)
        except Exception:
            rss_mb = -1.0
    rss_str = f"rss={rss_mb:.0f}MB" if rss_mb >= 0 else "rss=psutil-not-installed"
    logger.info(
        "MEM: %s cache_entries=%d/%d cache_bytes=%dKB ttl=%ds dtype=%s",
        rss_str,
        stats["entries"], stats["max_entries"],
        stats["approx_bytes"] // 1024,
        stats["ttl_sec"],
        getattr(cfg, "OHLCV_DTYPE", "float64"),
    )

setup_logging()
logger = logging.getLogger("main")

engine  = WaveSignalEngine()
_running = True

# Статистика дня
_stats = {"signals": 0, "strong": 0, "good": 0, "weak": 0, "cycles": 0}
_start_time = datetime.now(timezone.utc)

# Коррелированные группы — из каждой берём только лучший сигнал
CORR_GROUPS = [
    {"BTCUSDT", "ETHUSDT"},
    {"SOLUSDT", "AVAXUSDT"},
    {"ARBUSDT", "OPUSDT"},
    {"INJUSDT", "SUIUSDT", "APTUSDT"},
]


def _handle_stop(signum, frame):
    global _running
    logger.info("Stop signal received")
    _running = False


signal_module.signal(signal_module.SIGINT,  _handle_stop)
signal_module.signal(signal_module.SIGTERM, _handle_stop)


_error_counts: Dict[str, int] = {}

def scan_symbol(symbol: str) -> List[WaveSignal]:
    try:
        data = fetch_multi_tf(symbol)
        if not data:
            _error_counts[symbol] = _error_counts.get(symbol, 0) + 1
            if _error_counts[symbol] >= 3:
                logger.warning(f"⚠️ {symbol}: 3+ consecutive fetch errors")
            return []
        _error_counts[symbol] = 0  # сброс счётчика при успехе
        return engine.process(symbol, data)
    except Exception as e:
        logger.error(f"Error scanning {symbol}: {e}", exc_info=True)
        _error_counts[symbol] = _error_counts.get(symbol, 0) + 1
        return []


def process_signal(sig: WaveSignal) -> None:
    log_signal(sig)
    send_wave_signal(sig)

    _stats["signals"] += 1
    if "STRONG" in sig.label:   _stats["strong"] += 1
    elif "GOOD" in sig.label:   _stats["good"]   += 1
    else:                        _stats["weak"]   += 1

    logger.info(
        f"✅ SIGNAL: {sig.symbol} {sig.direction.upper()} "
        f"score={sig.score:.0f} rr={sig.rr_ratio:.2f} "
        f"entry={sig.entry_price:.4f} sl={sig.stop_loss:.4f}"
    )


def scan_symbols(symbols: list) -> List[WaveSignal]:
    """Сканирует список символов параллельно (4 потока), возвращает все найденные сигналы."""
    all_signals: List[WaveSignal] = []
    with ThreadPoolExecutor(max_workers=cfg.SCAN_WORKERS) as pool:
        futures = {pool.submit(scan_symbol, sym): sym for sym in symbols if _running}
        for future in as_completed(futures):
            if not _running:
                break
            try:
                all_signals.extend(future.result())
            except Exception as e:
                logger.error(f"Thread error for {futures[future]}: {e}")
    return all_signals


def filter_correlated(signals: List[WaveSignal]) -> List[WaveSignal]:
    """Из каждой коррелированной группы берём только лучший сигнал."""
    signals.sort(key=lambda s: s.score, reverse=True)
    filtered: List[WaveSignal] = []
    used_groups: set = set()
    for sig in signals:
        group_id = None
        for idx, group in enumerate(CORR_GROUPS):
            if sig.symbol in group:
                group_id = idx
                break
        if group_id is not None:
            if group_id in used_groups:
                logger.info(f"Skip {sig.symbol} (correlated group)")
                continue
            used_groups.add(group_id)
        filtered.append(sig)
    return filtered


def _prefetch_btc() -> None:
    """Обновляет BTC-кеш синхронно до параллельного сканирования, чтобы BTC-фильтр
    в signal_engine работал детерминированно для всех символов."""
    if not cfg.BTC_FILTER_ENABLED:
        return
    try:
        data = fetch_multi_tf("BTCUSDT")
        if data:
            update_btc_cache(data)
        else:
            logger.warning("BTC prefetch: fetch_multi_tf returned no data; filter will be a no-op this cycle")
    except Exception as e:
        logger.error(f"BTC prefetch error: {e}")


def run_cycle() -> int:
    # ── Шаг 0: обновляем BTC-кеш до пула ─────────────────────
    _prefetch_btc()

    # ── Шаг 1: сканируем основные 20 пар ─────────────────────
    logger.info(f"Scanning {len(cfg.SYMBOLS)} main symbols...")
    signals = scan_symbols(cfg.SYMBOLS)

    # ── Шаг 2: нет сигналов — подключаем расширенные 10 пар ──
    if not signals and cfg.SYMBOLS_EXTENDED:
        logger.info(
            f"No signals in main symbols. "
            f"Expanding to +{min(len(cfg.SYMBOLS_EXTENDED), max(0, cfg.EXTENDED_SYMBOLS_LIMIT))} pairs..."
        )
        extra = scan_symbols(cfg.SYMBOLS_EXTENDED[:max(0, cfg.EXTENDED_SYMBOLS_LIMIT)])
        signals.extend(extra)
        if signals:
            logger.info(f"Found {len(signals)} signals in extended pairs")
        else:
            logger.info("No signals in extended pairs either")

    # ── Шаг 3: фильтр корреляции и отправка ──────────────────
    filtered = filter_correlated(signals)
    for sig in filtered:
        process_signal(sig)

    if cfg.LOG_REJECT_SUMMARY_EVERY_CYCLE:
        reject_stats = engine.consume_reject_stats()
        if reject_stats:
            ordered = sorted(reject_stats.items(), key=lambda kv: (-kv[1], kv[0]))
            logger.info("Reject stats: " + ", ".join(f"{k}={v}" for k, v in ordered[:12]))
    return len(filtered)


def main() -> None:
    logger.info("=" * 60)
    logger.info("  WAVE_SCANNER_PRO v1.0 — Starting")
    logger.info(f"  Symbols: {len(cfg.SYMBOLS)}")
    logger.info(f"  Score band: {cfg.MIN_SCORE}..{cfg.MAX_SCORE}")
    logger.info(f"  RR range: {cfg.MIN_RR}..{cfg.MAX_RR}")
    logger.info(f"  Max entry distance ATR: {cfg.MAX_ENTRY_DISTANCE_ATR}")
    logger.info(f"  Volume confirm required: {cfg.VOLUME_CONFIRMATION_REQUIRED}")
    logger.info(f"  BTC long filter enabled: {cfg.BTC_FILTER_ENABLED}")
    logger.info(f"  Scan interval: {cfg.SCAN_INTERVAL_SEC}s")
    logger.info("=" * 60)

    send_status(
        f"🌊 WAVE SCANNER PRO запущен\n"
        f"📊 Пар: {len(cfg.SYMBOLS)}\n"
        f"🎯 Скор: {cfg.MIN_SCORE}..{cfg.MAX_SCORE}\n"
        f"⏱ Интервал: {cfg.SCAN_INTERVAL_SEC}s\n\n"
        f"Стратегия Игоря:\n"
        f"0️⃣ Фильтр боковика\n"
        f"1️⃣ Структура 1h/4h\n"
        f"2️⃣ Завершение коррекции\n"
        f"3️⃣ Импульс на 5m\n"
        f"4️⃣ Вход 50-61.8%\n"
        f"5️⃣ Цели TP1/TP2/TP3"
    )

    last_report_date = datetime.now(timezone.utc).date()
    cycle_count = 0

    while _running:
        cycle_start = time.time()
        cycle_count += 1
        _stats["cycles"] = cycle_count

        try:
            n = run_cycle()
            elapsed = time.time() - cycle_start
            logger.info(f"Cycle #{cycle_count} done in {elapsed:.1f}s — {n} signals")

            # Reclaim transient pandas/numpy allocations between cycles. Python's
            # generational GC skips long-lived objects but a scan cycle creates
            # thousands of short-lived Series / WavePoint / Counter entries, and
            # explicit collection keeps RSS from drifting upward over hours.
            gc.collect()
            _log_memory(cycle_count)

            # Дневной отчёт
            today = datetime.now(timezone.utc).date()
            if today != last_report_date:
                uptime = str(datetime.now(timezone.utc) - _start_time).split(".")[0]
                send_daily_report({**_stats, "date": str(today), "uptime": uptime})
                last_report_date = today
                _stats.update({"signals": 0, "strong": 0, "good": 0, "weak": 0})

            sleep_time = max(0, cfg.SCAN_INTERVAL_SEC - elapsed)
            if sleep_time > 0 and _running:
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}", exc_info=True)
            send_error(f"Ошибка цикла #{cycle_count}: {e}")
            time.sleep(30)

    logger.info("Scanner stopped.")
    send_status("⛔ WAVE SCANNER PRO остановлен")


if __name__ == "__main__":
    main()

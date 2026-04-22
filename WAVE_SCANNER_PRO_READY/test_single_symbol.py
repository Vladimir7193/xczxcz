#!/usr/bin/env python3
"""Тест сканирования одного символа для отладки."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import config as cfg
from logger import setup_logging
from data_fetcher import fetch_multi_tf
from signal_engine import WaveSignalEngine
from wave_analyzer import is_ranging, analyze_wave_structure, check_correction_complete
from impulse_detector import detect_first_impulse, calculate_entry

setup_logging()

# Тестируем BTC
symbol = "BTCUSDT"
print(f"\n{'='*60}")
print(f"Testing {symbol}")
print(f"{'='*60}\n")

# Загрузка данных
print("1. Fetching data...")
data = fetch_multi_tf(symbol)
if not data:
    print("❌ Failed to fetch data")
    sys.exit(1)

df_entry = data[cfg.TF_ENTRY]
df_htf = data[cfg.TF_HTF]
df_trend = data[cfg.TF_TREND]
print(f"✅ Data loaded: 5m={len(df_entry)}, 1h={len(df_htf)}, 4h={len(df_trend)}")

# Проверка боковика
print("\n2. Checking ranging...")
ranging, reason = is_ranging(df_htf)
print(f"   Ranging: {ranging} ({reason})")
if ranging:
    print("❌ Market is ranging, no signals")
    sys.exit(0)

# Анализ структуры
print("\n3. Analyzing wave structure...")
structure = analyze_wave_structure(df_htf, df_trend)
print(f"   Trend: {structure.trend}")
print(f"   Impulse size: {structure.impulse_size:.2f} ATR")
print(f"   Correction: {structure.correction_pct:.1%}")
print(f"   Fib level: {structure.fib_level:.3f}")
print(f"   A=C: {structure.a_equals_c}")
print(f"   Correction type: {structure.correction_type}")

if structure.trend not in ("up", "down"):
    print("❌ No clear trend")
    sys.exit(0)

# Проверка завершения коррекции
print("\n4. Checking correction complete...")
correction = check_correction_complete(df_entry, structure)
print(f"   Complete: {correction.complete}")
print(f"   Fib reached: {correction.fib_reached} ({correction.fib_level:.3f})")
print(f"   Liquidity swept: {correction.liquidity_swept}")
print(f"   A=C: {correction.a_equals_c}")
print(f"   Braking volume: {correction.braking_volume} (strength={correction.braking_strength:.2f})")
print(f"   Score: {correction.score:.1f}/100")
print(f"   Details: {correction.details}")

if not correction.complete:
    print("❌ Correction not complete")
    sys.exit(0)

# Детекция импульса
print("\n5. Detecting first impulse...")
impulse = detect_first_impulse(df_entry, structure)
print(f"   Found: {impulse.found}")
if impulse.found:
    print(f"   Start: {impulse.impulse_start:.4f}")
    print(f"   End: {impulse.impulse_end:.4f}")
    print(f"   Size: {impulse.impulse_size:.2f} ATR")
    print(f"   Comparable: {impulse.comparable}")
    print(f"   Breakout: {impulse.breakout}")
    print(f"   Bar index: {impulse.bar_index}")
    print(f"   Bars used: {impulse.bars_used}")
    
    bars_since = len(df_entry) - 1 - impulse.bar_index
    print(f"   Bars since: {bars_since}")
else:
    print("❌ No impulse found")
    sys.exit(0)

# Расчёт входа
print("\n6. Calculating entry...")
entry = calculate_entry(df_entry, structure, impulse)
print(f"   Valid: {entry.valid}")
if entry.valid:
    print(f"   Entry: {entry.entry_price:.4f}")
    print(f"   Stop: {entry.stop_loss:.4f}")
    print(f"   Cancel: {entry.cancel_level:.4f}")
    print(f"   TP1: {entry.tp1:.4f}")
    print(f"   TP2: {entry.tp2:.4f}")
    print(f"   TP3: {entry.tp3:.4f}")
    print(f"   R:R: {entry.rr_ratio:.2f}")
    print(f"   ATR: {entry.atr:.4f}")
else:
    print("❌ Entry not valid")
    sys.exit(0)

# Финальный скоринг
print("\n7. Final scoring...")
engine = WaveSignalEngine()
score = engine.calculate_final_score(correction, impulse, entry, structure, df_entry)
print(f"   Score: {score:.1f}/100")
print(f"   Min required: {cfg.MIN_SCORE}")

if score >= cfg.MIN_SCORE:
    print(f"\n✅ SIGNAL WOULD BE GENERATED!")
    print(f"   {symbol} {structure.trend.upper()} @ {entry.entry_price:.4f}")
else:
    print(f"\n❌ Score too low ({score:.1f} < {cfg.MIN_SCORE})")

print(f"\n{'='*60}\n")

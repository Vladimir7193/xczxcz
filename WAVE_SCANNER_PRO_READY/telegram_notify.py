# ============================================================
#  telegram_notify.py — Telegram уведомления
# ============================================================
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config as cfg
from signal_engine import WaveSignal

logger = logging.getLogger(__name__)

_send_lock = threading.Lock()
_last_send: float = 0.0
_session: Optional[requests.Session] = None


def _proxies() -> Optional[dict]:
    p = getattr(cfg, "TELEGRAM_PROXY", None)
    if p:
        return {"http": p, "https": p}
    return None


def _get_session() -> requests.Session:
    """Ленивая модульная сессия с retry; переиспользуется между вызовами."""
    global _session
    if _session is not None:
        return _session
    session = requests.Session()
    retry_strategy = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    _session = session
    return session


def _send(text: str) -> bool:
    global _last_send

    if not getattr(cfg, "TELEGRAM_ENABLED", False):
        return False
    if not cfg.TELEGRAM_BOT_TOKEN:
        return False

    proxies = _proxies()

    # Если используется SOCKS прокси, проверяем наличие PySocks
    if proxies and any('socks' in str(p).lower() for p in proxies.values()):
        try:
            import socks  # noqa: F401
        except ImportError:
            logger.error("SOCKS proxy requires 'PySocks' package. Install: pip install PySocks")
            return False

    # Сериализуем отправку и выдерживаем rate-limit 1 сообщение/сек.
    with _send_lock:
        elapsed = time.time() - _last_send
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
        timeout = 45 if proxies else 10

        try:
            session = _get_session()
            r = session.post(
                url,
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                proxies=proxies,
                timeout=timeout,
            )
            r.raise_for_status()
            _last_send = time.time()
            logger.info("Telegram message sent")
            return True

        except requests.exceptions.ProxyError as e:
            logger.error(f"Telegram proxy error: {e}. Check TELEGRAM_PROXY in .env.")
            return False
        except requests.exceptions.Timeout:
            logger.warning("Telegram timeout: proxy may be slow or blocked")
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Telegram connection error: {e}")
            return False
        except requests.exceptions.HTTPError as e:
            logger.error(f"Telegram HTTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"Telegram error: {e}", exc_info=True)
            return False

def send_wave_signal(sig: WaveSignal) -> bool:
    """Отправляет волновой сигнал с пошаговой инструкцией"""

    direction_emoji = "🟢 LONG" if sig.direction == "long" else "🔴 SHORT"

    # Оценка качества
    if sig.score >= 85:
        quality = "🔥🔥🔥 ОТЛИЧНЫЙ"
        confidence = "95%"
        action = "✅ ВХОДИТЬ СЕЙЧАС"
        size_pct = "100%"
        when = "⚡ СЕЙЧАС! Не жди, сигнал отличный"
    elif sig.score >= 70:
        quality = "🔥🔥 ХОРОШИЙ"
        confidence = "80%"
        action = "✅ ВХОДИТЬ"
        size_pct = "100%"
        when = "✅ Можно входить сразу"
    elif sig.score >= 55:
        quality = "✅ СРЕДНИЙ"
        confidence = "65%"
        action = "⚠️ ОСТОРОЖНО"
        size_pct = "70%"
        when = "⏳ Подожди 1-2 минуты, посмотри движение"
    else:
        quality = "⚠️ СЛАБЫЙ"
        confidence = "50%"
        action = "⚠️ ОСТОРОЖНО — МАЛЫЙ РАЗМЕР"
        size_pct = "50%"
        when = "⏳ Подожди 3-5 минут, убедись что цена идет в нужную сторону"

    # Детали коррекции
    fib_str     = f"{sig.correction.fib_level:.1%}" if sig.correction.fib_level else "—"
    swept_str   = "✅" if sig.correction.liquidity_swept else "❌"
    ac_str      = "✅" if sig.correction.a_equals_c else "❌"
    braking_str = f"✅ {sig.correction.braking_strength:.0%}" if sig.correction.braking_volume else "❌"

    # Тип следующей коррекции
    next_corr = {
        "sharp": "резкая (V-образная)",
        "flat":  "боковая (флаг)",
    }.get(sig.correction_type_next, "неизвестно")

    text = (
        f"<b>{quality}</b>\n"
        f"<b>🌊 WAVE {direction_emoji}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <b>{sig.symbol}</b>\n"
        f"📊 Скор: <b>{sig.score:.0f}/100</b> (уверенность {confidence})\n"
        f"⏰ Сессия: {sig.session}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 ЧТО ДЕЛАТЬ:</b>\n"
        f"{action}\n"
        f"\n"
        f"<b>📋 ПОШАГОВАЯ ИНСТРУКЦИЯ:</b>\n"
        f"\n"
        f"<b>ШАГ 1:</b> Открой Bybit\n"
        f"<b>ШАГ 2:</b> Найди пару <code>{sig.symbol}</code>\n"
        f"<b>ШАГ 3:</b> Выбери направление: <b>{sig.direction.upper()}</b>\n"
        f"\n"
        f"<b>ШАГ 4:</b> Установи уровни:\n"
        f"  🎯 Вход:   <code>{sig.entry_price:.4f}</code>\n"
        f"  🛑 Стоп:   <code>{sig.stop_loss:.4f}</code>\n"
        f"  🚫 Отмена: <code>{sig.cancel_level:.4f}</code>\n"
        f"  ✅ TP1:    <code>{sig.tp1:.4f}</code> (закрой 30%)\n"
        f"  ✅ TP2:    <code>{sig.tp2:.4f}</code> (закрой 40%)\n"
        f"  🏆 TP3:    <code>{sig.tp3:.4f}</code> (закрой 30%)\n"
        f"\n"
        f"<b>ШАГ 5:</b> Размер позиции:\n"
        f"  💰 {size_pct} от обычного размера\n"
        f"  🔧 Плечо: <b>5x</b>\n"
        f"  📐 R:R = <b>{sig.rr_ratio:.2f}</b>\n"
        f"\n"
        f"<b>ШАГ 6:</b> Когда входить:\n"
        f"  {when}\n"
        f"\n"
        f"<b>⚠️ ВАЖНО:</b>\n"
    )

    if sig.direction == "long":
        text += (
            f"  • Если цена упала >0.5% — НЕ входи\n"
            f"  • Если цена ниже отмены <code>{sig.cancel_level:.4f}</code> — ПРОПУСТИ\n"
        )
    else:
        text += (
            f"  • Если цена выросла >0.5% — НЕ входи\n"
            f"  • Если цена выше отмены <code>{sig.cancel_level:.4f}</code> — ПРОПУСТИ\n"
        )

    text += (
        f"  • Обязательно ставь стоп-лосс!\n"
        f"  • Не увеличивай размер позиции\n"
        f"\n"
        f"<b>📊 АНАЛИЗ ВОЛНЫ:</b>\n"
        f"  📏 Фибо {fib_str}: {'✅' if sig.correction.fib_reached else '❌'}\n"
        f"  🌊 Ликвидность снята: {swept_str}\n"
        f"  ⚖️ A = C: {ac_str}\n"
        f"  🛑 Тормозящий объём: {braking_str}\n"
    )

    if sig.structure.has_expanding:
        text += f"  ⚠️ Расширяющая формация!\n"

    text += (
        f"\n"
        f"  🔄 След. коррекция: {next_corr}\n"
        f"⏱ {sig.timestamp.strftime('%H:%M UTC')}"
    )

    return _send(text)


def send_status(msg: str) -> bool:
    return _send(f"ℹ️ {msg}")


def send_error(msg: str) -> bool:
    return _send(f"🚨 <b>ERROR</b>\n{msg}")


def send_daily_report(stats: dict) -> bool:
    text = (
        f"📊 <b>Дневной отчёт — WAVE SCANNER</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {stats.get('date', '—')}\n"
        f"📡 Сигналов: {stats.get('signals', 0)}\n"
        f"🔥 STRONG:   {stats.get('strong', 0)}\n"
        f"✅ GOOD:     {stats.get('good', 0)}\n"
        f"⚠️ WEAK:     {stats.get('weak', 0)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 Циклов:   {stats.get('cycles', 0)}\n"
        f"⏱ Время:    {stats.get('uptime', '—')}\n"
    )
    return _send(text)

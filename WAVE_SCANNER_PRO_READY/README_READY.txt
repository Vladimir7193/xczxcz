WAVE_SCANNER_PRO_READY_LIVE

Что внутри:
- очищенный проект без .venv, логов и pycache
- безопасный .env.example без токенов
- run_ready.bat для запуска сканера
- run_backtest.bat для бэктеста
- smoke_test.py для быстрой проверки окружения

Что улучшено:
- более рабочие live-дефолты
- ограничен нереалистичный RR сверху
- расширена допустимая дистанция до входа
- добавлена диагностика Reject stats по каждому циклу

Быстрый запуск:
1) Распакуй папку
2) Открой run_ready.bat
3) При первом запуске файл сам:
   - создаст .venv
   - установит зависимости
   - создаст .env из .env.example
   - выполнит smoke_test.py
   - запустит main.py

Перед запуском:
- открой .env
- если Telegram не нужен, оставь TELEGRAM_ENABLED=0
- если нужен Telegram, заполни TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID

Что смотреть в логах:
- logs\wave_scanner.log
- строку Reject stats: она показывает, что именно душит сигналы
  например: ranging, score_too_low, correction_incomplete, volume_fail

Базовые live-параметры:
- MIN_SCORE=62
- MIN_RR=1.8
- MAX_RR=5.0
- MAX_ENTRY_DISTANCE_ATR=1.6

Если сигналов совсем нет:
- сначала посмотри Reject stats
- только потом меняй фильтры
- обычно первым делом смотрят ranging / score_too_low / volume_fail


Recommended live defaults in this build:
- SCAN_INTERVAL_SEC=90
- SCAN_WORKERS=2
- EXTENDED_SYMBOLS_LIMIT=5
- MIN_SCORE=60
- VOLUME_CONFIRMATION_REQUIRED=0
- DATA_CACHE_TTL_SEC=180

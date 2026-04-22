WAVE_SCANNER_PRO — working build

Что исправлено дополнительно:
- .env теперь реально загружается автоматически из файла рядом с кодом
- символы вида BTCUSDT автоматически конвертируются в формат ccxt/Bybit: BTC/USDT:USDT
- архив очищен от логов и __pycache__
- добавлен smoke_test.py для быстрой проверки окружения

Быстрый запуск в Windows:
1. Распакуйте архив
2. Откройте PowerShell в папке проекта
3. Создайте venv: python -m venv .venv
4. Активируйте: .\.venv\Scripts\Activate.ps1
5. Установите зависимости: pip install -r requirements.txt
6. Скопируйте .env.example в .env
7. Проверка: python smoke_test.py
8. Сканер: python main.py
9. Бэктест: python backtest.py

Замечание:
- для работы нужны интернет и доступ к Bybit API через публичные OHLCV
- Telegram можно оставить выключенным через TELEGRAM_ENABLED=0

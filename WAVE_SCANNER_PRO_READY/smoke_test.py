from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import config as cfg
from data_fetcher import normalize_symbol


def main() -> int:
    cfg.validate_runtime_config()
    print('Config loaded OK')
    print('Telegram enabled:', cfg.TELEGRAM_ENABLED)
    print('Bybit symbol example:', normalize_symbol('BTCUSDT'))
    print('Min score:', cfg.MIN_SCORE)
    print('Smoke test passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

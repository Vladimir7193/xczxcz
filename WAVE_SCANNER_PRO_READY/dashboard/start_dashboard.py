#!/usr/bin/env python3
"""Backwards-compatible launcher for the WAVE Scanner dashboard.

Historically this file was an `http.server`-based static-file launcher that
opened ``ai_dashboard.html`` in the browser. It has been rewritten to start
the new FastAPI app from :mod:`server` while keeping the original semantics:

  * default port 3900;
  * the same URL ``/ai_dashboard.html`` still works (now an alias);
  * a browser tab is opened automatically (skip with ``--no-browser``);
  * Ctrl+C cleanly stops the server.

All previous text-encoding bugs (BOM at the top of the file, missing
characters in the Russian status messages) are fixed.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from server import find_free_port, main as server_main, parse_args  # type: ignore  # noqa: E402

DEFAULT_PORT = 3900


def _open_browser_when_ready(url: str, *, delay: float = 1.5) -> None:
    """Open the dashboard in the default browser after a short delay so the
    server has time to bind the port."""
    def _go() -> None:
        time.sleep(delay)
        try:
            webbrowser.open(url)
        except Exception:
            pass
    threading.Thread(target=_go, name="open-browser", daemon=True).start()


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    open_browser = "--no-browser" not in argv
    argv = [a for a in argv if a != "--no-browser"]

    # Parse just to discover the resolved port; server.main() reparses too.
    args = parse_args(argv)
    port = args.port if args.no_port_fallback else find_free_port(args.port)
    host = "127.0.0.1" if args.host in {"0.0.0.0", ""} else args.host
    url = f"http://{host}:{port}/"

    print("=" * 60)
    print("  WAVE Scanner — AI Dashboard")
    print(f"  Открываю {url}")
    print("  Нажмите Ctrl+C для остановки")
    print("=" * 60)

    if open_browser:
        _open_browser_when_ready(url)

    # Re-inject the resolved port so server.main() doesn't search again.
    # If --port was supplied but the requested port was busy, replace its value
    # with the resolved fallback port — otherwise uvicorn would try to bind to
    # the busy port we already opened the browser on.
    if "--port" not in argv:
        argv += ["--port", str(port)]
    else:
        idx = argv.index("--port")
        if idx + 1 < len(argv):
            argv[idx + 1] = str(port)
    if "--no-port-fallback" not in argv:
        argv += ["--no-port-fallback"]

    server_main(argv)


if __name__ == "__main__":
    # Ensure CWD is dashboard/ so relative paths in older tooling still work.
    os.chdir(THIS_DIR)
    main()

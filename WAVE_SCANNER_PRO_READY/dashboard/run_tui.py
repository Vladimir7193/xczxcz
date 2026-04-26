"""Textual-based TUI for the WAVE Scanner dashboard.

Connects to the FastAPI server via WebSocket (`/ws`) and REST endpoints to
show the same data as the web UI: System Health, Live Log Feed, and the
latest signals. Designed for power-users who prefer a terminal.

Keyboard shortcuts:
  Ctrl+R  refresh signals
  Ctrl+L  clear log pane
  T       cycle theme (dark/matrix/light)
  L       focus log pane
  S       focus signals pane
  H       focus health pane
  Q       quit

Usage:
    python run_tui.py [--url http://localhost:3900]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Container, Horizontal, Vertical
    from textual.reactive import reactive
    from textual.widgets import DataTable, Footer, Header, ProgressBar, RichLog, Static
except ImportError:  # pragma: no cover
    print(
        "textual is not installed. Run `pip install -r requirements.txt` "
        "in dashboard/ to install it.",
        file=sys.stderr,
    )
    raise

import websockets


def _ws_url(http_base: str) -> str:
    p = urlparse(http_base)
    scheme = "wss" if p.scheme == "https" else "ws"
    netloc = p.netloc or p.path
    return f"{scheme}://{netloc}/ws"


class HealthPanel(Static):
    cpu = reactive(0.0)
    mem = reactive(0.0)
    disk = reactive(0.0)
    load = reactive(0.0)
    uptime = reactive(0.0)
    procs = reactive(0)

    def render(self) -> str:
        bar = lambda pct, w=20: "█" * int(pct / 100 * w) + "·" * (w - int(pct / 100 * w))
        return (
            f"[bold cyan]System Health[/]\n"
            f"  CPU    [bold]{self.cpu:5.1f}%[/]  {bar(self.cpu)}\n"
            f"  Memory [bold]{self.mem:5.1f}%[/]  {bar(self.mem)}\n"
            f"  Disk   [bold]{self.disk:5.1f}%[/]\n"
            f"  Load1  [bold]{self.load:5.2f}[/]   procs [bold]{self.procs}[/]\n"
            f"  Uptime {int(self.uptime // 3600)}h {int((self.uptime % 3600) // 60):02d}m"
        )


class TUIApp(App[None]):
    TITLE = "WAVE Scanner — AI Dashboard TUI"
    SUB_TITLE = ""
    CSS = """
    Screen { layout: vertical; }
    #top { height: 30%; min-height: 8; layout: horizontal; }
    #health { width: 40%; min-width: 32; padding: 1 2; border: round $accent; }
    #signals { width: 60%; padding: 0 1; border: round $accent-darken-1; }
    #log { padding: 0 1; border: round $accent-darken-2; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+r", "refresh", "Refresh"),
        Binding("ctrl+l", "clear_log", "Clear log"),
        Binding("t", "cycle_theme", "Theme"),
        Binding("l", "focus_log", "Logs"),
        Binding("s", "focus_signals", "Signals"),
        Binding("h", "focus_health", "Health"),
    ]

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self.health = HealthPanel(id="health")
        self.signals_table = DataTable(id="signals", zebra_stripes=True)
        self.log_view = RichLog(id="log", highlight=True, markup=True, max_lines=2000)
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._signals_task: Optional[asyncio.Task[None]] = None
        self._theme_idx = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Container(id="top"):
            yield self.health
            yield self.signals_table
        yield self.log_view
        yield Footer()

    async def on_mount(self) -> None:
        self.signals_table.add_columns("time", "symbol", "dir", "score", "RR", "label")
        self.sub_title = self.base_url
        self.log_view.write(f"[dim]connecting to {self.base_url}…[/]")
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._signals_task = asyncio.create_task(self._signals_loop())

    async def on_unmount(self) -> None:
        for t in (self._ws_task, self._signals_task):
            if t:
                t.cancel()

    # ---- background loops ----
    async def _ws_loop(self) -> None:
        url = _ws_url(self.base_url)
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, max_size=4 * 1024 * 1024) as ws:
                    backoff = 1.0
                    self.log_view.write(f"[green]ws connected[/] {url}")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        self._handle_ws(msg)
            except (ConnectionRefusedError, OSError) as e:
                self.log_view.write(f"[red]ws connect failed:[/] {e}")
            except websockets.ConnectionClosed:
                self.log_view.write("[yellow]ws closed[/]")
            await asyncio.sleep(backoff)
            backoff = min(15.0, backoff * 1.6)

    def _handle_ws(self, msg: Dict[str, Any]) -> None:
        t = msg.get("type")
        if t in {"hello", "metrics"}:
            d = msg.get("data") or {}
            self.health.cpu = float(d.get("cpu_percent") or 0.0)
            self.health.mem = float(d.get("memory_percent") or 0.0)
            self.health.disk = float(d.get("disk_percent") or 0.0)
            la = d.get("load_avg") or [0.0]
            self.health.load = float(la[0])
            self.health.uptime = float(d.get("uptime_sec") or 0.0)
            self.health.procs = int(d.get("process_count") or 0)
            self.health.refresh()
        elif t == "log":
            self.log_view.write(self._color_log_line(msg.get("line", "")))
        elif t == "log_snapshot":
            for line in msg.get("lines") or []:
                self.log_view.write(self._color_log_line(line))

    @staticmethod
    def _color_log_line(line: str) -> str:
        if "[ERROR]" in line:    return f"[red]{line}[/]"
        if "[WARNING]" in line:  return f"[yellow]{line}[/]"
        if "[CRITICAL]" in line: return f"[bold red]{line}[/]"
        if "[DEBUG]" in line:    return f"[dim]{line}[/]"
        return line

    async def _signals_loop(self) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            while True:
                try:
                    r = await client.get(f"{self.base_url}/api/signals?limit=50")
                    r.raise_for_status()
                    data = r.json().get("signals") or []
                    self._render_signals(data)
                except Exception as e:
                    self.log_view.write(f"[red]signals fetch failed:[/] {e}")
                await asyncio.sleep(15)

    def _render_signals(self, rows: List[Dict[str, Any]]) -> None:
        self.signals_table.clear()
        for r in rows[:50]:
            ts = (r.get("timestamp") or "").replace("T", " ")[:19]
            self.signals_table.add_row(
                ts,
                r.get("symbol", ""),
                (r.get("direction") or "").upper(),
                str(r.get("score", "")),
                str(r.get("rr_ratio", "")),
                r.get("label", "") or "",
            )

    # ---- actions ----
    def action_clear_log(self) -> None:
        self.log_view.clear()

    async def action_refresh(self) -> None:
        if self._signals_task:
            self._signals_task.cancel()
        self._signals_task = asyncio.create_task(self._signals_loop())
        self.log_view.write("[cyan]refresh requested[/]")

    def action_cycle_theme(self) -> None:
        themes = ["textual-dark", "textual-light", "monokai"]
        self._theme_idx = (self._theme_idx + 1) % len(themes)
        try:
            self.theme = themes[self._theme_idx]
        except Exception:
            pass

    def action_focus_log(self) -> None:    self.log_view.focus()
    def action_focus_signals(self) -> None: self.signals_table.focus()
    def action_focus_health(self) -> None: self.health.focus()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WAVE Scanner TUI")
    p.add_argument("--url", default="http://localhost:3900",
                   help="dashboard server base URL (default http://localhost:3900)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    TUIApp(args.url).run()


if __name__ == "__main__":
    main()

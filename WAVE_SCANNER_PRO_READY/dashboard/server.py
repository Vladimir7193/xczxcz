"""WAVE_SCANNER_PRO dashboard backend.

FastAPI application that serves the web dashboard, exposes REST endpoints for
system metrics, log tails, and signal history, and pushes live metrics + log
lines over a WebSocket to both the browser UI and the Textual TUI.

Run directly:
    python -m wave_dashboard.server
or via the bundled entrypoint:
    python start_dashboard.py
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import socket
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Deque, Dict, List, Optional, Set

import psutil
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))

from conversations import ConversationStore
from files_browser import PathOutsideWorkspace, Workspace, list_dir, read_text
from ollama_client import (
    DEFAULT_BASE_URL as OLLAMA_BASE_URL,
    ChatMessage,
    OllamaError,
    chat_stream,
    list_models,
)

DASHBOARD_DIR = Path(__file__).resolve().parent
SCANNER_DIR = DASHBOARD_DIR.parent
DEFAULT_LOG_FILE = SCANNER_DIR / "logs" / "wave_scanner.log"
DEFAULT_SIGNALS_CSV = SCANNER_DIR / "logs" / "signals.csv"
DEFAULT_TRADES_CSV = SCANNER_DIR / "logs" / "trades.csv"
DEFAULT_WORKSPACE = SCANNER_DIR
DEFAULT_CONVERSATIONS_DIR = DASHBOARD_DIR / "conversations"

DASHBOARD_HTML = DASHBOARD_DIR / "dashboard.html"
STATIC_DIR = DASHBOARD_DIR / "static"

LOG_BUFFER_SIZE = 1000
METRICS_INTERVAL = 2.0
LOG_POLL_INTERVAL = 1.0

logger = logging.getLogger("dashboard.server")


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------


@dataclass
class AppState:
    started_at: float = field(default_factory=time.time)
    log_buffer: Deque[str] = field(default_factory=lambda: deque(maxlen=LOG_BUFFER_SIZE))
    log_path: Path = DEFAULT_LOG_FILE
    signals_path: Path = DEFAULT_SIGNALS_CSV
    trades_path: Path = DEFAULT_TRADES_CSV
    workspace: Optional[Workspace] = None
    conversations: Optional[ConversationStore] = None
    ollama_base_url: str = OLLAMA_BASE_URL
    clients: Set[WebSocket] = field(default_factory=set)
    metrics_task: Optional[asyncio.Task[None]] = None
    log_task: Optional[asyncio.Task[None]] = None


STATE = AppState()


def get_workspace() -> Workspace:
    if STATE.workspace is None:
        STATE.workspace = Workspace.from_path(DEFAULT_WORKSPACE)
    return STATE.workspace


def get_conversations() -> ConversationStore:
    if STATE.conversations is None:
        STATE.conversations = ConversationStore(DEFAULT_CONVERSATIONS_DIR)
    return STATE.conversations


def system_metrics() -> Dict[str, Any]:
    """Snapshot of CPU, memory, disk and load metrics."""
    cpu_percent = psutil.cpu_percent(interval=None)
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage(str(SCANNER_DIR))
    try:
        load_avg = os.getloadavg()
    except (AttributeError, OSError):
        load_avg = (0.0, 0.0, 0.0)
    return {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cpu_percent": round(cpu_percent, 1),
        "cpu_count": psutil.cpu_count(logical=True) or 1,
        "memory_percent": round(vm.percent, 1),
        "memory_used_mb": round(vm.used / (1024 * 1024), 1),
        "memory_total_mb": round(vm.total / (1024 * 1024), 1),
        "disk_percent": round(disk.percent, 1),
        "load_avg": [round(x, 2) for x in load_avg],
        "uptime_sec": round(time.time() - STATE.started_at, 1),
        "process_count": len(psutil.pids()),
    }


def read_recent_log_lines(path: Path, limit: int = 200) -> List[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            tail: Deque[str] = deque(fh, maxlen=limit)
        return [line.rstrip("\n") for line in tail]
    except OSError as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return []


def read_signals_csv(path: Path, limit: int = 200) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(row)
    except OSError as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return []
    return rows[-limit:][::-1]


# ---------------------------------------------------------------------------
# Background pumps
# ---------------------------------------------------------------------------


async def broadcast(payload: Dict[str, Any]) -> None:
    if not STATE.clients:
        return
    message = json.dumps(payload, default=str)
    dead: List[WebSocket] = []
    for ws in list(STATE.clients):
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        STATE.clients.discard(ws)


async def metrics_pump() -> None:
    # Prime psutil so first sample isn't 0.0.
    psutil.cpu_percent(interval=None)
    while True:
        try:
            await broadcast({"type": "metrics", "data": system_metrics()})
        except Exception as exc:
            logger.warning("metrics_pump error: %s", exc)
        await asyncio.sleep(METRICS_INTERVAL)


async def log_pump() -> None:
    """Tail the scanner log file and push new lines to all clients."""
    path = STATE.log_path
    last_size = 0
    last_inode: Optional[int] = None
    if path.exists():
        # Pre-fill buffer with existing tail so new clients see context.
        for line in read_recent_log_lines(path, limit=LOG_BUFFER_SIZE):
            STATE.log_buffer.append(line)
        try:
            stat = path.stat()
            last_size = stat.st_size
            last_inode = stat.st_ino
        except OSError:
            pass

    while True:
        try:
            if not path.exists():
                await asyncio.sleep(LOG_POLL_INTERVAL)
                continue
            stat = path.stat()
            if last_inode is not None and stat.st_ino != last_inode:
                last_size = 0
                last_inode = stat.st_ino
            if stat.st_size < last_size:
                last_size = 0
            if stat.st_size > last_size:
                with path.open("r", encoding="utf-8", errors="replace") as fh:
                    fh.seek(last_size)
                    chunk = fh.read()
                    last_size = fh.tell()
                if last_inode is None:
                    last_inode = stat.st_ino
                for raw in chunk.splitlines():
                    line = raw.rstrip()
                    if not line:
                        continue
                    STATE.log_buffer.append(line)
                    await broadcast({"type": "log", "line": line})
        except Exception as exc:
            logger.warning("log_pump error: %s", exc)
        await asyncio.sleep(LOG_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# AI command center
# ---------------------------------------------------------------------------


COMMAND_HELP = [
    ("help", "show available commands"),
    ("status", "server status, uptime, connected clients"),
    ("get logs [N]", "return last N log lines (default 50)"),
    ("get signals [N]", "return last N signals from logs/signals.csv"),
    ("refresh", "force broadcast of fresh metrics"),
    ("clean mode", "client: hide non-essential widgets"),
    ("full mode", "client: show all widgets"),
    ("theme dark|matrix|light", "client: switch theme"),
    ("focus logs|signals|terminal", "client: focus a widget"),
    ("clear", "client: clear terminal output"),
    ("ping", "round-trip check"),
]


def render_help() -> str:
    width = max(len(c) for c, _ in COMMAND_HELP)
    return "\n".join(f"  {cmd.ljust(width)}  {desc}" for cmd, desc in COMMAND_HELP)


async def execute_command(raw: str) -> Dict[str, Any]:
    """Parse a single command line and return a structured response.

    The response always contains an `output` string for terminal display, and
    may include `ui` actions interpreted by the web client (theme switch,
    layout changes, etc.).
    """
    text = (raw or "").strip()
    if not text:
        return {"ok": False, "output": "empty command", "ui": []}

    parts = text.split()
    cmd = parts[0].lower()
    args = parts[1:]
    ui: List[Dict[str, Any]] = []

    if cmd == "help":
        return {"ok": True, "output": "Available commands:\n" + render_help(), "ui": []}

    if cmd == "status":
        m = system_metrics()
        out = (
            f"server: ok\n"
            f"uptime: {m['uptime_sec']:.0f}s\n"
            f"cpu:    {m['cpu_percent']:.1f}% ({m['cpu_count']} cores)\n"
            f"memory: {m['memory_percent']:.1f}% "
            f"({m['memory_used_mb']:.0f}/{m['memory_total_mb']:.0f} MB)\n"
            f"disk:   {m['disk_percent']:.1f}%\n"
            f"clients: {len(STATE.clients)}\n"
            f"log:    {STATE.log_path}\n"
            f"signals:{STATE.signals_path}"
        )
        return {"ok": True, "output": out, "ui": []}

    if cmd in {"get", "show"} and args:
        sub = args[0].lower()
        n = 50
        if len(args) > 1 and args[1].isdigit():
            n = max(1, min(int(args[1]), LOG_BUFFER_SIZE))
        if sub in {"logs", "log"}:
            lines = list(STATE.log_buffer)[-n:]
            if not lines:
                lines = read_recent_log_lines(STATE.log_path, limit=n)
            out = "\n".join(lines) if lines else "(no log lines yet)"
            return {"ok": True, "output": out, "ui": []}
        if sub in {"signals", "signal"}:
            rows = read_signals_csv(STATE.signals_path, limit=n)
            if not rows:
                return {"ok": True, "output": "(no signals yet)", "ui": []}
            out_lines = []
            for r in rows[:n]:
                out_lines.append(
                    f"{r.get('timestamp', '?')}  {r.get('symbol', '?'):<10} "
                    f"{(r.get('direction') or '?').upper():<5} "
                    f"score={r.get('score', '?')} rr={r.get('rr_ratio', '?')} "
                    f"{r.get('label', '')}"
                )
            return {"ok": True, "output": "\n".join(out_lines), "ui": []}

    if cmd == "refresh":
        await broadcast({"type": "metrics", "data": system_metrics()})
        return {"ok": True, "output": "metrics refreshed", "ui": [{"action": "refresh"}]}

    if cmd == "clean" and (not args or args[0] == "mode"):
        ui.append({"action": "layout", "mode": "clean"})
        return {"ok": True, "output": "switched to clean mode", "ui": ui}

    if cmd == "full" and (not args or args[0] == "mode"):
        ui.append({"action": "layout", "mode": "full"})
        return {"ok": True, "output": "switched to full mode", "ui": ui}

    if cmd == "theme" and args:
        theme = args[0].lower()
        if theme not in {"dark", "matrix", "light"}:
            return {"ok": False, "output": f"unknown theme: {theme}", "ui": []}
        ui.append({"action": "theme", "theme": theme})
        return {"ok": True, "output": f"theme set to {theme}", "ui": ui}

    if cmd == "focus" and args:
        target = args[0].lower()
        if target not in {"logs", "signals", "terminal", "health"}:
            return {"ok": False, "output": f"unknown focus target: {target}", "ui": []}
        ui.append({"action": "focus", "target": target})
        return {"ok": True, "output": f"focused {target}", "ui": ui}

    if cmd == "clear":
        ui.append({"action": "clear"})
        return {"ok": True, "output": "", "ui": ui}

    if cmd == "ping":
        return {"ok": True, "output": "pong", "ui": []}

    return {
        "ok": False,
        "output": f"unknown command: {text!r}\ntype 'help' for available commands",
        "ui": [],
    }


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    STATE.metrics_task = asyncio.create_task(metrics_pump(), name="metrics_pump")
    STATE.log_task = asyncio.create_task(log_pump(), name="log_pump")
    logger.info("dashboard background tasks started")
    try:
        yield
    finally:
        for task in (STATE.metrics_task, STATE.log_task):
            if task is not None:
                task.cancel()
        for task in (STATE.metrics_task, STATE.log_task):
            if task is None:
                continue
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        for ws in list(STATE.clients):
            try:
                await ws.close()
            except Exception:
                pass
        STATE.clients.clear()
        logger.info("dashboard shut down cleanly")


app = FastAPI(title="WAVE Scanner Dashboard", version="2.0", lifespan=lifespan)


@app.middleware("http")
async def no_cache_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("Cache-Control", "no-store")
    return response


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index() -> FileResponse:
    if not DASHBOARD_HTML.exists():
        raise HTTPException(status_code=500, detail="dashboard.html missing")
    return FileResponse(str(DASHBOARD_HTML), media_type="text/html")


@app.get("/dashboard.html")
async def dashboard_alias() -> FileResponse:
    return await index()


@app.get("/ai_dashboard.html")
async def legacy_alias() -> FileResponse:
    # Backwards-compatible path for the old start_dashboard.py URL.
    return await index()


@app.get("/api/health")
async def api_health() -> Dict[str, Any]:
    return {"ok": True, "metrics": system_metrics(), "clients": len(STATE.clients)}


@app.get("/api/logs")
async def api_logs(limit: int = 200) -> Dict[str, Any]:
    limit = max(1, min(limit, LOG_BUFFER_SIZE))
    buffered = list(STATE.log_buffer)[-limit:]
    if not buffered:
        buffered = read_recent_log_lines(STATE.log_path, limit=limit)
    return {"ok": True, "lines": buffered, "log_file": str(STATE.log_path)}


@app.get("/api/signals")
async def api_signals(limit: int = 200) -> Dict[str, Any]:
    limit = max(1, min(limit, 1000))
    rows = read_signals_csv(STATE.signals_path, limit=limit)
    return {"ok": True, "signals": rows, "source": str(STATE.signals_path)}


class CommandRequest(BaseModel):
    command: str


@app.post("/api/command")
async def api_command(payload: CommandRequest) -> Dict[str, Any]:
    return await execute_command(payload.command)


@app.get("/api/help", response_class=PlainTextResponse)
async def api_help() -> str:
    return render_help()


# ---------------------------------------------------------------------------
# Files endpoints (sandboxed)
# ---------------------------------------------------------------------------


@app.get("/api/files/list")
async def api_files_list(path: str = "") -> Dict[str, Any]:
    try:
        return list_dir(get_workspace(), path)
    except (PathOutsideWorkspace, PermissionError) as e:
        raise HTTPException(status_code=403, detail=str(e))
    except (FileNotFoundError, NotADirectoryError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("files/list failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/files/read")
async def api_files_read(path: str) -> Dict[str, Any]:
    try:
        return read_text(get_workspace(), path)
    except (PathOutsideWorkspace, PermissionError) as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=415, detail=str(e))
    except Exception as e:
        logger.exception("files/read failed")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Conversations endpoints
# ---------------------------------------------------------------------------


class ConversationCreate(BaseModel):
    title: str = ""


class ConversationMessage(BaseModel):
    role: str = Field(default="user", pattern="^(system|user|assistant|model|tool)$")
    content: str
    model: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


@app.get("/api/conversations")
async def api_conversations_list() -> Dict[str, Any]:
    return {"ok": True, "conversations": get_conversations().list()}


@app.post("/api/conversations")
async def api_conversations_create(payload: ConversationCreate) -> Dict[str, Any]:
    conv = get_conversations().create(payload.title)
    return {"ok": True, "conversation": conv.to_dict()}


@app.get("/api/conversations/{conv_id}")
async def api_conversations_get(conv_id: str) -> Dict[str, Any]:
    try:
        conv = get_conversations().get(conv_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if conv is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    return {"ok": True, "conversation": conv.to_dict()}


@app.post("/api/conversations/{conv_id}/messages")
async def api_conversations_append(conv_id: str, msg: ConversationMessage) -> Dict[str, Any]:
    try:
        conv = get_conversations().append(conv_id, msg.model_dump(exclude_none=True))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "conversation": conv.to_dict()}


@app.delete("/api/conversations/{conv_id}")
async def api_conversations_delete(conv_id: str) -> Dict[str, Any]:
    try:
        ok = get_conversations().delete(conv_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="conversation not found")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Ollama endpoints (single-model + AI Council)
# ---------------------------------------------------------------------------


@app.get("/api/ollama/status")
async def api_ollama_status() -> Dict[str, Any]:
    try:
        models = await list_models(STATE.ollama_base_url)
        return {"ok": True, "base_url": STATE.ollama_base_url, "models": models}
    except OllamaError as e:
        return {
            "ok": False,
            "base_url": STATE.ollama_base_url,
            "error": str(e),
            "hint": e.hint,
            "models": [],
        }


@app.get("/api/ollama/models")
async def api_ollama_models() -> Dict[str, Any]:
    try:
        models = await list_models(STATE.ollama_base_url)
        return {"ok": True, "models": models}
    except OllamaError as e:
        raise HTTPException(status_code=503, detail={"error": str(e), "hint": e.hint})


def _sse(event: Dict[str, Any]) -> bytes:
    return ("data: " + json.dumps(event, ensure_ascii=False) + "\n\n").encode("utf-8")


class ChatStreamRequest(BaseModel):
    model: str
    messages: List[ConversationMessage]
    options: Optional[Dict[str, Any]] = None
    conversation_id: Optional[str] = None
    save: bool = False


def _normalize_messages(items: List[ConversationMessage]) -> List[ChatMessage]:
    norm: List[ChatMessage] = []
    for m in items:
        role = m.role
        if role == "model":
            role = "assistant"
        if role not in {"system", "user", "assistant"}:
            continue
        if not m.content.strip():
            continue
        norm.append(ChatMessage(role=role, content=m.content))
    return norm


async def _stream_one_model(
    model: str,
    messages: List[ChatMessage],
    *,
    options: Optional[Dict[str, Any]] = None,
    conversation_id: Optional[str] = None,
    save: bool = False,
) -> AsyncIterator[bytes]:
    yield _sse({"type": "start", "model": model})
    accum: List[str] = []
    try:
        async for chunk in chat_stream(
            model, messages, base_url=STATE.ollama_base_url, options=options
        ):
            if chunk["delta"]:
                accum.append(chunk["delta"])
                yield _sse({"type": "delta", "model": model, "delta": chunk["delta"]})
            if chunk["done"]:
                raw = chunk.get("raw") or {}
                yield _sse({
                    "type": "end",
                    "model": model,
                    "eval_count": raw.get("eval_count"),
                    "total_duration": raw.get("total_duration"),
                })
    except OllamaError as e:
        yield _sse({"type": "error", "model": model, "error": str(e), "hint": e.hint})
    except Exception as e:
        logger.exception("chat stream failed for %s", model)
        yield _sse({"type": "error", "model": model, "error": str(e)})

    text = "".join(accum)
    if save and conversation_id and text:
        try:
            get_conversations().append(
                conversation_id,
                {"role": "assistant", "model": model, "content": text, "ts": time.time()},
            )
        except Exception as e:
            logger.warning("conversation save failed: %s", e)


@app.post("/api/ollama/chat")
async def api_ollama_chat(req: ChatStreamRequest) -> StreamingResponse:
    messages = _normalize_messages(req.messages)
    if not messages:
        raise HTTPException(status_code=400, detail="messages must contain at least one entry")

    if req.save and req.conversation_id:
        try:
            user_last = next((m for m in reversed(req.messages) if m.role == "user"), None)
            if user_last:
                get_conversations().append(
                    req.conversation_id,
                    {"role": "user", "content": user_last.content, "ts": time.time()},
                )
        except Exception as e:
            logger.warning("conversation save (user) failed: %s", e)

    return StreamingResponse(
        _stream_one_model(
            req.model, messages,
            options=req.options,
            conversation_id=req.conversation_id,
            save=req.save,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class CouncilRequest(BaseModel):
    models: List[str]
    messages: List[ConversationMessage]
    options: Optional[Dict[str, Any]] = None
    conversation_id: Optional[str] = None
    save: bool = False


async def _stream_council(req: CouncilRequest) -> AsyncIterator[bytes]:
    messages = _normalize_messages(req.messages)
    if not req.models:
        yield _sse({"type": "error", "error": "no models selected"})
        return
    if not messages:
        yield _sse({"type": "error", "error": "no messages"})
        return

    if req.save and req.conversation_id:
        try:
            user_last = next((m for m in reversed(req.messages) if m.role == "user"), None)
            if user_last:
                get_conversations().append(
                    req.conversation_id,
                    {"role": "user", "content": user_last.content, "ts": time.time()},
                )
        except Exception as e:
            logger.warning("council conversation save (user) failed: %s", e)

    queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue()

    async def run(model: str) -> None:
        try:
            async for piece in _stream_one_model(
                model, messages,
                options=req.options,
                conversation_id=req.conversation_id,
                save=req.save,
            ):
                await queue.put(piece)
        finally:
            await queue.put(None)

    tasks = [asyncio.create_task(run(m), name=f"council:{m}") for m in req.models]
    remaining = len(tasks)

    yield _sse({"type": "council_start", "models": req.models})
    try:
        while remaining > 0:
            piece = await queue.get()
            if piece is None:
                remaining -= 1
                continue
            yield piece
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    yield _sse({"type": "council_end"})


@app.post("/api/ollama/council")
async def api_ollama_council(req: CouncilRequest) -> StreamingResponse:
    return StreamingResponse(
        _stream_council(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    STATE.clients.add(ws)
    logger.info("ws client connected (total=%d)", len(STATE.clients))
    try:
        await ws.send_text(json.dumps({"type": "hello", "data": system_metrics()}))
        if STATE.log_buffer:
            await ws.send_text(
                json.dumps({"type": "log_snapshot", "lines": list(STATE.log_buffer)[-200:]})
            )
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                msg = {"type": "command", "command": raw}
            mtype = msg.get("type")
            if mtype == "ping":
                await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
            elif mtype == "command":
                resp = await execute_command(msg.get("command", ""))
                await ws.send_text(json.dumps({"type": "command_result", "data": resp}))
            else:
                await ws.send_text(json.dumps({"type": "error", "error": f"unknown type: {mtype}"}))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("ws error: %s", exc)
    finally:
        STATE.clients.discard(ws)
        logger.info("ws client disconnected (total=%d)", len(STATE.clients))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def find_free_port(preferred: int) -> int:
    """Return `preferred` if free, otherwise an OS-assigned free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", preferred))
            return preferred
        except OSError:
            pass
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WAVE Scanner dashboard server")
    parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("DASHBOARD_PORT", "3900")))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--signals-csv", default=str(DEFAULT_SIGNALS_CSV))
    parser.add_argument("--trades-csv", default=str(DEFAULT_TRADES_CSV))
    parser.add_argument("--workspace", default=os.getenv("DASHBOARD_WORKSPACE", str(DEFAULT_WORKSPACE)),
                        help="root folder accessible from the Files panel (sandboxed)")
    parser.add_argument("--conversations-dir", default=str(DEFAULT_CONVERSATIONS_DIR),
                        help="directory where chat conversations are saved")
    parser.add_argument("--ollama-url", default=OLLAMA_BASE_URL,
                        help="base URL of the Ollama server (default http://localhost:11434)")
    parser.add_argument("--no-port-fallback", action="store_true",
                        help="fail instead of choosing a free port if --port is busy")
    parser.add_argument("--reload", action="store_true", help="dev autoreload")
    return parser.parse_args(argv)


def configure(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=os.getenv("DASHBOARD_LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    STATE.log_path = Path(args.log_file).expanduser()
    STATE.signals_path = Path(args.signals_csv).expanduser()
    STATE.trades_path = Path(args.trades_csv).expanduser()
    STATE.ollama_base_url = args.ollama_url.rstrip("/")
    try:
        STATE.workspace = Workspace.from_path(args.workspace)
    except FileNotFoundError as e:
        logger.warning("workspace not available: %s — falling back to scanner dir", e)
        STATE.workspace = Workspace.from_path(DEFAULT_WORKSPACE)
    STATE.conversations = ConversationStore(Path(args.conversations_dir).expanduser())
    if args.no_port_fallback:
        return args.port
    port = find_free_port(args.port)
    if port != args.port:
        logger.warning("port %d busy, switching to %d", args.port, port)
    return port


def main(argv: Optional[List[str]] = None) -> None:
    import uvicorn

    args = parse_args(argv)
    port = configure(args)
    ws_root = STATE.workspace.root if STATE.workspace else DEFAULT_WORKSPACE
    print("=" * 60)
    print("  WAVE Scanner Dashboard v2")
    print(f"  url:         http://localhost:{port}/")
    print(f"  log file:    {STATE.log_path}")
    print(f"  signals csv: {STATE.signals_path}")
    print(f"  workspace:   {ws_root}")
    print(f"  ollama url:  {STATE.ollama_base_url}")
    print("  press Ctrl+C to stop")
    print("=" * 60)
    uvicorn.run(
        "server:app" if args.reload else app,
        host=args.host,
        port=port,
        log_level="info",
        reload=args.reload,
    )


if __name__ == "__main__":
    sys.path.insert(0, str(DASHBOARD_DIR))
    main()

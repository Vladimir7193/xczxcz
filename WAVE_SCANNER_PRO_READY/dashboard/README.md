# WAVE Scanner — AI Dashboard

Modern, dual-interface (Web + Terminal) monitoring & AI-council dashboard for
the WAVE Scanner trading bot.

## What's inside

| File | Purpose |
| ---- | ------- |
| `server.py` | FastAPI + WebSocket backend (REST + SSE + Ollama proxy + sandboxed file browser). |
| `dashboard.html` + `static/` | Modern dark Web UI (Tailwind CDN + glass-morphism). |
| `start_dashboard.py` | Backwards-compatible launcher (port 3900, opens the browser). |
| `run_tui.py` | Textual-based Terminal UI showing the same data. |
| `ollama_client.py` | Async client for [Ollama](https://ollama.com). |
| `files_browser.py` | Sandboxed local-files reader. |
| `conversations.py` | On-disk JSON store for chat conversations. |
| `Запуск_Dashboard.bat` | Windows one-click launcher (auto-creates `.venv`). |
| `requirements.txt` | Python dependencies. |

## Features

- **AI Council**: pick any number of locally pulled Ollama models; the same
  question is sent to all of them in parallel, answers stream into separate
  panes side by side. Toggle to **Solo** mode to chat with a single model.
- **Local files**: sandboxed file browser rooted at `--workspace`. Click any
  text file to preview, then **+ attach** to include its content as context
  in the next prompt — perfect for "review my bot code" requests.
- **Conversations**: chats are saved to `conversations/*.json` so you can
  resume an earlier discussion.
- **System Health**: live CPU/memory/disk gauges and sparklines via
  `psutil`, pushed every 2 s through a WebSocket.
- **Live Log Feed**: tails the scanner's `logs/wave_scanner.log` and pushes
  new lines to every connected client (Web + TUI).
- **Signals table**: parses `logs/signals.csv` and displays the last 200
  signals with direction/score/RR.
- **AI Command Center**: in-page terminal emulator for dashboard control
  (`status`, `get logs 50`, `theme matrix`, `clean mode`, `refresh`, …).
- **Generative UI**: `clean mode` hides non-essential widgets, `theme
  matrix/dark/light` swaps the palette, view-switching is keyboard friendly.
- **Backwards compatible**: the original URL `/ai_dashboard.html` still
  loads the new dashboard, so the old `Запуск_Dashboard.bat` works as-is.

## Run it

### Linux / macOS

```bash
cd WAVE_SCANNER_PRO_READY/dashboard
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Start the server (opens browser automatically):
python start_dashboard.py
# or, equivalently:
python server.py
```

Then open <http://localhost:3900/>.

### Windows

Double-click `Запуск_Dashboard.bat` — it creates a `.venv`, installs
dependencies, and launches the server on port 3900.

### Terminal UI

In a separate shell, with the server already running:

```bash
python run_tui.py --url http://localhost:3900
```

Shortcuts: `Q` quit, `Ctrl+R` refresh, `Ctrl+L` clear log, `T` cycle theme,
`L`/`S`/`H` focus logs/signals/health.

## Ollama setup

The AI Council uses [Ollama](https://ollama.com) — a local model runner.

```bash
# 1. Install Ollama from https://ollama.com/download
# 2. Start it (auto-starts on most systems):
ollama serve
# 3. Pull a few free models:
ollama pull llama3
ollama pull qwen2
ollama pull mistral
ollama pull deepseek-coder
```

The dashboard auto-detects whatever models you have pulled. The model
picker shows them in the council toolbar.

## Configuration

```text
python server.py --help

  --host HOST                         bind address (default 0.0.0.0)
  --port PORT                         port (default 3900, auto-fallback if busy)
  --workspace PATH                    sandbox root for the Files panel
                                      (default: parent of dashboard/)
  --conversations-dir PATH            where to save chats (default dashboard/conversations)
  --ollama-url URL                    Ollama base URL (default http://localhost:11434)
  --log-file PATH                     scanner log to tail
  --signals-csv PATH                  CSV path for the signals widget
  --no-port-fallback                  fail instead of choosing a free port
  --reload                            uvicorn dev autoreload
```

Environment variable equivalents: `DASHBOARD_HOST`, `DASHBOARD_PORT`,
`DASHBOARD_WORKSPACE`, `OLLAMA_BASE_URL`, `DASHBOARD_LOG_LEVEL`.

## API

All endpoints return JSON unless noted.

| Method | Path | Description |
| ------ | ---- | ----------- |
| GET    | `/`                          | Dashboard HTML |
| GET    | `/ai_dashboard.html`         | Alias for `/` (legacy) |
| GET    | `/api/health`                | Current system metrics |
| GET    | `/api/logs?limit=N`          | Recent scanner log lines |
| GET    | `/api/signals?limit=N`       | Parsed signals from CSV |
| POST   | `/api/command`               | Dashboard control commands (terminal) |
| GET    | `/api/files/list?path=…`     | Sandboxed directory listing |
| GET    | `/api/files/read?path=…`     | Read a single text file |
| GET    | `/api/conversations`         | List saved chats |
| POST   | `/api/conversations`         | Create a new chat |
| GET    | `/api/conversations/{id}`    | Get one chat |
| POST   | `/api/conversations/{id}/messages` | Append a message |
| DELETE | `/api/conversations/{id}`    | Delete a chat |
| GET    | `/api/ollama/status`         | Ollama reachability + model list |
| GET    | `/api/ollama/models`         | Pulled models |
| POST   | `/api/ollama/chat`           | Single-model SSE stream |
| POST   | `/api/ollama/council`        | Multi-model parallel SSE stream |
| WS     | `/ws`                        | Live metrics & log push |

SSE responses use the standard `data: <json>\n\n` framing. Council events
include a `model` field so the client can route deltas to the right pane.

## Security notes

- The Files panel is **sandboxed** to `--workspace`. Any path that resolves
  outside that directory is rejected with HTTP 403.
- Files larger than 512 KB are truncated when read.
- Binary files (containing `\x00` or undecodable bytes) are not previewed.
- The dashboard does **not** implement authentication. Bind it to
  `127.0.0.1` (the default for `start_dashboard.py`) or put a reverse proxy
  with auth in front of it before exposing it on a network.

"""Thin async client for the Ollama HTTP API.

Two responsibilities only:
  * list locally pulled models   (GET /api/tags)
  * stream a chat completion     (POST /api/chat with stream=true → NDJSON)

All errors are mapped to :class:`OllamaError`, which carries a human-readable
hint so the UI can surface "Ollama is not running" or "model not found"
without leaking raw stack traces.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

logger = logging.getLogger("dashboard.ollama")

DEFAULT_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
CHAT_TIMEOUT = httpx.Timeout(connect=5.0, read=600.0, write=30.0, pool=5.0)
TAGS_TIMEOUT = httpx.Timeout(connect=3.0, read=10.0, write=5.0, pool=3.0)


class OllamaError(RuntimeError):
    """User-facing error with a short hint for the UI."""

    def __init__(self, message: str, hint: str = "") -> None:
        super().__init__(message)
        self.hint = hint


@dataclass
class ChatMessage:
    role: str
    content: str

    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}


def _connection_hint() -> str:
    return (
        "Ollama не отвечает по " + DEFAULT_BASE_URL + ". "
        "Установи https://ollama.com/download, запусти `ollama serve`, "
        "затем `ollama pull llama3` (или другую модель) и обнови страницу."
    )


async def list_models(base_url: str = DEFAULT_BASE_URL) -> List[Dict[str, Any]]:
    """Return models from Ollama. Raises :class:`OllamaError` on failure."""
    url = f"{base_url}/api/tags"
    try:
        async with httpx.AsyncClient(timeout=TAGS_TIMEOUT) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except httpx.ConnectError as e:
        raise OllamaError(f"connect failed: {e}", hint=_connection_hint()) from e
    except httpx.HTTPStatusError as e:
        raise OllamaError(
            f"ollama returned HTTP {e.response.status_code}",
            hint="Проверь, что `ollama serve` запущен и доступен.",
        ) from e
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        raise OllamaError(f"ollama list failed: {e}", hint=_connection_hint()) from e

    models = data.get("models") or []
    out: List[Dict[str, Any]] = []
    for m in models:
        out.append({
            "name": m.get("name") or m.get("model") or "",
            "size": m.get("size", 0),
            "modified_at": m.get("modified_at", ""),
            "family": (m.get("details") or {}).get("family", ""),
            "parameter_size": (m.get("details") or {}).get("parameter_size", ""),
            "quantization_level": (m.get("details") or {}).get("quantization_level", ""),
        })
    out.sort(key=lambda x: x["name"].lower())
    return out


async def chat_stream(
    model: str,
    messages: List[ChatMessage],
    *,
    base_url: str = DEFAULT_BASE_URL,
    options: Optional[Dict[str, Any]] = None,
    cancel_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Yield streaming chunks from Ollama's /api/chat endpoint.

    Each yielded dict has shape:
        {"delta": str, "done": bool, "raw": <full chunk dict>}
    The final `done=True` chunk also includes timing fields from Ollama.
    """
    url = f"{base_url}/api/chat"
    payload: Dict[str, Any] = {
        "model": model,
        "messages": [m.to_dict() for m in messages],
        "stream": True,
    }
    if options:
        payload["options"] = options

    try:
        async with httpx.AsyncClient(timeout=CHAT_TIMEOUT) as client:
            async with client.stream("POST", url, json=payload) as response:
                if response.status_code >= 400:
                    body = (await response.aread()).decode("utf-8", "replace")
                    raise OllamaError(
                        f"chat HTTP {response.status_code}: {body[:500]}",
                        hint=(
                            "Часто причина — модель не загружена. "
                            f"Запусти `ollama pull {model}`."
                        ),
                    )
                async for line in response.aiter_lines():
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    if not line:
                        continue
                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        logger.debug("non-JSON line from ollama: %r", line[:120])
                        continue
                    delta = ((chunk.get("message") or {}).get("content")) or ""
                    yield {"delta": delta, "done": bool(chunk.get("done")), "raw": chunk}
                    if chunk.get("done"):
                        return
    except OllamaError:
        raise
    except httpx.ConnectError as e:
        raise OllamaError(f"connect failed: {e}", hint=_connection_hint()) from e
    except httpx.HTTPError as e:
        raise OllamaError(f"ollama chat failed: {e}", hint=_connection_hint()) from e

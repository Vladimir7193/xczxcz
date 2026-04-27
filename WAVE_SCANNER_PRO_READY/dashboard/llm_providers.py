"""Provider abstraction over local Ollama and OpenAI-compatible cloud APIs.

The dashboard originally talked only to a local Ollama server. This module
adds a thin routing layer so the same /api/ollama/{chat,council,models}
endpoints can also serve cloud-hosted models from Groq, OpenAI, OpenRouter,
DeepInfra, Together — anything that speaks the OpenAI chat-completions
protocol.

Routing is purely by **model-name prefix**:

    qwen3-coder:30b-a3b-q4_K_M       → local Ollama (no prefix)
    groq/llama-3.3-70b-versatile     → Groq
    openai/gpt-4o-mini               → OpenAI
    openrouter/anthropic/claude-3.5  → OpenRouter (path keeps slashes)
    deepinfra/meta-llama/Llama-3-70b → DeepInfra
    together/meta-llama/Llama-3-70b  → Together

A provider only registers itself if its API-key env var is set. The
frontend never has to know about credentials — it just sees a richer model
list returned from /api/ollama/models.

Streaming chunks are normalised to the same shape Ollama already returns:

    {"delta": "<incremental text>", "done": False, "raw": <upstream chunk>}
    {"delta": "",                   "done": True,  "raw": {"eval_count": N, ...}}
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx

from ollama_client import (
    DEFAULT_BASE_URL as OLLAMA_BASE_URL_DEFAULT,
    ChatMessage,
    OllamaError,
    chat_stream as ollama_chat_stream,
    list_models as ollama_list_models,
)

logger = logging.getLogger("dashboard.providers")

CHAT_TIMEOUT = httpx.Timeout(connect=10.0, read=600.0, write=30.0, pool=10.0)
LIST_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)


class LLMError(RuntimeError):
    """User-facing error from any LLM provider; carries a UI hint."""

    def __init__(self, message: str, hint: str = "") -> None:
        super().__init__(message)
        self.hint = hint


@dataclass
class ProviderInfo:
    """Lightweight description of a configured provider for the UI."""

    name: str           # "ollama", "groq", "openai", ...
    label: str          # "Ollama (local)", "Groq", ...
    available: bool     # True if the provider can be queried right now
    reason: str = ""    # why unavailable (e.g. "GROQ_API_KEY not set")
    base_url: str = ""  # human-readable endpoint
    is_local: bool = False


@dataclass
class _OpenAICompatConfig:
    """Static config for one OpenAI-compatible cloud provider."""

    name: str
    label: str
    base_url: str       # e.g. "https://api.groq.com/openai/v1"
    api_key_env: str    # name of env var that holds the key
    extra_headers: Dict[str, str] = field(default_factory=dict)
    # Some providers gate /models behind auth or use a different path; we
    # always hit f"{base_url}/models" with the bearer token.


# Catalogue of known OpenAI-compatible providers. Order matters for the UI
# (we display in this order). Adding a new provider is one entry.
_OPENAI_COMPAT_PROVIDERS: Tuple[_OpenAICompatConfig, ...] = (
    _OpenAICompatConfig(
        name="groq",
        label="Groq",
        base_url="https://api.groq.com/openai/v1",
        api_key_env="GROQ_API_KEY",
    ),
    _OpenAICompatConfig(
        name="openai",
        label="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key_env="OPENAI_API_KEY",
    ),
    _OpenAICompatConfig(
        name="openrouter",
        label="OpenRouter",
        base_url="https://openrouter.ai/api/v1",
        api_key_env="OPENROUTER_API_KEY",
        extra_headers={
            "HTTP-Referer": "https://github.com/Vladimir7193/xczxcz",
            "X-Title": "WAVE Scanner Dashboard",
        },
    ),
    _OpenAICompatConfig(
        name="deepinfra",
        label="DeepInfra",
        base_url="https://api.deepinfra.com/v1/openai",
        api_key_env="DEEPINFRA_API_KEY",
    ),
    _OpenAICompatConfig(
        name="together",
        label="Together AI",
        base_url="https://api.together.xyz/v1",
        api_key_env="TOGETHER_API_KEY",
    ),
)


def _provider_info_unavailable(cfg: _OpenAICompatConfig) -> ProviderInfo:
    return ProviderInfo(
        name=cfg.name,
        label=cfg.label,
        available=False,
        reason=f"{cfg.api_key_env} not set",
        base_url=cfg.base_url,
        is_local=False,
    )


# ---------------------------------------------------------------------------
# OpenAI-compat streaming
# ---------------------------------------------------------------------------


def _ollama_options_to_openai(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Translate Ollama's `options` dict to OpenAI chat-completions params.

    Ollama uses ``num_ctx``, ``num_predict``, ``repeat_penalty``, etc. The
    OpenAI protocol exposes only ``temperature``, ``top_p``, ``max_tokens``,
    ``frequency_penalty``, ``presence_penalty``. Drop fields that have no
    direct equivalent rather than smuggling them through (the cloud API
    will reject unknown params on some providers).
    """
    if not options:
        return {}
    out: Dict[str, Any] = {}
    if "temperature" in options:
        out["temperature"] = float(options["temperature"])
    if "top_p" in options:
        out["top_p"] = float(options["top_p"])
    if "num_predict" in options:
        out["max_tokens"] = int(options["num_predict"])
    if "repeat_penalty" in options:
        # Ollama's repeat_penalty (1.0 = none, 1.1 = slight) maps roughly to
        # OpenAI's frequency_penalty (0 = none, 2 = max). 1.1 → 0.2.
        rp = float(options["repeat_penalty"])
        out["frequency_penalty"] = max(0.0, min(2.0, (rp - 1.0) * 2.0))
    return out


async def _openai_compat_chat_stream(
    cfg: _OpenAICompatConfig,
    api_key: str,
    model: str,
    messages: List[ChatMessage],
    *,
    options: Optional[Dict[str, Any]] = None,
    cancel_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Stream chat completions from any OpenAI-compatible API.

    Yields the same ``{"delta", "done", "raw"}`` shape as the Ollama client,
    so callers don't need to care which provider answered.
    """
    url = f"{cfg.base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        **cfg.extra_headers,
    }
    payload: Dict[str, Any] = {
        "model": model,
        "messages": [m.to_dict() for m in messages],
        "stream": True,
        **_ollama_options_to_openai(options),
    }

    eval_count = 0
    prompt_tokens = 0

    try:
        async with httpx.AsyncClient(timeout=CHAT_TIMEOUT) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as response:
                if response.status_code >= 400:
                    body = (await response.aread()).decode("utf-8", "replace")
                    hint = (
                        f"{cfg.label} вернул HTTP {response.status_code}. "
                        f"Проверь {cfg.api_key_env} и имя модели "
                        f"({model}, без префикса {cfg.name}/)."
                    )
                    raise LLMError(
                        f"{cfg.label} HTTP {response.status_code}: {body[:500]}",
                        hint=hint,
                    )
                async for line in response.aiter_lines():
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    if not line:
                        continue
                    # OpenAI SSE: each line starts with "data: ". Some providers
                    # also emit comment lines starting with ":" — skip those.
                    if line.startswith(":"):
                        continue
                    if line.startswith("data:"):
                        data = line[5:].strip()
                    else:
                        data = line.strip()
                    if not data:
                        continue
                    if data == "[DONE]":
                        yield {
                            "delta": "",
                            "done": True,
                            "raw": {
                                "eval_count": eval_count,
                                "prompt_eval_count": prompt_tokens,
                                "model": model,
                                "provider": cfg.name,
                            },
                        }
                        return
                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        logger.debug("non-JSON SSE line from %s: %r", cfg.name, data[:120])
                        continue
                    # Some providers stream a usage block at the end.
                    usage = chunk.get("usage") or {}
                    if usage:
                        eval_count = int(usage.get("completion_tokens") or 0)
                        prompt_tokens = int(usage.get("prompt_tokens") or 0)
                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta_obj = choices[0].get("delta") or {}
                    delta = delta_obj.get("content") or ""
                    finish = choices[0].get("finish_reason")
                    if delta:
                        yield {"delta": delta, "done": False, "raw": chunk}
                    if finish:
                        yield {
                            "delta": "",
                            "done": True,
                            "raw": {
                                "eval_count": eval_count,
                                "prompt_eval_count": prompt_tokens,
                                "model": model,
                                "provider": cfg.name,
                                "finish_reason": finish,
                            },
                        }
                        return
    except LLMError:
        raise
    except httpx.ConnectError as e:
        raise LLMError(
            f"{cfg.label} connect failed: {e}",
            hint=f"Сеть не пускает к {cfg.base_url}.",
        ) from e
    except httpx.HTTPError as e:
        raise LLMError(
            f"{cfg.label} stream failed: {e}",
            hint=f"Проверь {cfg.api_key_env} и сеть.",
        ) from e


async def _openai_compat_list_models(
    cfg: _OpenAICompatConfig, api_key: str
) -> List[Dict[str, Any]]:
    url = f"{cfg.base_url}/models"
    headers = {"Authorization": f"Bearer {api_key}", **cfg.extra_headers}
    try:
        async with httpx.AsyncClient(timeout=LIST_TIMEOUT) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        raise LLMError(
            f"{cfg.label} list HTTP {e.response.status_code}",
            hint=f"Проверь {cfg.api_key_env}.",
        ) from e
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        raise LLMError(
            f"{cfg.label} list failed: {e}",
            hint="Проверь сеть и ключ.",
        ) from e

    raw = data.get("data") if isinstance(data, dict) else None
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for m in raw:
        ident = m.get("id") or m.get("name")
        if not ident:
            continue
        out.append({
            "name": f"{cfg.name}/{ident}",  # prefixed so router knows where it goes
            "size": 0,                        # cloud models — size unknown
            "modified_at": "",
            "family": cfg.label,
            "parameter_size": "",
            "quantization_level": "cloud",
            "provider": cfg.name,
            "remote_id": ident,
        })
    out.sort(key=lambda x: x["name"].lower())
    return out


# ---------------------------------------------------------------------------
# Public router
# ---------------------------------------------------------------------------


def split_provider(model: str) -> Tuple[str, str]:
    """Return ``(provider_name, model_id_without_prefix)``.

    Unprefixed names go to the local Ollama for backward compatibility.
    """
    if "/" not in model:
        return "ollama", model
    head, _, rest = model.partition("/")
    head = head.lower()
    known = {cfg.name for cfg in _OPENAI_COMPAT_PROVIDERS} | {"ollama"}
    if head in known:
        return head, rest
    # Slash in the model id but not a known provider — treat as ollama (e.g.
    # "library/llama3:8b" custom registries).
    return "ollama", model


def _provider_config(name: str) -> Optional[_OpenAICompatConfig]:
    for cfg in _OPENAI_COMPAT_PROVIDERS:
        if cfg.name == name:
            return cfg
    return None


async def chat_stream_router(
    model: str,
    messages: List[ChatMessage],
    *,
    ollama_base_url: str = OLLAMA_BASE_URL_DEFAULT,
    options: Optional[Dict[str, Any]] = None,
    cancel_event: Optional[asyncio.Event] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """Route a chat call to the right provider based on prefix."""
    provider, real_model = split_provider(model)
    if provider == "ollama":
        # Re-yield from the existing client to keep the contract identical.
        async for chunk in ollama_chat_stream(
            real_model,
            messages,
            base_url=ollama_base_url,
            options=options,
            cancel_event=cancel_event,
        ):
            yield chunk
        return
    cfg = _provider_config(provider)
    if cfg is None:
        raise LLMError(
            f"unknown provider: {provider}",
            hint=f"Используй один из: ollama, " + ", ".join(c.name for c in _OPENAI_COMPAT_PROVIDERS),
        )
    api_key = os.getenv(cfg.api_key_env, "").strip()
    if not api_key:
        raise LLMError(
            f"{cfg.label} disabled: {cfg.api_key_env} not set",
            hint=f"Поставь {cfg.api_key_env} в окружение и перезапусти дашборд.",
        )
    async for chunk in _openai_compat_chat_stream(
        cfg, api_key, real_model, messages, options=options, cancel_event=cancel_event
    ):
        yield chunk


async def list_models_router(
    *, ollama_base_url: str = OLLAMA_BASE_URL_DEFAULT
) -> Tuple[List[Dict[str, Any]], List[ProviderInfo]]:
    """Aggregate models from every available provider in parallel.

    Returns (models, providers) where models is the merged list (each item
    carries a ``provider`` field) and providers is the per-provider status
    so the UI can show "groq ✓" / "openai ✗ — set OPENAI_API_KEY".
    """
    providers: List[ProviderInfo] = []
    tasks: List["asyncio.Future[List[Dict[str, Any]]]"] = []
    task_provider: List[str] = []

    # Always attempt local Ollama; mark unavailable if the call fails.
    async def _ollama_task() -> List[Dict[str, Any]]:
        items = await ollama_list_models(ollama_base_url)
        # Tag each item so the frontend knows where it's from.
        for it in items:
            it.setdefault("provider", "ollama")
        return items

    tasks.append(asyncio.ensure_future(_ollama_task()))
    task_provider.append("ollama")

    for cfg in _OPENAI_COMPAT_PROVIDERS:
        api_key = os.getenv(cfg.api_key_env, "").strip()
        if not api_key:
            providers.append(_provider_info_unavailable(cfg))
            continue
        tasks.append(asyncio.ensure_future(_openai_compat_list_models(cfg, api_key)))
        task_provider.append(cfg.name)

    merged: List[Dict[str, Any]] = []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for name, res in zip(task_provider, results):
        if name == "ollama":
            if isinstance(res, Exception):
                providers.append(ProviderInfo(
                    name="ollama",
                    label="Ollama (local)",
                    available=False,
                    reason=str(res)[:200],
                    base_url=ollama_base_url,
                    is_local=True,
                ))
            else:
                providers.insert(0, ProviderInfo(
                    name="ollama",
                    label="Ollama (local)",
                    available=True,
                    base_url=ollama_base_url,
                    is_local=True,
                ))
                merged.extend(res)
            continue
        cfg = _provider_config(name)
        if cfg is None:
            continue
        if isinstance(res, Exception):
            providers.append(ProviderInfo(
                name=cfg.name,
                label=cfg.label,
                available=False,
                reason=str(res)[:200],
                base_url=cfg.base_url,
                is_local=False,
            ))
        else:
            providers.append(ProviderInfo(
                name=cfg.name,
                label=cfg.label,
                available=True,
                base_url=cfg.base_url,
                is_local=False,
            ))
            merged.extend(res)

    merged.sort(key=lambda x: x.get("name", "").lower())
    return merged, providers


__all__ = [
    "ChatMessage",
    "LLMError",
    "OllamaError",
    "ProviderInfo",
    "chat_stream_router",
    "list_models_router",
    "split_provider",
]

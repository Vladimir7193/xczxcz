"""On-disk conversation store.

Conversations are tiny JSON blobs (`{"id", "title", "created_at",
"updated_at", "messages": [...]}`) saved into ``dashboard/conversations/``.
This is intentionally simple — no DB — so power-users can grep / version
their chats with the rest of their bot code.
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("dashboard.conversations")

_SAFE_ID = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


@dataclass
class Conversation:
    id: str
    title: str
    created_at: float
    updated_at: float
    messages: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ConversationStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, conv_id: str) -> Path:
        if not _SAFE_ID.match(conv_id):
            raise ValueError(f"invalid conversation id: {conv_id!r}")
        return self.root / f"{conv_id}.json"

    def list(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for p in sorted(self.root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            out.append({
                "id": data.get("id", p.stem),
                "title": data.get("title", p.stem),
                "updated_at": data.get("updated_at", p.stat().st_mtime),
                "messages": len(data.get("messages") or []),
            })
        return out

    def get(self, conv_id: str) -> Optional[Conversation]:
        p = self._path(conv_id)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("conversation load failed for %s: %s", p, e)
            return None
        return Conversation(
            id=data.get("id", conv_id),
            title=data.get("title", conv_id),
            created_at=float(data.get("created_at") or 0.0),
            updated_at=float(data.get("updated_at") or 0.0),
            messages=list(data.get("messages") or []),
        )

    def create(self, title: str = "") -> Conversation:
        cid = uuid.uuid4().hex[:12]
        now = time.time()
        conv = Conversation(id=cid, title=title or "New chat", created_at=now,
                            updated_at=now, messages=[])
        self.save(conv)
        return conv

    def save(self, conv: Conversation) -> None:
        conv.updated_at = time.time()
        path = self._path(conv.id)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(conv.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def append(self, conv_id: str, message: Dict[str, Any]) -> Conversation:
        conv = self.get(conv_id) or self.create()
        conv.messages.append(message)
        if conv.title in {"", "New chat"}:
            user_first = next((m for m in conv.messages if m.get("role") == "user"), None)
            if user_first and user_first.get("content"):
                conv.title = str(user_first["content"]).strip().splitlines()[0][:80]
        self.save(conv)
        return conv

    def delete(self, conv_id: str) -> bool:
        p = self._path(conv_id)
        if not p.exists():
            return False
        p.unlink()
        return True

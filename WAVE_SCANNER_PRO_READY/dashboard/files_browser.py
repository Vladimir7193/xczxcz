"""Sandboxed local-files browser used by the dashboard.

Everything is rooted at a single ``workspace`` directory (passed by the user
on the CLI). Any path that — after resolution — leaves that directory raises
:class:`PathOutsideWorkspace`, so the dashboard can never read arbitrary
files on the host.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("dashboard.files")

MAX_READ_BYTES = 512 * 1024  # 512 KB

# Heuristic: we only show / read files that look like text or structured
# config the council might reason about.
TEXT_EXTENSIONS = {
    ".py", ".pyi", ".js", ".jsx", ".ts", ".tsx", ".json", ".yaml", ".yml",
    ".toml", ".ini", ".cfg", ".env", ".md", ".rst", ".txt", ".log", ".csv",
    ".tsv", ".html", ".htm", ".css", ".scss", ".less", ".sh", ".bash", ".zsh",
    ".bat", ".ps1", ".sql", ".r", ".rb", ".go", ".rs", ".c", ".h", ".cpp",
    ".hpp", ".java", ".kt", ".swift", ".pine", ".dockerfile", "",
}

SKIP_DIRS = {
    "__pycache__", ".git", ".hg", ".svn", "node_modules", ".venv", "venv",
    "env", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".idea", ".vscode",
    "dist", "build", "site-packages", ".cache", "cache",
}


class PathOutsideWorkspace(PermissionError):
    pass


@dataclass
class Workspace:
    root: Path

    @classmethod
    def from_path(cls, raw: str | os.PathLike[str]) -> "Workspace":
        root = Path(raw).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise FileNotFoundError(f"workspace path does not exist: {root}")
        return cls(root=root)

    def resolve(self, rel: str) -> Path:
        """Resolve `rel` relative to root; reject anything that escapes."""
        rel = (rel or "").lstrip("/").replace("\\", "/")
        candidate = (self.root / rel).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise PathOutsideWorkspace(f"{candidate} is outside {self.root}") from exc
        return candidate

    def relpath(self, p: Path) -> str:
        try:
            return p.resolve().relative_to(self.root).as_posix()
        except ValueError:
            return p.as_posix()

    def is_text_file(self, p: Path) -> bool:
        if p.suffix.lower() in TEXT_EXTENSIONS:
            return True
        # Probe-read: if a small head decodes as utf-8 without NULs, treat as text.
        try:
            with p.open("rb") as fh:
                head = fh.read(2048)
        except OSError:
            return False
        if b"\x00" in head:
            return False
        try:
            head.decode("utf-8")
            return True
        except UnicodeDecodeError:
            return False


def list_dir(ws: Workspace, rel: str = "") -> Dict[str, Any]:
    target = ws.resolve(rel)
    if not target.exists():
        raise FileNotFoundError(rel)
    if not target.is_dir():
        raise NotADirectoryError(rel)

    entries: List[Dict[str, Any]] = []
    for child in sorted(target.iterdir(), key=lambda c: (not c.is_dir(), c.name.lower())):
        if child.name in SKIP_DIRS:
            continue
        try:
            stat = child.stat()
        except OSError:
            continue
        is_dir = child.is_dir()
        entries.append({
            "name": child.name,
            "path": ws.relpath(child),
            "is_dir": is_dir,
            "size": 0 if is_dir else stat.st_size,
            "mtime": stat.st_mtime,
            "text": False if is_dir else ws.is_text_file(child),
        })

    parent: Optional[str] = None
    if target != ws.root:
        parent = ws.relpath(target.parent)
        if parent == ".":
            parent = ""

    return {
        "ok": True,
        "root": str(ws.root),
        "path": ws.relpath(target),
        "parent": parent,
        "entries": entries,
    }


def read_text(ws: Workspace, rel: str, max_bytes: int = MAX_READ_BYTES) -> Dict[str, Any]:
    target = ws.resolve(rel)
    if not target.exists() or not target.is_file():
        raise FileNotFoundError(rel)
    if not ws.is_text_file(target):
        raise ValueError(f"{rel} does not look like a text file")
    size = target.stat().st_size
    truncated = size > max_bytes
    with target.open("rb") as fh:
        raw = fh.read(max_bytes)
    text = raw.decode("utf-8", errors="replace")
    return {
        "ok": True,
        "path": ws.relpath(target),
        "size": size,
        "truncated": truncated,
        "max_bytes": max_bytes,
        "content": text,
    }

"""User-facing path normalization and default storage locations."""

from __future__ import annotations

import os
import platform
import re
from pathlib import Path

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


def clean_user_path_text(raw: str) -> str:
    """Normalize a path string copied, typed, or drag-dropped by a user."""
    text = raw.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1]
    text = text.replace("\\ ", " ")
    return text.strip()


def looks_like_path_text(raw: str) -> bool:
    """Return True when the raw input looks like a filesystem path."""
    text = clean_user_path_text(raw)
    if not text:
        return False
    if "/" in text or "\\" in text:
        return True
    if text.startswith(("~", ".", "..")):
        return True
    return bool(_WINDOWS_DRIVE_RE.match(text))


def normalize_user_path(raw: str | Path, *, base_dir: Path | None = None) -> Path:
    """Resolve a user-supplied path without requiring it to already exist."""
    if isinstance(raw, Path):
        path = raw.expanduser()
    else:
        cleaned = clean_user_path_text(raw)
        if not cleaned:
            raise ValueError("Path text is empty.")
        path = Path(cleaned).expanduser()
    if not path.is_absolute():
        anchor = base_dir or Path.cwd()
        path = anchor / path
    return path.resolve(strict=False)


def default_app_state_root() -> Path:
    """Return the per-user default root for durable green2blue artifacts."""
    system = platform.system()
    if system == "Darwin":
        return (Path.home() / "Library" / "Application Support" / "green2blue").resolve()
    if system == "Windows":
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            return (Path(local_appdata) / "green2blue").resolve()
        return (Path.home() / "AppData" / "Local" / "green2blue").resolve()
    return (Path.home() / ".green2blue").resolve()

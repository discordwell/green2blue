"""Local browser review tool for Android export filtering."""

from __future__ import annotations

import copy
import io
import json
import threading
import webbrowser
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urlparse

from green2blue.parser.zip_reader import ExtractedExport, open_export_zip
from green2blue.user_paths import default_app_state_root

ANDROID_ATTACHMENT_ROOT = "/data/user/0/com.android.providers.telephony/app_parts"


@dataclass(frozen=True)
class ReviewMessage:
    id: str
    conversation_id: str
    conversation_label: str
    addresses: tuple[str, ...]
    primary_address: str
    kind: str
    direction: str
    timestamp_ms: int
    body_text: str
    subject: str | None
    attachment_count: int
    raw_record: dict[str, object]
    attachment_sources: tuple[tuple[int, Path], ...]


@dataclass(frozen=True)
class ReviewConversation:
    id: str
    label: str
    addresses: tuple[str, ...]
    primary_address: str
    message_count: int
    attachment_count: int
    latest_timestamp_ms: int


@dataclass(frozen=True)
class ReviewWorkflowContext:
    """Extra metadata for wizard-driven review sessions."""

    title: str
    summary: str
    next_step: str


@dataclass(frozen=True)
class ReviewWorkflowResult:
    """Result returned when a wizard review session completes."""

    action: str
    export_zip: Path | None


class ReviewSession:
    """Parsed export state backing the local review UI."""

    def __init__(
        self,
        export_zip: Path,
        export: ExtractedExport,
        messages: tuple[ReviewMessage, ...],
    ):
        self.export_zip = export_zip
        self.export = export
        self.messages = messages
        self.conversations = _build_conversations(messages)
        self.stats = _build_stats(messages, self.conversations)

    def payload(self, workflow_context: ReviewWorkflowContext | None = None) -> dict[str, object]:
        return {
            "export_name": self.export_zip.name,
            "workflow": (
                {
                    "mode": "wizard",
                    "title": workflow_context.title,
                    "summary": workflow_context.summary,
                    "next_step": workflow_context.next_step,
                }
                if workflow_context is not None
                else None
            ),
            "stats": dict(self.stats),
            "conversations": [
                {
                    "id": conversation.id,
                    "label": conversation.label,
                    "addresses": list(conversation.addresses),
                    "primary_address": conversation.primary_address,
                    "message_count": conversation.message_count,
                    "attachment_count": conversation.attachment_count,
                    "latest_timestamp_ms": conversation.latest_timestamp_ms,
                }
                for conversation in self.conversations
            ],
            "messages": [
                {
                    "id": message.id,
                    "conversation_id": message.conversation_id,
                    "conversation_label": message.conversation_label,
                    "addresses": list(message.addresses),
                    "primary_address": message.primary_address,
                    "kind": message.kind,
                    "direction": message.direction,
                    "timestamp_ms": message.timestamp_ms,
                    "body_text": message.body_text,
                    "subject": message.subject,
                    "attachment_count": message.attachment_count,
                }
                for message in self.messages
            ],
        }

    def export_selected_zip(self, selected_ids: set[str]) -> bytes:
        payload = io.BytesIO()
        self.write_selected_zip(payload, selected_ids)
        return payload.getvalue()

    def write_selected_zip(self, target: BinaryIO, selected_ids: set[str]) -> None:
        """Write a filtered export ZIP to ``target`` without buffering it in memory."""
        if not selected_ids:
            raise ValueError("No messages selected.")

        selected_messages = [message for message in self.messages if message.id in selected_ids]
        if not selected_messages:
            raise ValueError("The selected message IDs do not exist in this export.")

        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            ndjson_lines: list[str] = []
            for message in selected_messages:
                record = copy.deepcopy(message.raw_record)
                for part_index, source_path in message.attachment_sources:
                    if source_path is None or not source_path.exists():
                        continue
                    raw_parts = record.get("__parts")
                    if not isinstance(raw_parts, list) or part_index >= len(raw_parts):
                        continue
                    part = raw_parts[part_index]
                    if not isinstance(part, dict):
                        continue
                    basename = _export_attachment_name(message.id, part_index, part)
                    archive_name = f"data/{basename}"
                    part["_data"] = f"{ANDROID_ATTACHMENT_ROOT}/{basename}"
                    part["cl"] = basename
                    zf.write(source_path, arcname=archive_name)

                ndjson_lines.append(json.dumps(record, ensure_ascii=False, separators=(",", ":")))

            zf.writestr("messages.ndjson", "\n".join(ndjson_lines) + "\n")


@contextmanager
def open_review_session(export_zip: Path | str) -> Generator[ReviewSession, None, None]:
    export_path = Path(export_zip)
    with open_export_zip(export_path) as export:
        messages = tuple(_load_review_messages(export))
        yield ReviewSession(export_path, export, messages)


def _start_review_server(
    session: ReviewSession,
    *,
    host: str,
    port: int,
    open_browser: bool,
    instructions: str,
    workflow_context: ReviewWorkflowContext | None = None,
) -> tuple[_ReviewHTTPServer, str]:
    server = _ReviewHTTPServer(
        (host, port),
        _make_review_handler(session, workflow_context=workflow_context),
    )
    url = f"http://{host}:{server.server_address[1]}"
    print(f"Review UI: {url}")
    print(instructions)
    if open_browser:
        webbrowser.open(url)
    return server, url


def serve_review_app(
    export_zip: Path | str,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    open_browser: bool = True,
) -> str:
    """Launch the local browser review UI for an Android export."""
    with open_review_session(export_zip) as session:
        server, url = _start_review_server(
            session,
            host=host,
            port=port,
            open_browser=open_browser,
            instructions="Press Ctrl+C to stop the review server.",
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping review server.")
        finally:
            server.server_close()
        return url


def run_review_workflow(
    export_zip: Path | str,
    workflow_context: ReviewWorkflowContext,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    open_browser: bool = True,
) -> ReviewWorkflowResult:
    """Launch the review UI and wait for a wizard action from the browser.

    Raises KeyboardInterrupt if the user aborts from the terminal, so the
    wizard's normal Ctrl+C handling (exit code 130) applies.
    """
    with open_review_session(export_zip) as session:
        server, _ = _start_review_server(
            session,
            host=host,
            port=port,
            open_browser=open_browser,
            instructions="Use the browser buttons to continue the wizard.",
            workflow_context=workflow_context,
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping review server.")
            raise
        finally:
            server.server_close()

        return server.workflow_result or ReviewWorkflowResult(
            action="cancel",
            export_zip=None,
        )


class _ReviewHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow_result: ReviewWorkflowResult | None = None
        self._workflow_lock = threading.Lock()

    def claim_workflow_result(self, result: ReviewWorkflowResult) -> bool:
        """Record the workflow decision; only the first decision wins."""
        with self._workflow_lock:
            if self.workflow_result is not None:
                return False
            self.workflow_result = result
            return True


def _make_review_handler(
    session: ReviewSession,
    *,
    workflow_context: ReviewWorkflowContext | None = None,
):
    class ReviewHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._write_bytes(
                    HTTPStatus.OK,
                    _REVIEW_HTML.encode("utf-8"),
                    "text/html; charset=utf-8",
                )
                return
            if parsed.path == "/api/data":
                self._write_bytes(
                    HTTPStatus.OK,
                    json.dumps(
                        session.payload(workflow_context=workflow_context),
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
                return
            self._write_bytes(
                HTTPStatus.NOT_FOUND,
                b"Not found",
                "text/plain; charset=utf-8",
            )

        def do_POST(self) -> None:  # noqa: N802
            if not self._origin_allowed():
                self._write_bytes(
                    HTTPStatus.FORBIDDEN,
                    b"Cross-origin requests are not allowed.",
                    "text/plain; charset=utf-8",
                )
                return
            parsed = urlparse(self.path)
            if parsed.path == "/api/export":
                self._handle_export()
                return
            if parsed.path == "/api/apply" and workflow_context is not None:
                self._handle_workflow_apply()
                return
            self._write_bytes(
                HTTPStatus.NOT_FOUND,
                b"Not found",
                "text/plain; charset=utf-8",
            )

        def _origin_allowed(self) -> bool:
            """Reject cross-origin POSTs so other pages cannot drive the workflow."""
            origin = self.headers.get("Origin")
            if origin is None:
                return True
            host, port = self.server.server_address[:2]
            return origin in {
                f"http://{host}:{port}",
                f"http://127.0.0.1:{port}",
                f"http://localhost:{port}",
            }

        def _handle_export(self) -> None:
            payload = self._read_json_payload()
            if payload is None:
                return

            selected_ids = self._validated_selected_ids(payload)
            if selected_ids is None:
                return

            try:
                zip_bytes = session.export_selected_zip(selected_ids)
            except ValueError as exc:
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    str(exc).encode("utf-8"),
                    "text/plain; charset=utf-8",
                )
                return

            filename = f"{session.export_zip.stem}.filtered.zip"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(zip_bytes)))
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            self.wfile.write(zip_bytes)

        def _handle_workflow_apply(self) -> None:
            payload = self._read_json_payload()
            if payload is None:
                return

            action = payload.get("action")
            if action not in ("continue_full", "continue_selected", "cancel"):
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"Unknown workflow action.",
                    "text/plain; charset=utf-8",
                )
                return

            if action == "continue_full":
                result = ReviewWorkflowResult(action="full", export_zip=session.export_zip)
            elif action == "cancel":
                result = ReviewWorkflowResult(action="cancel", export_zip=None)
            else:
                selected_ids = self._validated_selected_ids(payload)
                if selected_ids is None:
                    return
                try:
                    output_path = _write_reviewed_export(session, selected_ids)
                except ValueError as exc:
                    self._write_bytes(
                        HTTPStatus.BAD_REQUEST,
                        str(exc).encode("utf-8"),
                        "text/plain; charset=utf-8",
                    )
                    return
                except OSError as exc:
                    self._write_bytes(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        f"Could not save the reviewed export: {exc}".encode(),
                        "text/plain; charset=utf-8",
                    )
                    return
                result = ReviewWorkflowResult(action="filtered", export_zip=output_path)

            if not self.server.claim_workflow_result(result):
                if result.action == "filtered" and result.export_zip is not None:
                    result.export_zip.unlink(missing_ok=True)
                self._write_bytes(
                    HTTPStatus.CONFLICT,
                    b"A workflow decision was already submitted.",
                    "text/plain; charset=utf-8",
                )
                return

            response: dict[str, object] = {"status": "ok", "action": result.action}
            if result.action == "filtered":
                response["path"] = str(result.export_zip)
            try:
                self._write_bytes(
                    HTTPStatus.OK,
                    json.dumps(response).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
            finally:
                # The decision is recorded; stop serving even if the browser
                # disconnected before the response could be delivered.
                self._shutdown_server()

        def log_message(self, _format: str, *_args) -> None:
            return

        def _write_bytes(self, status: HTTPStatus, payload: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _read_json_payload(self) -> dict[str, object] | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= _MAX_REQUEST_BYTES:
                    raise ValueError(f"Content-Length out of range: {length}")
                payload = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeDecodeError):
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"Invalid JSON payload.",
                    "text/plain; charset=utf-8",
                )
                return None
            if not isinstance(payload, dict):
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"JSON payload must be an object.",
                    "text/plain; charset=utf-8",
                )
                return None
            return payload

        def _validated_selected_ids(self, payload: dict[str, object]) -> set[str] | None:
            selected_ids = payload.get("selected_ids")
            if not isinstance(selected_ids, list) or not all(
                isinstance(item, str) for item in selected_ids
            ):
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"selected_ids must be a list of message IDs.",
                    "text/plain; charset=utf-8",
                )
                return None
            return set(selected_ids)

        def _shutdown_server(self) -> None:
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    return ReviewHandler


_MAX_REQUEST_BYTES = 64 * 1024 * 1024

_REVIEWED_EXPORTS_TO_KEEP = 5


def _write_reviewed_export(session: ReviewSession, selected_ids: set[str]) -> Path:
    """Stream a filtered export to the app state dir and prune older ones."""
    output_dir = default_app_state_root() / "reviewed_exports"
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_path = output_dir / f"{session.export_zip.stem}.{stamp}.filtered.zip"
    try:
        with output_path.open("wb") as fh:
            session.write_selected_zip(fh, selected_ids)
    except Exception:
        output_path.unlink(missing_ok=True)
        raise
    _prune_reviewed_exports(output_dir, keep=_REVIEWED_EXPORTS_TO_KEEP)
    return output_path


def _prune_reviewed_exports(output_dir: Path, *, keep: int) -> None:
    try:
        reviewed = sorted(
            (path for path in output_dir.glob("*.filtered.zip") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for stale in reviewed[keep:]:
            stale.unlink(missing_ok=True)
    except OSError:
        pass


def _load_review_messages(export: ExtractedExport) -> list[ReviewMessage]:
    messages: list[ReviewMessage] = []
    with export.ndjson_path.open(encoding="utf-8") as fh:
        for line_number, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Match the inspect/inject parsers, which skip malformed lines
                # instead of refusing the whole export.
                continue
            if not isinstance(record, dict):
                continue
            if _is_mms_record(record):
                messages.append(_build_mms_review_message(export, line_number, record))
            elif "body" in record and "address" in record:
                messages.append(_build_sms_review_message(line_number, record))
    return messages


def _build_sms_review_message(line_number: int, record: dict[str, object]) -> ReviewMessage:
    address = str(record.get("address", ""))
    conversation_id = json.dumps([address])
    body_text = str(record.get("body") or "")
    timestamp_ms = int(record.get("date") or 0)
    msg_type = int(record.get("type") or 1)
    return ReviewMessage(
        id=f"line-{line_number}",
        conversation_id=conversation_id,
        conversation_label=address or "(unknown)",
        addresses=(address,),
        primary_address=address,
        kind="sms",
        direction=_sms_direction(msg_type),
        timestamp_ms=timestamp_ms,
        body_text=body_text,
        subject=None,
        attachment_count=0,
        raw_record=record,
        attachment_sources=(),
    )


def _build_mms_review_message(
    export: ExtractedExport,
    line_number: int,
    record: dict[str, object],
) -> ReviewMessage:
    addresses = tuple(_mms_addresses(record))
    conversation_id = json.dumps(sorted(addresses))
    parts = record.get("__parts") if isinstance(record.get("__parts"), list) else []
    text_parts: list[str] = []
    attachment_sources: list[tuple[int, Path]] = []
    for part_index, raw_part in enumerate(parts):
        if not isinstance(raw_part, dict):
            continue
        text_value = raw_part.get("text")
        if isinstance(text_value, str) and text_value:
            text_parts.append(text_value)
        source_path = _resolve_attachment_source(export, raw_part.get("_data"))
        if source_path is not None:
            attachment_sources.append((part_index, source_path))

    timestamp = int(record.get("date") or 0)
    timestamp_ms = timestamp * 1000 if timestamp < 10_000_000_000 else timestamp
    label = ", ".join(addresses) if addresses else "(group MMS)"
    return ReviewMessage(
        id=f"line-{line_number}",
        conversation_id=conversation_id,
        conversation_label=label,
        addresses=addresses,
        primary_address=addresses[0] if addresses else "(unknown)",
        kind="mms",
        direction=_mms_direction(int(record.get("msg_box") or 1)),
        timestamp_ms=timestamp_ms,
        body_text="\n".join(text_parts),
        subject=str(record.get("sub")) if record.get("sub") else None,
        attachment_count=len(attachment_sources),
        raw_record=record,
        attachment_sources=tuple(attachment_sources),
    )


def _build_conversations(messages: tuple[ReviewMessage, ...]) -> tuple[ReviewConversation, ...]:
    grouped: dict[str, list[ReviewMessage]] = {}
    for message in messages:
        grouped.setdefault(message.conversation_id, []).append(message)

    conversations = []
    for conversation_id, group in grouped.items():
        first = group[0]
        conversations.append(
            ReviewConversation(
                id=conversation_id,
                label=first.conversation_label,
                addresses=first.addresses,
                primary_address=first.primary_address,
                message_count=len(group),
                attachment_count=sum(message.attachment_count for message in group),
                latest_timestamp_ms=max(message.timestamp_ms for message in group),
            )
        )

    conversations.sort(
        key=lambda conversation: (conversation.primary_address, -conversation.latest_timestamp_ms)
    )
    return tuple(conversations)


def _build_stats(
    messages: tuple[ReviewMessage, ...],
    conversations: tuple[ReviewConversation, ...],
) -> dict[str, int]:
    sms_messages = 0
    mms_messages = 0
    messages_with_attachments = 0
    total_attachments = 0
    for message in messages:
        if message.kind == "sms":
            sms_messages += 1
        elif message.kind == "mms":
            mms_messages += 1
        if message.attachment_count:
            messages_with_attachments += 1
            total_attachments += message.attachment_count
    return {
        "messages": len(messages),
        "conversations": len(conversations),
        "attachments": total_attachments,
        "sms_messages": sms_messages,
        "mms_messages": mms_messages,
        "messages_with_attachments": messages_with_attachments,
    }


def _is_mms_record(record: dict[str, object]) -> bool:
    return any(
        key in record
        for key in ("__parts", "__addresses", "__sender_address", "__recipient_addresses")
    )


def _sms_direction(msg_type: int) -> str:
    if msg_type == 1:
        return "incoming"
    if msg_type == 2:
        return "outgoing"
    return "unknown"


def _mms_direction(msg_box: int) -> str:
    if msg_box == 1:
        return "incoming"
    if msg_box == 2:
        return "outgoing"
    return "unknown"


def _mms_addresses(record: dict[str, object]) -> list[str]:
    addresses: list[str] = []
    sender = record.get("__sender_address")
    recipients = record.get("__recipient_addresses")
    if isinstance(sender, dict):
        address = str(sender.get("address", ""))
        if address:
            addresses.append(address)
    if isinstance(recipients, list):
        for raw_addr in recipients:
            if not isinstance(raw_addr, dict):
                continue
            address = str(raw_addr.get("address", ""))
            if address and address not in addresses:
                addresses.append(address)
    legacy = record.get("__addresses")
    if isinstance(legacy, list):
        for raw_addr in legacy:
            if not isinstance(raw_addr, dict):
                continue
            address = str(raw_addr.get("address", ""))
            if address and address not in addresses:
                addresses.append(address)
    return addresses


def _resolve_attachment_source(export: ExtractedExport, data_path: object) -> Path | None:
    if export.data_dir is None or not isinstance(data_path, str) or not data_path:
        return None

    raw = Path(data_path)
    candidates: list[Path] = []
    if raw.is_absolute():
        candidates.append(export.data_dir / raw.name)
    else:
        candidates.append(export.temp_dir / raw)
        if raw.parts and raw.parts[0] == "data":
            candidates.append(export.data_dir / Path(*raw.parts[1:]))
        candidates.append(export.data_dir / raw.name)

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate
    return None


def _export_attachment_name(message_id: str, part_index: int, part: dict[str, object]) -> str:
    raw_name = (
        str(part.get("cl") or part.get("fn") or part.get("name") or part.get("_data") or "")
    ).strip()
    basename = Path(raw_name).name or f"attachment_{part_index}"
    safe_basename = basename.replace("/", "_").replace("\\", "_")
    return f"{message_id}_{part_index}_{safe_basename}"


_REVIEW_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>green2blue Review</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4efe4;
      --bg-strong: #e8dcc4;
      --panel: rgba(255, 251, 244, 0.94);
      --panel-strong: #fff7ea;
      --line: #d7c7ab;
      --line-strong: #b9a27a;
      --ink: #1f231d;
      --muted: #665f52;
      --accent: #2f6b49;
      --accent-strong: #1f4e34;
      --accent-soft: #dcebdc;
      --warning-soft: #f2e4c9;
      --shadow: 0 20px 50px rgba(61, 47, 29, 0.12);
      --radius: 22px;
      --radius-sm: 14px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(255, 255, 255, 0.72), transparent 28%),
        linear-gradient(180deg, #efe4cf 0%, var(--bg) 24%, #f7f4ed 100%);
      padding-bottom: 96px;
    }
    button, input, select {
      font: inherit;
    }
    button {
      cursor: pointer;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fffdf8;
      color: var(--ink);
      padding: 10px 14px;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease;
    }
    button:hover:not(:disabled) {
      transform: translateY(-1px);
      border-color: var(--line-strong);
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.5;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      box-shadow: 0 10px 24px rgba(47, 107, 73, 0.22);
    }
    button.primary:hover:not(:disabled) {
      background: var(--accent-strong);
      border-color: var(--accent-strong);
    }
    button.ghost {
      background: transparent;
    }
    input, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 11px 13px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--ink);
    }
    input:focus, select:focus, button:focus {
      outline: 2px solid rgba(47, 107, 73, 0.18);
      outline-offset: 2px;
    }
    .hero {
      position: sticky;
      top: 0;
      z-index: 20;
      padding: 24px 24px 18px;
      backdrop-filter: blur(16px);
      background: rgba(247, 242, 232, 0.88);
      border-bottom: 1px solid rgba(185, 162, 122, 0.34);
    }
    .hero-bar {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
    }
    .eyebrow {
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    h1 {
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      font-size: 34px;
      line-height: 1.05;
    }
    .hero-copy {
      max-width: 760px;
    }
    .hero-summary {
      margin: 10px 0 0;
      color: var(--muted);
      line-height: 1.5;
    }
    .hero-note {
      margin-top: 14px;
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(185, 162, 122, 0.44);
      background: rgba(255, 255, 255, 0.72);
      color: var(--muted);
      line-height: 1.5;
    }
    .hero-note strong {
      color: var(--ink);
    }
    .hero-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .hero-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.74);
      border: 1px solid rgba(185, 162, 122, 0.52);
      color: var(--muted);
      white-space: nowrap;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 18px;
    }
    .stat-card {
      background: linear-gradient(180deg, rgba(255, 252, 246, 0.96), rgba(251, 244, 232, 0.88));
      border: 1px solid rgba(185, 162, 122, 0.38);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 10px 26px rgba(61, 47, 29, 0.06);
    }
    .stat-label {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 10px;
    }
    .stat-value {
      font-size: 28px;
      line-height: 1;
      font-weight: 700;
    }
    .stat-meta {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(300px, 340px) minmax(0, 1fr);
      gap: 18px;
      padding: 18px 24px 24px;
      align-items: start;
    }
    .workspace {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(280px, 0.8fr);
      gap: 18px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid rgba(185, 162, 122, 0.46);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      min-height: 0;
      overflow: hidden;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
      padding: 18px 18px 14px;
      border-bottom: 1px solid rgba(185, 162, 122, 0.28);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.58), rgba(255, 248, 236, 0.32));
    }
    .panel-head h2 {
      margin: 0;
      font-size: 20px;
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
    }
    .panel-subtitle {
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .panel-actions, .inline-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .sidebar-controls {
      display: grid;
      gap: 10px;
      padding: 16px 18px;
      border-bottom: 1px solid rgba(185, 162, 122, 0.24);
      background: rgba(255, 250, 241, 0.72);
    }
    .toggle-row, .select-row {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .toggle {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border: 1px solid rgba(185, 162, 122, 0.44);
      background: rgba(255, 255, 255, 0.72);
      border-radius: 14px;
      color: var(--muted);
      min-width: 0;
    }
    .toggle input {
      width: auto;
      margin: 0;
      padding: 0;
    }
    .list {
      padding: 10px;
      max-height: calc(100vh - 340px);
      overflow: auto;
    }
    .conversation-row,
    .message-row {
      display: grid;
      gap: 12px;
      padding: 12px;
      border-radius: 18px;
      border: 1px solid transparent;
      background: transparent;
      margin-bottom: 8px;
      transition: border-color 140ms ease, background 140ms ease, transform 140ms ease;
    }
    .conversation-row:hover,
    .message-row:hover {
      background: rgba(255, 255, 255, 0.68);
      border-color: rgba(185, 162, 122, 0.34);
    }
    .conversation-row.active,
    .message-row.active {
      background: linear-gradient(180deg, rgba(220, 235, 220, 0.94), rgba(245, 249, 239, 0.92));
      border-color: rgba(47, 107, 73, 0.38);
      transform: translateY(-1px);
    }
    .conversation-row {
      grid-template-columns: auto minmax(0, 1fr);
      align-items: start;
    }
    .message-row {
      grid-template-columns: auto minmax(0, 1fr);
      align-items: start;
    }
    .row-checkbox {
      margin-top: 4px;
      width: auto;
    }
    .row-main {
      border: 0;
      padding: 0;
      background: transparent;
      border-radius: 0;
      text-align: left;
      width: 100%;
      min-width: 0;
      box-shadow: none;
    }
    .row-main:hover:not(:disabled) {
      transform: none;
      border-color: transparent;
    }
    .row-title {
      font-size: 16px;
      font-weight: 700;
      margin: 0;
      color: var(--ink);
    }
    .row-meta {
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .row-preview {
      margin-top: 8px;
      color: #353126;
      font-size: 14px;
      line-height: 1.45;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
      white-space: pre-wrap;
    }
    .chip-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(255, 247, 234, 0.92);
      border: 1px solid rgba(185, 162, 122, 0.42);
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .pill.selected {
      background: rgba(220, 235, 220, 0.92);
      border-color: rgba(47, 107, 73, 0.34);
      color: var(--accent-strong);
    }
    .messages-panel .list {
      max-height: calc(100vh - 385px);
    }
    .detail-body {
      padding: 18px;
      display: grid;
      gap: 16px;
    }
    .detail-section {
      background: rgba(255, 255, 255, 0.62);
      border: 1px solid rgba(185, 162, 122, 0.26);
      border-radius: 18px;
      padding: 14px;
    }
    .detail-label {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 8px;
    }
    .detail-copy {
      line-height: 1.55;
      white-space: pre-wrap;
      color: #2a271f;
      word-break: break-word;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .empty {
      padding: 26px 18px;
      border: 1px dashed rgba(185, 162, 122, 0.54);
      border-radius: 18px;
      background: rgba(255, 252, 247, 0.72);
      color: var(--muted);
      line-height: 1.5;
    }
    .footer-note {
      padding: 0 18px 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .export-bar {
      position: fixed;
      left: 18px;
      right: 18px;
      bottom: 18px;
      z-index: 30;
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
      padding: 14px 16px;
      border-radius: 22px;
      border: 1px solid rgba(185, 162, 122, 0.46);
      background: rgba(255, 250, 242, 0.94);
      backdrop-filter: blur(14px);
      box-shadow: 0 18px 36px rgba(61, 47, 29, 0.18);
    }
    .export-copy {
      min-width: 0;
    }
    .export-title {
      font-weight: 700;
      margin-bottom: 4px;
    }
    .export-meta {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .export-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
      align-items: center;
    }
    @media (max-width: 1260px) {
      .workspace {
        grid-template-columns: minmax(0, 1fr);
      }
      .detail-grid {
        grid-template-columns: 1fr;
      }
    }
    @media (max-width: 1040px) {
      .hero-bar,
      .layout {
        grid-template-columns: 1fr;
        display: grid;
      }
      .stat-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .layout {
        padding-top: 16px;
      }
      .list,
      .messages-panel .list {
        max-height: none;
      }
    }
    @media (max-width: 760px) {
      body {
        padding-bottom: 136px;
      }
      .hero,
      .layout {
        padding-left: 16px;
        padding-right: 16px;
      }
      .stat-grid,
      .toggle-row,
      .select-row,
      .detail-grid {
        grid-template-columns: 1fr;
      }
      .hero-actions,
      .panel-actions,
      .inline-actions,
      .export-actions,
      .export-bar {
        justify-content: flex-start;
      }
      .export-bar {
        left: 12px;
        right: 12px;
        bottom: 12px;
        padding: 14px;
        display: grid;
      }
      .export-actions button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <header class="hero">
    <div class="hero-bar">
      <div class="hero-copy">
        <div class="eyebrow">Local Review</div>
        <h1>Review Android export before you trim it.</h1>
        <p class="hero-summary" id="heroSummary"></p>
        <div class="hero-note" id="workflowNote" hidden></div>
      </div>
      <div class="hero-actions">
        <div class="hero-chip" id="selectionChip">0 messages selected</div>
        <button type="button" id="clearSelection" class="ghost">Clear selection</button>
      </div>
    </div>
    <div class="stat-grid" id="statGrid"></div>
  </header>
  <main class="layout">
    <aside class="panel">
      <div class="panel-head">
        <div>
          <h2>Conversations</h2>
          <div class="panel-subtitle" id="conversationSummary"></div>
        </div>
      </div>
      <div class="sidebar-controls">
        <input id="searchFilter" placeholder="Search participants or message text">
        <div class="select-row">
          <select id="kindFilter">
            <option value="all">All message types</option>
            <option value="sms">SMS only</option>
            <option value="mms">MMS only</option>
          </select>
          <select id="conversationSort">
            <option value="latest_desc">Latest activity</option>
            <option value="count_desc">Most messages</option>
            <option value="phone_asc">Phone A-Z</option>
            <option value="phone_desc">Phone Z-A</option>
          </select>
        </div>
        <div class="toggle-row">
          <label class="toggle">
            <input type="checkbox" id="attachmentsOnly">
            Attachments only
          </label>
          <label class="toggle">
            <input type="checkbox" id="selectedOnly">
            Selected only
          </label>
        </div>
        <div class="inline-actions">
          <button type="button" id="selectVisibleConversations">Select visible</button>
          <button type="button" id="deselectVisibleConversations" class="ghost">
            Clear visible
          </button>
        </div>
      </div>
      <div class="list" id="conversationList"></div>
      <div class="footer-note" id="conversationFooter"></div>
    </aside>
    <section class="workspace">
      <section class="panel messages-panel">
        <div class="panel-head">
          <div>
            <h2 id="activeConversationTitle">Messages</h2>
            <div class="panel-subtitle" id="activeConversationMeta"></div>
          </div>
          <div class="panel-actions">
            <button type="button" id="selectActiveConversation">Select conversation</button>
            <button type="button" id="deselectActiveConversation" class="ghost">
              Clear conversation
            </button>
          </div>
        </div>
        <div class="sidebar-controls">
          <div class="inline-actions">
            <button type="button" id="selectFilteredMessages">Select filtered messages</button>
            <button type="button" id="deselectFilteredMessages" class="ghost">
              Clear filtered messages
            </button>
          </div>
        </div>
        <div class="list" id="messageList"></div>
        <div class="footer-note" id="messageFooter"></div>
      </section>
      <aside class="panel">
        <div class="panel-head">
          <div>
            <h2>Details</h2>
            <div class="panel-subtitle" id="detailSummary"></div>
          </div>
        </div>
        <div class="detail-body" id="detailCard"></div>
      </aside>
    </section>
  </main>
  <div class="export-bar">
    <div class="export-copy">
      <div class="export-title" id="exportTitle"></div>
      <div class="export-meta" id="exportMeta"></div>
    </div>
    <div class="export-actions">
      <button type="button" id="continueFull" class="ghost" hidden>
        Continue with full export
      </button>
      <button type="button" id="cancelWorkflow" class="ghost" hidden>Cancel wizard</button>
      <button type="button" id="selectAllFiltered" class="ghost">Select all filtered</button>
      <button type="button" id="clearFiltered" class="ghost">Clear filtered</button>
      <button type="button" id="exportSelected" class="primary">Export selected ZIP</button>
    </div>
  </div>
  <script>
    const state = {
      data: null,
      derived: null,
      selectedMessageIds: new Set(),
      filters: {
        query: "",
        kind: "all",
        attachmentsOnly: false,
        selectedOnly: false,
        conversationSort: "latest_desc",
      },
      activeConversationId: null,
      activeMessageId: null,
      workflowPending: false,
      workflowDoneAction: null,
    };

    function fmtTime(ms) {
      if (!ms) return "No timestamp";
      return new Date(ms).toLocaleString();
    }

    function fmtNumber(value) {
      return Number(value || 0).toLocaleString();
    }

    function el(tag, className, text) {
      const node = document.createElement(tag);
      if (className) node.className = className;
      if (text !== undefined) node.textContent = text;
      return node;
    }

    function previewForMessage(message) {
      return message.body_text || message.subject || "(attachment only)";
    }

    function workflowContext() {
      return state.data ? state.data.workflow : null;
    }

    function isWorkflowMode() {
      return Boolean(workflowContext());
    }

    function buildIndexes(data) {
      data.messageById = new Map();
      data.messagesByConversation = new Map();
      data.conversationById = new Map();
      data.messages.forEach((message) => {
        message.searchText = [
          message.primary_address,
          message.conversation_label,
          ...(message.addresses || []),
          message.body_text || "",
          message.subject || "",
        ].join(" ").toLowerCase();
        data.messageById.set(message.id, message);
        if (!data.messagesByConversation.has(message.conversation_id)) {
          data.messagesByConversation.set(message.conversation_id, []);
        }
        data.messagesByConversation.get(message.conversation_id).push(message);
      });
      data.messagesByConversation.forEach((messages) => {
        messages.sort((a, b) =>
          a.timestamp_ms - b.timestamp_ms
            || a.id.localeCompare(b.id, undefined, { numeric: true }));
      });
      data.conversations.forEach((conversation) => {
        data.conversationById.set(conversation.id, conversation);
      });
    }

    function messagesFor(conversationId) {
      return state.data.messagesByConversation.get(conversationId) || [];
    }

    function conversationSelectionState(conversationId, derived) {
      const total = messagesFor(conversationId).length;
      const selected = derived.selectedCountByConversation.get(conversationId) || 0;
      return {
        selected,
        total,
        all: total > 0 && selected === total,
        partial: selected > 0 && selected < total,
      };
    }

    function selectMessages(messages, shouldSelect) {
      messages.forEach((message) => {
        if (shouldSelect) state.selectedMessageIds.add(message.id);
        else state.selectedMessageIds.delete(message.id);
      });
    }

    function matchesFilters(message, needle) {
      if (state.filters.kind !== "all" && message.kind !== state.filters.kind) return false;
      if (state.filters.attachmentsOnly && message.attachment_count === 0) return false;
      if (state.filters.selectedOnly && !state.selectedMessageIds.has(message.id)) return false;
      return !needle || message.searchText.includes(needle);
    }

    function sortConversations(conversations) {
      const sort = state.filters.conversationSort;
      conversations.sort((a, b) => {
        if (sort === "phone_asc") return a.primary_address.localeCompare(b.primary_address);
        if (sort === "phone_desc") return b.primary_address.localeCompare(a.primary_address);
        if (sort === "count_desc") {
          return b.message_count - a.message_count
            || b.latest_timestamp_ms - a.latest_timestamp_ms
            || a.primary_address.localeCompare(b.primary_address);
        }
        return b.latest_timestamp_ms - a.latest_timestamp_ms
          || b.message_count - a.message_count
          || a.primary_address.localeCompare(b.primary_address);
      });
      return conversations;
    }

    function computeDerivedState() {
      if (!state.data) {
        return {
          filteredMessages: [],
          filteredMessagesByConversation: new Map(),
          visibleConversations: [],
          activeConversation: null,
          activeMessages: [],
          activeMessage: null,
          selectedConversations: 0,
          selectedAttachments: 0,
          selectedCountByConversation: new Map(),
        };
      }

      const needle = state.filters.query.trim().toLowerCase();
      const filteredMessages = [];
      const filteredMessagesByConversation = new Map();

      state.data.messagesByConversation.forEach((messages, conversationId) => {
        const matching = messages.filter((message) => matchesFilters(message, needle));
        if (!matching.length) return;
        filteredMessagesByConversation.set(conversationId, matching);
        filteredMessages.push(...matching);
      });

      const visibleConversations = sortConversations(
        state.data.conversations.filter((conversation) =>
          filteredMessagesByConversation.has(conversation.id)
        ).slice()
      );

      if (
        !visibleConversations.some(
          (conversation) => conversation.id === state.activeConversationId
        )
      ) {
        state.activeConversationId = visibleConversations.length
          ? visibleConversations[0].id
          : null;
      }

      const activeConversation = state.activeConversationId
        ? state.data.conversationById.get(state.activeConversationId) || null
        : null;
      const activeMessages = activeConversation
        ? (filteredMessagesByConversation.get(activeConversation.id) || [])
        : [];

      if (!activeMessages.some((message) => message.id === state.activeMessageId)) {
        state.activeMessageId = activeMessages.length
          ? activeMessages[activeMessages.length - 1].id
          : null;
      }

      const activeMessage = state.activeMessageId
        ? state.data.messageById.get(state.activeMessageId) || null
        : null;

      const selectedCountByConversation = new Map();
      let selectedAttachments = 0;
      state.data.messages.forEach((message) => {
        if (!state.selectedMessageIds.has(message.id)) return;
        selectedCountByConversation.set(
          message.conversation_id,
          (selectedCountByConversation.get(message.conversation_id) || 0) + 1
        );
        selectedAttachments += message.attachment_count;
      });

      return {
        filteredMessages,
        filteredMessagesByConversation,
        visibleConversations,
        activeConversation,
        activeMessages,
        activeMessage,
        selectedConversations: selectedCountByConversation.size,
        selectedAttachments,
        selectedCountByConversation,
      };
    }

    function renderHero(derived) {
      const stats = state.data.stats;
      const heroSummary = document.getElementById("heroSummary");
      const selectionChip = document.getElementById("selectionChip");
      const workflowNote = document.getElementById("workflowNote");
      const statGrid = document.getElementById("statGrid");
      const workflow = workflowContext();

      heroSummary.textContent = [
        `${state.data.export_name} contains ${fmtNumber(stats.messages)} messages`,
        `across ${fmtNumber(stats.conversations)} conversations.`,
        "Filter down to the people and moments you want to keep,",
        "then export a trimmed ZIP.",
      ].join(" ");
      selectionChip.textContent = `${fmtNumber(state.selectedMessageIds.size)} messages selected`;

      if (workflow) {
        workflowNote.hidden = false;
        workflowNote.textContent = "";
        workflowNote.appendChild(el("strong", null, workflow.title));
        workflowNote.appendChild(document.createTextNode(` ${workflow.summary}`));
        workflowNote.appendChild(el("br"));
        workflowNote.appendChild(el("strong", null, "Next:"));
        workflowNote.appendChild(document.createTextNode(` ${workflow.next_step}`));
      } else {
        workflowNote.hidden = true;
        workflowNote.textContent = "";
      }

      statGrid.innerHTML = "";
      const cards = [
        {
          label: "All Messages",
          value: fmtNumber(stats.messages),
          meta: `${fmtNumber(stats.sms_messages)} SMS and ${fmtNumber(stats.mms_messages)} MMS`,
        },
        {
          label: "Attachment Coverage",
          value: fmtNumber(stats.attachments),
          meta: `${fmtNumber(stats.messages_with_attachments)} messages include attachments`,
        },
        {
          label: "Visible Now",
          value: fmtNumber(derived.filteredMessages.length),
          meta: [
            `${fmtNumber(derived.visibleConversations.length)} conversations`,
            "match current filters",
          ].join(" "),
        },
        {
          label: "Selected",
          value: fmtNumber(state.selectedMessageIds.size),
          meta: selectionSummary(derived),
        },
      ];

      cards.forEach((card) => {
        const item = el("section", "stat-card");
        item.appendChild(el("div", "stat-label", card.label));
        item.appendChild(el("div", "stat-value", card.value));
        item.appendChild(el("div", "stat-meta", card.meta));
        statGrid.appendChild(item);
      });
    }

    function renderConversations(derived) {
      const list = document.getElementById("conversationList");
      const footer = document.getElementById("conversationFooter");
      const summary = document.getElementById("conversationSummary");
      const conversations = derived.visibleConversations;

      list.innerHTML = "";
      summary.textContent = `${fmtNumber(conversations.length)} matching conversations`;

      if (!conversations.length) {
        const empty = el("div", "empty");
        empty.textContent = [
          "No conversations match the current filters.",
          "Clear a filter or broaden your search.",
        ].join(" ");
        list.appendChild(empty);
      } else {
        conversations.forEach((conversation) => {
          const row = el(
            "div",
            `conversation-row${conversation.id === state.activeConversationId ? " active" : ""}`
          );

          const checkbox = el("input", "row-checkbox");
          checkbox.type = "checkbox";
          const selection = conversationSelectionState(conversation.id, derived);
          checkbox.checked = selection.all;
          checkbox.indeterminate = selection.partial;
          checkbox.addEventListener("change", () => {
            selectMessages(messagesFor(conversation.id), checkbox.checked);
            render();
          });

          const main = el("button", "row-main");
          main.type = "button";
          main.addEventListener("click", () => {
            state.activeConversationId = conversation.id;
            const messages =
              derived.filteredMessagesByConversation.get(conversation.id) || [];
            state.activeMessageId = messages.length ? messages[messages.length - 1].id : null;
            render();
          });

          main.appendChild(el("div", "row-title", conversation.label));

          const meta = el(
            "div",
            "row-meta",
            [
              `${fmtNumber(conversation.message_count)} messages,`,
              `${fmtNumber(conversation.attachment_count)} attachments,`,
              `latest ${fmtTime(conversation.latest_timestamp_ms)}`,
            ].join(" ")
          );
          main.appendChild(meta);

          const latestMessage = (
            derived.filteredMessagesByConversation.get(conversation.id) || []
          ).slice(-1)[0];
          if (latestMessage) {
            main.appendChild(el("div", "row-preview", previewForMessage(latestMessage)));
          }

          const chips = el("div", "chip-row");
          const addressChip = el("span", "pill", conversation.primary_address);
          chips.appendChild(addressChip);
          if (selection.selected) {
            chips.appendChild(
              el("span", "pill selected", `${fmtNumber(selection.selected)} selected`)
            );
          }
          main.appendChild(chips);

          row.appendChild(checkbox);
          row.appendChild(main);
          list.appendChild(row);
        });
      }
      footer.textContent = `${fmtNumber(conversations.length)} visible conversation(s)`;
    }

    function renderMessages(derived) {
      const title = document.getElementById("activeConversationTitle");
      const meta = document.getElementById("activeConversationMeta");
      const list = document.getElementById("messageList");
      const footer = document.getElementById("messageFooter");

      const conversation = derived.activeConversation;
      const messages = derived.activeMessages;

      if (!conversation) {
        title.textContent = "Messages";
        meta.textContent = "Choose a conversation to review its timeline.";
      } else {
        title.textContent = conversation.label;
        meta.textContent = [
          `${fmtNumber(messages.length)} filtered messages shown for`,
          conversation.primary_address,
        ].join(" ");
      }

      list.innerHTML = "";
      if (!conversation) {
        const empty = el("div", "empty");
        empty.textContent = [
          "No conversation is active.",
          "Pick one from the left to inspect its messages.",
        ].join(" ");
        list.appendChild(empty);
      } else if (!messages.length) {
        const empty = el("div", "empty");
        empty.textContent = [
          "This conversation does not have any messages",
          "that match the current filters.",
        ].join(" ");
        list.appendChild(empty);
      } else {
        messages.forEach((message) => {
          const row = el(
            "div",
            `message-row${message.id === state.activeMessageId ? " active" : ""}`
          );

          const checkbox = el("input", "row-checkbox");
          checkbox.type = "checkbox";
          checkbox.checked = state.selectedMessageIds.has(message.id);
          checkbox.addEventListener("change", () => {
            selectMessages([message], checkbox.checked);
            render();
          });

          const main = el("button", "row-main");
          main.type = "button";
          main.addEventListener("click", () => {
            state.activeMessageId = message.id;
            render();
          });

          main.appendChild(el("div", "row-title", previewForMessage(message)));
          main.appendChild(
            el(
              "div",
              "row-meta",
              [
                fmtTime(message.timestamp_ms),
                message.kind.toUpperCase(),
                message.direction,
              ].join(" - ")
            )
          );
          main.appendChild(el("div", "row-preview", `${message.primary_address}`));

          const chips = el("div", "chip-row");
          chips.appendChild(el("span", "pill", message.kind.toUpperCase()));
          chips.appendChild(el("span", "pill", message.direction));
          if (message.attachment_count) {
            chips.appendChild(
              el("span", "pill selected", `${fmtNumber(message.attachment_count)} attachments`)
            );
          }
          if (state.selectedMessageIds.has(message.id)) {
            chips.appendChild(el("span", "pill selected", "Selected"));
          }
          main.appendChild(chips);

          row.appendChild(checkbox);
          row.appendChild(main);
          list.appendChild(row);
        });
      }
      footer.textContent = `${fmtNumber(messages.length)} visible message(s)`;
    }

    function renderDetail(derived) {
      const summary = document.getElementById("detailSummary");
      const card = document.getElementById("detailCard");
      const message = derived.activeMessage;

      card.innerHTML = "";

      if (!message) {
        summary.textContent = "No message selected";
        const empty = el("div", "empty");
        empty.textContent = [
          "Pick a message to inspect its timestamp,",
          "participants, text, and attachment count.",
        ].join(" ");
        card.appendChild(empty);
        return;
      }

      summary.textContent = `${message.kind.toUpperCase()} from ${fmtTime(message.timestamp_ms)}`;

      const metaSection = el("section", "detail-section");
      metaSection.appendChild(el("div", "detail-label", "Message Snapshot"));
      const grid = el("div", "detail-grid");

      [
        ["Conversation", message.conversation_label],
        ["Primary Address", message.primary_address],
        ["Direction", message.direction],
        ["Attachment Count", fmtNumber(message.attachment_count)],
      ].forEach(([label, value]) => {
        const item = el("div", "detail-copy");
        item.textContent = `${label}: ${value}`;
        grid.appendChild(item);
      });
      metaSection.appendChild(grid);
      card.appendChild(metaSection);

      const participants = el("section", "detail-section");
      participants.appendChild(el("div", "detail-label", "Participants"));
      participants.appendChild(
        el("div", "detail-copy", (message.addresses || []).length
          ? message.addresses.join(", ")
          : message.primary_address)
      );
      card.appendChild(participants);

      if (message.subject) {
        const subject = el("section", "detail-section");
        subject.appendChild(el("div", "detail-label", "Subject"));
        subject.appendChild(el("div", "detail-copy", message.subject));
        card.appendChild(subject);
      }

      const body = el("section", "detail-section");
      body.appendChild(el("div", "detail-label", "Message Text"));
      body.appendChild(el("div", "detail-copy", previewForMessage(message)));
      card.appendChild(body);
    }

    function selectionSummary(derived, suffix) {
      return [
        `${fmtNumber(derived.selectedConversations)} conversations and`,
        `${fmtNumber(derived.selectedAttachments)} attachment files`,
        suffix,
      ].filter(Boolean).join(" ");
    }

    function renderExportBar(derived) {
      const selectedCount = state.selectedMessageIds.size;
      const exportTitle = document.getElementById("exportTitle");
      const exportMeta = document.getElementById("exportMeta");
      const exportButton = document.getElementById("exportSelected");
      const clearSelectionButton = document.getElementById("clearSelection");
      const clearFilteredButton = document.getElementById("clearFiltered");
      const selectAllFilteredButton = document.getElementById("selectAllFiltered");
      const continueFullButton = document.getElementById("continueFull");
      const cancelWorkflowButton = document.getElementById("cancelWorkflow");
      const workflow = workflowContext();

      continueFullButton.hidden = !workflow;
      cancelWorkflowButton.hidden = !workflow;
      exportButton.textContent = workflow ? "Use selection and continue" : "Export selected ZIP";

      if (state.workflowDoneAction) {
        exportTitle.textContent = state.workflowDoneAction === "cancel"
          ? "Wizard cancelled."
          : "Done — your choice was sent to the wizard.";
        exportMeta.textContent = state.workflowDoneAction === "cancel"
          ? "You can close this tab."
          : "Return to the terminal to finish the import. You can close this tab.";
        [
          exportButton, clearSelectionButton, clearFilteredButton,
          selectAllFilteredButton, continueFullButton, cancelWorkflowButton,
        ].forEach((button) => { button.disabled = true; });
        return;
      }

      if (!selectedCount) {
        exportTitle.textContent = "No messages selected yet";
        exportMeta.textContent = workflow
          ? "Select messages to keep, or continue with the full export unchanged."
          : "Select entire conversations or individual messages, then export a trimmed ZIP.";
      } else {
        exportTitle.textContent = workflow
          ? `${fmtNumber(selectedCount)} messages ready for the wizard`
          : `${fmtNumber(selectedCount)} messages ready to export`;
        exportMeta.textContent = selectionSummary(
          derived,
          workflow ? "will continue into the rest of the wizard." : "will be included."
        );
      }

      const busy = state.workflowPending;
      exportButton.disabled = busy || selectedCount === 0;
      clearSelectionButton.disabled = busy || selectedCount === 0;
      clearFilteredButton.disabled = busy || derived.filteredMessages.length === 0;
      selectAllFilteredButton.disabled = busy || derived.filteredMessages.length === 0;
      continueFullButton.disabled = busy;
      cancelWorkflowButton.disabled = busy;
    }

    function render() {
      if (!state.data) return;
      const derived = computeDerivedState();
      state.derived = derived;
      renderHero(derived);
      renderConversations(derived);
      renderMessages(derived);
      renderDetail(derived);
      renderExportBar(derived);
      syncActionStates(derived);
    }

    function syncActionStates(derived) {
      const activeConversation = derived.activeConversation;
      document.getElementById("selectActiveConversation").disabled = !activeConversation;
      document.getElementById("deselectActiveConversation").disabled = !activeConversation;
      document.getElementById("selectFilteredMessages").disabled = !derived.activeMessages.length;
      document.getElementById("deselectFilteredMessages").disabled = !derived.activeMessages.length;
      document.getElementById("selectVisibleConversations").disabled =
        !derived.visibleConversations.length;
      document.getElementById("deselectVisibleConversations").disabled =
        !derived.visibleConversations.length;
    }

    function hookFilter(id, key, accessor = (element) => element.value, debounceMs = 0) {
      const element = document.getElementById(id);
      let timer = null;
      element.addEventListener("input", (event) => {
        state.filters[key] = accessor(event.target);
        if (!debounceMs) {
          render();
          return;
        }
        clearTimeout(timer);
        timer = setTimeout(render, debounceMs);
      });
    }

    async function submitWorkflowAction(action, selectedIds = []) {
      if (state.workflowPending || state.workflowDoneAction) return;
      state.workflowPending = true;
      render();
      try {
        const response = await fetch("/api/apply", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action, selected_ids: selectedIds }),
        });
        if (!response.ok) {
          alert(await response.text());
          return;
        }
        const body = await response.json();
        state.workflowDoneAction = body.action;
      } catch (error) {
        alert(
          "Could not reach the review server. If the wizard already resumed "
            + "in the terminal, you can close this tab."
        );
      } finally {
        state.workflowPending = false;
        render();
      }
    }

    async function exportSelected() {
      const selectedIds = Array.from(state.selectedMessageIds);
      if (!selectedIds.length) {
        alert("Select at least one message before continuing.");
        return;
      }

      if (isWorkflowMode()) {
        await submitWorkflowAction("continue_selected", selectedIds);
        return;
      }

      let response;
      try {
        response = await fetch("/api/export", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ selected_ids: selectedIds }),
        });
      } catch (error) {
        alert("Could not reach the review server. It may have been stopped.");
        return;
      }
      if (!response.ok) {
        alert(await response.text());
        return;
      }
      const blob = await response.blob();
      const disposition = response.headers.get("Content-Disposition") || "";
      const match = disposition.match(/filename="([^"]+)"/);
      const filename = match ? match[1] : "filtered-export.zip";
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function hookSelectionButton(id, getMessages, shouldSelect) {
      document.getElementById(id).addEventListener("click", () => {
        selectMessages(getMessages(state.derived), shouldSelect);
        render();
      });
    }

    function visibleConversationMessages(derived) {
      const messages = [];
      derived.visibleConversations.forEach((conversation) => {
        messages.push(...messagesFor(conversation.id));
      });
      return messages;
    }

    function activeConversationMessages(derived) {
      return derived.activeConversation ? messagesFor(derived.activeConversation.id) : [];
    }

    async function boot() {
      try {
        const response = await fetch("/api/data");
        if (!response.ok) throw new Error(await response.text());
        state.data = await response.json();
      } catch (error) {
        document.getElementById("heroSummary").textContent =
          "Could not load the export from the review server. "
            + "Check the terminal for errors, then reload this page.";
        return;
      }
      buildIndexes(state.data);
      hookFilter("searchFilter", "query", undefined, 150);
      hookFilter("kindFilter", "kind");
      hookFilter("conversationSort", "conversationSort");
      hookFilter("attachmentsOnly", "attachmentsOnly", (element) => element.checked);
      hookFilter("selectedOnly", "selectedOnly", (element) => element.checked);
      hookSelectionButton("selectVisibleConversations", visibleConversationMessages, true);
      hookSelectionButton("deselectVisibleConversations", visibleConversationMessages, false);
      hookSelectionButton("selectFilteredMessages", (derived) => derived.activeMessages, true);
      hookSelectionButton("deselectFilteredMessages", (derived) => derived.activeMessages, false);
      hookSelectionButton("selectActiveConversation", activeConversationMessages, true);
      hookSelectionButton("deselectActiveConversation", activeConversationMessages, false);
      hookSelectionButton("selectAllFiltered", (derived) => derived.filteredMessages, true);
      hookSelectionButton("clearFiltered", (derived) => derived.filteredMessages, false);
      document.getElementById("clearSelection").addEventListener("click", () => {
        state.selectedMessageIds.clear();
        render();
      });
      document.getElementById("continueFull").addEventListener("click", async () => {
        await submitWorkflowAction("continue_full");
      });
      document.getElementById("cancelWorkflow").addEventListener("click", async () => {
        await submitWorkflowAction("cancel");
      });
      document.getElementById("exportSelected").addEventListener("click", exportSelected);
      render();
    }

    boot();
  </script>
</body>
</html>
"""

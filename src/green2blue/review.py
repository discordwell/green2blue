"""Local browser review tool for Android export filtering."""

from __future__ import annotations

import copy
import io
import json
import webbrowser
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from green2blue.parser.zip_reader import ExtractedExport, open_export_zip

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

    def payload(self) -> dict[str, object]:
        return {
            "export_name": self.export_zip.name,
            "stats": {
                "messages": len(self.messages),
                "conversations": len(self.conversations),
                "attachments": sum(message.attachment_count for message in self.messages),
            },
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
        if not selected_ids:
            raise ValueError("No messages selected.")

        selected_messages = [message for message in self.messages if message.id in selected_ids]
        if not selected_messages:
            raise ValueError("The selected message IDs do not exist in this export.")

        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as zf:
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

        return payload.getvalue()


@contextmanager
def open_review_session(export_zip: Path | str) -> Generator[ReviewSession, None, None]:
    export_path = Path(export_zip)
    with open_export_zip(export_path) as export:
        messages = tuple(_load_review_messages(export))
        yield ReviewSession(export_path, export, messages)


def serve_review_app(
    export_zip: Path | str,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    open_browser: bool = True,
) -> str:
    """Launch the local browser review UI for an Android export."""
    with open_review_session(export_zip) as session:
        server = _ReviewHTTPServer((host, port), _make_review_handler(session))
        url = f"http://{host}:{server.server_address[1]}"
        print(f"Review UI: {url}")
        print("Press Ctrl+C to stop the review server.")
        if open_browser:
            webbrowser.open(url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping review server.")
        finally:
            server.server_close()
        return url


class _ReviewHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


def _make_review_handler(session: ReviewSession):
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
                    json.dumps(session.payload(), ensure_ascii=False).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
                return
            self._write_bytes(
                HTTPStatus.NOT_FOUND,
                b"Not found",
                "text/plain; charset=utf-8",
            )

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/api/export":
                self._write_bytes(
                    HTTPStatus.NOT_FOUND,
                    b"Not found",
                    "text/plain; charset=utf-8",
                )
                return

            length = int(self.headers.get("Content-Length", "0"))
            try:
                payload = json.loads(self.rfile.read(length))
            except json.JSONDecodeError:
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"Invalid JSON payload.",
                    "text/plain; charset=utf-8",
                )
                return

            selected_ids = payload.get("selected_ids")
            if not isinstance(selected_ids, list) or not all(
                isinstance(item, str) for item in selected_ids
            ):
                self._write_bytes(
                    HTTPStatus.BAD_REQUEST,
                    b"selected_ids must be a list of message IDs.",
                    "text/plain; charset=utf-8",
                )
                return

            try:
                zip_bytes = session.export_selected_zip(set(selected_ids))
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

        def log_message(self, _format: str, *_args) -> None:
            return

        def _write_bytes(self, status: HTTPStatus, payload: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return ReviewHandler


def _load_review_messages(export: ExtractedExport) -> list[ReviewMessage]:
    messages: list[ReviewMessage] = []
    with export.ndjson_path.open(encoding="utf-8") as fh:
        for line_number, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line:
                continue
            record = json.loads(line)
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
    :root { color-scheme: light; --bg:#f5f1e8; --panel:#fffaf0;
      --line:#d7ccb7; --ink:#1e1c19; --muted:#6b6257;
      --accent:#2d6a4f; --accent-soft:#d9efe3; }
    body { margin:0; font-family: "Iowan Old Style",
      "Palatino Linotype", Georgia, serif;
      background:linear-gradient(180deg,#efe7d7,#f8f5ed);
      color:var(--ink); }
    header { padding:18px 24px; border-bottom:1px solid var(--line);
      background:rgba(255,250,240,.92); position:sticky; top:0;
      backdrop-filter:blur(10px); z-index:3; }
    h1 { margin:0 0 8px; font-size:24px; }
    .stats, .controls { display:flex; flex-wrap:wrap;
      gap:10px 16px; align-items:center; }
    .stats span { color:var(--muted); font-size:14px; }
    .controls { margin-top:12px; }
    input, select, button { font:inherit;
      border:1px solid var(--line); background:white;
      border-radius:10px; padding:8px 10px; }
    button { cursor:pointer; background:var(--accent-soft);
      border-color:#a5cdb8; }
    button.primary { background:var(--accent); color:white; border-color:var(--accent); }
    button.ghost { background:white; }
    main { display:grid;
      grid-template-columns: minmax(280px, 32%) 1fr;
      gap:16px; padding:16px; }
    .panel { background:var(--panel);
      border:1px solid var(--line); border-radius:16px;
      overflow:hidden; min-height:70vh; }
    .panel h2 { margin:0; padding:14px 16px; font-size:18px;
      border-bottom:1px solid var(--line);
      background:rgba(255,255,255,.75); }
    .panel-body { padding:12px; overflow:auto;
      max-height:calc(100vh - 220px); }
    .row { display:grid; gap:8px; padding:10px 8px;
      border-bottom:1px solid #efe7d7; }
    .row:last-child { border-bottom:none; }
    .conversation-row { grid-template-columns:auto 1fr auto;
      align-items:start; }
    .message-row {
      grid-template-columns:auto 160px 110px 1fr auto;
      align-items:start; }
    .label { font-weight:600; }
    .meta, .preview { color:var(--muted); font-size:13px; }
    .preview { white-space:pre-wrap; }
    .pill { display:inline-block; font-size:12px;
      padding:2px 8px; border-radius:999px;
      background:#efe7d7; color:var(--muted); }
    .footer { padding:12px 16px;
      border-top:1px solid var(--line); display:flex;
      justify-content:space-between; align-items:center;
      gap:12px; color:var(--muted); font-size:13px; }
    .empty { padding:20px; color:var(--muted); }
    @media (max-width: 900px) {
      main { grid-template-columns: 1fr; }
      .panel-body { max-height:none; }
      .message-row { grid-template-columns:auto 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>green2blue Export Review</h1>
    <div class="stats" id="stats"></div>
    <div class="controls">
      <input id="phoneFilter" placeholder="Filter by phone number / participant">
      <input id="textFilter" placeholder="Filter by message text">
      <select id="kindFilter">
        <option value="all">All message types</option>
        <option value="sms">SMS only</option>
        <option value="mms">MMS only</option>
      </select>
      <label><input type="checkbox" id="attachmentsOnly"> Attachments only</label>
      <label><input type="checkbox" id="selectedOnly"> Selected only</label>
      <select id="conversationSort">
        <option value="phone_asc">Phone # A-Z</option>
        <option value="phone_desc">Phone # Z-A</option>
        <option value="latest_desc">Latest first</option>
        <option value="count_desc">Most messages</option>
      </select>
      <button id="selectVisibleConversations">Select visible #s</button>
      <button id="deselectVisibleConversations" class="ghost">Deselect visible #s</button>
      <button id="selectVisibleMessages">Select visible messages</button>
      <button id="deselectVisibleMessages" class="ghost">Deselect visible messages</button>
      <button id="selectAll" class="ghost">Select all</button>
      <button id="deselectAll" class="ghost">Deselect all</button>
      <button id="exportSelected" class="primary">Export selected ZIP</button>
    </div>
  </header>
  <main>
    <section class="panel">
      <h2>Conversations / Phone Numbers</h2>
      <div class="panel-body" id="conversationList"></div>
      <div class="footer" id="conversationFooter"></div>
    </section>
    <section class="panel">
      <h2>Messages</h2>
      <div class="panel-body" id="messageList"></div>
      <div class="footer" id="messageFooter"></div>
    </section>
  </main>
  <script>
    const state = {
      data: null,
      selectedMessageIds: new Set(),
      filters: {
        phone: "",
        text: "",
        kind: "all",
        attachmentsOnly: false,
        selectedOnly: false,
        conversationSort: "phone_asc",
      },
    };

    function fmtTime(ms) {
      if (!ms) return "(no timestamp)";
      return new Date(ms).toLocaleString();
    }

    function visibleMessages() {
      if (!state.data) return [];
      const phoneNeedle = state.filters.phone.trim().toLowerCase();
      const textNeedle = state.filters.text.trim().toLowerCase();
      return state.data.messages.filter((message) => {
        if (state.filters.kind !== "all" && message.kind !== state.filters.kind) return false;
        if (state.filters.attachmentsOnly && message.attachment_count === 0) return false;
        if (state.filters.selectedOnly && !state.selectedMessageIds.has(message.id)) return false;
        if (phoneNeedle) {
          const haystack = [
            message.primary_address,
            message.conversation_label,
            ...(message.addresses || []),
          ].join(" ").toLowerCase();
          if (!haystack.includes(phoneNeedle)) return false;
        }
        if (textNeedle) {
          const haystack = [message.body_text || "", message.subject || ""].join(" ").toLowerCase();
          if (!haystack.includes(textNeedle)) return false;
        }
        return true;
      });
    }

    function visibleConversations() {
      if (!state.data) return [];
      const visibleIds = new Set(visibleMessages().map((message) => message.conversation_id));
      const conversations = state.data.conversations
        .filter((c) => visibleIds.has(c.id));
      const sort = state.filters.conversationSort;
      conversations.sort((a, b) => {
        if (sort === "phone_desc") return b.primary_address.localeCompare(a.primary_address);
        if (sort === "latest_desc") return b.latest_timestamp_ms - a.latest_timestamp_ms;
        if (sort === "count_desc") return (
          b.message_count - a.message_count
          || a.primary_address.localeCompare(b.primary_address));
        return a.primary_address.localeCompare(b.primary_address);
      });
      return conversations;
    }

    function conversationSelectionState(conversationId) {
      const ids = state.data.messages
        .filter((m) => m.conversation_id === conversationId)
        .map((m) => m.id);
      const selected = ids.filter((id) => state.selectedMessageIds.has(id)).length;
      if (selected === 0) return "none";
      if (selected === ids.length) return "all";
      return "partial";
    }

    function render() {
      renderStats();
      renderConversations();
      renderMessages();
    }

    function renderStats() {
      const stats = document.getElementById("stats");
      if (!state.data) {
        stats.textContent = "Loading…";
        return;
      }
      stats.innerHTML = "";
      const items = [
        `${state.data.export_name}`,
        `${state.data.stats.messages} messages`,
        `${state.data.stats.conversations} conversations`,
        `${state.selectedMessageIds.size} selected`,
      ];
      items.forEach((text) => {
        const span = document.createElement("span");
        span.textContent = text;
        stats.appendChild(span);
      });
    }

    function renderConversations() {
      const list = document.getElementById("conversationList");
      const footer = document.getElementById("conversationFooter");
      const conversations = visibleConversations();
      list.innerHTML = "";
      if (!conversations.length) {
        list.innerHTML = '<div class="empty">No conversations match the current filters.</div>';
      } else {
        conversations.forEach((conversation) => {
          const row = document.createElement("label");
          row.className = "row conversation-row";

          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          const selectionState = conversationSelectionState(conversation.id);
          checkbox.checked = selectionState === "all";
          checkbox.indeterminate = selectionState === "partial";
          checkbox.addEventListener("change", () => {
            const ids = state.data.messages
              .filter((message) => message.conversation_id === conversation.id)
              .map((message) => message.id);
            ids.forEach((id) => {
              if (checkbox.checked) state.selectedMessageIds.add(id);
              else state.selectedMessageIds.delete(id);
            });
            render();
          });

          const body = document.createElement("div");
          const metaText = `${conversation.message_count} messages`
            + ` · ${conversation.attachment_count} attachments`
            + ` · ${fmtTime(conversation.latest_timestamp_ms)}`;
          body.innerHTML = `<div class="label">${conversation.label}</div>
            <div class="meta">${metaText}</div>`;

          const badge = document.createElement("span");
          badge.className = "pill";
          badge.textContent = conversation.primary_address;

          row.appendChild(checkbox);
          row.appendChild(body);
          row.appendChild(badge);
          list.appendChild(row);
        });
      }
      footer.textContent = `${conversations.length} visible conversation(s)`;
    }

    function renderMessages() {
      const list = document.getElementById("messageList");
      const footer = document.getElementById("messageFooter");
      const messages = visibleMessages().slice().sort((a, b) =>
        b.timestamp_ms - a.timestamp_ms
        || a.primary_address.localeCompare(b.primary_address));
      list.innerHTML = "";
      if (!messages.length) {
        list.innerHTML = '<div class="empty">No messages match the current filters.</div>';
      } else {
        messages.forEach((message) => {
          const row = document.createElement("label");
          row.className = "row message-row";

          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.checked = state.selectedMessageIds.has(message.id);
          checkbox.addEventListener("change", () => {
            if (checkbox.checked) state.selectedMessageIds.add(message.id);
            else state.selectedMessageIds.delete(message.id);
            render();
          });

          const when = document.createElement("div");
          when.className = "meta";
          when.textContent = fmtTime(message.timestamp_ms);

          const direction = document.createElement("div");
          direction.innerHTML = `<span class="pill">`
            + `${message.kind.toUpperCase()}`
            + ` · ${message.direction}</span>`;

          const body = document.createElement("div");
          const preview = message.body_text || message.subject || "(attachment only)";
          body.innerHTML = `<div class="label">${message.primary_address}</div>
            <div class="preview">${preview}</div>`;

          const attach = document.createElement("div");
          attach.className = "meta";
          attach.textContent = message.attachment_count
            ? `${message.attachment_count} attachment(s)` : "";

          row.appendChild(checkbox);
          row.appendChild(when);
          row.appendChild(direction);
          row.appendChild(body);
          row.appendChild(attach);
          list.appendChild(row);
        });
      }
      footer.textContent = `${messages.length} visible message(s)`;
    }

    function hookFilter(id, key, accessor = (el) => el.value) {
      document.getElementById(id).addEventListener("input", (event) => {
        state.filters[key] = accessor(event.target);
        render();
      });
      document.getElementById(id).addEventListener("change", (event) => {
        state.filters[key] = accessor(event.target);
        render();
      });
    }

    async function exportSelected() {
      const selectedIds = Array.from(state.selectedMessageIds);
      if (!selectedIds.length) {
        alert("Select at least one message before exporting.");
        return;
      }
      const response = await fetch("/api/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ selected_ids: selectedIds }),
      });
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

    async function boot() {
      const response = await fetch("/api/data");
      state.data = await response.json();
      state.data.messages.forEach((message) => state.selectedMessageIds.add(message.id));
      hookFilter("phoneFilter", "phone");
      hookFilter("textFilter", "text");
      hookFilter("kindFilter", "kind");
      hookFilter("conversationSort", "conversationSort");
      hookFilter("attachmentsOnly", "attachmentsOnly", (el) => el.checked);
      hookFilter("selectedOnly", "selectedOnly", (el) => el.checked);
      document.getElementById("selectVisibleConversations").addEventListener("click", () => {
        visibleConversations().forEach((conversation) => {
          state.data.messages
            .filter((message) => message.conversation_id === conversation.id)
            .forEach((message) => state.selectedMessageIds.add(message.id));
        });
        render();
      });
      document.getElementById("deselectVisibleConversations").addEventListener("click", () => {
        visibleConversations().forEach((conversation) => {
          state.data.messages
            .filter((message) => message.conversation_id === conversation.id)
            .forEach((message) => state.selectedMessageIds.delete(message.id));
        });
        render();
      });
      document.getElementById("selectVisibleMessages").addEventListener("click", () => {
        visibleMessages().forEach((message) => state.selectedMessageIds.add(message.id));
        render();
      });
      document.getElementById("deselectVisibleMessages").addEventListener("click", () => {
        visibleMessages().forEach((message) => state.selectedMessageIds.delete(message.id));
        render();
      });
      document.getElementById("selectAll").addEventListener("click", () => {
        state.data.messages.forEach((message) => state.selectedMessageIds.add(message.id));
        render();
      });
      document.getElementById("deselectAll").addEventListener("click", () => {
        state.selectedMessageIds.clear();
        render();
      });
      document.getElementById("exportSelected").addEventListener("click", exportSelected);
      render();
    }

    boot();
  </script>
</body>
</html>
"""

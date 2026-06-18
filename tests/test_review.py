"""Tests for the local Android export review tool."""

from __future__ import annotations

import io
import json
import os
import socket
import threading
import zipfile
from contextlib import contextmanager
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from green2blue.review import (
    ReviewWorkflowContext,
    _make_review_handler,
    _prune_reviewed_exports,
    _ReviewHTTPServer,
    open_review_session,
    run_review_workflow,
)

WORKFLOW = ReviewWorkflowContext(
    title="Review checkpoint",
    summary="Trim this export before the wizard continues.",
    next_step="The wizard will resume in the terminal.",
)


@contextmanager
def _serve_workflow(session):
    server = _ReviewHTTPServer(
        ("127.0.0.1", 0),
        _make_review_handler(session, workflow_context=WORKFLOW),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_address[1]}", thread
    finally:
        if thread.is_alive():
            server.shutdown()
            thread.join(timeout=2)
        server.server_close()


def _post_apply(base_url: str, payload: dict, headers: dict | None = None):
    request = Request(
        f"{base_url}/api/apply",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    return urlopen(request)


def _post_export(base_url: str, payload: dict, headers: dict | None = None):
    request = Request(
        f"{base_url}/api/export",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    return urlopen(request)


class TestReviewSession:
    def test_open_review_session_builds_payload(self, sample_export_zip):
        with open_review_session(sample_export_zip) as session:
            payload = session.payload()

        assert payload["stats"]["messages"] == 3
        assert payload["stats"]["conversations"] == 3
        assert payload["stats"]["attachments"] == 1
        assert payload["stats"]["sms_messages"] == 2
        assert payload["stats"]["mms_messages"] == 1
        assert payload["stats"]["messages_with_attachments"] == 1
        assert any(message["kind"] == "mms" for message in payload["messages"])
        assert any(
            conversation["primary_address"] == "+12025551234"
            for conversation in payload["conversations"]
        )

    def test_review_session_skips_malformed_ndjson_lines(self, tmp_dir):
        zip_path = tmp_dir / "corrupt.zip"
        lines = [
            json.dumps({"address": "+15551230001", "body": "first", "date": "1", "type": "1"}),
            '{"address": "+15551230002", "body": "trunca',
            json.dumps({"address": "+15551230003", "body": "third", "date": "2", "type": "1"}),
        ]
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages.ndjson", "\n".join(lines) + "\n")

        with open_review_session(zip_path) as session:
            bodies = [message.body_text for message in session.messages]

        assert bodies == ["first", "third"]

    def test_review_preview_labels_outgoing_buckets(self, tmp_dir):
        """OUTBOX/FAILED/DRAFT must preview as outgoing, matching the import.

        Regression: the preview hardcoded type==2/msg_box==2, so these showed
        as 'unknown' while the actual injection treats them as the user's own.
        """
        zip_path = tmp_dir / "directions.zip"
        lines = [
            json.dumps({"address": "+1555000001", "body": "in", "date": "1", "type": "1"}),
            json.dumps({"address": "+1555000002", "body": "sent", "date": "2", "type": "2"}),
            json.dumps({"address": "+1555000003", "body": "outbox", "date": "3", "type": "4"}),
            json.dumps({"address": "+1555000004", "body": "failed", "date": "4", "type": "5"}),
        ]
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages.ndjson", "\n".join(lines) + "\n")

        with open_review_session(zip_path) as session:
            directions = {m.body_text: m.direction for m in session.messages}

        assert directions == {
            "in": "incoming",
            "sent": "outgoing",
            "outbox": "outgoing",
            "failed": "outgoing",
        }

    def test_export_selected_zip_filters_messages_and_attachments(self, sample_export_zip):
        with open_review_session(sample_export_zip) as session:
            zip_bytes = session.export_selected_zip({"line-1", "line-3"})

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = set(zf.namelist())
            assert "messages.ndjson" in names
            attachment_names = [name for name in names if name.startswith("data/")]
            assert len(attachment_names) == 1
            lines = zf.read("messages.ndjson").decode("utf-8").strip().splitlines()
            assert len(lines) == 2
            payloads = [json.loads(line) for line in lines]

        texts = [
            payload.get("body") or payload.get("__parts", [{}])[0].get("text", "")
            for payload in payloads
        ]
        assert "Hello from Android!" in texts
        assert "Check out this photo!" in texts
        assert "Hello from me!" not in texts

    def test_write_selected_zip_streams_same_archive(self, sample_export_zip, tmp_dir):
        output_path = tmp_dir / "streamed.zip"
        with open_review_session(sample_export_zip) as session:
            zip_bytes = session.export_selected_zip({"line-1"})
            with output_path.open("wb") as fh:
                session.write_selected_zip(fh, {"line-1"})

        with (
            zipfile.ZipFile(io.BytesIO(zip_bytes)) as from_bytes,
            zipfile.ZipFile(output_path) as from_file,
        ):
            assert from_bytes.namelist() == from_file.namelist()
            assert from_bytes.read("messages.ndjson") == from_file.read("messages.ndjson")

    def test_export_selected_zip_requires_selection(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            pytest.raises(ValueError, match="No messages selected"),
        ):
            session.export_selected_zip(set())


class TestWorkflowServer:
    def test_workflow_apply_saves_filtered_export_and_sets_server_result(
        self,
        sample_export_zip,
        tmp_dir,
    ):
        with (
            patch("green2blue.review.default_app_state_root", return_value=tmp_dir / "app-state"),
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, thread),
        ):
            with urlopen(f"{base_url}/api/data") as response:
                payload = json.load(response)
            assert payload["workflow"]["mode"] == "wizard"

            with _post_apply(
                base_url,
                {"action": "continue_selected", "selected_ids": ["line-1", "line-3"]},
            ) as response:
                body = json.load(response)
            assert body["action"] == "filtered"

            thread.join(timeout=2)
            assert server.workflow_result is not None
            assert server.workflow_result.action == "filtered"
            filtered_path = server.workflow_result.export_zip
            assert filtered_path is not None and filtered_path.exists()
            assert str(filtered_path) == body["path"]

            with zipfile.ZipFile(filtered_path, "r") as zf:
                lines = zf.read("messages.ndjson").decode("utf-8").strip().splitlines()
            assert len(lines) == 2

    def test_first_workflow_decision_wins(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, _thread),
        ):
            # Keep the server alive after the first decision so the second
            # request deterministically exercises the claim guard.
            with patch.object(
                server.RequestHandlerClass,
                "_shutdown_server",
                lambda self: None,
            ):
                with _post_apply(base_url, {"action": "continue_full"}) as response:
                    assert json.load(response)["action"] == "full"

                with pytest.raises(HTTPError) as exc_info:
                    _post_apply(base_url, {"action": "cancel"})
                assert exc_info.value.code == 409

            assert server.workflow_result is not None
            assert server.workflow_result.action == "full"

    def test_cross_origin_post_is_rejected(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, _thread),
        ):
            with pytest.raises(HTTPError) as exc_info:
                _post_apply(
                    base_url,
                    {"action": "continue_full"},
                    headers={"Origin": "http://evil.example"},
                )
            assert exc_info.value.code == 403
            assert server.workflow_result is None

            same_origin = f"http://127.0.0.1:{server.server_address[1]}"
            with _post_apply(
                base_url,
                {"action": "continue_full"},
                headers={"Origin": same_origin},
            ) as response:
                assert json.load(response)["action"] == "full"

    def test_malformed_content_length_returns_400(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, _base_url, _thread),
        ):
            host, port = server.server_address[:2]
            with socket.create_connection((host, port), timeout=5) as sock:
                sock.sendall(
                    b"POST /api/apply HTTP/1.1\r\n"
                    b"Host: 127.0.0.1\r\n"
                    b"Content-Length: abc\r\n"
                    b"Connection: close\r\n"
                    b"\r\n"
                )
                response = b""
                while chunk := sock.recv(4096):
                    response += chunk
            assert b"400" in response.split(b"\r\n", 1)[0]
            assert server.workflow_result is None

    def test_write_failure_returns_500_and_keeps_server_alive(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, thread),
        ):
            with patch(
                "green2blue.review._write_reviewed_export",
                side_effect=OSError("disk full"),
            ):
                with pytest.raises(HTTPError) as exc_info:
                    _post_apply(
                        base_url,
                        {"action": "continue_selected", "selected_ids": ["line-1"]},
                    )
                assert exc_info.value.code == 500
                assert server.workflow_result is None

            # The browser can still cancel (or retry) after the failure.
            with _post_apply(base_url, {"action": "cancel"}) as response:
                assert json.load(response)["action"] == "cancel"
            thread.join(timeout=2)
            assert server.workflow_result is not None
            assert server.workflow_result.action == "cancel"

    def test_export_io_error_returns_500_and_keeps_server_alive(self, sample_export_zip):
        """A selected attachment that becomes unreadable must not crash the
        request thread. The /api/export handler should surface a 500 (mirroring
        the /api/apply handler) instead of letting OSError escape."""
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, _thread),
        ):
            with patch.object(
                session,
                "export_selected_zip",
                side_effect=OSError("attachment vanished"),
            ):
                with pytest.raises(HTTPError) as exc_info:
                    _post_export(base_url, {"selected_ids": ["line-1"]})
                assert exc_info.value.code == 500

            # The server keeps serving: a normal export still succeeds.
            with _post_export(base_url, {"selected_ids": ["line-1"]}) as response:
                assert response.status == 200
                body = response.read()
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                assert "messages.ndjson" in zf.namelist()

    def test_export_invalid_selection_returns_400(self, sample_export_zip):
        """An empty selection is a client error, not a server error."""
        with (
            open_review_session(sample_export_zip) as session,
            _serve_workflow(session) as (server, base_url, _thread),
        ):
            with pytest.raises(HTTPError) as exc_info:
                _post_export(base_url, {"selected_ids": []})
            assert exc_info.value.code == 400

    def test_run_review_workflow_reraises_keyboard_interrupt(self, sample_export_zip):
        with (
            patch.object(_ReviewHTTPServer, "serve_forever", side_effect=KeyboardInterrupt),
            pytest.raises(KeyboardInterrupt),
        ):
            run_review_workflow(sample_export_zip, WORKFLOW, open_browser=False)


class TestReviewedExportPruning:
    def test_prune_keeps_newest_files(self, tmp_dir):
        output_dir = tmp_dir / "reviewed_exports"
        output_dir.mkdir()
        for index in range(7):
            path = output_dir / f"export.2026010{index}_000000_000000.filtered.zip"
            path.write_bytes(b"zip")
            os.utime(path, (1700000000 + index, 1700000000 + index))

        _prune_reviewed_exports(output_dir, keep=5)

        remaining = sorted(path.name for path in output_dir.glob("*.filtered.zip"))
        assert len(remaining) == 5
        assert remaining == [
            f"export.2026010{index}_000000_000000.filtered.zip" for index in range(2, 7)
        ]

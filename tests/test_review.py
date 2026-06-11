"""Tests for the local Android export review tool."""

from __future__ import annotations

import io
import json
import threading
import zipfile
from unittest.mock import patch
from urllib.request import Request, urlopen

import pytest

from green2blue.review import (
    ReviewWorkflowContext,
    _make_review_handler,
    _ReviewHTTPServer,
    open_review_session,
)


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
        assert payload["default_conversation_id"] in {
            conversation["id"] for conversation in payload["conversations"]
        }
        assert any(message["kind"] == "mms" for message in payload["messages"])
        assert any(
            conversation["primary_address"] == "+12025551234"
            for conversation in payload["conversations"]
        )

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

    def test_export_selected_zip_requires_selection(self, sample_export_zip):
        with (
            open_review_session(sample_export_zip) as session,
            pytest.raises(ValueError, match="No messages selected"),
        ):
            session.export_selected_zip(set())

    def test_workflow_apply_saves_filtered_export_and_sets_server_result(
        self,
        sample_export_zip,
        tmp_dir,
    ):
        workflow = ReviewWorkflowContext(
            title="Review checkpoint",
            summary="Trim this export before the wizard continues.",
            next_step="The wizard will resume in the terminal.",
        )

        with (
            patch("green2blue.review.default_app_state_root", return_value=tmp_dir / "app-state"),
            open_review_session(sample_export_zip) as session,
        ):
            server = _ReviewHTTPServer(
                ("127.0.0.1", 0),
                _make_review_handler(session, workflow_context=workflow),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{server.server_address[1]}"

            filtered_path = None
            try:
                with urlopen(f"{base_url}/api/data") as response:
                    payload = json.load(response)
                assert payload["workflow"]["mode"] == "wizard"

                request = Request(
                    f"{base_url}/api/apply",
                    data=json.dumps(
                        {
                            "action": "continue_selected",
                            "selected_ids": ["line-1", "line-3"],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    body = json.load(response)
                assert body["action"] == "filtered"

                thread.join(timeout=2)
                assert server.workflow_result is not None
                assert server.workflow_result.action == "filtered"
                filtered_path = server.workflow_result.export_zip
                assert filtered_path is not None and filtered_path.exists()

                with zipfile.ZipFile(filtered_path, "r") as zf:
                    lines = zf.read("messages.ndjson").decode("utf-8").strip().splitlines()
                assert len(lines) == 2
            finally:
                if thread.is_alive():
                    server.shutdown()
                    thread.join(timeout=2)
                server.server_close()
                if filtered_path is not None:
                    filtered_path.unlink(missing_ok=True)

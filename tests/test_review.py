"""Tests for the local Android export review tool."""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from green2blue.review import open_review_session


class TestReviewSession:
    def test_open_review_session_builds_payload(self, sample_export_zip):
        with open_review_session(sample_export_zip) as session:
            payload = session.payload()

        assert payload["stats"]["messages"] == 3
        assert payload["stats"]["conversations"] == 3
        assert payload["stats"]["attachments"] == 1
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

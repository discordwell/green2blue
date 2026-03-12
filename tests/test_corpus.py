"""Tests for representative corpus capture."""

from __future__ import annotations

import json
import zipfile

from green2blue.corpus import capture_android_corpus
from green2blue.parser.zip_reader import open_export_zip


class TestCorpusCapture:
    def test_capture_android_corpus_redacts_sample_export(self, sample_export_zip, tmp_dir):
        output_zip = tmp_dir / "corpus.zip"

        result = capture_android_corpus(sample_export_zip, output_zip)

        assert output_zip.exists()
        assert result.selected_messages == 3
        assert result.attachments_written == 1
        assert "sms_incoming" in result.buckets_covered
        assert "sms_outgoing" in result.buckets_covered
        assert "mms_captioned_media" in result.buckets_covered

        with open_export_zip(output_zip) as export:
            rows = [json.loads(line) for line in export.ndjson_path.read_text().splitlines() if line.strip()]
            assert len(rows) == 3
            assert rows[0]["address"] != "+12025551234"
            assert rows[0]["body"].startswith("CLAUDEUS CORPUS")
            assert export.has_attachments()

        with zipfile.ZipFile(output_zip) as zf:
            names = set(zf.namelist())
            assert "messages.ndjson" in names
            assert any(name.startswith("data/") for name in names)

    def test_capture_can_preserve_text(self, sample_export_zip, tmp_dir):
        output_zip = tmp_dir / "corpus_preserve.zip"

        capture_android_corpus(sample_export_zip, output_zip, preserve_text=True)

        with open_export_zip(output_zip) as export:
            rows = [json.loads(line) for line in export.ndjson_path.read_text().splitlines() if line.strip()]
            bodies = [row.get("body") for row in rows if "body" in row]
            assert "Hello from Android!" in bodies

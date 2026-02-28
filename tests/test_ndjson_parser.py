"""Tests for NDJSON parser."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from green2blue.models import AndroidMMS, AndroidSMS
from green2blue.parser.ndjson_parser import count_messages, parse_ndjson
from tests.conftest import (
    SAMPLE_GROUP_MMS,
    SAMPLE_MMS,
    SAMPLE_SMS_RECEIVED,
    SAMPLE_SMS_SENT,
)


def _write_ndjson(tmp_dir: Path, *records: dict | str) -> Path:
    """Write records to a temporary NDJSON file."""
    path = tmp_dir / "messages.ndjson"
    lines = []
    for r in records:
        if isinstance(r, str):
            lines.append(r)
        else:
            lines.append(json.dumps(r))
    path.write_text("\n".join(lines) + "\n")
    return path


class TestSMSParsing:
    def test_received_sms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_SMS_RECEIVED)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1
        msg = messages[0]
        assert isinstance(msg, AndroidSMS)
        assert msg.address == "+12025551234"
        assert msg.body == "Hello from Android!"
        assert msg.date == 1700000000000
        assert msg.type == 1
        assert msg.read == 1

    def test_sent_sms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_SMS_SENT)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1
        msg = messages[0]
        assert isinstance(msg, AndroidSMS)
        assert msg.type == 2
        assert msg.body == "Hello from me!"

    def test_multiple_sms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_SMS_RECEIVED, SAMPLE_SMS_SENT)
        messages = list(parse_ndjson(path))
        assert len(messages) == 2

    def test_sms_with_unicode(self, tmp_dir):
        record = {**SAMPLE_SMS_RECEIVED, "body": "Hello 🌍 こんにちは"}
        path = _write_ndjson(tmp_dir, record)
        messages = list(parse_ndjson(path))
        assert messages[0].body == "Hello 🌍 こんにちは"

    def test_sms_missing_date_sent(self, tmp_dir):
        record = {k: v for k, v in SAMPLE_SMS_RECEIVED.items() if k != "date_sent"}
        path = _write_ndjson(tmp_dir, record)
        messages = list(parse_ndjson(path))
        assert messages[0].date_sent is None


class TestMMSParsing:
    def test_basic_mms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_MMS)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1
        msg = messages[0]
        assert isinstance(msg, AndroidMMS)
        assert msg.msg_box == 1
        assert len(msg.parts) == 2
        assert len(msg.addresses) == 2

    def test_mms_parts(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_MMS)
        msg = list(parse_ndjson(path))[0]
        assert msg.parts[0].content_type == "text/plain"
        assert msg.parts[0].text == "Check out this photo!"
        assert msg.parts[1].content_type == "image/jpeg"
        assert msg.parts[1].data_path == "data/parts/image_001.jpg"

    def test_mms_addresses(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_MMS)
        msg = list(parse_ndjson(path))[0]
        from_addrs = [a for a in msg.addresses if a.type == 137]
        to_addrs = [a for a in msg.addresses if a.type == 151]
        assert len(from_addrs) == 1
        assert len(to_addrs) == 1

    def test_group_mms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_GROUP_MMS)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1
        msg = messages[0]
        assert len(msg.addresses) == 3
        to_addrs = [a for a in msg.addresses if a.type == 151]
        assert len(to_addrs) == 2

    def test_mms_timestamp_in_seconds(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_MMS)
        msg = list(parse_ndjson(path))[0]
        # MMS date is in seconds, should be much smaller than SMS ms values
        assert msg.date == 1700000002


class TestMixedMessages:
    def test_sms_and_mms(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_SMS_RECEIVED, SAMPLE_MMS, SAMPLE_SMS_SENT)
        messages = list(parse_ndjson(path))
        assert len(messages) == 3
        assert isinstance(messages[0], AndroidSMS)
        assert isinstance(messages[1], AndroidMMS)
        assert isinstance(messages[2], AndroidSMS)


class TestMalformedData:
    def test_empty_file(self, tmp_dir):
        path = tmp_dir / "messages.ndjson"
        path.write_text("")
        messages = list(parse_ndjson(path))
        assert messages == []

    def test_blank_lines(self, tmp_dir):
        path = _write_ndjson(tmp_dir, SAMPLE_SMS_RECEIVED)
        content = "\n\n" + path.read_text() + "\n\n"
        path.write_text(content)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1

    def test_invalid_json_line(self, tmp_dir):
        path = _write_ndjson(tmp_dir, "not valid json{{{", SAMPLE_SMS_RECEIVED)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1

    def test_non_object_line(self, tmp_dir):
        path = _write_ndjson(tmp_dir, '"just a string"', SAMPLE_SMS_RECEIVED)
        # "just a string" is valid JSON but not an object — written raw
        content = '"just a string"\n' + json.dumps(SAMPLE_SMS_RECEIVED) + "\n"
        path.write_text(content)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1

    def test_unrecognized_record(self, tmp_dir):
        unknown = {"foo": "bar", "baz": 42}
        path = _write_ndjson(tmp_dir, unknown, SAMPLE_SMS_RECEIVED)
        messages = list(parse_ndjson(path))
        assert len(messages) == 1

    def test_file_not_found(self, tmp_dir):
        from green2blue.exceptions import ParseError

        with pytest.raises(ParseError):
            list(parse_ndjson(tmp_dir / "nonexistent.ndjson"))


class TestCountMessages:
    def test_count_mixed(self, tmp_dir):
        path = _write_ndjson(
            tmp_dir, SAMPLE_SMS_RECEIVED, SAMPLE_SMS_SENT, SAMPLE_MMS
        )
        counts = count_messages(path)
        assert counts["sms"] == 2
        assert counts["mms"] == 1
        assert counts["total"] == 3
        assert counts["errors"] == 0
        assert counts["unknown"] == 0

    def test_count_with_errors(self, tmp_dir):
        content = "bad json\n" + json.dumps(SAMPLE_SMS_RECEIVED) + "\n"
        path = tmp_dir / "messages.ndjson"
        path.write_text(content)
        counts = count_messages(path)
        assert counts["errors"] == 1
        assert counts["sms"] == 1
        assert counts["total"] == 2

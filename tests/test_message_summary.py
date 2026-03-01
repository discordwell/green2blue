"""Tests for message_summary_info blob generation."""

from __future__ import annotations

import plistlib

from green2blue.ios.message_summary import (
    _SMS_BLOB,
    build_message_summary_info,
)
from green2blue.ios.sms_db import SMSDatabase
from green2blue.models import (
    ConversionResult,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
)


class TestBuildMessageSummaryInfo:
    """Tests for the build_message_summary_info function."""

    def test_sms_with_text_returns_blob(self):
        """SMS message with text should get a message_summary_info blob."""
        blob = build_message_summary_info(service="SMS", has_text=True)
        assert blob is not None
        assert isinstance(blob, bytes)
        assert blob.startswith(b"bplist00")

    def test_sms_without_text_returns_none(self):
        """Message without text should get NULL message_summary_info."""
        blob = build_message_summary_info(service="SMS", has_text=False)
        assert blob is None

    def test_blob_parses_as_valid_plist(self):
        """The generated blob must parse as a valid binary plist."""
        blob = build_message_summary_info(service="SMS", has_text=True)
        parsed = plistlib.loads(blob)
        assert isinstance(parsed, dict)

    def test_blob_has_required_keys(self):
        """The blob must contain the two universal keys cmmS\\x10 and cmmAO."""
        blob = build_message_summary_info(service="SMS", has_text=True)
        parsed = plistlib.loads(blob)
        assert "cmmS\x10" in parsed
        assert "cmmAO" in parsed
        assert parsed["cmmS\x10"] == 0
        assert parsed["cmmAO"] == 0

    def test_sent_and_received_produce_same_blob(self):
        """Both sent and received SMS messages get the same blob."""
        blob_sent = build_message_summary_info(
            service="SMS", is_from_me=True, has_text=True
        )
        blob_recv = build_message_summary_info(
            service="SMS", is_from_me=False, has_text=True
        )
        assert blob_sent == blob_recv

    def test_blob_is_reused_singleton(self):
        """The blob should be the pre-computed singleton for efficiency."""
        blob = build_message_summary_info(service="SMS", has_text=True)
        assert blob is _SMS_BLOB

    def test_blob_roundtrip_semantic_match(self):
        """Parsing and re-serializing should produce semantically identical data."""
        blob = build_message_summary_info(service="SMS", has_text=True)
        parsed = plistlib.loads(blob)
        re_serialized = plistlib.dumps(parsed, fmt=plistlib.FMT_BINARY)
        re_parsed = plistlib.loads(re_serialized)
        assert parsed == re_parsed

    def test_imessage_with_text_returns_blob(self):
        """iMessage with text should also get a blob (service doesn't matter)."""
        blob = build_message_summary_info(service="iMessage", has_text=True)
        assert blob is not None

    def test_rcs_with_text_returns_blob(self):
        """RCS with text should also get a blob."""
        blob = build_message_summary_info(service="RCS", has_text=True)
        assert blob is not None


class TestMessageSummaryInfoInjection:
    """Tests that message_summary_info is correctly written during injection."""

    def _make_handle(self, phone="+12025551234"):
        return iOSHandle(id=phone, country="us", service="SMS")

    def _make_chat(self, phone="+12025551234"):
        return iOSChat(
            guid=f"any;-;{phone}",
            style=45,
            chat_identifier=phone,
            service_name="SMS",
        )

    def _make_message(self, phone="+12025551234", text="hello", date=721692800000000000):
        return iOSMessage(
            guid=f"green2blue:test-{date}",
            text=text,
            handle_id=phone,
            date=date,
            date_read=date,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier=phone,
        )

    def _make_result(self, messages=None, handles=None, chats=None):
        r = ConversionResult()
        r.messages = messages or []
        r.handles = handles or []
        r.chats = chats or []
        return r

    def test_injected_message_has_summary_info(self, empty_sms_db):
        """Injected SMS message with text should have non-NULL message_summary_info."""
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message(text="Hello from Android!")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT message_summary_info FROM message")
            row = cursor.fetchone()
            blob = row["message_summary_info"]
            assert blob is not None
            parsed = plistlib.loads(blob)
            assert parsed == {"cmmS\x10": 0, "cmmAO": 0}

    def test_injected_attachment_only_message_null_summary(self, empty_sms_db):
        """Message with no text (attachment-only) should have NULL message_summary_info."""
        att = iOSAttachment(
            guid="green2blue-att:test",
            filename="~/Library/SMS/Attachments/ab/uuid/photo.jpg",
            mime_type="image/jpeg",
            uti="public.jpeg",
            transfer_name="photo.jpg",
            total_bytes=1024,
            created_date=721692800,
        )
        msg = iOSMessage(
            guid="green2blue:no-text-msg",
            text=None,
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            attachments=(att,),
        )
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT message_summary_info FROM message")
            row = cursor.fetchone()
            assert row["message_summary_info"] is None

    def test_sent_message_has_summary_info(self, empty_sms_db):
        """Sent messages should also have message_summary_info."""
        msg = iOSMessage(
            guid="green2blue:sent-summary",
            text="Sent message",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=721692800000000000,
            is_from_me=True,
            is_sent=True,
            service="SMS",
            chat_identifier="+12025551234",
        )
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT message_summary_info FROM message")
            row = cursor.fetchone()
            assert row["message_summary_info"] is not None

    def test_multiple_messages_all_have_summary_info(self, empty_sms_db):
        """All injected messages with text should have message_summary_info."""
        msgs = [
            self._make_message(text=f"Message {i}", date=721692800000000000 + i * 1000000)
            for i in range(5)
        ]
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=msgs,
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result, skip_duplicates=False)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM message WHERE message_summary_info IS NOT NULL"
            )
            assert cursor.fetchone()["cnt"] == 5

    def test_summary_info_matches_real_ios_format(self, empty_sms_db):
        """The injected blob should match the format found in real iOS sms.db."""
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT message_summary_info FROM message")
            blob = cursor.fetchone()["message_summary_info"]

            # Must be a binary plist
            assert blob[:8] == b"bplist00"

            # Must parse to the canonical SMS dict
            parsed = plistlib.loads(blob)
            assert set(parsed.keys()) == {"cmmS\x10", "cmmAO"}
            assert all(v == 0 for v in parsed.values())

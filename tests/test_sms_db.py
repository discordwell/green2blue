"""Tests for sms.db injection."""

from __future__ import annotations

import sqlite3

import pytest

from green2blue.ios.sms_db import SMSDatabase
from green2blue.models import (
    ConversionResult,
    generate_ck_record_id,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
)


def _make_handle(phone="+12025551234", service="SMS"):
    return iOSHandle(id=phone, country="us", service=service)


def _make_chat(phone="+12025551234", service="SMS"):
    account_login = "e:" if service == "iMessage" else "E:"
    return iOSChat(
        guid=f"any;-;{phone}",
        style=45,
        chat_identifier=phone,
        service_name=service,
        account_login=account_login,
    )


def _make_message(phone="+12025551234", text="hello", date=721692800000000000, service="SMS"):
    return iOSMessage(
        guid=f"green2blue:test-{date}",
        text=text,
        handle_id=phone,
        date=date,
        date_read=date,
        date_delivered=0,
        is_from_me=False,
        service=service,
        chat_identifier=phone,
    )


def _make_result(messages=None, handles=None, chats=None):
    r = ConversionResult()
    r.messages = messages or []
    r.handles = handles or []
    r.chats = chats or []
    return r


class TestSMSDatabase:
    def test_open_and_close(self, empty_sms_db):
        db = SMSDatabase(empty_sms_db)
        db.open()
        assert db.conn is not None
        db.close()
        assert db.conn is None

    def test_context_manager(self, empty_sms_db):
        with SMSDatabase(empty_sms_db) as db:
            assert db.conn is not None
        assert db.conn is None

    def test_integrity_check(self, empty_sms_db):
        with SMSDatabase(empty_sms_db) as db:
            assert db.integrity_check()


class TestHandleInsertion:
    def test_insert_handle(self, empty_sms_db):
        result = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234")],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.handles_inserted == 1

            # Verify in DB
            cursor = db.conn.cursor()
            cursor.execute("SELECT id, country, service FROM handle")
            row = cursor.fetchone()
            assert row["id"] == "+12025551234"
            assert row["country"] == "us"
            assert row["service"] == "SMS"

    def test_handle_dedup(self, empty_sms_db):
        result1 = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234", date=100000000)],
        )
        result2 = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234", text="second", date=200000000)],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result1)
            stats2 = db.inject(result2)
            assert stats2.handles_existing == 1
            assert stats2.handles_inserted == 0

            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM handle")
            assert cursor.fetchone()["cnt"] == 1


class TestChatInsertion:
    def test_insert_1to1_chat(self, empty_sms_db):
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.chats_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT guid, style, chat_identifier FROM chat")
            row = cursor.fetchone()
            assert row["guid"] == "any;-;+12025551234"
            assert row["style"] == 45

    def test_insert_group_chat(self, empty_sms_db):
        group_chat = iOSChat(
            guid="any;-;chatabc123",
            style=43,
            chat_identifier="+12025551111,+12025552222,+12025553333",
            service_name="SMS",
        )
        msg = iOSMessage(
            guid="green2blue:group-test",
            text="group msg",
            handle_id="+12025551111",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551111,+12025552222,+12025553333",
            group_members=("+12025551111", "+12025552222", "+12025553333"),
        )
        result = _make_result(
            handles=[
                _make_handle("+12025551111"),
                _make_handle("+12025552222"),
                _make_handle("+12025553333"),
            ],
            chats=[group_chat],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.chats_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT style FROM chat")
            assert cursor.fetchone()["style"] == 43


class TestMessageInsertion:
    def test_insert_received_message(self, empty_sms_db):
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.messages_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT text, is_from_me, service FROM message")
            row = cursor.fetchone()
            assert row["text"] == "hello"
            assert row["is_from_me"] == 0
            assert row["service"] == "SMS"

    def test_insert_sent_message(self, empty_sms_db):
        msg = iOSMessage(
            guid="green2blue:sent-test",
            text="sent message",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=721692800000000000,
            is_from_me=True,
            is_sent=True,
            is_delivered=True,
            service="SMS",
            chat_identifier="+12025551234",
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT is_from_me, is_sent, is_delivered FROM message")
            row = cursor.fetchone()
            assert row["is_from_me"] == 1
            assert row["is_sent"] == 1
            assert row["is_delivered"] == 1

    def test_message_count(self, empty_sms_db):
        msgs = [_make_message(date=i * 1000000) for i in range(5)]
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=msgs,
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result, skip_duplicates=False)
            assert db.get_message_count() == 5


class TestJoinTables:
    def test_chat_handle_join(self, empty_sms_db):
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM chat_handle_join")
            assert cursor.fetchone()["cnt"] == 1

    def test_chat_message_join(self, empty_sms_db):
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM chat_message_join")
            assert cursor.fetchone()["cnt"] == 1


class TestDuplicateSkipping:
    def test_skip_exact_duplicate(self, empty_sms_db):
        msg = _make_message()
        result1 = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result1)
            # Inject same content again
            result2 = _make_result(
                handles=[_make_handle()],
                chats=[_make_chat()],
                messages=[_make_message(text="hello", date=721692800000000000)],
            )
            # Need different GUID to avoid unique constraint
            result2.messages = [
                iOSMessage(
                    guid="green2blue:dupe-test",
                    text="hello",
                    handle_id="+12025551234",
                    date=721692800000000000,
                    date_read=721692800000000000,
                    date_delivered=0,
                    is_from_me=False,
                    service="SMS",
                    chat_identifier="+12025551234",
                )
            ]
            stats2 = db.inject(result2)
            assert stats2.messages_skipped == 1
            assert stats2.messages_inserted == 0
            assert db.get_message_count() == 1


class TestAttachmentInsertion:
    def test_insert_attachment(self, empty_sms_db):
        att = iOSAttachment(
            guid="green2blue-att:test",
            filename="~/Library/SMS/Attachments/ab/test-uuid/photo.jpg",
            mime_type="image/jpeg",
            uti="public.jpeg",
            transfer_name="photo.jpg",
            total_bytes=1024,
            created_date=721692800,  # Apple epoch seconds
        )
        msg = iOSMessage(
            guid="green2blue:att-test",
            text="check this",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            attachments=(att,),
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.attachments_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT mime_type, uti, transfer_name FROM attachment")
            row = cursor.fetchone()
            assert row["mime_type"] == "image/jpeg"
            assert row["uti"] == "public.jpeg"

            # Check join table
            cursor.execute("SELECT COUNT(*) as cnt FROM message_attachment_join")
            assert cursor.fetchone()["cnt"] == 1


class TestCloudKitMetadata:
    def test_message_default_ck_state_zero(self, empty_sms_db):
        """Messages without CK metadata should have ck_sync_state=0."""
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_sync_state, ck_record_id, ck_record_change_tag FROM message")
            row = cursor.fetchone()
            assert row["ck_sync_state"] == 0
            assert row["ck_record_id"] == ""
            assert row["ck_record_change_tag"] == ""

    def test_message_fake_synced(self, empty_sms_db):
        """Messages with fake-synced strategy should have ck_sync_state=1 and record ID."""
        record_id = generate_ck_record_id("green2blue:ck-test")
        msg = iOSMessage(
            guid="green2blue:ck-test",
            text="fake synced",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            ck_sync_state=1,
            ck_record_id=record_id,
            ck_record_change_tag="1",
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_sync_state, ck_record_id, ck_record_change_tag FROM message")
            row = cursor.fetchone()
            assert row["ck_sync_state"] == 1
            assert row["ck_record_id"] == record_id
            assert len(row["ck_record_id"]) == 64
            assert row["ck_record_change_tag"] == "1"

    def test_message_pending_upload(self, empty_sms_db):
        """Messages with pending-upload strategy should have ck_sync_state=0 and record ID."""
        record_id = generate_ck_record_id("green2blue:pending-test")
        msg = iOSMessage(
            guid="green2blue:pending-test",
            text="pending upload",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            ck_sync_state=0,
            ck_record_id=record_id,
            ck_record_change_tag="",
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_sync_state, ck_record_id, ck_record_change_tag FROM message")
            row = cursor.fetchone()
            assert row["ck_sync_state"] == 0
            assert row["ck_record_id"] == record_id
            assert row["ck_record_change_tag"] == ""

    def test_chat_ck_metadata(self, empty_sms_db):
        """Chat CK metadata should be written to the chat table."""
        chat_record_id = generate_ck_record_id("any;-;+12025551234", salt="green2blue-ck-chat")
        chat = iOSChat(
            guid="any;-;+12025551234",
            style=45,
            chat_identifier="+12025551234",
            service_name="SMS",
            ck_sync_state=1,
            cloudkit_record_id=chat_record_id,
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[chat],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_sync_state, cloudkit_record_id FROM chat")
            row = cursor.fetchone()
            assert row["ck_sync_state"] == 1
            assert row["cloudkit_record_id"] == chat_record_id

    def test_generate_ck_record_id_format(self):
        """Record IDs should be 64-char hex strings."""
        record_id = generate_ck_record_id("test-guid")
        assert len(record_id) == 64
        assert all(c in "0123456789abcdef" for c in record_id)

    def test_generate_ck_record_id_deterministic(self):
        """Same input should produce same record ID."""
        id1 = generate_ck_record_id("test-guid")
        id2 = generate_ck_record_id("test-guid")
        assert id1 == id2

    def test_generate_ck_record_id_unique_per_guid(self):
        """Different GUIDs should produce different record IDs."""
        id1 = generate_ck_record_id("guid-1")
        id2 = generate_ck_record_id("guid-2")
        assert id1 != id2

    def test_generate_ck_record_id_salt_matters(self):
        """Different salts should produce different record IDs."""
        id1 = generate_ck_record_id("guid", salt="salt-a")
        id2 = generate_ck_record_id("guid", salt="salt-b")
        assert id1 != id2


class TestDestinationCallerId:
    def test_detect_from_existing_messages(self, empty_sms_db):
        """Should detect device owner's phone from existing messages."""
        # Pre-populate with real iOS-style messages that have destination_caller_id
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO message (guid, text, service, destination_caller_id, date) "
            "VALUES ('real-1', 'hi', 'SMS', '+15052289549', 100)"
        )
        conn.execute(
            "INSERT INTO message (guid, text, service, destination_caller_id, date) "
            "VALUES ('real-2', 'hey', 'SMS', '+15052289549', 200)"
        )
        conn.commit()
        conn.close()

        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT destination_caller_id FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["destination_caller_id"] == "+15052289549"

    def test_empty_db_returns_null(self, empty_sms_db):
        """No existing messages → destination_caller_id should be NULL."""
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT destination_caller_id FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["destination_caller_id"] is None

    def test_picks_most_frequent(self, empty_sms_db):
        """Should pick the most frequent destination_caller_id value."""
        conn = sqlite3.connect(empty_sms_db)
        for i in range(3):
            conn.execute(
                "INSERT INTO message (guid, text, service, destination_caller_id, date) "
                f"VALUES ('a-{i}', 'hi', 'SMS', '+11111111111', {i * 100})"
            )
        for i in range(5):
            conn.execute(
                "INSERT INTO message (guid, text, service, destination_caller_id, date) "
                f"VALUES ('b-{i}', 'hi', 'SMS', '+12222222222', {(i + 10) * 100})"
            )
        conn.commit()
        conn.close()

        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT destination_caller_id FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["destination_caller_id"] == "+12222222222"


class TestCkChatId:
    def test_1to1_chat_id(self, empty_sms_db):
        """ck_chat_id for 1:1 should be SMS;-;+phone."""
        result = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_chat_id FROM message")
            row = cursor.fetchone()
            assert row["ck_chat_id"] == "SMS;-;+12025551234"

    def test_group_chat_id(self, empty_sms_db):
        """ck_chat_id for group should be SMS;-;chat{hash}."""
        from green2blue.models import compute_chat_guid

        members = ("+12025551111", "+12025552222", "+12025553333")
        chat_guid = compute_chat_guid(
            "+12025551111,+12025552222,+12025553333", members
        )
        expected_ck = chat_guid.replace("any;-;", "SMS;-;", 1)

        group_chat = iOSChat(
            guid=chat_guid,
            style=43,
            chat_identifier="+12025551111,+12025552222,+12025553333",
            service_name="SMS",
        )
        msg = iOSMessage(
            guid="green2blue:group-ck-test",
            text="group msg",
            handle_id="+12025551111",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551111,+12025552222,+12025553333",
            group_members=members,
        )
        result = _make_result(
            handles=[
                _make_handle("+12025551111"),
                _make_handle("+12025552222"),
                _make_handle("+12025553333"),
            ],
            chats=[group_chat],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_chat_id FROM message")
            row = cursor.fetchone()
            assert row["ck_chat_id"] == expected_ck
            assert row["ck_chat_id"].startswith("SMS;-;chat")


class TestAccountDetection:
    def test_detect_account_from_existing(self, empty_sms_db):
        """Should detect SMS account string from existing messages."""
        conn = sqlite3.connect(empty_sms_db)
        for i in range(3):
            conn.execute(
                "INSERT INTO message (guid, text, service, account, date) "
                f"VALUES ('acct-{i}', 'hi', 'SMS', 'P:+15052289549', {i * 100})"
            )
        conn.commit()
        conn.close()

        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT account FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["account"] == "P:+15052289549"

    def test_detect_account_guid_from_existing(self, empty_sms_db):
        """Should detect account_guid UUID from existing messages."""
        test_guid = "AD9A6DB5-8CDA-48CD-9819-25C5F91E775D"
        conn = sqlite3.connect(empty_sms_db)
        for i in range(3):
            conn.execute(
                "INSERT INTO message (guid, text, service, account_guid, date) "
                f"VALUES ('ag-{i}', 'hi', 'SMS', '{test_guid}', {i * 100})"
            )
        conn.commit()
        conn.close()

        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT account_guid FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["account_guid"] == test_guid

    def test_empty_db_uses_null(self, empty_sms_db):
        """No existing messages → account and account_guid should be NULL."""
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT account, account_guid FROM message WHERE guid LIKE 'green2blue:%'"
            )
            row = cursor.fetchone()
            assert row["account"] is None
            assert row["account_guid"] is None


class TestTriggerManagement:
    def test_triggers_restored_on_success(self, empty_sms_db):
        # Add a simple trigger
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            """CREATE TRIGGER test_trigger AFTER INSERT ON message
               BEGIN SELECT 1; END"""
        )
        conn.commit()
        conn.close()

        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[_make_message()],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)

            # Trigger should be restored
            cursor = db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
            triggers = [r["name"] for r in cursor.fetchall()]
            assert "test_trigger" in triggers

    def test_triggers_restored_on_failure(self, empty_sms_db):
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            """CREATE TRIGGER test_trigger2 AFTER INSERT ON message
               BEGIN SELECT 1; END"""
        )
        conn.commit()
        conn.close()

        # Create a result that will cause a unique constraint violation
        msg = _make_message()
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg, msg],  # Duplicate GUID will fail
        )
        with SMSDatabase(empty_sms_db) as db:
            with pytest.raises(sqlite3.IntegrityError):
                db.inject(result, skip_duplicates=False)

            # Trigger should still be restored
            cursor = db.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
            triggers = [r["name"] for r in cursor.fetchall()]
            assert "test_trigger2" in triggers


class TestMissingHandle:
    def test_missing_handle_skips_message(self, empty_sms_db):
        """Message with unknown handle_id should be skipped, not inserted with handle_id=0."""
        # Create a message referencing a handle that is NOT in the handles list
        msg = iOSMessage(
            guid="green2blue:orphan-handle",
            text="orphan message",
            handle_id="+19995550000",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+19995550000",
        )
        chat = iOSChat(
            guid="any;-;+19995550000",
            style=45,
            chat_identifier="+19995550000",
            service_name="SMS",
        )
        # Deliberately omit the handle for +19995550000
        result = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[chat],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.messages_skipped == 1
            assert stats.messages_inserted == 0
            assert db.get_message_count() == 0

    def test_missing_handle_does_not_insert_zero(self, empty_sms_db):
        """Ensure no message is inserted with handle_id=0."""
        msg = iOSMessage(
            guid="green2blue:zero-handle",
            text="zero handle msg",
            handle_id="+19995550000",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+19995550000",
        )
        result = _make_result(
            handles=[],
            chats=[],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM message WHERE handle_id = 0")
            assert cursor.fetchone()["cnt"] == 0


class TestReducedSchema:
    """Tests against an older iOS schema missing optional columns."""

    def test_inject_sms_reduced_schema(self, reduced_schema_sms_db):
        """SMS injection should work on a schema without optional columns."""
        result = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234")],
        )
        with SMSDatabase(reduced_schema_sms_db) as db:
            stats = db.inject(result)
            assert stats.messages_inserted == 1
            assert stats.handles_inserted == 1
            assert stats.chats_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT text, service FROM message")
            row = cursor.fetchone()
            assert row["text"] == "hello"
            assert row["service"] == "SMS"

    def test_inject_mms_reduced_schema(self, reduced_schema_sms_db):
        """MMS with attachment should work on reduced schema."""
        att = iOSAttachment(
            guid="green2blue-att:reduced",
            filename="~/Library/SMS/Attachments/ab/test-uuid/photo.jpg",
            mime_type="image/jpeg",
            uti="public.jpeg",
            transfer_name="photo.jpg",
            total_bytes=1024,
            created_date=721692800,
        )
        msg = iOSMessage(
            guid="green2blue:reduced-mms",
            text="check this",
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            attachments=(att,),
        )
        result = _make_result(
            handles=[_make_handle()],
            chats=[_make_chat()],
            messages=[msg],
        )
        with SMSDatabase(reduced_schema_sms_db) as db:
            stats = db.inject(result)
            assert stats.messages_inserted == 1
            assert stats.attachments_inserted == 1

            # Verify attachment doesn't have preview_generation_state or original_guid
            cursor = db.conn.cursor()
            cols = {r[1] for r in cursor.execute("PRAGMA table_info(attachment)").fetchall()}
            assert "preview_generation_state" not in cols
            assert "original_guid" not in cols

    def test_schema_detection(self, reduced_schema_sms_db):
        """_inspect_schema should correctly report missing optional columns."""
        with SMSDatabase(reduced_schema_sms_db) as db:
            assert "message_summary_info" not in db._msg_schema
            assert "destination_caller_id" not in db._msg_schema
            assert "ck_chat_id" not in db._msg_schema
            assert "sr_ck_sync_state" not in db._msg_schema
            assert "preview_generation_state" not in db._att_schema
            assert "original_guid" not in db._att_schema
            assert "sr_ck_sync_state" not in db._att_schema


class TestCompositeHandleKey:
    def test_same_phone_different_service_both_inserted(self, empty_sms_db):
        """Same phone as SMS and iMessage should create two separate handles."""
        sms_result = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234", date=100000000)],
        )
        im_result = _make_result(
            handles=[_make_handle(service="iMessage", phone="+12025551234")],
            chats=[_make_chat(service="iMessage", phone="+12025551234")],
            messages=[_make_message(
                service="iMessage", phone="+12025551234", text="imsg", date=200000000,
            )],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats1 = db.inject(sms_result)
            assert stats1.handles_inserted == 1

            stats2 = db.inject(im_result)
            assert stats2.handles_inserted == 1
            assert stats2.handles_existing == 0

            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM handle")
            assert cursor.fetchone()["cnt"] == 2

            cursor.execute("SELECT id, service FROM handle ORDER BY ROWID")
            rows = cursor.fetchall()
            services = {(r["id"], r["service"]) for r in rows}
            assert ("+12025551234", "SMS") in services
            assert ("+12025551234", "iMessage") in services

    def test_same_phone_same_service_deduped(self, empty_sms_db):
        """Same phone and service should be deduped."""
        result1 = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234", date=100000000)],
        )
        result2 = _make_result(
            handles=[_make_handle("+12025551234")],
            chats=[_make_chat("+12025551234")],
            messages=[_make_message("+12025551234", text="second", date=200000000)],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result1)
            stats2 = db.inject(result2)
            assert stats2.handles_existing == 1
            assert stats2.handles_inserted == 0

    def test_load_existing_handles_composite_key(self, empty_sms_db):
        """_load_existing_handles should return {(id, service): ROWID}."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (id, country, service) VALUES (?, ?, ?)",
            ("+12025551234", "us", "SMS"),
        )
        conn.execute(
            "INSERT INTO handle (id, country, service) VALUES (?, ?, ?)",
            ("+12025551234", "us", "iMessage"),
        )
        conn.commit()
        conn.close()

        with SMSDatabase(empty_sms_db) as db:
            handles = db._load_existing_handles()
            assert ("+12025551234", "SMS") in handles
            assert ("+12025551234", "iMessage") in handles
            assert handles[("+12025551234", "SMS")] != handles[("+12025551234", "iMessage")]


class TestIMessageInjection:
    def test_inject_imessage(self, empty_sms_db):
        """iMessage injection should set correct service fields."""
        result = _make_result(
            handles=[_make_handle(service="iMessage")],
            chats=[_make_chat(service="iMessage")],
            messages=[_make_message(service="iMessage")],
        )
        with SMSDatabase(empty_sms_db) as db:
            stats = db.inject(result)
            assert stats.messages_inserted == 1

            cursor = db.conn.cursor()
            cursor.execute("SELECT service FROM message")
            assert cursor.fetchone()["service"] == "iMessage"

            cursor.execute("SELECT service FROM handle")
            assert cursor.fetchone()["service"] == "iMessage"

            cursor.execute("SELECT service_name, account_login FROM chat")
            row = cursor.fetchone()
            assert row["service_name"] == "iMessage"
            assert row["account_login"] == "e:"

    def test_imessage_ck_chat_id(self, empty_sms_db):
        """ck_chat_id should use iMessage prefix for iMessage messages."""
        result = _make_result(
            handles=[_make_handle(service="iMessage", phone="+12025551234")],
            chats=[_make_chat(service="iMessage", phone="+12025551234")],
            messages=[_make_message(service="iMessage", phone="+12025551234")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT ck_chat_id FROM message")
            row = cursor.fetchone()
            assert row["ck_chat_id"] == "iMessage;-;+12025551234"

    def test_imessage_chat_handle_join(self, empty_sms_db):
        """Chat-handle join should work for iMessage handles."""
        result = _make_result(
            handles=[_make_handle(service="iMessage")],
            chats=[_make_chat(service="iMessage")],
            messages=[_make_message(service="iMessage")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM chat_handle_join")
            assert cursor.fetchone()["cnt"] == 1


class TestServiceDetection:
    def test_detect_account_for_imessage(self, empty_sms_db):
        """Account detection should filter by service."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO message (guid, text, service, account, date) "
            "VALUES ('sms-1', 'hi', 'SMS', 'P:+15052289549', 100)"
        )
        conn.execute(
            "INSERT INTO message (guid, text, service, account, date) "
            "VALUES ('im-1', 'hi', 'iMessage', 'e:user@icloud.com', 200)"
        )
        conn.commit()
        conn.close()

        with SMSDatabase(empty_sms_db) as db:
            sms_account = db._detect_account("SMS")
            assert sms_account == "P:+15052289549"

            im_account = db._detect_account("iMessage")
            assert im_account == "e:user@icloud.com"

"""Tests for sms.db injection."""

from __future__ import annotations

import sqlite3

import pytest

from green2blue.ios.sms_db import SMSDatabase
from green2blue.models import (
    ConversionResult,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
)


def _make_handle(phone="+12025551234"):
    return iOSHandle(id=phone, country="us", service="SMS")


def _make_chat(phone="+12025551234"):
    return iOSChat(
        guid=f"SMS;-;{phone}",
        style=45,
        chat_identifier=phone,
        service_name="SMS",
    )


def _make_message(phone="+12025551234", text="hello", date=721692800000000000):
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
            assert row["guid"] == "SMS;-;+12025551234"
            assert row["style"] == 45

    def test_insert_group_chat(self, empty_sms_db):
        group_chat = iOSChat(
            guid="SMS;-;chatabc123",
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
            created_date=721692800000000000,
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

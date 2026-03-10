"""Tests for prepare_sync module."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from green2blue.ios.prepare_sync import prepare_sync


@pytest.fixture
def sms_db_with_injected(empty_sms_db: Path) -> Path:
    """Create an sms.db with injected messages that have CK metadata set."""
    conn = sqlite3.connect(empty_sms_db)

    # Insert a handle
    conn.execute(
        "INSERT INTO handle (ROWID, id, country, service) VALUES (1, '+12025551234', 'us', 'SMS')"
    )

    # Insert a chat
    conn.execute(
        "INSERT INTO chat (ROWID, guid, style, chat_identifier, "
        "service_name, ck_sync_state, cloudkit_record_id, "
        "server_change_token) "
        "VALUES (1, 'any;-;+12025551234', 45, '+12025551234', "
        "'SMS', 1, 'fake-ck-id-chat', 'token123')"
    )

    # Insert injected messages with fake-synced CK metadata
    conn.execute(
        "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
        "ck_sync_state, ck_record_id, ck_record_change_tag) "
        "VALUES (1, 'green2blue:msg-001', 'Hello from Android', 1, 'SMS', 1000000, "
        "1, 'abcd1234' || '0000000000000000000000000000000000000000000000000000000a', '1')"
    )
    conn.execute(
        "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
        "ck_sync_state, ck_record_id, ck_record_change_tag) "
        "VALUES (2, 'green2blue:msg-002', 'Second message', 1, 'SMS', 1000001, "
        "1, 'abcd1234' || '0000000000000000000000000000000000000000000000000000000b', '1')"
    )

    # Link messages to chat
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, "
        "message_date) VALUES (1, 1, 1000000)"
    )
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, "
        "message_date) VALUES (1, 2, 1000001)"
    )

    # Link handle to chat
    conn.execute("INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (1, 1)")

    conn.commit()
    conn.close()
    return empty_sms_db


class TestPrepareSyncMessages:
    """Tests for message CK metadata reset."""

    def test_resets_message_ck_metadata(self, sms_db_with_injected: Path) -> None:
        """Injected messages with CK metadata get reset to ck_sync_state=0."""
        result = prepare_sync(sms_db_with_injected)

        assert result.messages_updated == 2

        conn = sqlite3.connect(sms_db_with_injected)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE guid LIKE 'green2blue:%' ORDER BY ROWID"
        ).fetchall()
        conn.close()

        for row in rows:
            assert row["ck_sync_state"] == 0
            assert row["ck_record_id"] == ""
            assert row["ck_record_change_tag"] == ""

    def test_already_clean_messages_unchanged(self, empty_sms_db: Path) -> None:
        """Messages already at ck_sync_state=0 with no CK IDs are counted but not updated."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (ROWID, id, country, service) "
            "VALUES (1, '+12025551234', 'us', 'SMS')"
        )
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
            "ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES (1, 'green2blue:clean-001', 'Already clean', 1, 'SMS', 1000000, 0, NULL, NULL)"
        )
        conn.commit()
        conn.close()

        result = prepare_sync(empty_sms_db)

        assert result.messages_updated == 0
        assert result.messages_already_clean == 1

    def test_idempotent(self, sms_db_with_injected: Path) -> None:
        """Running prepare_sync twice produces clean results on second run."""
        prepare_sync(sms_db_with_injected)
        result = prepare_sync(sms_db_with_injected)

        assert result.messages_updated == 0
        assert result.messages_already_clean == 2
        assert result.chats_token_cleared == 0

    def test_does_not_modify_non_injected_messages(self, sms_db_with_injected: Path) -> None:
        """Non-green2blue messages are left untouched."""
        conn = sqlite3.connect(sms_db_with_injected)
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
            "ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES (100, 'native-ios-msg-001', 'Native iOS message', 1, 'SMS', 2000000, "
            "1, 'real-ck-record-id-native', '5')"
        )
        conn.commit()
        conn.close()

        prepare_sync(sms_db_with_injected)

        conn = sqlite3.connect(sms_db_with_injected)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE guid = 'native-ios-msg-001'"
        ).fetchone()
        conn.close()

        assert row["ck_sync_state"] == 1
        assert row["ck_record_id"] == "real-ck-record-id-native"
        assert row["ck_record_change_tag"] == "5"

    def test_resets_explicit_message_rowids(self, empty_sms_db: Path) -> None:
        """Explicit rowid targeting should work for overwrite/clone rows without prefixes."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (ROWID, id, country, service) "
            "VALUES (1, '+12025551234', 'us', 'SMS')"
        )
        conn.execute(
            "INSERT INTO chat (ROWID, guid, style, chat_identifier, service_name, "
            "ck_sync_state, cloudkit_record_id, server_change_token) "
            "VALUES (1, 'any;-;+12025551234', 45, '+12025551234', "
            "'SMS', 1, 'real-chat-ck-id', 'change-token')"
        )
        conn.execute(
            "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (1, 1)"
        )
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
            "ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES (1, 'native-overwrite-001', 'target', 1, 'SMS', 1000000, "
            "1, 'target-ck-id', '1')"
        )
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, service, date, "
            "ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES (2, 'native-keep-001', 'keep', 1, 'SMS', 1000001, "
            "1, 'keep-ck-id', '2')"
        )
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) "
            "VALUES (1, 1, 1000000)"
        )
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) "
            "VALUES (1, 2, 1000001)"
        )
        conn.commit()
        conn.close()

        result = prepare_sync(empty_sms_db, message_rowids=[1])

        assert result.messages_updated == 1
        assert result.messages_already_clean == 0
        assert result.chats_preserved == 1
        assert result.chats_token_cleared == 1

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        target = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE ROWID = 1"
        ).fetchone()
        untouched = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE ROWID = 2"
        ).fetchone()
        chat = conn.execute(
            "SELECT ck_sync_state, cloudkit_record_id, server_change_token "
            "FROM chat WHERE ROWID = 1"
        ).fetchone()
        conn.close()

        assert target["ck_sync_state"] == 0
        assert target["ck_record_id"] == ""
        assert target["ck_record_change_tag"] == ""
        assert untouched["ck_sync_state"] == 1
        assert untouched["ck_record_id"] == "keep-ck-id"
        assert untouched["ck_record_change_tag"] == "2"
        assert chat["ck_sync_state"] == 1
        assert chat["cloudkit_record_id"] == "real-chat-ck-id"
        assert chat["server_change_token"] == ""


class TestPrepareSyncChats:
    """Tests for chat CK state handling."""

    def test_clears_server_change_token_on_affected_chats(
        self, sms_db_with_injected: Path
    ) -> None:
        """server_change_token is cleared on chats with injected messages."""
        result = prepare_sync(sms_db_with_injected)

        assert result.chats_token_cleared == 1

        conn = sqlite3.connect(sms_db_with_injected)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT server_change_token FROM chat WHERE ROWID = 1"
        ).fetchone()
        conn.close()

        assert row["server_change_token"] == ""

    def test_does_not_clear_token_on_unaffected_chats(
        self, sms_db_with_injected: Path
    ) -> None:
        """Chats without injected messages keep their server_change_token."""
        conn = sqlite3.connect(sms_db_with_injected)
        conn.execute(
            "INSERT INTO chat (ROWID, guid, style, chat_identifier, service_name, "
            "server_change_token) "
            "VALUES (99, 'any;-;+15551112222', 45, '+15551112222', 'SMS', 'keep-this-token')"
        )
        conn.commit()
        conn.close()

        prepare_sync(sms_db_with_injected)

        conn = sqlite3.connect(sms_db_with_injected)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT server_change_token FROM chat WHERE ROWID = 99"
        ).fetchone()
        conn.close()

        assert row["server_change_token"] == "keep-this-token"

    def test_resets_ck_on_pure_injected_chats(
        self, sms_db_with_injected: Path
    ) -> None:
        """Pure-injected chats (only green2blue messages) get full CK reset."""
        result = prepare_sync(sms_db_with_injected)

        assert result.chats_ck_reset == 1

        conn = sqlite3.connect(sms_db_with_injected)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state, cloudkit_record_id FROM chat WHERE ROWID = 1"
        ).fetchone()
        conn.close()

        assert row["ck_sync_state"] == 0
        assert row["cloudkit_record_id"] == ""

    def test_preserves_ck_on_mixed_chats(self, empty_sms_db: Path) -> None:
        """Mixed chats (pre-existing + injected) keep CK state, only lose server_change_token."""
        conn = sqlite3.connect(empty_sms_db)

        # Handle
        conn.execute(
            "INSERT INTO handle (ROWID, id, country, service) "
            "VALUES (1, '+12025551234', 'us', 'SMS')"
        )

        # Chat with CK metadata
        conn.execute(
            "INSERT INTO chat (ROWID, guid, style, chat_identifier, "
            "service_name, ck_sync_state, cloudkit_record_id, "
            "server_change_token) "
            "VALUES (1, 'any;-;+12025551234', 45, '+12025551234', "
            "'SMS', 1, 'real-chat-ck-id', 'change-token-xyz')"
        )

        # Native iOS message (pre-existing)
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, "
            "service, date, ck_sync_state, ck_record_id) "
            "VALUES (1, 'native-msg-001', 'Original iOS message', "
            "1, 'SMS', 500000, 1, 'ck-native-1')"
        )
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, "
            "message_date) VALUES (1, 1, 500000)"
        )

        # Injected message
        conn.execute(
            "INSERT INTO message (ROWID, guid, text, handle_id, "
            "service, date, ck_sync_state, ck_record_id, "
            "ck_record_change_tag) "
            "VALUES (2, 'green2blue:mixed-001', "
            "'Injected into existing chat', 1, 'SMS', 1000000, "
            "1, 'fake-ck-injected', '1')"
        )
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, "
            "message_date) VALUES (1, 2, 1000000)"
        )

        conn.commit()
        conn.close()

        result = prepare_sync(empty_sms_db)

        assert result.chats_preserved == 1
        assert result.chats_ck_reset == 0
        # server_change_token should still be cleared
        assert result.chats_token_cleared == 1

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state, cloudkit_record_id, "
            "server_change_token FROM chat WHERE ROWID = 1"
        ).fetchone()
        conn.close()

        # CK state preserved
        assert row["ck_sync_state"] == 1
        assert row["cloudkit_record_id"] == "real-chat-ck-id"
        # But server_change_token cleared
        assert row["server_change_token"] == ""


class TestPrepareSyncAttachments:
    """Tests for attachment CK metadata reset."""

    def test_resets_attachment_ck_metadata(self, empty_sms_db: Path) -> None:
        """Injected attachments with CK metadata get reset."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, "
            "total_bytes, ck_sync_state, ck_record_id) "
            "VALUES (1, 'green2blue-att:att-001', '~/Library/SMS/Attachments/aa/photo.jpg', "
            "'public.jpeg', 'image/jpeg', 'photo.jpg', 12345, 1, 'fake-att-ck-id')"
        )
        conn.commit()
        conn.close()

        result = prepare_sync(empty_sms_db)

        assert result.attachments_updated == 1

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state, ck_record_id FROM attachment WHERE ROWID = 1"
        ).fetchone()
        conn.close()

        assert row["ck_sync_state"] == 0
        assert row["ck_record_id"] is None

    def test_resets_explicit_attachment_rowids(self, empty_sms_db: Path) -> None:
        """Explicit attachment rowids should be reset even without green2blue GUIDs."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, "
            "total_bytes, ck_sync_state, ck_record_id) "
            "VALUES (1, 'native-att-001', '~/Library/SMS/Attachments/aa/one.jpg', "
            "'public.jpeg', 'image/jpeg', 'one.jpg', 1, 1, 'target-att-ck-id')"
        )
        conn.execute(
            "INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, "
            "total_bytes, ck_sync_state, ck_record_id) "
            "VALUES (2, 'native-att-002', '~/Library/SMS/Attachments/ab/two.jpg', "
            "'public.jpeg', 'image/jpeg', 'two.jpg', 1, 1, 'keep-att-ck-id')"
        )
        conn.commit()
        conn.close()

        result = prepare_sync(empty_sms_db, attachment_rowids=[1])

        assert result.attachments_updated == 1
        assert result.attachments_already_clean == 0

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        target = conn.execute(
            "SELECT ck_sync_state, ck_record_id FROM attachment WHERE ROWID = 1"
        ).fetchone()
        untouched = conn.execute(
            "SELECT ck_sync_state, ck_record_id FROM attachment WHERE ROWID = 2"
        ).fetchone()
        conn.close()

        assert target["ck_sync_state"] == 0
        assert target["ck_record_id"] is None
        assert untouched["ck_sync_state"] == 1
        assert untouched["ck_record_id"] == "keep-att-ck-id"


class TestPrepareSyncEdgeCases:
    """Edge case tests."""

    def test_empty_database_no_error(self, empty_sms_db: Path) -> None:
        """prepare_sync on an empty database completes without error."""
        result = prepare_sync(empty_sms_db)

        assert result.messages_updated == 0
        assert result.messages_already_clean == 0
        assert result.attachments_updated == 0
        assert result.chats_token_cleared == 0
        assert result.chats_ck_reset == 0
        assert result.chats_preserved == 0

    def test_result_counts_accurate(self, sms_db_with_injected: Path) -> None:
        """All result counts match actual database state."""
        # Add an attachment to the fixture
        conn = sqlite3.connect(sms_db_with_injected)
        conn.execute(
            "INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, "
            "total_bytes, ck_sync_state, ck_record_id) "
            "VALUES (1, 'green2blue-att:acc-001', '~/Library/SMS/Attachments/ab/img.jpg', "
            "'public.jpeg', 'image/jpeg', 'img.jpg', 5000, 1, 'att-ck-id')"
        )
        # Add an already-clean attachment
        conn.execute(
            "INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, "
            "total_bytes, ck_sync_state, ck_record_id) "
            "VALUES (2, 'green2blue-att:acc-002', '~/Library/SMS/Attachments/ac/img2.jpg', "
            "'public.jpeg', 'image/jpeg', 'img2.jpg', 3000, 0, NULL)"
        )
        conn.commit()
        conn.close()

        result = prepare_sync(sms_db_with_injected)

        assert result.messages_updated == 2
        assert result.messages_already_clean == 0
        assert result.attachments_updated == 1
        assert result.attachments_already_clean == 1
        assert result.chats_token_cleared == 1
        assert result.chats_ck_reset == 1
        assert result.chats_preserved == 0

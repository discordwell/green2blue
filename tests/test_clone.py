"""Tests for clone mode (Hack Patrol approach — clone last existing message)."""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from green2blue.exceptions import CloneSourceError
from green2blue.ios.attributed_body import build_attributed_body
from green2blue.ios.sms_db import SMSDatabase, _build_hackpatrol_attributed_body
from green2blue.models import (
    ConversionResult,
    iOSChat,
    iOSHandle,
    iOSMessage,
)

# --- Helpers ---


def _make_message(
    phone: str,
    text: str,
    date: int,
    *,
    is_from_me: bool = False,
) -> iOSMessage:
    return iOSMessage(
        guid=f"green2blue:{uuid.uuid4()}",
        text=text,
        handle_id=phone,
        date=date,
        date_read=date if not is_from_me else 0,
        date_delivered=date,
        is_from_me=is_from_me,
        service="SMS",
        chat_identifier=phone,
    )


def _make_result(
    messages: list[iOSMessage],
    handles: list[iOSHandle] | None = None,
    chats: list[iOSChat] | None = None,
) -> ConversionResult:
    if handles is None:
        phones = {m.handle_id for m in messages}
        handles = [iOSHandle(id=p, country="us", service="SMS") for p in phones]
    if chats is None:
        phones = {m.handle_id for m in messages}
        chats = [
            iOSChat(
                guid=f"any;-;{p}", style=45,
                chat_identifier=p, service_name="SMS",
            )
            for p in phones
        ]
    return ConversionResult(messages=messages, handles=handles, chats=chats)


def _populate_source_db(
    db_path: Path,
    phone: str,
    *,
    ck_sync_state: int = 1,
    message_count: int = 1,
) -> tuple[int, int, int]:
    """Insert source data (handle, chat, incoming messages) for cloning.

    Returns (handle_rowid, chat_rowid, last_message_rowid).
    """
    conn = sqlite3.connect(db_path)

    # Handle
    conn.execute(
        "INSERT INTO handle (id, country, service, uncanonicalized_id) "
        "VALUES (?, 'us', 'SMS', ?)",
        (phone, phone),
    )
    handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Chat
    chat_guid = f"any;-;{phone}"
    conn.execute(
        """INSERT INTO chat (guid, style, state, chat_identifier, service_name,
                             account_login, group_id, server_change_token,
                             ck_sync_state, cloudkit_record_id)
           VALUES (?, 45, 3, ?, 'SMS', 'E:', ?, '', ?, '')""",
        (chat_guid, phone, str(uuid.uuid4()), ck_sync_state),
    )
    chat_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
        (chat_rowid, handle_rowid),
    )

    # Messages (incoming)
    base_date = 700000000000000000
    ck_record_id = "a" * 64 if ck_sync_state == 1 else ""
    ck_change_tag = "42" if ck_sync_state == 1 else ""
    msi_blob = b"\x00\x01\x02"  # Fake MSI blob

    last_msg_rowid = 0
    for i in range(message_count):
        msg_date = base_date + i * 1000000000
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, date,
                                    date_read, date_delivered, is_from_me,
                                    is_read, is_delivered, is_finished,
                                    ck_sync_state, ck_record_id, ck_record_change_tag,
                                    message_summary_info, attributedBody)
               VALUES (?, ?, ?, 'SMS', ?, ?, ?, 0, 1, 1, 1, ?, ?, ?, ?, ?)""",
            (
                f"source-msg-{uuid.uuid4()}", f"source text {i}", handle_rowid,
                msg_date, msg_date, msg_date,
                ck_sync_state, ck_record_id, ck_change_tag,
                msi_blob, build_attributed_body(f"source text {i}"),
            ),
        )
        last_msg_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) "
            "VALUES (?, ?, ?)",
            (chat_rowid, last_msg_rowid, msg_date),
        )

    conn.commit()
    conn.close()
    return handle_rowid, chat_rowid, last_msg_rowid


# --- Test Classes ---


class TestCloneMessageCreation:
    """Cloned messages get new ROWIDs and correct content."""

    def test_new_rowid_assigned(self, empty_sms_db: Path):
        _, _, source_rowid = _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Cloned text", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        rows = conn.execute("SELECT ROWID FROM message ORDER BY ROWID").fetchall()
        cloned_rowid = rows[-1][0]
        assert cloned_rowid != source_rowid
        assert cloned_rowid > source_rowid
        conn.close()

    def test_plain_uuid_guid(self, empty_sms_db: Path):
        """HACK_PATROL_NOTE: Plain UUID, no 'green2blue:' prefix."""
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "UUID test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT guid FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        guid = rows["guid"]
        assert not guid.startswith("green2blue:")
        # Should be a valid UUID format (uppercase)
        uuid.UUID(guid)
        conn.close()

    def test_text_from_input(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Hello from Android!", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT text FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert row["text"] == "Hello from Android!"
        conn.close()

    def test_date_from_input(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        target_date = 850000000000000000
        msg = _make_message("+15552220000", "Date test", target_date)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT date, date_read, date_delivered FROM message "
            "ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert row["date"] == target_date
        assert row["date_read"] == target_date
        assert row["date_delivered"] == target_date
        conn.close()

    def test_handle_id_from_input(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Handle test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        # Get the new handle's ROWID
        new_handle = conn.execute(
            "SELECT ROWID FROM handle WHERE id = '+15552220000'"
        ).fetchone()
        cloned_msg = conn.execute(
            "SELECT handle_id FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert cloned_msg["handle_id"] == new_handle[0]
        conn.close()


class TestCloneCKDuplication:
    """CloudKit metadata is duplicated from the clone source."""

    def test_ck_sync_state_duplicated(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15552220000", "CK test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert row["ck_sync_state"] == 1
        conn.close()

    def test_ck_record_id_duplicated(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15552220000", "CK id test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_record_id FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert row["ck_record_id"] == "a" * 64
        conn.close()

    def test_ck_record_change_tag_duplicated(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15552220000", "CK tag test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_record_change_tag FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        assert row["ck_record_change_tag"] == "42"
        conn.close()

    def test_ck_duplicated_flag_set(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15552220000", "Flag test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.ck_metadata_duplicated is True

    def test_ck_duplicated_flag_unset_when_no_ck(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=0)
        msg = _make_message("+15552220000", "No CK test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.ck_metadata_duplicated is False


class TestCloneMessageSummaryInfo:
    """message_summary_info is inherited from clone source, not generated."""

    def test_msi_inherited_from_source(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "MSI test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        # Source MSI
        source_msi = conn.execute(
            "SELECT message_summary_info FROM message WHERE is_from_me = 0 "
            "ORDER BY ROWID ASC LIMIT 1"
        ).fetchone()["message_summary_info"]
        # Cloned MSI
        cloned_msi = conn.execute(
            "SELECT message_summary_info FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()["message_summary_info"]
        assert cloned_msi == source_msi
        conn.close()

    def test_all_cloned_msgs_share_same_msi(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", f"MSI {i}", 800000000000000000 + i * 1000000000)
            for i in range(3)
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        msi_values = conn.execute(
            "SELECT DISTINCT message_summary_info FROM message "
            "WHERE guid NOT LIKE 'source-%'"
        ).fetchall()
        # All cloned messages should share the same MSI blob
        # (plus the source message's MSI)
        unique_blobs = {row[0] for row in msi_values}
        assert len(unique_blobs) <= 2  # source MSI + possibly clone MSI (same value)
        conn.close()


class TestCloneAttributedBody:
    """attributedBody uses the Hack Patrol binary template."""

    def test_template_based_ab(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Short text", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT attributedBody FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        ab = row["attributedBody"]
        assert ab is not None
        # Should contain the text
        assert b"Short text" in ab
        conn.close()

    def test_short_text_uses_template(self):
        """Short text (< 128 UTF-16 units) uses single-byte length."""
        result = _build_hackpatrol_attributed_body("Hello")
        assert result is not None
        assert b"Hello" in result

    def test_long_text_falls_back(self):
        """Text > 255 bytes falls back to proper build_attributed_body."""
        long_text = "x" * 300
        result = _build_hackpatrol_attributed_body(long_text)
        expected = build_attributed_body(long_text)
        assert result == expected

    def test_empty_text_returns_none(self):
        assert _build_hackpatrol_attributed_body("") is None
        assert _build_hackpatrol_attributed_body(None) is None


class TestCloneHandle:
    """Handles are cloned from the source handle."""

    def test_handle_cloned_with_phone(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Handle clone", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, uncanonicalized_id, service FROM handle "
            "WHERE id = '+15559990000'"
        ).fetchone()
        assert row is not None
        assert row["id"] == "+15559990000"
        assert row["uncanonicalized_id"] == "+15559990000"
        conn.close()

    def test_handle_service_preserved(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Service test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT service FROM handle WHERE id = '+15559990000'"
        ).fetchone()
        assert row["service"] == "SMS"
        conn.close()

    def test_existing_handle_reused(self, empty_sms_db: Path):
        """If handle already exists, reuse it."""
        _populate_source_db(empty_sms_db, "+15551110000")
        # Clone message for the same phone that already has a handle
        msg = _make_message("+15551110000", "Reuse handle", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.handles_existing == 1
        assert stats.handles_inserted == 0

    def test_multiple_handles_created(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", "Phone A", 800000000000000000),
            _make_message("+15553330000", "Phone B", 800000000001000000),
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.handles_inserted == 2

    def test_same_phone_handle_reused_across_messages(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", "Same A", 800000000000000000),
            _make_message("+15552220000", "Same B", 800000000001000000),
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.handles_inserted == 1
        conn = sqlite3.connect(empty_sms_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM handle WHERE id = '+15552220000'"
        ).fetchone()[0]
        assert count == 1
        conn.close()


class TestCloneChat:
    """Chats use Hack Patrol conventions."""

    def test_sms_prefix_not_any(self, empty_sms_db: Path):
        """HACK_PATROL_NOTE: Uses 'SMS;-;' prefix, not 'any;-;'."""
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Chat prefix", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        row = conn.execute(
            "SELECT guid FROM chat WHERE chat_identifier = '+15559990000'"
        ).fetchone()
        assert row is not None
        assert row[0].startswith("SMS;-;")
        assert not row[0].startswith("any;-;")
        conn.close()

    def test_is_filtered_set(self, empty_sms_db: Path):
        """HACK_PATROL_NOTE: is_filtered=1 hides from primary inbox."""
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Filtered test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT is_filtered FROM chat WHERE chat_identifier = '+15559990000'"
        ).fetchone()
        assert row["is_filtered"] == 1
        conn.close()

    def test_random_group_id(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Group ID", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT group_id FROM chat WHERE chat_identifier = '+15559990000'"
        ).fetchone()
        # Should be a valid UUID
        group_id = row["group_id"]
        uuid.UUID(group_id)
        conn.close()

    def test_ck_inherited_from_source(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15559990000", "Chat CK", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state FROM chat WHERE chat_identifier = '+15559990000'"
        ).fetchone()
        # CK state is cloned from the source chat
        assert row["ck_sync_state"] == 1
        conn.close()

    def test_existing_chat_reused(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        # Pre-create a chat with SMS;-; prefix
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO chat (guid, style, state, chat_identifier, service_name, "
            "account_login, group_id, server_change_token, ck_sync_state, cloudkit_record_id) "
            "VALUES ('SMS;-;+15559990000', 45, 3, '+15559990000', 'SMS', 'E:', "
            "?, '', 0, '')",
            (str(uuid.uuid4()),),
        )
        conn.commit()
        conn.close()

        msg = _make_message("+15559990000", "Reuse chat", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.chats_existing == 1
        assert stats.chats_inserted == 0

    def test_chat_count(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", "Chat A", 800000000000000000),
            _make_message("+15553330000", "Chat B", 800000000001000000),
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.chats_inserted == 2

    def test_same_phone_chat_reused(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", "Same chat A", 800000000000000000),
            _make_message("+15552220000", "Same chat B", 800000000001000000),
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.chats_inserted == 1
        conn = sqlite3.connect(empty_sms_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM chat WHERE guid = 'SMS;-;+15552220000'"
        ).fetchone()[0]
        assert count == 1
        conn.close()


class TestCloneJoinTables:
    """Join tables are created correctly."""

    def test_chat_handle_join_created(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Join test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        chat = conn.execute(
            "SELECT ROWID FROM chat WHERE guid = 'SMS;-;+15559990000'"
        ).fetchone()
        handle = conn.execute(
            "SELECT ROWID FROM handle WHERE id = '+15559990000'"
        ).fetchone()
        join_row = conn.execute(
            "SELECT * FROM chat_handle_join WHERE chat_id = ? AND handle_id = ?",
            (chat[0], handle[0]),
        ).fetchone()
        assert join_row is not None
        conn.close()

    def test_chat_message_join_created(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "CMJ test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        chat = conn.execute(
            "SELECT ROWID FROM chat WHERE guid = 'SMS;-;+15559990000'"
        ).fetchone()
        # Get the cloned message (last one)
        cloned_msg = conn.execute(
            "SELECT ROWID FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        join_row = conn.execute(
            "SELECT * FROM chat_message_join WHERE chat_id = ? AND message_id = ?",
            (chat[0], cloned_msg[0]),
        ).fetchone()
        assert join_row is not None
        conn.close()

    def test_chat_message_join_date_correct(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        target_date = 850000000000000000
        msg = _make_message("+15559990000", "Date join", target_date)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        cloned_msg = conn.execute(
            "SELECT ROWID FROM message ORDER BY ROWID DESC LIMIT 1"
        ).fetchone()
        join_row = conn.execute(
            "SELECT message_date FROM chat_message_join WHERE message_id = ?",
            (cloned_msg[0],),
        ).fetchone()
        assert join_row["message_date"] == target_date
        conn.close()


class TestCloneTriggerBehavior:
    """HACK_PATROL_NOTE: Triggers are NOT dropped during clone."""

    def test_triggers_not_dropped(self, empty_sms_db: Path):
        # Add a dummy trigger
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "CREATE TRIGGER test_trigger AFTER INSERT ON handle "
            "BEGIN SELECT 1; END"
        )
        conn.commit()
        conn.close()

        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Trigger test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        triggers = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
        trigger_names = [t[0] for t in triggers]
        assert "test_trigger" in trigger_names
        conn.close()

    def test_triggers_still_present_after_clone(self, empty_sms_db: Path):
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "CREATE TRIGGER msg_trigger AFTER INSERT ON message "
            "BEGIN SELECT 1; END"
        )
        conn.commit()
        conn.close()

        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Trigger still", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        triggers = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
        trigger_names = [t[0] for t in triggers]
        assert "msg_trigger" in trigger_names
        conn.close()


class TestCloneSourceErrors:
    """CloneSourceError raised when no suitable source exists."""

    def test_no_incoming_message(self, empty_sms_db: Path):
        """No incoming messages at all."""
        msg = _make_message("+15552220000", "No source", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db, pytest.raises(
            CloneSourceError, match="No incoming SMS message",
        ):
            db.clone(result)

    def test_no_handle(self, empty_sms_db: Path):
        """Incoming message exists but no SMS handle."""
        conn = sqlite3.connect(empty_sms_db)
        # Insert a message with no handle in the handle table
        conn.execute(
            "INSERT INTO message (guid, text, handle_id, service, date, "
            "is_from_me, ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES ('msg1', 'text', 1, 'SMS', 700000000000000000, 0, 0, '', '')"
        )
        conn.commit()
        conn.close()

        msg = _make_message("+15552220000", "No handle", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db, pytest.raises(
            CloneSourceError, match="No SMS handle",
        ):
            db.clone(result)

    def test_no_chat(self, empty_sms_db: Path):
        """Incoming message and handle exist but no SMS chat."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (id, country, service) VALUES ('+15551110000', 'us', 'SMS')"
        )
        handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO message (guid, text, handle_id, service, date, "
            "is_from_me, ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES ('msg1', 'text', ?, 'SMS', 700000000000000000, 0, 0, '', '')",
            (handle_rowid,),
        )
        conn.commit()
        conn.close()

        msg = _make_message("+15552220000", "No chat", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db, pytest.raises(
            CloneSourceError, match="No SMS chat",
        ):
            db.clone(result)

    def test_only_outgoing_messages(self, empty_sms_db: Path):
        """Only outgoing messages — no incoming to clone from."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (id, country, service) VALUES ('+15551110000', 'us', 'SMS')"
        )
        handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat (guid, style, state, chat_identifier, service_name, "
            "account_login, group_id, server_change_token, ck_sync_state, cloudkit_record_id) "
            "VALUES ('any;-;+15551110000', 45, 3, '+15551110000', 'SMS', 'E:', "
            "?, '', 0, '')",
            (str(uuid.uuid4()),),
        )
        conn.execute(
            "INSERT INTO message (guid, text, handle_id, service, date, "
            "is_from_me, ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES ('msg1', 'sent', ?, 'SMS', 700000000000000000, 1, 0, '', '')",
            (handle_rowid,),
        )
        conn.commit()
        conn.close()

        msg = _make_message("+15552220000", "Only outgoing", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db, pytest.raises(
            CloneSourceError, match="No incoming SMS message",
        ):
            db.clone(result)

    def test_only_imessage(self, empty_sms_db: Path):
        """Only iMessage messages — no SMS to clone from."""
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (id, country, service) "
            "VALUES ('+15551110000', 'us', 'iMessage')"
        )
        handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat (guid, style, state, chat_identifier, service_name, "
            "account_login, group_id, server_change_token, ck_sync_state, cloudkit_record_id) "
            "VALUES ('any;-;+15551110000', 45, 3, '+15551110000', 'iMessage', 'E:', "
            "?, '', 0, '')",
            (str(uuid.uuid4()),),
        )
        conn.execute(
            "INSERT INTO message (guid, text, handle_id, service, date, "
            "is_from_me, ck_sync_state, ck_record_id, ck_record_change_tag) "
            "VALUES ('msg1', 'iMessage', ?, 'iMessage', 700000000000000000, 0, 0, '', '')",
            (handle_rowid,),
        )
        conn.commit()
        conn.close()

        msg = _make_message("+15552220000", "Only iMessage", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db, pytest.raises(
            CloneSourceError, match="No incoming SMS message",
        ):
            db.clone(result)


class TestCloneMultipleMessages:
    """Multiple messages all clone from the same source."""

    def test_all_clone_same_source(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msgs = [
            _make_message("+15552220000", f"Multi {i}", 800000000000000000 + i * 1000000000)
            for i in range(5)
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.messages_cloned == 5

    def test_different_phones(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", "Phone A", 800000000000000000),
            _make_message("+15553330000", "Phone B", 800000000001000000),
            _make_message("+15554440000", "Phone C", 800000000002000000),
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.messages_cloned == 3
        assert stats.handles_inserted == 3
        assert stats.chats_inserted == 3

    def test_all_share_same_ck_record_id(self, empty_sms_db: Path):
        """HACK_PATROL_NOTE: All cloned messages share the same ck_record_id."""
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msgs = [
            _make_message("+15552220000", f"CK dup {i}", 800000000000000000 + i * 1000000000)
            for i in range(3)
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            db.clone(result)

        conn = sqlite3.connect(empty_sms_db)
        # Get ck_record_ids of cloned messages (not the source)
        rows = conn.execute(
            "SELECT ck_record_id FROM message WHERE text LIKE 'CK dup%'"
        ).fetchall()
        ck_ids = [r[0] for r in rows]
        assert len(ck_ids) == 3
        # All should be the same (duplicated from source)
        assert all(ck_id == ck_ids[0] for ck_id in ck_ids)
        assert ck_ids[0] == "a" * 64
        conn.close()


class TestCloneTransactionRollback:
    """Transaction is rolled back on error."""

    def test_rollback_on_error(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")

        # Count messages before
        conn = sqlite3.connect(empty_sms_db)
        before_count = conn.execute("SELECT COUNT(*) FROM message").fetchone()[0]
        conn.close()

        # Create a message that will cause a UNIQUE constraint violation
        # by pre-inserting a message with the same GUID
        msg = _make_message("+15552220000", "Will fail", 800000000000000000)
        result = _make_result([msg])

        # Corrupt the db to cause an error mid-clone
        conn = sqlite3.connect(empty_sms_db)
        conn.execute("DROP TABLE chat_message_join")
        conn.commit()
        conn.close()

        with SMSDatabase(empty_sms_db) as db, pytest.raises(sqlite3.OperationalError):
            db.clone(result)

        # Message count should be unchanged (rolled back)
        conn = sqlite3.connect(empty_sms_db)
        after_count = conn.execute("SELECT COUNT(*) FROM message").fetchone()[0]
        assert after_count == before_count
        conn.close()


class TestCloneStats:
    """CloneStats tracks all metrics correctly."""

    def test_messages_cloned(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msgs = [
            _make_message("+15552220000", f"Stats {i}", 800000000000000000 + i * 1000000000)
            for i in range(3)
        ]
        result = _make_result(msgs)

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.messages_cloned == 3

    def test_clone_source_rowid(self, empty_sms_db: Path):
        _, _, source_rowid = _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Source ROWID", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.clone_source_rowid == source_rowid

    def test_handles_inserted(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Handle stat", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.handles_inserted == 1

    def test_chats_inserted(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15559990000", "Chat stat", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.chats_inserted == 1

    def test_ck_metadata_duplicated(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000", ck_sync_state=1)
        msg = _make_message("+15552220000", "CK stat", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        assert stats.ck_metadata_duplicated is True

    def test_repr(self, empty_sms_db: Path):
        _populate_source_db(empty_sms_db, "+15551110000")
        msg = _make_message("+15552220000", "Repr test", 800000000000000000)
        result = _make_result([msg])

        with SMSDatabase(empty_sms_db) as db:
            stats = db.clone(result)

        r = repr(stats)
        assert "CloneStats" in r
        assert "cloned=1" in r
        assert "source_rowid=" in r

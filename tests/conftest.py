"""Shared test fixtures for green2blue."""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import zipfile
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# --- Sample NDJSON data ---

SAMPLE_SMS_RECEIVED = {
    "address": "+12025551234",
    "body": "Hello from Android!",
    "date": "1700000000000",
    "type": "1",
    "read": "1",
    "date_sent": "1700000000000",
}

SAMPLE_SMS_SENT = {
    "address": "+12025559876",
    "body": "Hello from me!",
    "date": "1700000001000",
    "type": "2",
    "read": "1",
    "date_sent": "1700000001000",
}

SAMPLE_MMS = {
    "date": "1700000002",
    "msg_box": "1",
    "read": "1",
    "sub": None,
    "ct_t": "application/vnd.wap.multipart.related",
    "__display_name": "Test Sender",
    "__parts": [
        {
            "seq": "0",
            "ct": "text/plain",
            "text": "Check out this photo!",
        },
        {
            "seq": "1",
            "ct": "image/jpeg",
            "_data": "data/parts/image_001.jpg",
            "cl": "photo.jpg",
        },
    ],
    "__addresses": [
        {"address": "+12025551234", "type": "137", "charset": "106"},
        {"address": "+12025559876", "type": "151", "charset": "106"},
    ],
}

SAMPLE_GROUP_MMS = {
    "date": "1700000003",
    "msg_box": "1",
    "read": "1",
    "sub": None,
    "ct_t": "application/vnd.wap.multipart.related",
    "__parts": [
        {
            "seq": "0",
            "ct": "text/plain",
            "text": "Group message!",
        },
    ],
    "__addresses": [
        {"address": "+12025551111", "type": "137", "charset": "106"},
        {"address": "+12025552222", "type": "151", "charset": "106"},
        {"address": "+12025553333", "type": "151", "charset": "106"},
    ],
}

# --- Real SMS Import/Export format fixtures ---
# These match the actual format produced by the SMS IE Android app.
# MMS uses __sender_address (object) + __recipient_addresses (array).
# _data contains full Android filesystem paths.

REAL_FORMAT_MMS = {
    "date": "1700000002",
    "date_sent": "1700000001",
    "msg_box": "1",
    "read": "1",
    "sub": None,
    "ct_t": "application/vnd.wap.multipart.related",
    "__display_name": "Test Sender",
    "__parts": [
        {
            "seq": "0",
            "ct": "text/plain",
            "text": "Check out this photo!",
        },
        {
            "seq": "1",
            "ct": "image/jpeg",
            "_data": (
                "/data/user/0/com.android.providers.telephony"
                "/app_parts/PART_1700000002_image.jpg"
            ),
            "cl": "photo.jpg",
        },
    ],
    "__sender_address": {
        "address": "+12025551234",
        "type": "137",
        "charset": "106",
    },
    "__recipient_addresses": [
        {"address": "+12025559876", "type": "151", "charset": "106"},
    ],
}

REAL_FORMAT_GROUP_MMS = {
    "date": "1700000003",
    "msg_box": "1",
    "read": "1",
    "sub": "Weekend plans",
    "ct_t": "application/vnd.wap.multipart.related",
    "__parts": [
        {
            "seq": "0",
            "ct": "text/plain",
            "text": "Who's coming Saturday?",
        },
    ],
    "__sender_address": {
        "address": "+12025551111",
        "type": "137",
        "charset": "106",
    },
    "__recipient_addresses": [
        {"address": "+12025552222", "type": "151", "charset": "106"},
        {"address": "+12025553333", "type": "151", "charset": "106"},
    ],
}

SAMPLE_RCS_SMS = {
    "address": "+12025551234",
    "body": "RCS message via Google Messages",
    "date": "1700000005000",
    "type": "1",
    "read": "1",
    "date_sent": "1700000005000",
    "rcs_message_type": "1",
    "rcs_delivery_status": "delivered",
    "creator": "com.google.android.apps.messaging",
}

SAMPLE_RCS_MMS = {
    "date": "1700000006",
    "msg_box": "1",
    "read": "1",
    "sub": None,
    "ct_t": "application/vnd.wap.multipart.related",
    "rcs_message_type": "1",
    "creator": "com.google.android.apps.messaging",
    "__parts": [
        {
            "seq": "0",
            "ct": "image/jpeg",
            "_data": "/data/user/0/com.android.providers.telephony/app_parts/PART_rcs_photo.jpg",
            "cl": "rcs_photo.jpg",
        },
    ],
    "__sender_address": {
        "address": "+12025551234",
        "type": "137",
        "charset": "106",
    },
    "__recipient_addresses": [
        {"address": "+12025559876", "type": "151", "charset": "106"},
    ],
}


def make_ndjson_content(*records: dict) -> str:
    """Create NDJSON content from dictionaries."""
    return "\n".join(json.dumps(r) for r in records) + "\n"


@pytest.fixture
def tmp_dir():
    """Provide a temporary directory that is cleaned up after the test."""
    d = tempfile.mkdtemp(prefix="g2b_test_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_export_zip(tmp_dir: Path) -> Path:
    """Create a sample export ZIP file with SMS and MMS data."""
    zip_path = tmp_dir / "export.zip"
    content = make_ndjson_content(SAMPLE_SMS_RECEIVED, SAMPLE_SMS_SENT, SAMPLE_MMS)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", content)
        # Add a fake attachment file
        zf.writestr("data/parts/image_001.jpg", b"\xff\xd8\xff\xe0fake_jpeg_data")

    return zip_path


@pytest.fixture
def empty_sms_db(tmp_dir: Path) -> Path:
    """Create a minimal empty sms.db with the iOS schema."""
    db_path = tmp_dir / "sms.db"
    sql_path = Path(__file__).parent.parent / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(db_path)
    if sql_path.exists():
        conn.executescript(sql_path.read_text())
    else:
        _create_minimal_sms_schema(conn)
    conn.close()
    return db_path


def _create_minimal_sms_schema(conn: sqlite3.Connection) -> None:
    """Create a minimal sms.db schema for testing."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS handle (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL,
            country TEXT DEFAULT 'us',
            service TEXT DEFAULT 'SMS',
            uncanonicalized_id TEXT
        );

        CREATE TABLE IF NOT EXISTS chat (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            guid TEXT UNIQUE NOT NULL,
            style INTEGER DEFAULT 45,
            state INTEGER DEFAULT 3,
            account_id TEXT DEFAULT 'p:0',
            properties BLOB,
            chat_identifier TEXT,
            service_name TEXT DEFAULT 'SMS',
            room_name TEXT,
            account_login TEXT,
            is_archived INTEGER DEFAULT 0,
            last_addressed_handle TEXT,
            display_name TEXT DEFAULT '',
            group_id TEXT,
            is_filtered INTEGER DEFAULT 0,
            successful_query INTEGER DEFAULT 1,
            engram_id TEXT,
            server_change_token TEXT,
            ck_sync_state INTEGER DEFAULT 0,
            original_group_id TEXT,
            last_read_message_timestamp INTEGER DEFAULT 0,
            sr_server_change_token TEXT,
            sr_ck_sync_state INTEGER DEFAULT 0,
            cloudkit_record_id TEXT,
            sr_cloudkit_record_id TEXT,
            last_addressed_sim_id TEXT,
            is_blackholed INTEGER DEFAULT 0,
            syndication_date INTEGER DEFAULT 0,
            syndication_type INTEGER DEFAULT 0,
            is_recovered INTEGER DEFAULT 0,
            is_deleting_incoming_messages INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS message (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            guid TEXT UNIQUE NOT NULL,
            text TEXT,
            replace INTEGER DEFAULT 0,
            service_center TEXT,
            handle_id INTEGER DEFAULT 0,
            subject TEXT,
            country TEXT,
            attributedBody BLOB,
            version INTEGER DEFAULT 1,
            type INTEGER DEFAULT 0,
            service TEXT DEFAULT 'SMS',
            account TEXT DEFAULT 'p:0',
            account_guid TEXT DEFAULT 'p:0',
            error INTEGER DEFAULT 0,
            date INTEGER DEFAULT 0,
            date_read INTEGER DEFAULT 0,
            date_delivered INTEGER DEFAULT 0,
            is_delivered INTEGER DEFAULT 0,
            is_finished INTEGER DEFAULT 1,
            is_emote INTEGER DEFAULT 0,
            is_from_me INTEGER DEFAULT 0,
            is_empty INTEGER DEFAULT 0,
            is_delayed INTEGER DEFAULT 0,
            is_auto_reply INTEGER DEFAULT 0,
            is_prepared INTEGER DEFAULT 0,
            is_read INTEGER DEFAULT 0,
            is_system_message INTEGER DEFAULT 0,
            is_sent INTEGER DEFAULT 0,
            has_dd_results INTEGER DEFAULT 0,
            is_service_message INTEGER DEFAULT 0,
            is_forward INTEGER DEFAULT 0,
            was_downgraded INTEGER DEFAULT 0,
            is_archive INTEGER DEFAULT 0,
            cache_has_attachments INTEGER DEFAULT 0,
            cache_roomnames TEXT,
            was_data_detected INTEGER DEFAULT 0,
            was_deduplicated INTEGER DEFAULT 0,
            is_audio_message INTEGER DEFAULT 0,
            is_played INTEGER DEFAULT 0,
            date_played INTEGER DEFAULT 0,
            item_type INTEGER DEFAULT 0,
            other_handle INTEGER DEFAULT 0,
            group_title TEXT,
            group_action_type INTEGER DEFAULT 0,
            share_status INTEGER DEFAULT 0,
            share_direction INTEGER DEFAULT 0,
            is_expirable INTEGER DEFAULT 0,
            expire_state INTEGER DEFAULT 0,
            message_action_type INTEGER DEFAULT 0,
            message_source INTEGER DEFAULT 0,
            associated_message_guid TEXT,
            associated_message_type INTEGER DEFAULT 0,
            balloon_bundle_id TEXT,
            payload_data BLOB,
            expressive_send_style_id TEXT,
            associated_message_range_location INTEGER DEFAULT 0,
            associated_message_range_length INTEGER DEFAULT 0,
            time_expressive_send_played INTEGER DEFAULT 0,
            message_summary_info BLOB,
            ck_sync_state INTEGER DEFAULT 0,
            ck_record_id TEXT,
            ck_record_change_tag TEXT,
            destination_caller_id TEXT,
            sr_ck_sync_state INTEGER DEFAULT 0,
            sr_ck_record_id TEXT,
            sr_ck_record_change_tag TEXT,
            is_corrupt INTEGER DEFAULT 0,
            reply_to_guid TEXT,
            date_recovered INTEGER DEFAULT 0,
            sort_id INTEGER DEFAULT 0,
            is_spam INTEGER DEFAULT 0,
            has_unseen_mention INTEGER DEFAULT 0,
            thread_originator_guid TEXT,
            thread_originator_part TEXT,
            syndication_ranges BLOB,
            was_delivered_quietly INTEGER DEFAULT 0,
            did_notify_recipient INTEGER DEFAULT 0,
            synced_syndication_ranges BLOB,
            date_retracted INTEGER DEFAULT 0,
            date_edited INTEGER DEFAULT 0,
            was_detonated INTEGER DEFAULT 0,
            part_count INTEGER DEFAULT 1,
            is_stewie INTEGER DEFAULT 0,
            is_kt_verified INTEGER DEFAULT 0,
            is_sos INTEGER DEFAULT 0,
            is_critical INTEGER DEFAULT 0,
            bia_reference_id TEXT,
            fallback_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS attachment (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            guid TEXT UNIQUE NOT NULL,
            created_date INTEGER DEFAULT 0,
            start_date INTEGER DEFAULT 0,
            filename TEXT,
            uti TEXT,
            mime_type TEXT,
            transfer_state INTEGER DEFAULT 5,
            is_outgoing INTEGER DEFAULT 0,
            user_info BLOB,
            transfer_name TEXT,
            total_bytes INTEGER DEFAULT 0,
            is_sticker INTEGER DEFAULT 0,
            sticker_user_info BLOB,
            attribution_info BLOB,
            hide_attachment INTEGER DEFAULT 0,
            ck_sync_state INTEGER DEFAULT 0,
            ck_record_id TEXT,
            original_guid TEXT,
            preview_generation_state INTEGER DEFAULT 0,
            sr_ck_sync_state INTEGER DEFAULT 0,
            sr_ck_record_id TEXT,
            is_commsafety_sensitive INTEGER DEFAULT 0,
            emoji_image_short_description TEXT,
            synced_fallback_image BLOB
        );

        CREATE TABLE IF NOT EXISTS chat_handle_join (
            chat_id INTEGER,
            handle_id INTEGER,
            UNIQUE(chat_id, handle_id)
        );

        CREATE TABLE IF NOT EXISTS chat_message_join (
            chat_id INTEGER,
            message_id INTEGER,
            message_date INTEGER DEFAULT 0,
            UNIQUE(chat_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS message_attachment_join (
            message_id INTEGER,
            attachment_id INTEGER,
            UNIQUE(message_id, attachment_id)
        );
    """)
    conn.commit()


@pytest.fixture
def sample_backup_dir(tmp_dir: Path, empty_sms_db: Path) -> Path:
    """Create a minimal iPhone backup directory structure."""
    backup_dir = tmp_dir / "backup" / "00000000-AAAA-BBBB-CCCC-DDDDDDDDDDDD"
    backup_dir.mkdir(parents=True)

    # Hash for sms.db: SHA1("HomeDomain-Library/SMS/sms.db")
    sms_hash = "3d0d7e5fb2ce288813306e4d4636395e047a3d28"
    hash_dir = backup_dir / sms_hash[:2]
    hash_dir.mkdir()
    shutil.copy2(empty_sms_db, hash_dir / sms_hash)

    # Create Manifest.db
    manifest_path = backup_dir / "Manifest.db"
    conn = sqlite3.connect(manifest_path)
    conn.execute("""
        CREATE TABLE Files (
            fileID TEXT PRIMARY KEY,
            domain TEXT,
            relativePath TEXT,
            flags INTEGER,
            file BLOB
        )
    """)
    conn.execute(
        "INSERT INTO Files (fileID, domain, relativePath, flags, file) VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    # Create Info.plist (minimal)
    import plistlib

    info_plist = {
        "Device Name": "Test iPhone",
        "Product Version": "17.0",
        "Unique Identifier": "00000000-AAAA-BBBB-CCCC-DDDDDDDDDDDD",
    }
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps(info_plist))

    # Create Manifest.plist
    manifest_plist = {
        "IsEncrypted": False,
        "Version": "3.3",
    }
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps(manifest_plist))

    # Create Status.plist
    status_plist = {
        "IsFullBackup": True,
        "Version": "3.3",
        "BackupState": "new",
        "Date": "2024-01-01T00:00:00Z",
    }
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps(status_plist))

    return backup_dir

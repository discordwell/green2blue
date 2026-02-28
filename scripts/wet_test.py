#!/usr/bin/env python3
"""Wet test: create realistic test data and run green2blue end-to-end.

Creates:
  1. A synthetic Android SMS/MMS export ZIP (like SMS Import/Export produces)
  2. A synthetic iPhone backup directory with empty sms.db
  3. Runs the full CLI flow: inspect → dry-run → inject → verify

Usage:
    python scripts/wet_test.py           # full wet test with synthetic backup
    python scripts/wet_test.py --real    # use a real iPhone backup (must exist)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

# Ensure green2blue is importable from the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


# ---------------------------------------------------------------------------
# Android export builder
# ---------------------------------------------------------------------------

def _ts_ms(dt: datetime) -> str:
    """Unix timestamp in milliseconds (SMS format)."""
    return str(int(dt.timestamp() * 1000))


def _ts_s(dt: datetime) -> str:
    """Unix timestamp in seconds (MMS format)."""
    return str(int(dt.timestamp()))


def build_android_export(out_dir: Path) -> Path:
    """Create a realistic Android SMS/MMS export ZIP."""
    zip_path = out_dir / "android_export.zip"

    # Base time: a week ago
    base = datetime.now() - timedelta(days=7)

    # Contacts (using obviously-fake 555 numbers)
    alice = "+12025551001"
    bob = "+12025551002"
    carol = "+12025551003"
    me = "+12025559999"

    records: list[dict] = []

    # --- Conversation 1: 1-on-1 SMS with Alice ---
    msgs = [
        (alice, "1", "Hey! Are you coming to the party tonight?", 0),
        (me, "2", "Definitely! What time does it start?", 1),
        (alice, "1", "7pm at the usual spot. Bring snacks!", 3),
        (me, "2", "Will do. See you there!", 5),
        (alice, "1", "Actually can you pick up ice too?", 8),
        (me, "2", "Sure thing", 10),
        (alice, "1", "You're the best!", 12),
    ]
    for addr, msg_type, body, offset_min in msgs:
        dt = base + timedelta(minutes=offset_min)
        records.append({
            "address": addr if msg_type == "1" else alice,
            "body": body,
            "date": _ts_ms(dt),
            "type": msg_type,
            "read": "1",
            "date_sent": _ts_ms(dt),
        })

    # --- Conversation 2: 1-on-1 SMS with Bob ---
    bob_msgs = [
        (bob, "1", "Did you see the game last night?", 60),
        (me, "2", "Yeah, incredible finish!", 62),
        (bob, "1", "That last play was unreal", 63),
        (me, "2", "We should go to a game sometime", 65),
        (bob, "1", "I'm down. Let me check schedules", 70),
    ]
    for addr, msg_type, body, offset_min in bob_msgs:
        dt = base + timedelta(minutes=offset_min)
        records.append({
            "address": addr if msg_type == "1" else bob,
            "body": body,
            "date": _ts_ms(dt),
            "type": msg_type,
            "read": "1",
            "date_sent": _ts_ms(dt),
        })

    # --- Conversation 3: MMS with photo from Alice ---
    dt_mms = base + timedelta(hours=2)
    records.append({
        "date": _ts_s(dt_mms),
        "msg_box": "1",
        "read": "1",
        "sub": None,
        "ct_t": "application/vnd.wap.multipart.related",
        "__parts": [
            {"seq": "0", "ct": "text/plain", "text": "Look at this sunset!"},
            {
                "seq": "1",
                "ct": "image/jpeg",
                "_data": "data/parts/sunset.jpg",
                "cl": "sunset.jpg",
            },
        ],
        "__addresses": [
            {"address": alice, "type": "137", "charset": "106"},
            {"address": me, "type": "151", "charset": "106"},
        ],
    })

    # --- Conversation 4: Group MMS (Alice + Bob + Carol) ---
    dt_group = base + timedelta(hours=4)
    group_msgs = [
        (alice, "137", "Weekend camping trip - who's in?", 0),
        (bob, "137", "Count me in!", 5),
        (carol, "137", "Same! What should I bring?", 8),
        (alice, "137", "Tents and sleeping bags. I'll handle food.", 12),
    ]
    for sender, stype, body, offset_min in group_msgs:
        dt = dt_group + timedelta(minutes=offset_min)
        all_addrs = [alice, bob, carol, me]
        addresses = []
        for addr in all_addrs:
            if addr == sender:
                addresses.append({"address": addr, "type": "137", "charset": "106"})
            else:
                addresses.append({"address": addr, "type": "151", "charset": "106"})
        records.append({
            "date": _ts_s(dt),
            "msg_box": "1" if sender != me else "2",
            "read": "1",
            "sub": "Camping Trip",
            "ct_t": "application/vnd.wap.multipart.related",
            "__parts": [
                {"seq": "0", "ct": "text/plain", "text": body},
            ],
            "__addresses": addresses,
        })

    # --- Conversation 5: RCS message (should be counted but treated as SMS) ---
    dt_rcs = base + timedelta(hours=6)
    records.append({
        "address": bob,
        "body": "Sent via RCS - does this work?",
        "date": _ts_ms(dt_rcs),
        "type": "1",
        "read": "1",
        "date_sent": _ts_ms(dt_rcs),
        "rcs_message_type": "1",
        "rcs_delivery_status": "delivered",
        "creator": "com.google.android.apps.messaging",
    })

    # Build the NDJSON content
    ndjson = "\n".join(json.dumps(r) for r in records) + "\n"

    # Create a small fake JPEG for the MMS attachment
    # (JFIF header so it looks plausible)
    fake_jpeg = (
        b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
        + b"\x00" * 100
        + b"\xff\xd9"
    )

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)
        zf.writestr("data/parts/sunset.jpg", fake_jpeg)

    return zip_path


# ---------------------------------------------------------------------------
# Synthetic iPhone backup builder
# ---------------------------------------------------------------------------

SMS_DB_DOMAIN_PATH = b"HomeDomain-Library/SMS/sms.db"


def build_synthetic_backup(out_dir: Path) -> Path:
    """Create a minimal but valid iPhone backup directory."""
    udid = "WETTEST-0000-AAAA-BBBB-CCCCDDDDEEEE"
    backup_root = out_dir / "backups"
    backup_dir = backup_root / udid
    backup_dir.mkdir(parents=True)

    # sms.db with full schema
    sms_hash = hashlib.sha1(SMS_DB_DOMAIN_PATH).hexdigest()
    hash_dir = backup_dir / sms_hash[:2]
    hash_dir.mkdir()
    sms_db_path = hash_dir / sms_hash
    _create_sms_db(sms_db_path)

    # Manifest.db
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
        "INSERT INTO Files (fileID, domain, relativePath, flags, file) "
        "VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    # Info.plist
    info = {
        "Device Name": "Wet Test iPhone",
        "Product Version": "18.3",
        "Unique Identifier": udid,
        "Build Version": "22D60",
    }
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps(info))

    # Manifest.plist
    manifest_plist = {"IsEncrypted": False, "Version": "3.3"}
    (backup_dir / "Manifest.plist").write_bytes(plistlib.dumps(manifest_plist))

    # Status.plist
    status = {
        "IsFullBackup": True,
        "Version": "3.3",
        "BackupState": "new",
        "Date": datetime.now().isoformat(),
    }
    (backup_dir / "Status.plist").write_bytes(plistlib.dumps(status))

    return backup_dir


def _create_sms_db(path: Path) -> None:
    """Create a minimal empty sms.db with the iOS schema."""
    conn = sqlite3.connect(path)
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
    conn.close()


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

def run_g2b(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run green2blue as a subprocess and print output."""
    cmd = [sys.executable, "-m", "green2blue", *args]
    print(f"\n{'='*60}")
    print(f"$ green2blue {' '.join(args)}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if check and result.returncode != 0:
        print(f"\n[FAIL] Exit code {result.returncode}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"[OK] Exit code {result.returncode}")
    return result


def dump_sms_db(backup_dir: Path) -> None:
    """Print a summary of what's in the sms.db after injection."""
    sms_hash = hashlib.sha1(SMS_DB_DOMAIN_PATH).hexdigest()
    sms_path = backup_dir / sms_hash[:2] / sms_hash
    if not sms_path.exists():
        print("[WARN] sms.db not found for post-injection dump")
        return

    conn = sqlite3.connect(sms_path)
    print(f"\n{'='*60}")
    print("POST-INJECTION DATABASE CONTENTS")
    print(f"{'='*60}")

    # Handles
    handles = conn.execute("SELECT ROWID, id, service FROM handle ORDER BY ROWID").fetchall()
    print(f"\nHandles ({len(handles)}):")
    for rowid, hid, svc in handles:
        print(f"  [{rowid}] {hid} ({svc})")

    # Chats
    chats = conn.execute(
        "SELECT ROWID, guid, style, chat_identifier, display_name FROM chat ORDER BY ROWID"
    ).fetchall()
    print(f"\nChats ({len(chats)}):")
    for rowid, guid, style, ci, dn in chats:
        style_name = "group" if style == 43 else "1-on-1"
        print(f"  [{rowid}] {guid} ({style_name}) identifier={ci}")

    # Messages
    messages = conn.execute(
        "SELECT m.ROWID, m.guid, m.text, m.is_from_me, h.id as handle "
        "FROM message m LEFT JOIN handle h ON m.handle_id = h.ROWID "
        "ORDER BY m.date"
    ).fetchall()
    print(f"\nMessages ({len(messages)}):")
    for rowid, guid, text, from_me, handle in messages:
        direction = "SENT" if from_me else "RECV"
        text_preview = (text[:50] + "...") if text and len(text) > 50 else (text or "[attachment]")
        print(f"  [{rowid}] {direction} {handle}: {text_preview}")

    # Attachments
    attachments = conn.execute(
        "SELECT ROWID, guid, filename, mime_type, total_bytes FROM attachment"
    ).fetchall()
    print(f"\nAttachments ({len(attachments)}):")
    for rowid, guid, fname, mime, size in attachments:
        print(f"  [{rowid}] {mime} {fname} ({size} bytes)")

    # Join table counts
    chj = conn.execute("SELECT COUNT(*) FROM chat_handle_join").fetchone()[0]
    cmj = conn.execute("SELECT COUNT(*) FROM chat_message_join").fetchone()[0]
    maj = conn.execute("SELECT COUNT(*) FROM message_attachment_join").fetchone()[0]
    print(f"\nJoin tables: chat_handle={chj}, chat_message={cmj}, message_attachment={maj}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="green2blue wet test")
    parser.add_argument(
        "--real", action="store_true",
        help="Use a real iPhone backup instead of synthetic",
    )
    parser.add_argument(
        "--keep", action="store_true",
        help="Keep test artifacts after completion",
    )
    args = parser.parse_args()

    tmpdir = Path(tempfile.mkdtemp(prefix="g2b_wettest_"))
    print(f"Working directory: {tmpdir}")

    try:
        # Step 1: Build Android export
        print("\n" + "=" * 60)
        print("STEP 1: Building Android SMS export ZIP")
        print("=" * 60)
        export_zip = build_android_export(tmpdir)
        print(f"Created: {export_zip}")
        print(f"Size: {export_zip.stat().st_size} bytes")

        # Step 2: Set up backup
        if args.real:
            print("\n" + "=" * 60)
            print("STEP 2: Using real iPhone backup")
            print("=" * 60)
            run_g2b("list-backups")
            backup_root = None
            backup_arg = []
        else:
            print("\n" + "=" * 60)
            print("STEP 2: Building synthetic iPhone backup")
            print("=" * 60)
            backup_dir = build_synthetic_backup(tmpdir)
            backup_root = tmpdir / "backups"
            backup_arg = ["--backup-root", str(backup_root)]
            print(f"Created backup: {backup_dir}")

            # Verify it's detected
            run_g2b("list-backups", *backup_arg)

        # Step 3: Inspect the export
        print("\n" + "=" * 60)
        print("STEP 3: Inspecting Android export")
        print("=" * 60)
        run_g2b("inspect", str(export_zip))

        # Step 4: Dry run
        print("\n" + "=" * 60)
        print("STEP 4: Dry run (no modifications)")
        print("=" * 60)
        run_g2b("inject", str(export_zip), *backup_arg, "--dry-run", "-y", "-v")

        # Step 5: Real injection
        print("\n" + "=" * 60)
        print("STEP 5: LIVE INJECTION")
        print("=" * 60)
        run_g2b("inject", str(export_zip), *backup_arg, "-y", "-v")

        # Step 6: Verify
        if not args.real:
            print("\n" + "=" * 60)
            print("STEP 6: Post-injection verification")
            print("=" * 60)
            run_g2b("verify", str(backup_dir))

            # Step 7: Dump database contents
            dump_sms_db(backup_dir)

            # Step 8: Test duplicate prevention (re-inject)
            print("\n" + "=" * 60)
            print("STEP 7: Re-injection (duplicate prevention test)")
            print("=" * 60)
            run_g2b("inject", str(export_zip), *backup_arg, "-y",
                     "--backup", str(backup_dir))

            # Step 9: Verify again
            print("\n" + "=" * 60)
            print("STEP 8: Post-re-injection verification")
            print("=" * 60)
            run_g2b("verify", str(backup_dir))

        print("\n" + "=" * 60)
        print("WET TEST COMPLETE")
        print("=" * 60)

    finally:
        if args.keep:
            print(f"\nArtifacts preserved at: {tmpdir}")
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"\nCleaned up: {tmpdir}")


if __name__ == "__main__":
    main()

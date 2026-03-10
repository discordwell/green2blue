#!/usr/bin/env python3
"""Wet test: full overwrite pipeline with +16283105601 conversation.

Tests the complete overwrite mode pipeline:
1. Finds the chat ROWID for +16283105601 in the backup
2. Creates a synthetic Android export with test messages
3. Runs the full pipeline with --mode overwrite + --disable-icloud-sync
4. After restore, diagnoses what survived CK reconciliation

Usage:
    # Phase 1: Find sacrifice chat and show info
    python scripts/wet_test_full_overwrite.py --password glorious1 --info

    # Phase 2: Run the full overwrite pipeline
    python scripts/wet_test_full_overwrite.py --password glorious1

    # Phase 3: After restore + Apple ID sign-in, diagnose
    python scripts/wet_test_full_overwrite.py --password glorious1 --diagnose
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from green2blue.ios.backup import find_backup, get_sms_db_path  # noqa: E402
from green2blue.ios.crypto import EncryptedBackup  # noqa: E402
from green2blue.ios.manifest import ManifestDB, compute_file_id  # noqa: E402

logger = logging.getLogger(__name__)

# Target conversation: +16283105601 (already in iMessage with visible content)
TARGET_PHONE = "+16283105601"

# Marker so we can find overwritten messages after restore
MARKER = "[G2B-FULL]"

# Test messages — mix of sent/received to cover both directions
TEST_MESSAGES = [
    {"text": f"{MARKER} Overwrite A: Does the full pipeline work?", "type": "1"},
    {"text": f"{MARKER} Overwrite B: CK metadata preserved?", "type": "2"},
    {"text": f"{MARKER} Overwrite C: iCloud sync disabled?", "type": "1"},
    {"text": f"{MARKER} Overwrite D: Will Apple notice the swap?", "type": "2"},
    {"text": f"{MARKER} Overwrite E: Content changed, CK intact", "type": "1"},
    {"text": f"{MARKER} Overwrite F: Final overwrite test msg", "type": "2"},
]


def decrypt_sms_db(backup_info, password: str):
    """Decrypt sms.db and return (temp_db_path, eb, temp_manifest)."""
    eb = EncryptedBackup(backup_info.path, password)
    eb.unlock()

    temp_manifest = eb.decrypt_manifest_db()
    sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    with ManifestDB(temp_manifest) as manifest:
        sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)

    sms_db_path = get_sms_db_path(backup_info.path)
    decrypted = eb.decrypt_db_file(
        sms_db_path.read_bytes(), sms_enc_key, sms_prot_class,
    )

    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    tmp_path = Path(tmp)
    tmp_path.write_bytes(decrypted)

    return tmp_path, eb, temp_manifest


def find_sacrifice_chat(conn: sqlite3.Connection, phone: str) -> dict | None:
    """Find the chat for a phone number and return info about it."""
    conn.row_factory = sqlite3.Row

    # Find chat by chat_identifier
    row = conn.execute(
        "SELECT ROWID, chat_identifier, display_name, ck_sync_state "
        "FROM chat WHERE chat_identifier = ?",
        (phone,),
    ).fetchone()

    if not row:
        return None

    chat_rowid = row["ROWID"]

    # Count messages in this chat
    msg_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM chat_message_join WHERE chat_id = ?",
        (chat_rowid,),
    ).fetchone()["cnt"]

    # Get first and last messages
    first = conn.execute(
        """
        SELECT m.ROWID, m.text, m.date, m.is_from_me,
               m.ck_sync_state, m.ck_record_id, m.ck_record_change_tag
        FROM message m
        JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        WHERE cmj.chat_id = ?
        ORDER BY m.date ASC LIMIT 1
        """,
        (chat_rowid,),
    ).fetchone()

    last = conn.execute(
        """
        SELECT m.ROWID, m.text, m.date, m.is_from_me,
               m.ck_sync_state, m.ck_record_id, m.ck_record_change_tag
        FROM message m
        JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        WHERE cmj.chat_id = ?
        ORDER BY m.date DESC LIMIT 1
        """,
        (chat_rowid,),
    ).fetchone()

    return {
        "chat_rowid": chat_rowid,
        "chat_identifier": row["chat_identifier"],
        "display_name": row["display_name"],
        "ck_sync_state": row["ck_sync_state"],
        "msg_count": msg_count,
        "first": dict(first) if first else None,
        "last": dict(last) if last else None,
    }


def show_info(conn: sqlite3.Connection) -> int:
    """Show info about the sacrifice conversation."""
    info = find_sacrifice_chat(conn, TARGET_PHONE)

    if not info:
        print(f"ERROR: No conversation found for {TARGET_PHONE}")
        print("Make sure you've texted this number and backed up your phone.")
        return 1

    print(f"\n=== Sacrifice Chat: {TARGET_PHONE} ===\n")
    print(f"Chat ROWID:       {info['chat_rowid']}")
    print(f"Chat identifier:  {info['chat_identifier']}")
    print(f"Display name:     {info['display_name'] or '(none)'}")
    print(f"CK sync state:    {info['ck_sync_state']}")
    print(f"Message count:    {info['msg_count']}")
    print(f"Test messages:    {len(TEST_MESSAGES)}")

    if info['msg_count'] < len(TEST_MESSAGES):
        print(f"\nWARNING: Only {info['msg_count']} messages available, "
              f"need {len(TEST_MESSAGES)}!")
        print(f"Send more messages to/from {TARGET_PHONE} and re-backup.")
        return 1

    if info['first']:
        f = info['first']
        direction = "sent" if f['is_from_me'] else "received"
        ck_id = f['ck_record_id'][:16] + '...' if f['ck_record_id'] else '(none)'
        print("\nOldest message (will be overwritten first):")
        print(f"  ROWID={f['ROWID']} {direction}: {repr((f['text'] or '')[:60])}")
        print(f"  ck_sync_state={f['ck_sync_state']}, ck_record_id={ck_id}")

    if info['last']:
        last = info['last']
        direction = "sent" if last['is_from_me'] else "received"
        print("\nNewest message:")
        print(f"  ROWID={last['ROWID']} {direction}: {repr((last['text'] or '')[:60])}")

    print("\n--- CLI Command ---")
    print("python scripts/wet_test_full_overwrite.py --password glorious1")
    print("\nThis will run:")
    print("  green2blue inject <export.zip> \\")
    print("    --mode overwrite \\")
    print(f"    --sacrifice-chat {info['chat_rowid']} \\")
    print("    --disable-icloud-sync \\")
    print("    --password glorious1")

    return 0


def build_export_zip(tmp_dir: Path) -> Path:
    """Create a synthetic Android export ZIP with test messages to/from TARGET_PHONE."""
    zip_path = tmp_dir / "overwrite_test_export.zip"
    base = datetime.now() - timedelta(days=1)

    records = []
    for i, msg in enumerate(TEST_MESSAGES):
        dt = base + timedelta(minutes=i * 5)
        ts_ms = str(int(dt.timestamp() * 1000))
        records.append({
            "address": TARGET_PHONE,
            "body": msg["text"],
            "date": ts_ms,
            "type": msg["type"],  # 1=received, 2=sent
            "read": "1",
            "date_sent": ts_ms,
        })

    ndjson = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)

    return zip_path


def run_overwrite(args) -> int:
    """Run the full overwrite pipeline."""
    from green2blue.models import CKStrategy, InjectionMode
    from green2blue.pipeline import run_pipeline

    backup_info = find_backup(args.backup, args.backup_root)
    print(f"Backup: {backup_info.device_name} ({backup_info.path.name})")
    print(f"Encrypted: {backup_info.is_encrypted}")

    # First, find the chat ROWID
    if backup_info.is_encrypted:
        if not args.password:
            print("ERROR: --password required for encrypted backups")
            return 1
        tmp_db, eb, temp_manifest = decrypt_sms_db(backup_info, args.password)
        conn = sqlite3.connect(tmp_db)
    else:
        sms_db_path = get_sms_db_path(backup_info.path)
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        tmp_db = Path(tmp)
        tmp_db.write_bytes(sms_db_path.read_bytes())
        conn = sqlite3.connect(tmp_db)

    info = find_sacrifice_chat(conn, TARGET_PHONE)
    conn.close()
    tmp_db.unlink(missing_ok=True)

    if not info:
        print(f"ERROR: No conversation found for {TARGET_PHONE}")
        return 1

    chat_rowid = info['chat_rowid']
    print(f"\nSacrifice chat: {TARGET_PHONE} (ROWID={chat_rowid}, "
          f"{info['msg_count']} messages)")

    if info['msg_count'] < len(TEST_MESSAGES):
        print(f"ERROR: Only {info['msg_count']} messages, need {len(TEST_MESSAGES)}")
        return 1

    # Create synthetic Android export
    tmp_dir = Path(tempfile.mkdtemp(prefix="g2b_wet_"))
    export_zip = build_export_zip(tmp_dir)
    print(f"Test export: {export_zip} ({len(TEST_MESSAGES)} messages)")

    if args.dry_run:
        print("\n[DRY RUN] Would run overwrite pipeline. Exiting.")
        tmp_dir.cleanup() if hasattr(tmp_dir, 'cleanup') else None
        return 0

    # Run the full pipeline
    print("\nRunning overwrite pipeline...")
    print("  --mode overwrite")
    print(f"  --sacrifice-chat {chat_rowid}")
    print("  --disable-icloud-sync")

    result = run_pipeline(
        export_path=export_zip,
        backup_path_or_udid=str(backup_info.path),
        backup_root=args.backup_root,
        country="US",
        skip_duplicates=True,
        include_attachments=False,
        dry_run=False,
        password=args.password,
        ck_strategy=CKStrategy.NONE,
        service="iMessage",
        injection_mode=InjectionMode.OVERWRITE,
        sacrifice_chats=[chat_rowid],
        disable_icloud_sync=True,
    )

    # Print results
    ow = result.overwrite_stats
    if ow:
        print("\n--- Overwrite Results ---")
        print(f"Sacrifice pool:       {ow.sacrifice_pool_size}")
        print(f"Messages overwritten: {ow.messages_overwritten}")
        print(f"Messages skipped:     {ow.messages_skipped}")
        print(f"Handles created:      {ow.handles_inserted} (reused: {ow.handles_existing})")
        print(f"Chats created:        {ow.chats_inserted} (reused: {ow.chats_existing})")
        print(f"Attachments:          {ow.attachments_inserted}")

    if result.verification:
        v = result.verification
        status = "PASSED" if v.passed else "FAILED"
        print(f"Verification:         {status} ({v.checks_passed}/{v.checks_run})")
        for err in v.errors:
            print(f"  ERROR: {err}")

    if result.safety_copy_path:
        print(f"Safety copy:          {result.safety_copy_path}")

    # Cleanup
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)

    print("\nDone! Backup modified with overwrite + iCloud sync disabled.")
    print("\nNext steps:")
    print("  1. Restore this backup via Finder")
    print("  2. Sign into Apple ID (iCloud Messages should be off)")
    print(f"  3. Open Messages — check {TARGET_PHONE} conversation")
    print(f"  4. Look for messages starting with '{MARKER}'")
    pw = f" --password {args.password}" if args.password else ""
    print("  5. Back up again, then run:")
    print(f"     python scripts/wet_test_full_overwrite.py --diagnose{pw}")

    return 0


def diagnose(args) -> int:
    """Check which overwritten messages survived after restore + sync."""
    backup_info = find_backup(args.backup, args.backup_root)
    print(f"Backup: {backup_info.device_name} ({backup_info.path.name})")

    if backup_info.is_encrypted:
        if not args.password:
            print("ERROR: --password required")
            return 1
        tmp_db, eb, temp_manifest = decrypt_sms_db(backup_info, args.password)
        conn = sqlite3.connect(tmp_db)
    else:
        sms_db_path = get_sms_db_path(backup_info.path)
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        tmp_db = Path(tmp)
        tmp_db.write_bytes(sms_db_path.read_bytes())
        conn = sqlite3.connect(tmp_db)

    conn.row_factory = sqlite3.Row

    print("\n=== Overwrite Test Results ===\n")

    # Check for marker messages
    rows = conn.execute(
        "SELECT m.ROWID, m.guid, m.text, m.is_from_me, "
        "m.ck_sync_state, m.ck_record_id, m.ck_record_change_tag, "
        "m.date, m.handle_id, h.id as handle "
        "FROM message m LEFT JOIN handle h ON m.handle_id = h.ROWID "
        "WHERE m.text LIKE ?",
        (f"{MARKER}%",),
    ).fetchall()

    if rows:
        print(f"SURVIVED: {len(rows)}/{len(TEST_MESSAGES)} overwritten messages found!\n")
        for row in rows:
            direction = "sent" if row["is_from_me"] else "received"
            ck_id = row["ck_record_id"]
            ck_short = ck_id[:16] + '...' if ck_id else '(none)'
            print(f"  ROWID={row['ROWID']} [{direction}] {row['text'][:60]}")
            print(f"    handle={row['handle']}, ck_state={row['ck_sync_state']}, "
                  f"ck_id={ck_short}, tag={row['ck_record_change_tag']}")
    else:
        print("NONE of the overwritten messages survived.")

    # Check the target conversation
    info = find_sacrifice_chat(conn, TARGET_PHONE)
    if info:
        print(f"\n--- {TARGET_PHONE} conversation ---")
        print(f"Messages: {info['msg_count']}")

        # Show all messages in the conversation
        all_msgs = conn.execute(
            """
            SELECT m.ROWID, m.text, m.is_from_me, m.date,
                   m.ck_sync_state, m.ck_record_id, m.ck_record_change_tag
            FROM message m
            JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
            WHERE cmj.chat_id = ?
            ORDER BY m.date ASC
            """,
            (info['chat_rowid'],),
        ).fetchall()

        for msg in all_msgs:
            direction = ">" if msg["is_from_me"] else "<"
            txt = (msg["text"] or "")[:55]
            marker = " *** OVERWRITTEN ***" if txt.startswith(MARKER) else ""
            ck_id = msg["ck_record_id"]
            has_ck = "CK" if ck_id else "no-CK"
            print(f"  {direction} ROWID={msg['ROWID']} state={msg['ck_sync_state']} "
                  f"[{has_ck}] {txt}{marker}")
    else:
        print(f"\nConversation with {TARGET_PHONE} not found in backup!")

    # Check iCloud sync status
    print("\n--- iCloud Messages Status ---")
    madrid_file_id = compute_file_id(
        "HomeDomain", "Library/Preferences/com.apple.madrid.plist",
    )
    madrid_path = backup_info.path / madrid_file_id[:2] / madrid_file_id
    if madrid_path.exists():
        import plistlib
        if backup_info.is_encrypted:
            with ManifestDB(temp_manifest) as manifest:
                enc_key, prot_class = manifest.get_file_encryption_info(madrid_file_id)
            data = eb.decrypt_db_file(madrid_path.read_bytes(), enc_key, prot_class)
        else:
            data = madrid_path.read_bytes()
        plist = plistlib.loads(data)
        ck_enabled = plist.get("CloudKitSyncingEnabled", "NOT SET")
        print(f"CloudKitSyncingEnabled: {ck_enabled}")
    else:
        print("com.apple.madrid.plist not found in backup")

    conn.close()
    tmp_db.unlink(missing_ok=True)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Full overwrite pipeline test with +16283105601",
    )
    parser.add_argument("--password", help="Backup encryption password")
    parser.add_argument("--backup", help="Backup path or UDID")
    parser.add_argument("--backup-root", type=Path, default=None)
    parser.add_argument("--info", action="store_true",
                        help="Show sacrifice chat info only")
    parser.add_argument("--diagnose", action="store_true",
                        help="Check results after restore")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without modifying")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if args.info:
        backup_info = find_backup(args.backup, args.backup_root)
        print(f"Backup: {backup_info.device_name} ({backup_info.path.name})")

        if backup_info.is_encrypted:
            if not args.password:
                print("ERROR: --password required")
                return 1
            tmp_db, _, _ = decrypt_sms_db(backup_info, args.password)
            conn = sqlite3.connect(tmp_db)
            result = show_info(conn)
            conn.close()
            tmp_db.unlink(missing_ok=True)
            return result
        else:
            sms_db_path = get_sms_db_path(backup_info.path)
            conn = sqlite3.connect(sms_db_path)
            result = show_info(conn)
            conn.close()
            return result

    if args.diagnose:
        return diagnose(args)

    return run_overwrite(args)


if __name__ == "__main__":
    sys.exit(main())

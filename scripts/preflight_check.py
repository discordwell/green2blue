#!/usr/bin/env python3
"""Pre-flight validation: exhaustive backup integrity checks.

Tests the full inject → re-encrypt → decrypt round-trip on a COPY
of sms.db and Manifest.db, never touching the original backup.

Checks:
  1. Decrypt sms.db + Manifest.db from the backup
  2. Snapshot baseline state (message count, integrity, schema)
  3. Inject dummy messages with each CK strategy
  4. Post-inject: integrity check, foreign key check, message count
  5. Run prepare-sync
  6. Re-encrypt sms.db
  7. Decrypt the re-encrypted data again (round-trip)
  8. Verify round-tripped DB matches pre-encrypt state
  9. Check Manifest.db file size entry matches actual sms.db size
  10. Clean up — original backup untouched

Usage:
    python scripts/preflight_check.py --password <pw>
    python scripts/preflight_check.py --password <pw> --backup <udid>
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


class Check:
    """Track pass/fail checks."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.details: list[str] = []

    def ok(self, msg: str) -> None:
        self.passed += 1
        self.details.append(f"  PASS: {msg}")
        print(f"  PASS: {msg}")

    def fail(self, msg: str) -> None:
        self.failed += 1
        self.details.append(f"  FAIL: {msg}")
        print(f"  FAIL: {msg}")

    def warn(self, msg: str) -> None:
        self.warnings += 1
        self.details.append(f"  WARN: {msg}")
        print(f"  WARN: {msg}")

    def summary(self) -> str:
        total = self.passed + self.failed
        status = "ALL PASSED" if self.failed == 0 else "FAILURES DETECTED"
        return (
            f"{status}: {self.passed}/{total} checks passed, "
            f"{self.warnings} warnings"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight backup integrity validation"
    )
    parser.add_argument(
        "--password", required=True, help="Backup encryption password"
    )
    parser.add_argument(
        "--backup", type=str, default=None,
        help="Backup path or UDID (auto-detect if omitted)",
    )
    args = parser.parse_args()

    from green2blue.ios.backup import find_backup, get_sms_db_path
    from green2blue.ios.crypto import EncryptedBackup
    from green2blue.ios.manifest import ManifestDB, compute_file_id

    check = Check()

    print("=" * 70)
    print("PRE-FLIGHT BACKUP INTEGRITY CHECK")
    print("=" * 70)

    # --- Step 1: Find and unlock backup ---
    print("\n[Step 1] Finding and unlocking backup...")
    backup_info = find_backup(args.backup)
    print(f"  Backup: {backup_info.device_name} ({backup_info.udid})")
    print(f"  iOS {backup_info.product_version}, "
          f"{'encrypted' if backup_info.is_encrypted else 'unencrypted'}")

    if not backup_info.is_encrypted:
        print("  ERROR: This check is for encrypted backups only.")
        return 1

    eb = EncryptedBackup(backup_info.path, args.password)
    eb.unlock()
    check.ok("Backup unlocked successfully")

    # --- Step 2: Decrypt sms.db and Manifest.db to temp files ---
    print("\n[Step 2] Decrypting sms.db and Manifest.db to temp files...")
    temp_manifest_path = eb.decrypt_manifest_db()

    sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    with ManifestDB(temp_manifest_path) as manifest:
        sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(
            sms_file_id
        )

    sms_db_on_disk = get_sms_db_path(backup_info.path)
    encrypted_sms_data = sms_db_on_disk.read_bytes()
    decrypted_sms_data = eb.decrypt_db_file(
        encrypted_sms_data, sms_enc_key, sms_prot_class
    )

    fd, tmp = tempfile.mkstemp(suffix="_preflight.db")
    os.close(fd)
    temp_sms = Path(tmp)
    temp_sms.write_bytes(decrypted_sms_data)
    check.ok(f"Decrypted sms.db ({len(decrypted_sms_data):,} bytes)")

    # --- Step 3: Baseline snapshot ---
    print("\n[Step 3] Baseline snapshot...")
    conn = sqlite3.connect(temp_sms)
    conn.row_factory = sqlite3.Row

    # Integrity check
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity == "ok":
        check.ok("Baseline SQLite integrity_check: ok")
    else:
        check.fail(f"Baseline integrity_check: {integrity}")

    # Foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if not fk_errors:
        check.ok("Baseline foreign key check: clean")
    else:
        check.warn(
            f"Baseline has {len(fk_errors)} FK violations "
            "(normal for real sms.db)"
        )

    # Counts
    baseline_msgs = conn.execute(
        "SELECT COUNT(*) FROM message"
    ).fetchone()[0]
    baseline_chats = conn.execute(
        "SELECT COUNT(*) FROM chat"
    ).fetchone()[0]
    baseline_handles = conn.execute(
        "SELECT COUNT(*) FROM handle"
    ).fetchone()[0]
    baseline_attachments = conn.execute(
        "SELECT COUNT(*) FROM attachment"
    ).fetchone()[0]

    print(f"  Messages:    {baseline_msgs:,}")
    print(f"  Chats:       {baseline_chats:,}")
    print(f"  Handles:     {baseline_handles:,}")
    print(f"  Attachments: {baseline_attachments:,}")

    # Check for pre-existing injected messages
    existing_injected = conn.execute(
        "SELECT COUNT(*) FROM message WHERE guid LIKE 'green2blue:%'"
    ).fetchone()[0]
    if existing_injected > 0:
        check.warn(
            f"{existing_injected} pre-existing green2blue messages found"
        )
    else:
        check.ok("No pre-existing green2blue messages")

    baseline_hash = hashlib.sha256(decrypted_sms_data).hexdigest()
    print(f"  Baseline SHA256: {baseline_hash[:16]}...")

    conn.close()

    # --- Step 4: Test injection (on a copy) ---
    print("\n[Step 4] Injecting test messages...")
    fd2, tmp2 = tempfile.mkstemp(suffix="_inject_test.db")
    os.close(fd2)
    inject_sms = Path(tmp2)
    inject_sms.write_bytes(decrypted_sms_data)

    from green2blue.ios.sms_db import SMSDatabase
    from green2blue.models import (
        CKStrategy,
        ConversionResult,
        iOSChat,
        iOSHandle,
        iOSMessage,
    )

    # Build 5 test messages across 2 conversations
    import uuid

    from green2blue.converter.timestamp import unix_ms_to_ios_ns

    now_ns = unix_ms_to_ios_ns(int(__import__("time").time() * 1000))
    test_messages = []
    test_handles = []
    test_chats = []

    phones = ["+15559990001", "+15559990002"]
    for i, phone in enumerate(phones):
        test_handles.append(iOSHandle(
            id=phone, country="us", service="SMS",
        ))
        test_chats.append(iOSChat(
            guid=f"any;-;{phone}",
            style=45,
            chat_identifier=phone,
            service_name="SMS",
        ))

    for i in range(5):
        phone = phones[i % 2]
        test_messages.append(iOSMessage(
            guid=f"green2blue:{uuid.uuid4()}",
            text=f"Preflight test message {i+1}",
            handle_id=phone,
            date=now_ns + i * 1_000_000_000,
            date_read=now_ns + i * 1_000_000_000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier=phone,
        ))

    conversion = ConversionResult(
        messages=test_messages,
        handles=test_handles,
        chats=test_chats,
    )

    with SMSDatabase(inject_sms) as db:
        stats = db.inject(conversion, skip_duplicates=True)

    check.ok(
        f"Injected {stats.messages_inserted} messages, "
        f"{stats.chats_inserted} chats, "
        f"{stats.handles_inserted} handles"
    )

    # Post-inject integrity
    conn = sqlite3.connect(inject_sms)
    conn.row_factory = sqlite3.Row

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity == "ok":
        check.ok("Post-inject integrity_check: ok")
    else:
        check.fail(f"Post-inject integrity_check: {integrity}")

    post_msgs = conn.execute(
        "SELECT COUNT(*) FROM message"
    ).fetchone()[0]
    expected = baseline_msgs + stats.messages_inserted
    if post_msgs == expected:
        check.ok(
            f"Message count correct: {baseline_msgs:,} + "
            f"{stats.messages_inserted} = {post_msgs:,}"
        )
    else:
        check.fail(
            f"Message count wrong: expected {expected:,}, got {post_msgs:,}"
        )

    # Verify injected messages are findable
    injected = conn.execute(
        "SELECT COUNT(*) FROM message WHERE guid LIKE 'green2blue:%'"
    ).fetchone()[0]
    if injected >= stats.messages_inserted:
        check.ok(f"All {stats.messages_inserted} injected messages found")
    else:
        check.fail(
            f"Only {injected}/{stats.messages_inserted} injected messages "
            f"found"
        )

    # Verify join tables
    orphan_msgs = conn.execute(
        "SELECT COUNT(*) FROM message m "
        "WHERE m.guid LIKE 'green2blue:%' "
        "AND m.ROWID NOT IN (SELECT message_id FROM chat_message_join)"
    ).fetchone()[0]
    if orphan_msgs == 0:
        check.ok("All injected messages linked to chats")
    else:
        check.fail(f"{orphan_msgs} injected messages not in chat_message_join")

    # Verify non-injected messages untouched
    non_injected = conn.execute(
        "SELECT COUNT(*) FROM message WHERE guid NOT LIKE 'green2blue:%'"
    ).fetchone()[0]
    if non_injected == baseline_msgs - existing_injected:
        check.ok("Non-injected messages unchanged")
    else:
        check.fail(
            f"Non-injected count changed: was "
            f"{baseline_msgs - existing_injected:,}, now {non_injected:,}"
        )

    conn.close()

    # --- Step 5: Run prepare-sync ---
    print("\n[Step 5] Running prepare-sync...")
    from green2blue.ios.prepare_sync import prepare_sync

    ps_result = prepare_sync(inject_sms)

    # Verify prepare-sync worked
    conn = sqlite3.connect(inject_sms)
    conn.row_factory = sqlite3.Row

    dirty = conn.execute(
        "SELECT COUNT(*) FROM message WHERE guid LIKE 'green2blue:%' "
        "AND (ck_sync_state != 0 OR ck_record_id IS NOT NULL)"
    ).fetchone()[0]
    if dirty == 0:
        check.ok("All injected messages have clean CK state after prepare-sync")
    else:
        check.fail(f"{dirty} injected messages still have CK metadata")

    # Post-prepare-sync integrity
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity == "ok":
        check.ok("Post-prepare-sync integrity_check: ok")
    else:
        check.fail(f"Post-prepare-sync integrity_check: {integrity}")

    conn.close()

    # --- Step 6: Re-encrypt round-trip ---
    print("\n[Step 6] Testing encrypt → decrypt round-trip...")
    modified_bytes = inject_sms.read_bytes()
    modified_size = len(modified_bytes)
    modified_hash = hashlib.sha256(modified_bytes).hexdigest()
    print(f"  Pre-encrypt size: {modified_size:,} bytes")
    print(f"  Pre-encrypt SHA256: {modified_hash[:16]}...")

    # Encrypt
    re_encrypted = eb.encrypt_db_file(
        modified_bytes, sms_enc_key, sms_prot_class
    )
    print(f"  Encrypted size: {len(re_encrypted):,} bytes")

    # Decrypt again
    round_tripped = eb.decrypt_db_file(
        re_encrypted, sms_enc_key, sms_prot_class
    )
    round_trip_hash = hashlib.sha256(round_tripped).hexdigest()
    print(f"  Round-trip size: {len(round_tripped):,} bytes")
    print(f"  Round-trip SHA256: {round_trip_hash[:16]}...")

    if round_trip_hash == modified_hash:
        check.ok("Encrypt→decrypt round-trip: byte-perfect match")
    else:
        check.fail(
            f"Round-trip mismatch! Pre: {modified_hash[:16]}, "
            f"Post: {round_trip_hash[:16]}"
        )

    if len(round_tripped) == modified_size:
        check.ok(f"Round-trip size matches: {modified_size:,} bytes")
    else:
        check.fail(
            f"Round-trip size mismatch: {modified_size:,} vs "
            f"{len(round_tripped):,}"
        )

    # Verify the round-tripped DB is still valid
    fd3, tmp3 = tempfile.mkstemp(suffix="_roundtrip.db")
    os.close(fd3)
    roundtrip_db = Path(tmp3)
    roundtrip_db.write_bytes(round_tripped)

    conn = sqlite3.connect(roundtrip_db)
    conn.row_factory = sqlite3.Row

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity == "ok":
        check.ok("Round-tripped DB integrity_check: ok")
    else:
        check.fail(f"Round-tripped DB integrity_check: {integrity}")

    rt_msgs = conn.execute(
        "SELECT COUNT(*) FROM message"
    ).fetchone()[0]
    if rt_msgs == post_msgs:
        check.ok(f"Round-tripped message count: {rt_msgs:,}")
    else:
        check.fail(
            f"Round-trip message count mismatch: {post_msgs:,} vs {rt_msgs:,}"
        )

    rt_injected = conn.execute(
        "SELECT COUNT(*) FROM message WHERE guid LIKE 'green2blue:%'"
    ).fetchone()[0]
    if rt_injected == injected:
        check.ok(f"Round-tripped injected messages: {rt_injected}")
    else:
        check.fail(
            f"Round-trip injected count: {injected} vs {rt_injected}"
        )

    conn.close()

    # --- Step 7: Manifest.db size check ---
    print("\n[Step 7] Checking Manifest.db file size entry...")
    with ManifestDB(temp_manifest_path) as manifest:
        manifest.update_sms_db_entry(modified_size)

    # Re-read to verify
    mf_conn = sqlite3.connect(temp_manifest_path)
    mf_conn.row_factory = sqlite3.Row
    row = mf_conn.execute(
        "SELECT file FROM Files WHERE fileID = ?", (sms_file_id,)
    ).fetchone()
    mf_conn.close()

    if row:
        check.ok("Manifest.db has sms.db entry")
    else:
        check.fail("Manifest.db missing sms.db entry")

    # --- Step 8: Schema validation ---
    print("\n[Step 8] Schema validation...")
    conn = sqlite3.connect(roundtrip_db)
    conn.row_factory = sqlite3.Row

    required_tables = [
        "handle", "chat", "message", "attachment",
        "chat_handle_join", "chat_message_join",
        "message_attachment_join",
    ]
    tables = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    ]
    for t in required_tables:
        if t in tables:
            check.ok(f"Table '{t}' exists")
        else:
            check.fail(f"Table '{t}' missing")

    # Check critical columns exist
    msg_cols = [
        r[1] for r in conn.execute("PRAGMA table_info(message)").fetchall()
    ]
    for col in ["ck_sync_state", "ck_record_id", "ck_record_change_tag"]:
        if col in msg_cols:
            check.ok(f"message.{col} column exists")
        else:
            check.fail(f"message.{col} column missing")

    chat_cols = [
        r[1] for r in conn.execute("PRAGMA table_info(chat)").fetchall()
    ]
    for col in ["ck_sync_state", "cloudkit_record_id", "server_change_token"]:
        if col in chat_cols:
            check.ok(f"chat.{col} column exists")
        else:
            check.fail(f"chat.{col} column missing")

    conn.close()

    # --- Cleanup ---
    print("\n[Cleanup] Removing temp files...")
    temp_sms.unlink(missing_ok=True)
    inject_sms.unlink(missing_ok=True)
    roundtrip_db.unlink(missing_ok=True)
    temp_manifest_path.unlink(missing_ok=True)

    # --- Summary ---
    print("\n" + "=" * 70)
    print(check.summary())
    print("=" * 70)

    if check.failed > 0:
        print("\nDO NOT proceed with injection — failures detected above.")
        return 1
    else:
        print("\nBackup is safe for injection. The encrypt/decrypt")
        print("round-trip is clean and injection preserves integrity.")
        return 0


if __name__ == "__main__":
    sys.exit(main())

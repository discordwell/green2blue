#!/usr/bin/env python3
"""Wet test: iCloud Messages sync survival test matrix.

Creates 6 test messages, each with a different CloudKit metadata strategy,
and injects them into a backup. After restore + iCloud sync, check which
messages survived to determine the winning strategy.

Test Messages:
  1. +15550000001: "Test A: no sync metadata"      — ck_sync_state=0, no record ID
  2. +15550000002: "Test B: state=1, no record"     — ck_sync_state=1, no record ID
  3. +15550000003: "Test C: state=1, fake record"   — ck_sync_state=1, record ID, tag "1"
  4. +15550000004: "Test D: state=1, full metadata"  — same as C + ck_chat_id
  5. +15550000005: "Test E: state=0, with record"   — ck_sync_state=0, record ID
  6. +15550000006: "Test F: state=2, with record"   — ck_sync_state=2, record ID

Usage:
    python scripts/wet_test_sync.py                    # synthetic backup
    python scripts/wet_test_sync.py --real              # real backup (interactive)
    python scripts/wet_test_sync.py --real --password X  # encrypted real backup
    python scripts/wet_test_sync.py --diagnose BACKUP   # check which messages survived
"""

from __future__ import annotations

import argparse
import hashlib
import json
import plistlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from green2blue.models import generate_ck_record_id  # noqa: E402, I001


# ---------------------------------------------------------------------------
# Test matrix definitions
# ---------------------------------------------------------------------------

TEST_MESSAGES = [
    {
        "phone": "+15550000001",
        "text": "Test A: no sync metadata",
        "ck_sync_state": 0,
        "ck_record_id": None,
        "ck_record_change_tag": None,
        "chat_ck_sync_state": 0,
        "chat_cloudkit_record_id": None,
        "strategy": "Current behavior (no CK metadata)",
    },
    {
        "phone": "+15550000002",
        "text": "Test B: state=1, no record",
        "ck_sync_state": 1,
        "ck_record_id": None,
        "ck_record_change_tag": None,
        "chat_ck_sync_state": 1,
        "chat_cloudkit_record_id": None,
        "strategy": "Sync state=1 only",
    },
    {
        "phone": "+15550000003",
        "text": "Test C: state=1, fake record",
        "ck_sync_state": 1,
        "ck_record_id": "GENERATE",
        "ck_record_change_tag": "1",
        "chat_ck_sync_state": 1,
        "chat_cloudkit_record_id": "GENERATE",
        "strategy": "Fake synced (state=1 + record ID + tag)",
    },
    {
        "phone": "+15550000004",
        "text": "Test D: state=1, full metadata",
        "ck_sync_state": 1,
        "ck_record_id": "GENERATE",
        "ck_record_change_tag": "1",
        "chat_ck_sync_state": 1,
        "chat_cloudkit_record_id": "GENERATE",
        "strategy": "Full fake metadata (same as C + chat record)",
    },
    {
        "phone": "+15550000005",
        "text": "Test E: state=0, with record",
        "ck_sync_state": 0,
        "ck_record_id": "GENERATE",
        "ck_record_change_tag": None,
        "chat_ck_sync_state": 0,
        "chat_cloudkit_record_id": "GENERATE",
        "strategy": "Pending upload (state=0 + record ID, no tag)",
    },
    {
        "phone": "+15550000006",
        "text": "Test F: state=2, with record",
        "ck_sync_state": 2,
        "ck_record_id": "GENERATE",
        "ck_record_change_tag": None,
        "chat_ck_sync_state": 2,
        "chat_cloudkit_record_id": "GENERATE",
        "strategy": "State=2 pending (state=2 + record ID)",
    },
]


def _ts_ms(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1000))


def build_test_export(out_dir: Path) -> Path:
    """Create an Android export ZIP with 6 test messages."""
    zip_path = out_dir / "sync_test_export.zip"
    base = datetime.now() - timedelta(days=1)

    records = []
    for i, test in enumerate(TEST_MESSAGES):
        dt = base + timedelta(minutes=i * 5)
        records.append({
            "address": test["phone"],
            "body": test["text"],
            "date": _ts_ms(dt),
            "type": "1",  # received
            "read": "1",
            "date_sent": _ts_ms(dt),
        })

    ndjson = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)

    return zip_path


# ---------------------------------------------------------------------------
# Direct injection (bypasses converter to set per-message CK fields)
# ---------------------------------------------------------------------------

def inject_test_matrix(sms_db_path: Path) -> None:
    """Inject 6 test messages directly into sms.db with varying CK metadata."""
    from green2blue.converter.timestamp import unix_ms_to_ios_ns

    base = datetime.now() - timedelta(days=1)
    conn = sqlite3.connect(sms_db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Drop triggers (real sms.db has triggers that call iOS internal functions)
    saved_triggers = []
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
    for row in cursor.fetchall():
        if row["sql"]:
            saved_triggers.append(row["sql"])
        name = row["name"]
        if all(c.isalnum() or c == '_' for c in name):
            cursor.execute(f"DROP TRIGGER IF EXISTS [{name}]")
    conn.commit()
    print(f"  Dropped {len(saved_triggers)} triggers")

    for i, test in enumerate(TEST_MESSAGES):
        phone = test["phone"]
        dt = base + timedelta(minutes=i * 5)
        date_ns = unix_ms_to_ios_ns(int(dt.timestamp() * 1000))
        msg_guid = f"green2blue:sync-test-{i+1}"
        chat_guid = f"SMS;-;{phone}"

        # Generate record IDs where needed
        ck_record_id = test["ck_record_id"]
        if ck_record_id == "GENERATE":
            ck_record_id = generate_ck_record_id(msg_guid)

        chat_ck_record_id = test["chat_cloudkit_record_id"]
        if chat_ck_record_id == "GENERATE":
            chat_ck_record_id = generate_ck_record_id(chat_guid, salt="green2blue-ck-chat")

        # Insert handle
        cursor.execute(
            "INSERT OR IGNORE INTO handle (id, country, service) VALUES (?, 'us', 'SMS')",
            (phone,),
        )
        handle_rowid = cursor.execute(
            "SELECT ROWID FROM handle WHERE id = ?", (phone,)
        ).fetchone()[0]

        # Insert chat with CK metadata
        cursor.execute(
            """INSERT OR IGNORE INTO chat
               (guid, style, state, account_id, chat_identifier, service_name,
                display_name, account_login, ck_sync_state, cloudkit_record_id)
               VALUES (?, 45, 3, 'p:0', ?, 'SMS', '', 'p:0', ?, ?)""",
            (chat_guid, phone, test["chat_ck_sync_state"], chat_ck_record_id),
        )
        chat_rowid = cursor.execute(
            "SELECT ROWID FROM chat WHERE guid = ?", (chat_guid,)
        ).fetchone()[0]

        # Insert message with CK metadata
        cursor.execute(
            """INSERT INTO message (
                guid, text, handle_id, service, account, account_guid,
                date, date_read, date_delivered,
                is_from_me, is_read, is_finished,
                ck_sync_state, ck_record_id, ck_record_change_tag,
                part_count
            ) VALUES (?, ?, ?, 'SMS', 'p:0', 'p:0', ?, ?, 0, 0, 1, 1, ?, ?, ?, 1)""",
            (
                msg_guid, test["text"], handle_rowid,
                date_ns, date_ns,
                test["ck_sync_state"], ck_record_id, test["ck_record_change_tag"],
            ),
        )
        msg_rowid = cursor.lastrowid

        # Join tables
        cursor.execute(
            "INSERT OR IGNORE INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
            (chat_rowid, handle_rowid),
        )
        cursor.execute(
            "INSERT OR IGNORE INTO chat_message_join "
            "(chat_id, message_id, message_date) VALUES (?, ?, ?)",
            (chat_rowid, msg_rowid, date_ns),
        )

    conn.commit()

    # Restore triggers
    restored = 0
    for sql in saved_triggers:
        try:
            cursor.execute(sql)
            restored += 1
        except sqlite3.Error as e:
            print(f"  Warning: failed to restore trigger: {e}")
    conn.commit()
    print(f"  Restored {restored}/{len(saved_triggers)} triggers")

    conn.close()
    print(f"Injected {len(TEST_MESSAGES)} test messages into {sms_db_path}")


# ---------------------------------------------------------------------------
# Synthetic backup builder (reuses wet_test.py pattern)
# ---------------------------------------------------------------------------

SMS_DB_DOMAIN_PATH = b"HomeDomain-Library/SMS/sms.db"


def build_synthetic_backup(out_dir: Path) -> Path:
    """Create a minimal iPhone backup for sync testing."""
    udid = "SYNCTEST-0000-AAAA-BBBB-CCCCDDDDEEEE"
    backup_root = out_dir / "backups"
    backup_dir = backup_root / udid
    backup_dir.mkdir(parents=True)

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
            fileID TEXT PRIMARY KEY, domain TEXT, relativePath TEXT,
            flags INTEGER, file BLOB
        )
    """)
    conn.execute(
        "INSERT INTO Files (fileID, domain, relativePath, flags, file) VALUES (?, ?, ?, ?, ?)",
        (sms_hash, "HomeDomain", "Library/SMS/sms.db", 1, b""),
    )
    conn.commit()
    conn.close()

    info = {
        "Device Name": "Sync Test iPhone",
        "Product Version": "18.3",
        "Unique Identifier": udid,
        "Build Version": "22D60",
    }
    (backup_dir / "Info.plist").write_bytes(plistlib.dumps(info))
    (backup_dir / "Manifest.plist").write_bytes(
        plistlib.dumps({"IsEncrypted": False, "Version": "3.3"})
    )
    (backup_dir / "Status.plist").write_bytes(
        plistlib.dumps({
            "IsFullBackup": True, "Version": "3.3",
            "BackupState": "new", "Date": datetime.now().isoformat(),
        })
    )

    return backup_dir


def _create_sms_db(path: Path) -> None:
    """Create a minimal sms.db with the iOS schema."""
    sql_path = PROJECT_ROOT / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(path)
    conn.executescript(sql_path.read_text())
    conn.close()


# ---------------------------------------------------------------------------
# Diagnostic: check which messages survived sync
# ---------------------------------------------------------------------------

def diagnose_sync(backup_path: Path, password: str | None = None) -> None:
    """Check which sync test messages exist in the backup's sms.db."""
    from green2blue.ios.backup import get_sms_db_path

    sms_db_path = get_sms_db_path(Path(backup_path))

    # Handle encrypted backups
    if password:
        import os
        import tempfile

        from green2blue.ios.backup import find_backup
        from green2blue.ios.crypto import EncryptedBackup
        from green2blue.ios.manifest import ManifestDB, compute_file_id

        backup_info = find_backup(str(backup_path))
        eb = EncryptedBackup(backup_info.path, password)
        eb.unlock()
        temp_manifest = eb.decrypt_manifest_db()
        sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        with ManifestDB(temp_manifest) as manifest:
            sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)
        encrypted_data = sms_db_path.read_bytes()
        decrypted_data = eb.decrypt_db_file(encrypted_data, sms_enc_key, sms_prot_class)
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        sms_db_path = Path(tmp)
        sms_db_path.write_bytes(decrypted_data)
        temp_manifest.unlink(missing_ok=True)

    conn = sqlite3.connect(sms_db_path)
    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 70)
    print("SYNC TEST DIAGNOSTIC")
    print("=" * 70)

    # Check for our test messages
    survived = 0
    total = len(TEST_MESSAGES)

    for i, test in enumerate(TEST_MESSAGES):
        msg_guid = f"green2blue:sync-test-{i+1}"
        row = conn.execute(
            "SELECT ROWID, text, ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE guid = ?",
            (msg_guid,),
        ).fetchone()

        status = "SURVIVED" if row else "GONE"
        if row:
            survived += 1

        print(f"\n  Test {chr(65+i)}: {test['strategy']}")
        print(f"   Phone: {test['phone']}")
        print(f"   Text:  {test['text']}")
        print(f"   CK strategy: state={test['ck_sync_state']}, "
              f"record={'yes' if test['ck_record_id'] else 'no'}, "
              f"tag={test['ck_record_change_tag']}")
        print(f"   Status: {status}")
        if row:
            print(f"   DB state: ck_sync_state={row['ck_sync_state']}, "
                  f"ck_record_id={row['ck_record_id']}, "
                  f"ck_record_change_tag={row['ck_record_change_tag']}")

    print(f"\n{'='*70}")
    print(f"RESULT: {survived}/{total} messages survived iCloud sync")
    print(f"{'='*70}")

    if survived == 0:
        print("\nNo messages survived. All strategies failed.")
    elif survived == total:
        print("\nAll messages survived! Any strategy works.")
    else:
        print("\nPartial survival — check which strategies worked above.")
        print("The winning strategy should be set as the default.")

    # Also show overall CK sync state distribution
    rows = conn.execute(
        "SELECT ck_sync_state, COUNT(*) as cnt FROM message GROUP BY ck_sync_state"
    ).fetchall()
    print("\nOverall message CK sync state distribution:")
    for row in rows:
        print(f"  ck_sync_state={row['ck_sync_state']}: {row['cnt']} messages")

    conn.close()

    # Clean up temp file if we created one
    if password:
        sms_db_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

def run_g2b(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "green2blue", *args]
    print(f"\n$ green2blue {' '.join(args)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if check and result.returncode != 0:
        print(f"\n[FAIL] Exit code {result.returncode}", file=sys.stderr)
        sys.exit(1)
    return result


def print_injection_summary(sms_db_path: Path) -> None:
    """Print what we injected."""
    conn = sqlite3.connect(sms_db_path)
    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 70)
    print("INJECTED TEST MATRIX")
    print("=" * 70)

    for i, test in enumerate(TEST_MESSAGES):
        msg_guid = f"green2blue:sync-test-{i+1}"
        row = conn.execute(
            "SELECT ck_sync_state, ck_record_id, ck_record_change_tag "
            "FROM message WHERE guid = ?",
            (msg_guid,),
        ).fetchone()

        print(f"\n  Test {chr(65+i)}: {test['strategy']}")
        print(f"    Phone: {test['phone']}")
        print(f"    Text:  {test['text']}")
        if row:
            print(f"    ck_sync_state:      {row['ck_sync_state']}")
            print(f"    ck_record_id:       {row['ck_record_id'] or '(none)'}")
            print(f"    ck_record_change_tag: {row['ck_record_change_tag'] or '(none)'}")

    # Chat CK state
    print("\n  Chat CloudKit states:")
    for _i, test in enumerate(TEST_MESSAGES):
        chat_guid = f"SMS;-;{test['phone']}"
        row = conn.execute(
            "SELECT ck_sync_state, cloudkit_record_id FROM chat WHERE guid = ?",
            (chat_guid,),
        ).fetchone()
        if row:
            print(f"    {chat_guid}: state={row['ck_sync_state']}, "
                  f"record={row['cloudkit_record_id'] or '(none)'}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="iCloud Messages sync test matrix")
    parser.add_argument("--real", action="store_true", help="Use real iPhone backup")
    parser.add_argument("--password", type=str, help="Backup encryption password")
    parser.add_argument("--keep", action="store_true", help="Keep test artifacts")
    parser.add_argument(
        "--diagnose", type=Path, metavar="BACKUP_PATH",
        help="Diagnose which test messages survived in the given backup",
    )
    args = parser.parse_args()

    # Diagnose mode
    if args.diagnose:
        diagnose_sync(args.diagnose, args.password)
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="g2b_synctest_"))
    print(f"Working directory: {tmpdir}")

    try:
        if args.real:
            # Real backup mode: build export, inject via CLI
            print("\n" + "=" * 70)
            print("STEP 1: Building test export ZIP")
            print("=" * 70)
            export_zip = build_test_export(tmpdir)
            print(f"Created: {export_zip}")

            print("\n" + "=" * 70)
            print("STEP 2: Injecting via green2blue CLI")
            print("=" * 70)
            print("NOTE: This uses the standard pipeline, which applies a single")
            print("CK strategy to all messages. For the A/B test matrix, we need")
            print("to inject directly. Use --diagnose after restore+sync.")

            # For a real backup, we inject directly into sms.db
            # to get per-message CK metadata variation
            from green2blue.ios.backup import find_backup, get_sms_db_path

            backup_info = find_backup()
            sms_db_path = get_sms_db_path(backup_info.path)

            if backup_info.is_encrypted:
                if not args.password:
                    print("ERROR: Encrypted backup requires --password", file=sys.stderr)
                    sys.exit(1)
                import os

                from green2blue.ios.crypto import EncryptedBackup
                from green2blue.ios.manifest import ManifestDB, compute_file_id

                eb = EncryptedBackup(backup_info.path, args.password)
                eb.unlock()
                temp_manifest = eb.decrypt_manifest_db()
                sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
                with ManifestDB(temp_manifest) as manifest:
                    sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)
                encrypted_data = sms_db_path.read_bytes()
                decrypted_data = eb.decrypt_db_file(encrypted_data, sms_enc_key, sms_prot_class)

                fd, tmp = tempfile.mkstemp(suffix=".db")
                os.close(fd)
                temp_sms = Path(tmp)
                temp_sms.write_bytes(decrypted_data)

                inject_test_matrix(temp_sms)
                print_injection_summary(temp_sms)

                # Re-encrypt and write back
                re_encrypted = eb.encrypt_db_file(
                    temp_sms.read_bytes(), sms_enc_key, sms_prot_class,
                )
                sms_db_path.write_bytes(re_encrypted)

                # Update Manifest.db
                with ManifestDB(temp_manifest) as manifest:
                    manifest.update_sms_db_entry(temp_sms.stat().st_size)
                eb.re_encrypt_manifest_db(temp_manifest)

                temp_sms.unlink(missing_ok=True)
                temp_manifest.unlink(missing_ok=True)
            else:
                inject_test_matrix(sms_db_path)
                print_injection_summary(sms_db_path)

            print("\n" + "=" * 70)
            print("NEXT STEPS")
            print("=" * 70)
            print("1. Restore this backup to your iPhone")
            print("2. Verify all 6 test messages appear in Messages app")
            print("3. Enable iCloud Messages (Settings > Apple Account > iCloud > Messages)")
            print("4. Wait for sync to complete")
            print("5. Make a new backup and run:")
            print("   python scripts/wet_test_sync.py --diagnose <new_backup_path>")

        else:
            # Synthetic mode: build everything from scratch
            print("\n" + "=" * 70)
            print("STEP 1: Building synthetic iPhone backup")
            print("=" * 70)
            backup_dir = build_synthetic_backup(tmpdir)
            print(f"Created backup: {backup_dir}")

            sms_hash = hashlib.sha1(SMS_DB_DOMAIN_PATH).hexdigest()
            sms_db_path = backup_dir / sms_hash[:2] / sms_hash

            print("\n" + "=" * 70)
            print("STEP 2: Injecting test matrix directly into sms.db")
            print("=" * 70)
            inject_test_matrix(sms_db_path)

            print("\n" + "=" * 70)
            print("STEP 3: Injection summary")
            print("=" * 70)
            print_injection_summary(sms_db_path)

            print("\n" + "=" * 70)
            print("STEP 4: Running diagnostic on synthetic backup")
            print("=" * 70)
            diagnose_sync(backup_dir)

            print("\n" + "=" * 70)
            print("SYNTHETIC TEST COMPLETE")
            print("=" * 70)
            print("All 6 strategies injected successfully.")
            print("To test with a real device, run with --real flag.")

    finally:
        if args.keep:
            print(f"\nArtifacts preserved at: {tmpdir}")
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"\nCleaned up: {tmpdir}")


if __name__ == "__main__":
    main()

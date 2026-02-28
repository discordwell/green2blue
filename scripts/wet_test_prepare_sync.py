#!/usr/bin/env python3
"""Wet test: iCloud sync reset via prepare-sync workflow.

Tests the recommended iCloud Messages survival approach:
  1. Inject messages with fake-synced CK metadata
  2. Run prepare-sync to reset CK state
  3. Verify all injected messages have clean CK state (ck_sync_state=0, no record IDs)

Modes:
  Synthetic — Build a minimal backup, inject, prepare-sync, verify (default)
  Real      — Inject into a real backup, prepare-sync, print workflow instructions
  Diagnose  — After restore + re-enable iCloud, check which messages survived

Usage:
    python scripts/wet_test_prepare_sync.py                      # synthetic mode
    python scripts/wet_test_prepare_sync.py --real                # real backup
    python scripts/wet_test_prepare_sync.py --real --password X   # encrypted
    python scripts/wet_test_prepare_sync.py --diagnose BACKUP     # post-restore check
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

SMS_DB_DOMAIN_PATH = b"HomeDomain-Library/SMS/sms.db"

NUM_TEST_MESSAGES = 4


def _ts_ms(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1000))


def build_test_export(out_dir: Path) -> Path:
    """Create an Android export ZIP with test messages."""
    zip_path = out_dir / "prepare_sync_test.zip"
    base = datetime.now() - timedelta(days=1)

    records = []
    for i in range(NUM_TEST_MESSAGES):
        dt = base + timedelta(minutes=i * 5)
        records.append({
            "address": f"+1555000100{i+1}",
            "body": f"Prepare-sync test message {i+1}",
            "date": _ts_ms(dt),
            "type": "1",
            "read": "1",
            "date_sent": _ts_ms(dt),
        })

    ndjson = "\n".join(json.dumps(r) for r in records) + "\n"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)

    return zip_path


def _create_sms_db(path: Path) -> None:
    """Create a minimal sms.db."""
    sql_path = PROJECT_ROOT / "scripts" / "create_empty_smsdb.sql"
    conn = sqlite3.connect(path)
    conn.executescript(sql_path.read_text())
    conn.close()


def build_synthetic_backup(out_dir: Path) -> Path:
    """Create a minimal iPhone backup for testing."""
    udid = "PREPSYNC-0000-AAAA-BBBB-CCCCDDDDEEEE"
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
        "Device Name": "Prepare-Sync Test iPhone",
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


def verify_db_state(sms_db_path: Path) -> bool:
    """Verify that all injected messages have clean CK state."""
    conn = sqlite3.connect(sms_db_path)
    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 70)
    print("VERIFICATION: Checking injected message CK state")
    print("=" * 70)

    messages = conn.execute(
        "SELECT guid, text, ck_sync_state, ck_record_id, ck_record_change_tag "
        "FROM message WHERE guid LIKE 'green2blue:%' ORDER BY ROWID"
    ).fetchall()

    all_clean = True
    for msg in messages:
        is_clean = (
            msg["ck_sync_state"] == 0
            and msg["ck_record_id"] is None
            and msg["ck_record_change_tag"] is None
        )
        status = "CLEAN" if is_clean else "DIRTY"
        if not is_clean:
            all_clean = False
        print(f"  [{status}] {msg['guid']}: state={msg['ck_sync_state']}, "
              f"record={msg['ck_record_id'] or '(none)'}, "
              f"tag={msg['ck_record_change_tag'] or '(none)'}")

    # Check chats
    chats = conn.execute(
        "SELECT c.guid, c.ck_sync_state, c.cloudkit_record_id, c.server_change_token "
        "FROM chat c "
        "INNER JOIN chat_message_join cmj ON cmj.chat_id = c.ROWID "
        "INNER JOIN message m ON m.ROWID = cmj.message_id "
        "WHERE m.guid LIKE 'green2blue:%' "
        "GROUP BY c.ROWID"
    ).fetchall()

    print("\n  Chat states:")
    for chat in chats:
        token_status = "cleared" if chat["server_change_token"] is None else "SET"
        print(f"  {chat['guid']}: state={chat['ck_sync_state']}, "
              f"record={chat['cloudkit_record_id'] or '(none)'}, "
              f"server_change_token={token_status}")

    conn.close()

    print(f"\n  Overall: {'PASS' if all_clean else 'FAIL'}")
    return all_clean


def diagnose_survival(backup_path: Path, password: str | None = None) -> None:
    """Check which injected messages survived after restore + re-enable iCloud."""
    from green2blue.ios.backup import get_sms_db_path

    sms_db_path = get_sms_db_path(Path(backup_path))
    temp_path = None

    if password:
        import os

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
        temp_path = Path(tmp)
        temp_path.write_bytes(decrypted_data)
        temp_manifest.unlink(missing_ok=True)
        sms_db_path = temp_path

    conn = sqlite3.connect(sms_db_path)
    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 70)
    print("POST-RESTORE DIAGNOSTIC")
    print("=" * 70)

    messages = conn.execute(
        "SELECT guid, text, ck_sync_state, ck_record_id "
        "FROM message WHERE guid LIKE 'green2blue:%' ORDER BY ROWID"
    ).fetchall()

    survived = len(messages)
    print(f"\n  {survived} injected messages found in backup")

    for msg in messages:
        print(f"  SURVIVED: {msg['text']}")
        print(f"    ck_sync_state={msg['ck_sync_state']}, "
              f"ck_record_id={msg['ck_record_id'] or '(none)'}")

    if survived == 0:
        print("\n  No injected messages found — they were deleted during sync.")
        print("  The icloud-reset workflow may not have been followed correctly.")
    else:
        print(f"\n  SUCCESS: {survived} messages survived the iCloud sync reset workflow!")

    conn.close()
    if temp_path:
        temp_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Test prepare-sync iCloud sync reset workflow"
    )
    parser.add_argument("--real", action="store_true", help="Use real iPhone backup")
    parser.add_argument("--password", type=str, help="Backup encryption password")
    parser.add_argument("--keep", action="store_true", help="Keep test artifacts")
    parser.add_argument(
        "--diagnose", type=Path, metavar="BACKUP_PATH",
        help="Check which messages survived after restore + re-enable iCloud",
    )
    args = parser.parse_args()

    if args.diagnose:
        diagnose_survival(args.diagnose, args.password)
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="g2b_prepsync_"))
    print(f"Working directory: {tmpdir}")

    try:
        if args.real:
            # Real backup mode
            print("\n" + "=" * 70)
            print("STEP 1: Building test export ZIP")
            print("=" * 70)
            export_zip = build_test_export(tmpdir)
            print(f"Created: {export_zip}")

            print("\n" + "=" * 70)
            print("STEP 2: Injecting via green2blue with --ck-strategy fake-synced")
            print("=" * 70)
            inject_args = ["inject", str(export_zip), "--ck-strategy", "fake-synced", "-y"]
            if args.password:
                inject_args += ["--password", args.password]
            run_g2b(*inject_args)

            print("\n" + "=" * 70)
            print("STEP 3: Running prepare-sync")
            print("=" * 70)
            ps_args = ["prepare-sync"]
            if args.password:
                ps_args += ["--password", args.password]
            run_g2b(*ps_args)

            print("\n" + "=" * 70)
            print("NEXT STEPS")
            print("=" * 70)
            print("1. Disable iCloud Messages on your iPhone")
            print("   Settings > [your name] > iCloud > Messages > toggle OFF")
            print("2. Restore this backup via Finder/iTunes")
            print("3. Re-enable iCloud Messages")
            print("4. Wait for sync to complete")
            print("5. Make a new backup and run:")
            print("   python scripts/wet_test_prepare_sync.py --diagnose <new_backup_path>")

        else:
            # Synthetic mode
            print("\n" + "=" * 70)
            print("STEP 1: Building synthetic backup")
            print("=" * 70)
            backup_dir = build_synthetic_backup(tmpdir)
            print(f"Created backup: {backup_dir}")

            print("\n" + "=" * 70)
            print("STEP 2: Injecting with --ck-strategy fake-synced")
            print("=" * 70)
            export_zip = build_test_export(tmpdir)
            run_g2b(
                "inject", str(export_zip),
                "--backup", str(backup_dir),
                "--backup-root", str(tmpdir / "backups"),
                "--ck-strategy", "fake-synced",
                "-y",
            )

            print("\n" + "=" * 70)
            print("STEP 3: Verifying fake-synced state BEFORE prepare-sync")
            print("=" * 70)
            sms_hash = hashlib.sha1(SMS_DB_DOMAIN_PATH).hexdigest()
            sms_db_path = backup_dir / sms_hash[:2] / sms_hash
            verify_db_state(sms_db_path)

            print("\n" + "=" * 70)
            print("STEP 4: Running prepare-sync")
            print("=" * 70)
            run_g2b(
                "prepare-sync",
                "--backup", str(backup_dir),
                "--backup-root", str(tmpdir / "backups"),
            )

            print("\n" + "=" * 70)
            print("STEP 5: Verifying clean state AFTER prepare-sync")
            print("=" * 70)
            success = verify_db_state(sms_db_path)

            print("\n" + "=" * 70)
            if success:
                print("SYNTHETIC TEST PASSED")
                print("All injected messages have clean CK state.")
            else:
                print("SYNTHETIC TEST FAILED")
                print("Some messages still have CK metadata — prepare-sync did not clean them.")
            print("=" * 70)

    finally:
        if args.keep:
            print(f"\nArtifacts preserved at: {tmpdir}")
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"\nCleaned up: {tmpdir}")


if __name__ == "__main__":
    main()

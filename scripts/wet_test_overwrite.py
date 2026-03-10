"""Proof-of-concept: UPDATE existing messages instead of INSERT.

Tests whether CloudKit reconciliation detects content changes in messages
that retain their original ck_record_id and ck_record_change_tag.

Strategy:
1. Pick 6 sacrificial messages from the +16283105601 conversation
2. UPDATE only content columns (text, attributedBody, message_summary_info)
3. Leave ALL CK metadata, handle_id, dates, join tables untouched
4. Restore backup → re-enable iCloud Messages → check if changes survive

Usage:
    # Phase 1: Modify messages in backup
    python scripts/wet_test_overwrite.py --password glorious1

    # Phase 2: After restore + iCloud sync, check what survived
    python scripts/wet_test_overwrite.py --diagnose --password glorious1
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from green2blue.ios.attributed_body import build_attributed_body
from green2blue.ios.backup import find_backup, get_sms_db_path
from green2blue.ios.crypto import EncryptedBackup
from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.message_summary import build_message_summary_info

logger = logging.getLogger(__name__)

# Marker prefix so we can find our modified messages later
MARKER = "[G2B-OW]"

# Test messages to overwrite into sacrificial slots
TEST_MESSAGES = [
    f"{MARKER} Test A: Simple text overwrite",
    f"{MARKER} Test B: Does CK notice content change?",
    f"{MARKER} Test C: Same ck_record_id, different text",
    f"{MARKER} Test D: Change tag preserved, content swapped",
    f"{MARKER} Test E: Will this survive iCloud sync?",
    f"{MARKER} Test F: Overwrite strategy proof of concept",
]

# Target: +16283105601 test conversation (sacrifice messages for overwrite testing)
TARGET_CHAT_IDENTIFIER = "+16283105601"


def decrypt_sms_db(backup_info, password: str) -> tuple[bytes, bytes, int, Path]:
    """Decrypt sms.db from an encrypted backup.

    Returns (decrypted_data, encryption_key, protection_class, manifest_temp).
    """
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
    return decrypted, sms_enc_key, sms_prot_class, temp_manifest, eb


def find_sacrificial_messages(
    conn: sqlite3.Connection, chat_identifier: str, count: int,
) -> list[dict]:
    """Find messages to overwrite from a target conversation.

    Picks the oldest messages (least likely to be missed).
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT m.ROWID, m.guid, m.text, m.date, m.handle_id,
               m.ck_record_id, m.ck_record_change_tag, m.ck_sync_state,
               m.is_from_me, m.service
        FROM message m
        JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        JOIN chat c ON c.ROWID = cmj.chat_id
        WHERE c.chat_identifier = ?
        ORDER BY m.date ASC
        LIMIT ?
        """,
        (chat_identifier, count),
    ).fetchall()
    return [dict(r) for r in rows]


def overwrite_messages(conn: sqlite3.Connection, targets: list[dict]) -> int:
    """UPDATE sacrificial messages with test content.

    Only modifies: text, attributedBody, message_summary_info.
    Preserves: ROWID, guid, handle_id, date, all CK metadata, join tables.
    """
    updated = 0

    # Check if attributedBody and message_summary_info columns exist
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(message)")
    }
    has_ab = "attributedBody" in cols
    has_msi = "message_summary_info" in cols

    for i, target in enumerate(targets):
        new_text = TEST_MESSAGES[i]

        # Build the update parts
        set_parts = ["text = ?"]
        params: list = [new_text]

        if has_ab:
            ab_blob = build_attributed_body(new_text)
            set_parts.append("attributedBody = ?")
            params.append(ab_blob)

        if has_msi:
            msi_blob = build_message_summary_info(
                is_from_me=bool(target["is_from_me"]),
            )
            set_parts.append("message_summary_info = ?")
            params.append(msi_blob)

        # Also ensure is_empty=0 since we're setting text
        set_parts.append("is_empty = 0")

        params.append(target["ROWID"])
        sql = f"UPDATE message SET {', '.join(set_parts)} WHERE ROWID = ?"

        conn.execute(sql, params)
        updated += 1

        print(f"  [{i+1}] ROWID={target['ROWID']} "
              f"ck_id={target['ck_record_id'][:16]}... "
              f"tag={target['ck_record_change_tag']} "
              f"old={repr((target['text'] or '')[:40])} "
              f"-> {repr(new_text[:40])}")

    conn.commit()
    return updated


def diagnose(conn: sqlite3.Connection) -> None:
    """Check which overwritten messages survived after restore + sync."""
    print("\n=== Overwrite Test Results ===\n")

    # Find our marker messages
    rows = conn.execute(
        "SELECT ROWID, guid, text, ck_sync_state, ck_record_id, "
        "ck_record_change_tag, date, handle_id "
        "FROM message WHERE text LIKE ?",
        (f"{MARKER}%",),
    ).fetchall()

    if rows:
        print(f"FOUND {len(rows)} overwritten messages (survived!):\n")
        for row in rows:
            print(f"  ROWID={row[0]} ck_state={row[3]} "
                  f"tag={row[5]} text={row[2][:50]}")
    else:
        print("NO overwritten messages found.")
        print("Checking if original messages were restored by CK...")

    # Also check the target conversation for any anomalies
    rows = conn.execute(
        """
        SELECT m.ROWID, m.text, m.ck_sync_state, m.ck_record_change_tag
        FROM message m
        JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
        JOIN chat c ON c.ROWID = cmj.chat_id
        WHERE c.chat_identifier = ?
        ORDER BY m.date ASC
        LIMIT 10
        """,
        (TARGET_CHAT_IDENTIFIER,),
    ).fetchall()

    print(f"\nFirst 10 messages in {TARGET_CHAT_IDENTIFIER} conversation:")
    for row in rows:
        marker = " <-- OVERWRITTEN" if row[1] and row[1].startswith(MARKER) else ""
        txt = (row[1] or "")[:60]
        print(f"  ROWID={row[0]} ck_state={row[2]} tag={row[3]} text={txt}{marker}")


def main():
    parser = argparse.ArgumentParser(description="Test overwrite strategy for CK survival")
    parser.add_argument("--password", help="Backup encryption password (required for encrypted backups)")
    parser.add_argument("--backup", help="Backup path or UDID")
    parser.add_argument("--backup-root", help="Custom backup root directory")
    parser.add_argument("--diagnose", action="store_true", help="Check results after restore")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    backup_info = find_backup(args.backup, args.backup_root)
    print(f"Backup: {backup_info.device_name} ({backup_info.path.name})")
    print(f"Encrypted: {backup_info.is_encrypted}")

    if backup_info.is_encrypted:
        if not args.password:
            print("ERROR: --password required for encrypted backups")
            return 1
        decrypted, sms_enc_key, sms_prot_class, temp_manifest, eb = decrypt_sms_db(
            backup_info, args.password,
        )
        # Write decrypted sms.db to temp file
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        tmp_path = Path(tmp)
        tmp_path.write_bytes(decrypted)
    else:
        # Unencrypted: work directly on a copy of sms.db
        sms_db_path = get_sms_db_path(backup_info.path)
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        tmp_path = Path(tmp)
        tmp_path.write_bytes(sms_db_path.read_bytes())
        eb = None
        sms_enc_key = None
        sms_prot_class = None
        temp_manifest = None

    conn = sqlite3.connect(tmp)

    if args.diagnose:
        diagnose(conn)
        conn.close()
        tmp_path.unlink()
        return 0

    # Phase 1: Overwrite messages
    print(f"\nFinding sacrificial messages in '{TARGET_CHAT_IDENTIFIER}' conversation...")
    targets = find_sacrificial_messages(conn, TARGET_CHAT_IDENTIFIER, len(TEST_MESSAGES))

    if len(targets) < len(TEST_MESSAGES):
        print(f"ERROR: Only found {len(targets)} messages, need {len(TEST_MESSAGES)}")
        conn.close()
        tmp_path.unlink()
        return 1

    print(f"Found {len(targets)} candidates:\n")
    for i, t in enumerate(targets):
        print(f"  [{i+1}] ROWID={t['ROWID']} ck_id={t['ck_record_id'][:16]}... "
              f"tag={t['ck_record_change_tag']} "
              f"from_me={t['is_from_me']} text={repr((t['text'] or '')[:50])}")

    if args.dry_run:
        print("\n[DRY RUN] Would overwrite the above messages. Exiting.")
        conn.close()
        tmp_path.unlink()
        return 0

    print(f"\nOverwriting {len(targets)} messages...")
    updated = overwrite_messages(conn, targets)
    conn.close()

    print(f"\nUpdated {updated} messages. Writing back...")

    # Write modified sms.db back to backup
    modified_data = tmp_path.read_bytes()
    sms_db_path = get_sms_db_path(backup_info.path)

    if backup_info.is_encrypted:
        re_encrypted = eb.encrypt_db_file(modified_data, sms_enc_key, sms_prot_class)
        sms_db_path.write_bytes(re_encrypted)

        # Update Manifest.db with new sms.db size and digest
        new_digest = hashlib.sha1(modified_data).digest()
        with ManifestDB(temp_manifest) as manifest:
            manifest.update_sms_db_entry(len(modified_data), new_digest=new_digest)
        eb.re_encrypt_manifest_db(temp_manifest)
    else:
        sms_db_path.write_bytes(modified_data)

        # Update Manifest.db with new sms.db size and digest
        new_digest = hashlib.sha1(modified_data).digest()
        manifest_db_path = backup_info.path / "Manifest.db"
        with ManifestDB(manifest_db_path) as manifest:
            manifest.update_sms_db_entry(len(modified_data), new_digest=new_digest)

    print("Done! Backup updated with overwritten messages.")
    pw_flag = f" --password {args.password}" if args.password else ""
    print(f"\nNext steps:")
    print(f"  1. Restore this backup to your device via Finder")
    print(f"  2. Let iCloud Messages sync complete")
    print(f"  3. Run: python scripts/wet_test_overwrite.py --diagnose{pw_flag}")
    print(f"  4. Check Messages app for '{TARGET_CHAT_IDENTIFIER}' conversation")

    tmp_path.unlink()
    return 0


if __name__ == "__main__":
    sys.exit(main())

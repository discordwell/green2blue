"""Prepare an injected sms.db for the iCloud sync reset workflow.

The recommended approach for iCloud Messages survival:
1. Inject messages into backup
2. Run prepare-sync to reset CK metadata on injected messages
3. Disable iCloud Messages on device
4. Restore backup via Finder
5. Re-enable iCloud Messages → iOS does bidirectional merge

Messages with ck_sync_state=0 and no CK record IDs look like "new local
messages" and get uploaded to iCloud rather than deleted.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from green2blue.ios.trigger_utils import drop_triggers, restore_triggers

logger = logging.getLogger(__name__)


@dataclass
class PrepareSyncResult:
    """Counts from a prepare-sync operation."""

    messages_updated: int = 0
    messages_already_clean: int = 0
    attachments_updated: int = 0
    attachments_already_clean: int = 0
    chats_token_cleared: int = 0
    chats_ck_reset: int = 0
    chats_preserved: int = 0


def prepare_sync(db_path: Path) -> PrepareSyncResult:
    """Prepare an injected sms.db for the iCloud sync reset workflow.

    Resets CloudKit metadata on injected messages so iOS treats them as
    new local messages during bidirectional merge.

    Operations:
    1. Reset CK metadata on injected messages (green2blue: prefix)
    2. Reset CK metadata on injected attachments (green2blue-att: prefix)
    3. Clear server_change_token on chats containing injected messages
    4. Reset CK state on pure-injected chats (only green2blue messages)
    5. Preserve CK state on mixed chats (pre-existing + injected)

    Args:
        db_path: Path to the sms.db file.

    Returns:
        PrepareSyncResult with operation counts.
    """
    result = PrepareSyncResult()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # Drop triggers — iOS triggers call internal functions that
        # would fail outside the device
        saved_triggers = drop_triggers(conn)

        cursor = conn.cursor()

        # --- 1. Reset CK metadata on injected messages ---
        # Find injected messages that need updating
        needs_update = cursor.execute(
            "SELECT COUNT(*) as cnt FROM message "
            "WHERE guid LIKE 'green2blue:%' "
            "AND (ck_sync_state != 0 OR ck_record_id IS NOT NULL "
            "     OR ck_record_change_tag IS NOT NULL)"
        ).fetchone()["cnt"]
        result.messages_updated = needs_update

        already_clean = cursor.execute(
            "SELECT COUNT(*) as cnt FROM message "
            "WHERE guid LIKE 'green2blue:%' "
            "AND ck_sync_state = 0 AND ck_record_id IS NULL "
            "AND ck_record_change_tag IS NULL"
        ).fetchone()["cnt"]
        result.messages_already_clean = already_clean

        if needs_update > 0:
            cursor.execute(
                "UPDATE message SET ck_sync_state = 0, ck_record_id = NULL, "
                "ck_record_change_tag = NULL "
                "WHERE guid LIKE 'green2blue:%' "
                "AND (ck_sync_state != 0 OR ck_record_id IS NOT NULL "
                "     OR ck_record_change_tag IS NOT NULL)"
            )

        # --- 2. Reset CK metadata on injected attachments ---
        att_needs_update = cursor.execute(
            "SELECT COUNT(*) as cnt FROM attachment "
            "WHERE guid LIKE 'green2blue-att:%' "
            "AND (ck_sync_state != 0 OR ck_record_id IS NOT NULL)"
        ).fetchone()["cnt"]
        result.attachments_updated = att_needs_update

        att_already_clean = cursor.execute(
            "SELECT COUNT(*) as cnt FROM attachment "
            "WHERE guid LIKE 'green2blue-att:%' "
            "AND ck_sync_state = 0 AND ck_record_id IS NULL"
        ).fetchone()["cnt"]
        result.attachments_already_clean = att_already_clean

        if att_needs_update > 0:
            cursor.execute(
                "UPDATE attachment SET ck_sync_state = 0, ck_record_id = NULL "
                "WHERE guid LIKE 'green2blue-att:%' "
                "AND (ck_sync_state != 0 OR ck_record_id IS NOT NULL)"
            )

        # --- 3 & 4 & 5. Handle chats ---
        # Find chats that contain at least one injected message
        affected_chats = cursor.execute(
            "SELECT DISTINCT c.ROWID, c.guid, c.server_change_token, "
            "c.ck_sync_state, c.cloudkit_record_id "
            "FROM chat c "
            "INNER JOIN chat_message_join cmj ON cmj.chat_id = c.ROWID "
            "INNER JOIN message m ON m.ROWID = cmj.message_id "
            "WHERE m.guid LIKE 'green2blue:%'"
        ).fetchall()

        for chat in affected_chats:
            chat_rowid = chat["ROWID"]

            # Check if chat has any non-injected messages (mixed chat)
            non_injected = cursor.execute(
                "SELECT COUNT(*) as cnt FROM chat_message_join cmj "
                "INNER JOIN message m ON m.ROWID = cmj.message_id "
                "WHERE cmj.chat_id = ? AND m.guid NOT LIKE 'green2blue:%'",
                (chat_rowid,),
            ).fetchone()["cnt"]

            is_mixed = non_injected > 0

            # Clear server_change_token on all affected chats
            # (forces full reconciliation for those conversations)
            if chat["server_change_token"] is not None:
                cursor.execute(
                    "UPDATE chat SET server_change_token = NULL WHERE ROWID = ?",
                    (chat_rowid,),
                )
                result.chats_token_cleared += 1

            if is_mixed:
                # Preserve CK state on mixed chats to prevent duplicate
                # conversations — only clear server_change_token (above)
                result.chats_preserved += 1
            else:
                # Pure-injected chat: safe to reset CK state entirely
                if (chat["ck_sync_state"] != 0
                        or (chat["cloudkit_record_id"] is not None
                            and chat["cloudkit_record_id"] != "")):
                    cursor.execute(
                        "UPDATE chat SET ck_sync_state = 0, "
                        "cloudkit_record_id = '' WHERE ROWID = ?",
                        (chat_rowid,),
                    )
                    result.chats_ck_reset += 1

        conn.commit()

    finally:
        restore_triggers(conn, saved_triggers)
        conn.close()

    return result

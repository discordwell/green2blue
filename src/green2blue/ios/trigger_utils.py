"""Shared trigger management utilities for iOS sms.db.

Real sms.db files have ~22 triggers that call iOS internal functions
(e.g., verify_chat). These must be dropped before any injection or
modification, then restored afterward.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


def drop_triggers(conn: sqlite3.Connection) -> list[str]:
    """Drop all triggers from sms.db, saving their CREATE SQL.

    DDL statements (DROP TRIGGER) auto-commit in Python's sqlite3,
    so this must run outside any data transaction.

    Args:
        conn: Open sqlite3 connection to sms.db.

    Returns:
        List of CREATE TRIGGER SQL statements for restoration.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
    )
    saved = []
    names = []
    for row in cursor.fetchall():
        name, sql = row[0], row[1]
        if sql:
            saved.append(sql)
        names.append(name)

    for name in names:
        # Parameterized queries can't be used for DDL identifiers,
        # so we validate the name contains only safe characters
        if not all(c.isalnum() or c == '_' for c in name):
            logger.warning("Skipping trigger with unsafe name: %r", name)
            continue
        cursor.execute(f"DROP TRIGGER IF EXISTS [{name}]")
    conn.commit()
    logger.debug("Dropped %d triggers", len(names))
    return saved


def restore_triggers(
    conn: sqlite3.Connection, saved_triggers: list[str]
) -> None:
    """Restore previously dropped triggers.

    Args:
        conn: Open sqlite3 connection to sms.db.
        saved_triggers: CREATE TRIGGER SQL statements from drop_triggers().
    """
    if not saved_triggers:
        return
    cursor = conn.cursor()
    restored = 0
    for sql in saved_triggers:
        try:
            cursor.execute(sql)
            restored += 1
        except sqlite3.Error as e:
            logger.warning("Failed to restore trigger: %s", e)
    conn.commit()
    logger.debug(
        "Restored %d/%d triggers", restored, len(saved_triggers)
    )

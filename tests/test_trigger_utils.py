"""Tests for shared sms.db trigger management utilities."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from green2blue.ios.trigger_utils import drop_triggers, restore_triggers


def _db_with_triggers(tmp_dir: Path) -> tuple[Path, sqlite3.Connection]:
    """Create a db with one safe-named trigger and one unsafe-named trigger."""
    db_path = tmp_dir / "triggers.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
    conn.execute("CREATE TRIGGER safe_trigger AFTER INSERT ON t BEGIN SELECT 1; END")
    # A name with a dash is treated as unsafe by drop_triggers (only
    # alphanumerics and underscore pass its identifier whitelist).
    conn.execute('CREATE TRIGGER "bad-name" AFTER UPDATE ON t BEGIN SELECT 1; END')
    conn.commit()
    return db_path, conn


def _trigger_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }


class TestDropTriggers:
    def test_drops_safe_trigger_and_skips_unsafe_name(self, tmp_dir):
        _db_path, conn = _db_with_triggers(tmp_dir)
        try:
            saved = drop_triggers(conn)
            remaining = _trigger_names(conn)
        finally:
            conn.close()

        # The safe trigger is dropped; the unsafe-named one is left intact.
        assert "safe_trigger" not in remaining
        assert "bad-name" in remaining
        # CREATE SQL for both is still saved (restoration is best-effort).
        assert len(saved) == 2

    def test_logs_actual_dropped_count_not_total(self, tmp_dir, caplog):
        """The debug log must count triggers actually dropped, not all found.

        With one safe and one unsafe-named trigger, exactly one is dropped, so
        the log should say "Dropped 1 triggers" rather than 2.
        """
        _db_path, conn = _db_with_triggers(tmp_dir)
        try:
            with caplog.at_level(logging.DEBUG, logger="green2blue.ios.trigger_utils"):
                drop_triggers(conn)
        finally:
            conn.close()

        assert "Dropped 1 triggers" in caplog.text
        assert "Dropped 2 triggers" not in caplog.text


class TestRestoreTriggers:
    def test_roundtrip_restores_dropped_trigger(self, tmp_dir):
        db_path = tmp_dir / "roundtrip.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.execute("CREATE TRIGGER safe_trigger AFTER INSERT ON t BEGIN SELECT 1; END")
        conn.commit()
        try:
            saved = drop_triggers(conn)
            assert "safe_trigger" not in _trigger_names(conn)
            restore_triggers(conn, saved)
            assert "safe_trigger" in _trigger_names(conn)
        finally:
            conn.close()

    def test_restore_empty_list_is_noop(self, tmp_dir):
        db_path = tmp_dir / "empty.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        try:
            # Should not raise and should leave no triggers behind.
            restore_triggers(conn, [])
            assert _trigger_names(conn) == set()
        finally:
            conn.close()

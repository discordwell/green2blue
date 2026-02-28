#!/usr/bin/env python3
"""Wet test: inject a single test message into a real iPhone backup.

Injects one SMS from "Claude" (+15550000001) saying "How are you?"
into your actual iPhone backup. Uses --dry-run first, then prompts
before the live injection.

Prerequisites:
  1. Connect your iPhone via USB
  2. Create a backup via Finder (Settings > General > Back Up)
  3. Run this script

Usage:
    python scripts/wet_test_real.py                     # unencrypted
    python scripts/wet_test_real.py --password SECRET    # encrypted
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def build_single_message_export(out_dir: Path) -> Path:
    """Create an export ZIP with a single SMS from 'Claude'."""
    zip_path = out_dir / "claude_test.zip"

    now_ms = str(int(datetime.now().timestamp() * 1000))

    record = {
        "address": "+15550000001",
        "body": "How are you?",
        "date": now_ms,
        "type": "1",       # received
        "read": "1",
        "date_sent": now_ms,
    }

    ndjson = json.dumps(record) + "\n"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)

    return zip_path


def run_g2b(*args: str) -> subprocess.CompletedProcess:
    """Run green2blue and stream output."""
    cmd = [sys.executable, "-m", "green2blue", *args]
    print(f"\n$ green2blue {' '.join(args)}")
    print("-" * 50)
    result = subprocess.run(cmd, text=True, cwd=PROJECT_ROOT)
    return result


def main():
    parser = argparse.ArgumentParser(description="green2blue real-device wet test")
    parser.add_argument(
        "--password", type=str, default=None,
        help="Backup encryption password (will prompt if backup is encrypted and not provided)",
    )
    args = parser.parse_args()

    tmpdir = Path(tempfile.mkdtemp(prefix="g2b_real_"))
    password = args.password

    try:
        # Build the single-message export
        export_zip = build_single_message_export(tmpdir)
        print(f"Created test export: {export_zip}")
        print('Contains: 1 SMS from +15550000001 saying "How are you?"')

        # List backups so the user can see what's available
        print("\n=== Available iPhone backups ===")
        result = run_g2b("list-backups")
        if result.returncode != 0:
            print("\nNo backups found. Please:")
            print("  1. Connect your iPhone via USB")
            print("  2. Open Finder and select your iPhone")
            print("  3. Click 'Back Up Now'")
            print("  4. Re-run this script")
            return 1

        # If no password provided, check if backup is encrypted and prompt
        if password is None:
            # Quick check: see if the backup is encrypted from list output
            check = subprocess.run(
                [sys.executable, "-m", "green2blue", "list-backups"],
                capture_output=True, text=True, cwd=PROJECT_ROOT,
            )
            if "ENCRYPTED" in check.stdout:
                password = getpass.getpass("Backup is encrypted. Enter password: ")

        # Build password args
        pw_args = ["--password", password] if password else []

        # Inspect the export
        print("\n=== Inspecting export ===")
        run_g2b("inspect", str(export_zip))

        # Dry run first
        print("\n=== Dry run (no changes) ===")
        result = run_g2b("inject", str(export_zip), "--dry-run", "-v", *pw_args)
        if result.returncode != 0:
            print("\nDry run failed. Check the errors above.")
            return 1

        # Prompt for live injection
        print("\n" + "=" * 50)
        print("Dry run succeeded! Ready for live injection.")
        print("This will:")
        print("  - Create a safety copy of your backup")
        print('  - Inject 1 message: "How are you?" from +15550000001')
        print("  - Run post-injection verification")
        print("=" * 50)
        run_g2b("inject", str(export_zip), "-v", *pw_args)

    finally:
        # Clean up the temp export
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)

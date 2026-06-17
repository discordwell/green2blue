"""Headless smoke test for the JavaScript embedded in the review UI.

The review page is a single-file app inside ``_REVIEW_HTML``; nothing else
exercises it. This test extracts the script, feeds it a real
``ReviewSession.payload()``, and runs it in node against a minimal DOM stub
(see tests/fixtures/review_ui_smoke.js). Skipped when node is not installed.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from green2blue.review import _REVIEW_HTML, ReviewWorkflowContext, open_review_session

HARNESS = Path(__file__).parent / "fixtures" / "review_ui_smoke.js"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_review_ui_logic_smoke(sample_export_zip, tmp_dir):
    match = re.search(r"<script>(.*)</script>", _REVIEW_HTML, re.DOTALL)
    assert match, "review HTML must embed exactly one script block"
    ui_script_path = tmp_dir / "review_ui.js"
    ui_script_path.write_text(match.group(1), encoding="utf-8")

    context = ReviewWorkflowContext(
        title="Review checkpoint",
        summary="Trim this export before the wizard continues.",
        next_step="The wizard will resume in the terminal.",
    )
    with open_review_session(sample_export_zip) as session:
        payload_path = tmp_dir / "payload.json"
        payload_path.write_text(
            json.dumps(session.payload(workflow_context=context)),
            encoding="utf-8",
        )

    result = subprocess.run(
        ["node", str(HARNESS), str(payload_path), str(ui_script_path)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "JS SMOKE TEST OK" in result.stdout

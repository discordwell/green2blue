"""Durable archive workflow orchestration for large-history runs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from green2blue.archive.android_import import AndroidArchiveImportResult, import_android_export
from green2blue.archive.ios_import import IOSArchiveImportResult, import_ios_backup
from green2blue.archive.merge import ArchiveMergeResult, merge_archive
from green2blue.archive.report import ArchiveReport, build_archive_report
from green2blue.archive.stage import IOSStageResult, stage_ios_export
from green2blue.archive.verify import ArchiveVerificationResult, verify_archive
from green2blue.ios.backup import BackupInfo, find_backup


@dataclass(frozen=True)
class IOSWorkflowPreparationResult:
    workflow_dir: Path
    state_path: Path
    archive_path: Path
    stage_dir: Path
    export_zip: Path
    backup_info: BackupInfo
    android_import: AndroidArchiveImportResult
    ios_import: IOSArchiveImportResult
    merge: ArchiveMergeResult
    report: ArchiveReport
    archive_verification: ArchiveVerificationResult
    stage: IOSStageResult | None


@dataclass(frozen=True)
class IOSWorkflowStatus:
    workflow_dir: Path
    state_path: Path
    status: str
    current_step: str | None
    created_at: str | None
    updated_at: str | None
    inputs: dict[str, object]
    artifacts: dict[str, object]
    steps: dict[str, object]
    last_error: dict[str, object] | None


def prepare_ios_workflow(
    export_zip: Path | str,
    backup: Path | str | None,
    workflow_dir: Path | str,
    *,
    backup_root: Path | None = None,
    password: str | None = None,
    country: str = "US",
    resume: bool = True,
) -> IOSWorkflowPreparationResult:
    """Prepare a durable merged-archive workflow directory for iPhone injection."""
    export_zip = Path(export_zip).resolve()
    workflow_dir = Path(workflow_dir)
    state_path = workflow_dir / "workflow_state.json"
    archive_path = workflow_dir / "merged.g2b.sqlite"
    stage_dir = workflow_dir / "stage"
    workflow_dir.mkdir(parents=True, exist_ok=True)

    backup_info = find_backup(str(backup) if backup is not None else None, backup_root)
    state = _load_state(state_path)
    state.update(
        {
            "workflow_version": 1,
            "updated_at": _utcnow(),
            "status": "running",
            "inputs": {
                "export_zip": str(export_zip),
                "backup_path": str(backup_info.path),
                "backup_udid": backup_info.udid,
                "country": country,
                "resume": resume,
            },
            "artifacts": {
                "archive_path": str(archive_path),
                "stage_dir": str(stage_dir),
            },
        }
    )
    if "created_at" not in state:
        state["created_at"] = _utcnow()
    _write_state(state_path, state)

    try:
        _set_current_step(state, state_path, "android_import")
        android_result = import_android_export(export_zip, archive_path, resume=resume)
        state["steps"] = state.get("steps", {})
        state["steps"]["android_import"] = _android_import_payload(android_result)
        _write_state(state_path, state)

        _set_current_step(state, state_path, "ios_import")
        ios_result = import_ios_backup(
            backup_info.path,
            archive_path,
            backup_root=backup_root,
            password=password,
            resume=resume,
        )
        state["steps"]["ios_import"] = _ios_import_payload(ios_result)
        _write_state(state_path, state)

        _set_current_step(state, state_path, "merge")
        merge_result = None
        if resume and android_result.reused_existing and ios_result.reused_existing:
            merge_result = _latest_merge_result(archive_path)
        if merge_result is None:
            merge_result = merge_archive(archive_path, country=country)
        state["steps"]["merge"] = _merge_payload(merge_result)
        _write_state(state_path, state)

        _set_current_step(state, state_path, "report")
        report = build_archive_report(archive_path)
        state["steps"]["report"] = _report_payload(report)
        _write_state(state_path, state)

        _set_current_step(state, state_path, "archive_verify")
        archive_verify_result = verify_archive(archive_path)
        state["steps"]["archive_verify"] = _archive_verify_payload(archive_verify_result)
        _write_state(state_path, state)

        stage_result = None
        if archive_verify_result.passed:
            _set_current_step(state, state_path, "stage")
            stage_result = stage_ios_export(
                archive_path,
                stage_dir,
                merge_run_id=merge_result.merge_run_id,
                country=country,
                resume=resume,
            )
            state["steps"]["stage"] = _stage_payload(stage_result)
            state["status"] = "completed"
        else:
            state["status"] = "blocked"

        state["current_step"] = None
        state["updated_at"] = _utcnow()
        _write_state(state_path, state)
    except Exception as exc:
        state["status"] = "failed"
        state["current_step"] = None
        state["updated_at"] = _utcnow()
        state["last_error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_state(state_path, state)
        raise

    return IOSWorkflowPreparationResult(
        workflow_dir=workflow_dir,
        state_path=state_path,
        archive_path=archive_path,
        stage_dir=stage_dir,
        export_zip=export_zip,
        backup_info=backup_info,
        android_import=android_result,
        ios_import=ios_result,
        merge=merge_result,
        report=report,
        archive_verification=archive_verify_result,
        stage=stage_result,
    )


def load_ios_workflow_status(workflow_dir: Path | str) -> IOSWorkflowStatus:
    workflow_dir = Path(workflow_dir)
    state_path = workflow_dir / "workflow_state.json"
    state = _load_state(state_path)
    return IOSWorkflowStatus(
        workflow_dir=workflow_dir,
        state_path=state_path,
        status=str(state.get("status", "missing")),
        current_step=state.get("current_step"),
        created_at=state.get("created_at"),
        updated_at=state.get("updated_at"),
        inputs=dict(state.get("inputs", {})),
        artifacts=dict(state.get("artifacts", {})),
        steps=dict(state.get("steps", {})),
        last_error=state.get("last_error"),
    )


def _load_state(state_path: Path) -> dict[str, object]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text())
    except (OSError, ValueError, TypeError):
        return {}


def _write_state(state_path: Path, payload: dict[str, object]) -> None:
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set_current_step(
    state: dict[str, object],
    state_path: Path,
    step: str,
) -> None:
    state["current_step"] = step
    state["updated_at"] = _utcnow()
    _write_state(state_path, state)


def _latest_merge_result(archive_path: Path) -> ArchiveMergeResult | None:
    conn = sqlite3.connect(archive_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT id, merged_conversation_count, merged_message_count, duplicate_message_count
            FROM merge_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    return ArchiveMergeResult(
        archive_path=archive_path,
        merge_run_id=int(row["id"]),
        merged_conversations=int(row["merged_conversation_count"]),
        merged_messages=int(row["merged_message_count"]),
        duplicate_messages=int(row["duplicate_message_count"]),
    )


def _android_import_payload(result: AndroidArchiveImportResult) -> dict[str, object]:
    return {
        "archive_path": str(result.archive_path),
        "import_run_id": result.import_run_id,
        "reused_existing": result.reused_existing,
        "messages_imported": result.messages_imported,
        "messages_deduped": result.messages_deduped,
        "attachments_imported": result.attachments_imported,
        "blobs_imported": result.blobs_imported,
    }


def _ios_import_payload(result: IOSArchiveImportResult) -> dict[str, object]:
    return {
        "archive_path": str(result.archive_path),
        "import_run_id": result.import_run_id,
        "reused_existing": result.reused_existing,
        "messages_imported": result.messages_imported,
        "messages_deduped": result.messages_deduped,
        "attachments_imported": result.attachments_imported,
        "blobs_imported": result.blobs_imported,
        "backup_path": str(result.backup_path),
        "backup_udid": result.backup_udid,
    }


def _merge_payload(result: ArchiveMergeResult) -> dict[str, object]:
    return {
        "merge_run_id": result.merge_run_id,
        "merged_conversations": result.merged_conversations,
        "merged_messages": result.merged_messages,
        "duplicate_messages": result.duplicate_messages,
    }


def _report_payload(report: ArchiveReport) -> dict[str, object]:
    return {
        "messages": report.summary.messages,
        "messages_with_attachments": report.messages_with_attachments,
        "messages_with_url": report.messages_with_url,
        "warnings": list(report.warnings),
        "latest_merge": report.latest_merge,
        "latest_merge_winner_source_counts": report.latest_merge_winner_source_counts,
        "unsupported_feature_counts": report.unsupported_feature_counts,
    }


def _archive_verify_payload(result: ArchiveVerificationResult) -> dict[str, object]:
    return {
        "passed": result.passed,
        "checks_run": result.checks_run,
        "checks_passed": result.checks_passed,
        "errors": list(result.errors),
        "warnings": list(result.warnings),
        "latest_merge_id": result.latest_merge_id,
        "ios_inject_candidate_messages": result.ios_inject_candidate_messages,
    }


def _stage_payload(result: IOSStageResult) -> dict[str, object]:
    return {
        "stage_dir": str(result.stage_dir),
        "output_zip": str(result.output_zip),
        "metadata_path": str(result.metadata_path),
        "archive_path": str(result.archive_path),
        "merge_run_id": result.merge_run_id,
        "reused_existing": result.reused_existing,
        "records_written": result.records_written,
        "attachment_files_written": result.attachment_files_written,
        "attachments_missing_data": result.attachments_missing_data,
        "verification_passed": result.verification_passed,
        "verification_errors": list(result.verification_errors),
    }

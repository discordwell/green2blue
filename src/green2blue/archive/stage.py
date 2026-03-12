"""Durable staging for merged archive exports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from green2blue.archive.export_android import AndroidArchiveExportResult, export_merged_android_zip
from green2blue.archive.report import build_archive_report
from green2blue.parser.ndjson_parser import count_messages
from green2blue.parser.zip_reader import open_export_zip


@dataclass(frozen=True)
class IOSStageResult:
    stage_dir: Path
    output_zip: Path
    metadata_path: Path
    archive_path: Path
    merge_run_id: int
    reused_existing: bool
    records_written: int
    attachment_files_written: int
    attachments_missing_data: int
    verification_passed: bool
    verification_errors: tuple[str, ...]


def stage_ios_export(
    archive_path: Path | str,
    stage_dir: Path | str,
    *,
    merge_run_id: int | None = None,
    country: str = "US",
    resume: bool = True,
) -> IOSStageResult:
    archive_path = Path(archive_path).resolve()
    stage_dir = Path(stage_dir)
    output_zip = stage_dir / "merged_export.zip"
    metadata_path = stage_dir / "stage_metadata.json"

    if resume:
        existing = _load_existing_stage(metadata_path, output_zip)
        if existing is not None:
            same_archive = existing.get("archive_path") == str(archive_path)
            same_merge = existing.get("requested_merge_run_id") == merge_run_id
            same_country = existing.get("country") == country
            same_mode = existing.get("mode") == "ios-inject"
            if same_archive and same_merge and same_country and same_mode:
                return IOSStageResult(
                    stage_dir=stage_dir,
                    output_zip=output_zip,
                    metadata_path=metadata_path,
                    archive_path=archive_path,
                    merge_run_id=int(existing["resolved_merge_run_id"]),
                    reused_existing=True,
                    records_written=int(existing["records_written"]),
                    attachment_files_written=int(existing["attachment_files_written"]),
                    attachments_missing_data=int(existing["attachments_missing_data"]),
                    verification_passed=bool(existing.get("verification_passed", True)),
                    verification_errors=tuple(existing.get("verification_errors", [])),
                )

    stage_dir.mkdir(parents=True, exist_ok=True)
    export_result = export_merged_android_zip(
        archive_path,
        output_zip,
        merge_run_id=merge_run_id,
        country=country,
        mode="ios-inject",
    )
    verification_errors = _verify_staged_export(output_zip, export_result)
    report = build_archive_report(archive_path)
    payload = {
        "archive_path": str(archive_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "requested_merge_run_id": merge_run_id,
        "resolved_merge_run_id": export_result.merge_run_id,
        "country": country,
        "mode": "ios-inject",
        "records_written": export_result.records_written,
        "attachment_files_written": export_result.attachment_files_written,
        "attachments_missing_data": export_result.attachments_missing_data,
        "verification_passed": not verification_errors,
        "verification_errors": verification_errors,
        "report": {
            "messages": report.summary.messages,
            "messages_with_attachments": report.messages_with_attachments,
            "messages_with_url": report.messages_with_url,
            "warnings": list(report.warnings),
            "latest_merge": report.latest_merge,
            "latest_merge_winner_source_counts": report.latest_merge_winner_source_counts,
            "unsupported_feature_counts": report.unsupported_feature_counts,
        },
    }
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    return IOSStageResult(
        stage_dir=stage_dir,
        output_zip=output_zip,
        metadata_path=metadata_path,
        archive_path=archive_path,
        merge_run_id=export_result.merge_run_id,
        reused_existing=False,
        records_written=export_result.records_written,
        attachment_files_written=export_result.attachment_files_written,
        attachments_missing_data=export_result.attachments_missing_data,
        verification_passed=not verification_errors,
        verification_errors=tuple(verification_errors),
    )


def _load_existing_stage(metadata_path: Path, output_zip: Path) -> dict[str, object] | None:
    if not metadata_path.exists() or not output_zip.exists():
        return None
    try:
        return json.loads(metadata_path.read_text())
    except (OSError, ValueError, TypeError):
        return None


def _verify_staged_export(
    output_zip: Path,
    export_result: AndroidArchiveExportResult,
) -> list[str]:
    errors: list[str] = []
    with open_export_zip(output_zip) as export:
        counts = count_messages(export.ndjson_path)
        actual_total = counts["total"]
        has_attachments = export.has_attachments()

    if actual_total != export_result.records_written:
        errors.append(
            f"Stage ZIP contains {actual_total} records, expected {export_result.records_written}.",
        )
    if export_result.attachment_files_written > 0 and not has_attachments:
        errors.append(
            "Stage ZIP exported attachment files but the archive reader found no attachments.",
        )
    return errors

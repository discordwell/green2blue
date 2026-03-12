"""Durable staging for merged archive exports."""

from __future__ import annotations

from collections import Counter
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import zipfile

from green2blue.archive.db import CanonicalArchive
from green2blue.archive.export_android import export_merged_android_zip
from green2blue.archive.export_android import (
    _build_android_record,
    _load_merged_participants,
    _load_merged_winners,
    _load_message_parts,
)
from green2blue.archive.report import build_archive_report


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
                resolved_merge_run_id = int(existing["resolved_merge_run_id"])
                verification_errors = _verify_staged_export(
                    archive_path,
                    output_zip,
                    merge_run_id=resolved_merge_run_id,
                )
                if not verification_errors:
                    return IOSStageResult(
                        stage_dir=stage_dir,
                        output_zip=output_zip,
                        metadata_path=metadata_path,
                        archive_path=archive_path,
                        merge_run_id=resolved_merge_run_id,
                        reused_existing=True,
                        records_written=int(existing["records_written"]),
                        attachment_files_written=int(existing["attachment_files_written"]),
                        attachments_missing_data=int(existing["attachments_missing_data"]),
                        verification_passed=True,
                        verification_errors=(),
                    )

    stage_dir.mkdir(parents=True, exist_ok=True)
    export_result = export_merged_android_zip(
        archive_path,
        output_zip,
        merge_run_id=merge_run_id,
        country=country,
        mode="ios-inject",
    )
    verification_errors = _verify_staged_export(
        archive_path,
        output_zip,
        merge_run_id=export_result.merge_run_id,
    )
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
    archive_path: Path,
    output_zip: Path,
    *,
    merge_run_id: int,
) -> list[str]:
    errors: list[str] = []
    expected = _expected_stage_render(archive_path, merge_run_id)
    actual = _actual_stage_render(output_zip)

    if actual["records_written"] != expected["records_written"]:
        errors.append(
            f"Stage ZIP contains {actual['records_written']} records, expected {expected['records_written']}.",
        )

    if actual["attachment_files_written"] != expected["attachment_files_written"]:
        errors.append(
            "Stage ZIP attachment file count does not match the archive render plan "
            f"({actual['attachment_files_written']} vs {expected['attachment_files_written']}).",
        )

    if actual["record_signatures"] != expected["record_signatures"]:
        errors.append("Stage ZIP message content does not match the archive render plan.")

    if actual["attachment_names"] != expected["attachment_names"]:
        errors.append("Stage ZIP attachment filenames do not match the archive render plan.")

    return errors


def _expected_stage_render(archive_path: Path, merge_run_id: int) -> dict[str, object]:
    with CanonicalArchive(archive_path) as archive:
        conn = archive.conn
        assert conn is not None
        participants = _load_merged_participants(conn, merge_run_id)
        attachments = _load_message_parts(conn, merge_run_id)
        messages = _load_merged_winners(conn, merge_run_id, mode="ios-inject")

        thread_map: dict[int, int] = {}
        record_signatures: Counter[str] = Counter()
        attachment_names: list[str] = []

        for index, message in enumerate(messages, start=1):
            merged_conversation_id = int(message["merged_conversation_id"])
            thread_id = thread_map.setdefault(merged_conversation_id, len(thread_map) + 1)
            record, new_files, _missing = _build_android_record(
                message,
                participants.get(merged_conversation_id, ()),
                attachments.get(int(message["id"]), ()),
                thread_id=thread_id,
                ordinal=index,
            )
            record_signatures[_stable_json(record)] += 1
            attachment_names.extend(rel_path for rel_path, _payload in new_files)

    return {
        "records_written": sum(record_signatures.values()),
        "attachment_files_written": len(attachment_names),
        "record_signatures": record_signatures,
        "attachment_names": tuple(sorted(attachment_names)),
    }


def _actual_stage_render(output_zip: Path) -> dict[str, object]:
    with zipfile.ZipFile(output_zip, "r") as zf:
        names = zf.namelist()
        if "messages.ndjson" not in names:
            return {
                "records_written": 0,
                "attachment_files_written": len([name for name in names if name.startswith("data/")]),
                "record_signatures": Counter(),
                "attachment_names": tuple(sorted(name for name in names if name.startswith("data/"))),
            }

        raw = zf.read("messages.ndjson").decode("utf-8")
        record_signatures: Counter[str] = Counter()
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            record_signatures[_stable_json(json.loads(line))] += 1

        attachment_names = tuple(
            sorted(
                name for name in names
                if name.startswith("data/") and not name.endswith("/")
            )
        )
        return {
            "records_written": sum(record_signatures.values()),
            "attachment_files_written": len(attachment_names),
            "record_signatures": record_signatures,
            "attachment_names": attachment_names,
        }


def _stable_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

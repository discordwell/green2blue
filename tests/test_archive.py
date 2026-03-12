"""Tests for the canonical archive import path."""

from __future__ import annotations

import sqlite3
import zipfile
from pathlib import Path

from green2blue.archive import (
    ArchiveMergeResult,
    AndroidArchiveExportResult,
    IOSRenderedTargetVerificationResult,
    IOSWorkflowPreparationResult,
    IOSWorkflowStatus,
    ArchiveVerificationResult,
    CanonicalArchive,
    build_archive_report,
    export_merged_android_zip,
    import_android_export,
    import_ios_backup,
    load_ios_workflow_status,
    merge_archive,
    prepare_ios_workflow,
    stage_ios_export,
    verify_ios_render_target,
    verify_archive,
)
from green2blue.converter.timestamp import unix_ms_to_ios_ns
from green2blue.ios.manifest import compute_file_id
from green2blue.models import ATTACHMENT_PLACEHOLDER
from green2blue.parser.ndjson_parser import count_messages
from green2blue.parser.zip_reader import open_export_zip
from green2blue.pipeline import run_pipeline


class TestAndroidArchiveImport:
    def test_import_android_export_creates_archive(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "sample.g2b.sqlite"

        result = import_android_export(sample_export_zip, archive_path)

        assert archive_path.exists()
        assert result.reused_existing is False
        assert result.messages_imported == 3
        assert result.messages_deduped == 0
        assert result.attachments_imported == 1
        assert result.blobs_imported == 1

        with CanonicalArchive(archive_path) as archive:
            summary = archive.summary()

        assert summary.import_runs == 1
        assert summary.messages == 3
        assert summary.attachment_parts >= 2
        assert summary.blobs == 1
        assert summary.blob_bytes > 0

    def test_reimport_reuses_existing_import_by_default(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "sample.g2b.sqlite"

        first = import_android_export(sample_export_zip, archive_path)
        second = import_android_export(sample_export_zip, archive_path)

        assert first.messages_imported == 3
        assert second.reused_existing is True
        assert second.messages_imported == 3
        assert second.messages_deduped == 0

        conn = sqlite3.connect(archive_path)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0] == 1
        conn.close()

    def test_reimport_without_resume_creates_second_import_run(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "sample.g2b.sqlite"

        first = import_android_export(sample_export_zip, archive_path)
        second = import_android_export(sample_export_zip, archive_path, resume=False)

        assert first.messages_imported == 3
        assert second.reused_existing is False
        assert second.messages_imported == 0
        assert second.messages_deduped == 3

        conn = sqlite3.connect(archive_path)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0] == 2
        conn.close()


def _populate_ios_backup(sample_backup_dir: Path) -> None:
    sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    sms_db = sample_backup_dir / sms_hash[:2] / sms_hash
    conn = sqlite3.connect(sms_db)
    conn.execute(
        "INSERT INTO handle (ROWID, id, service, uncanonicalized_id) VALUES (1, ?, 'SMS', ?)",
        ("+12025550101", "+12025550101"),
    )
    conn.execute(
        """
        INSERT INTO chat (
            ROWID, guid, chat_identifier, service_name, display_name, style
        ) VALUES (1, ?, ?, 'SMS', ?, 45)
        """,
        ("any;+;+12025550101", "+12025550101", "+12025550101"),
    )
    conn.execute("INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (1, 1)")

    date_one = unix_ms_to_ios_ns(1_700_000_000_000)
    date_two = unix_ms_to_ios_ns(1_700_000_001_000)
    conn.execute(
        """
        INSERT INTO message (
            ROWID, guid, text, handle_id, service, date, date_read, is_from_me,
            is_read, is_sent, is_finished, is_delivered, cache_has_attachments,
            part_count
        ) VALUES (?, ?, ?, ?, 'SMS', ?, ?, 0, 1, 0, 1, 0, 0, 1)
        """,
        (1, "msg-1", "Hello from iPhone", 1, date_one, date_one),
    )
    conn.execute(
        """
        INSERT INTO message (
            ROWID, guid, text, handle_id, service, date, date_read, is_from_me,
            is_read, is_sent, is_finished, is_delivered, cache_has_attachments,
            part_count
        ) VALUES (?, ?, ?, ?, 'SMS', ?, ?, 0, 0, 0, 1, 0, 1, 2)
        """,
        (
            2,
            "msg-2",
            ATTACHMENT_PLACEHOLDER + "Photo caption",
            1,
            date_two,
            0,
        ),
    )
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (1, 1, ?)",
        (date_one,),
    )
    conn.execute(
        "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (1, 2, ?)",
        (date_two,),
    )
    conn.execute(
        """
        INSERT INTO attachment (
            ROWID, guid, filename, mime_type, transfer_name, total_bytes
        ) VALUES (1, ?, ?, 'image/jpeg', ?, ?)
        """,
        (
            "att-1",
            "~/Library/SMS/Attachments/ab/cd/ATT-1/image000000.jpg",
            "image000000.jpg",
            15,
        ),
    )
    conn.execute(
        "INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (2, 1)",
    )
    conn.commit()
    conn.close()

    attachment_rel = "Library/SMS/Attachments/ab/cd/ATT-1/image000000.jpg"
    attachment_id = compute_file_id("HomeDomain", attachment_rel)
    attachment_dir = sample_backup_dir / attachment_id[:2]
    attachment_dir.mkdir(exist_ok=True)
    (attachment_dir / attachment_id).write_bytes(b"real-jpeg-bytes")


class TestIOSArchiveImport:
    def test_import_ios_backup_creates_archive(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "ios.g2b.sqlite"

        result = import_ios_backup(sample_backup_dir, archive_path)

        assert archive_path.exists()
        assert result.reused_existing is False
        assert result.messages_imported == 2
        assert result.messages_deduped == 0
        assert result.attachments_imported == 1
        assert result.blobs_imported == 1

        conn = sqlite3.connect(archive_path)
        conn.row_factory = sqlite3.Row
        summary = CanonicalArchive(archive_path)
        with summary as archive:
            archive_summary = archive.summary()
        assert archive_summary.import_runs == 1
        assert archive_summary.messages == 2
        assert archive_summary.blobs == 1

        photo_row = conn.execute(
            "SELECT body_text, has_attachments FROM messages WHERE source_uid = 'ios:msg-2'",
        ).fetchone()
        assert photo_row["body_text"] == "Photo caption"
        assert photo_row["has_attachments"] == 1
        attachment_row = conn.execute(
            "SELECT filename FROM message_attachments WHERE message_id = 2",
        ).fetchone()
        assert attachment_row["filename"] == "image000000.jpg"
        conn.close()

    def test_reimport_ios_backup_reuses_existing_import_by_default(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "ios.g2b.sqlite"

        first = import_ios_backup(sample_backup_dir, archive_path)
        second = import_ios_backup(sample_backup_dir, archive_path)

        assert first.messages_imported == 2
        assert second.reused_existing is True
        assert second.messages_imported == 2
        assert second.messages_deduped == 0

        conn = sqlite3.connect(archive_path)
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0] == 1
        conn.close()


class TestArchiveReport:
    def test_report_includes_cross_source_warning(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"

        import_android_export(sample_export_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)

        report = build_archive_report(archive_path)

        assert report.summary.import_runs == 2
        assert len(report.import_run_summaries) == 2
        assert report.source_type_counts["android.sms"] >= 1
        assert report.source_type_counts["ios.message"] == 2
        assert report.messages_with_attachments >= 2
        assert report.messages_with_url == 0
        assert report.missing_attachment_blobs == 0
        assert any("merged view has been materialized" in warning for warning in report.warnings)


def _create_matching_android_export(tmp_dir: Path) -> Path:
    zip_path = tmp_dir / "matching_android.zip"
    content = (
        '{"address":"+12025550101","body":"Hello from iPhone","date":"1700000000000","type":"1","read":"1"}\n'
    )
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("messages.ndjson", content)
    return zip_path


class TestArchiveMerge:
    def test_merge_archive_materializes_cross_source_dedup(
        self,
        sample_backup_dir,
        tmp_dir,
    ):
        _populate_ios_backup(sample_backup_dir)
        android_zip = _create_matching_android_export(tmp_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"

        import_android_export(android_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)

        result = merge_archive(archive_path)

        assert isinstance(result, ArchiveMergeResult)
        assert result.merged_conversations == 1
        assert result.merged_messages == 2
        assert result.duplicate_messages == 1

        conn = sqlite3.connect(archive_path)
        conn.row_factory = sqlite3.Row
        merge_run = conn.execute(
            "SELECT merged_conversation_count, merged_message_count, duplicate_message_count FROM merge_runs",
        ).fetchone()
        assert dict(merge_run) == {
            "merged_conversation_count": 1,
            "merged_message_count": 2,
            "duplicate_message_count": 1,
        }
        dedup_rows = conn.execute(
            """
            SELECT is_duplicate
            FROM merged_messages
            ORDER BY is_duplicate, message_id
            """
        ).fetchall()
        assert [row["is_duplicate"] for row in dedup_rows] == [0, 0, 1]
        conn.close()

    def test_report_shows_latest_merge(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        android_zip = _create_matching_android_export(tmp_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"

        import_android_export(android_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)
        merge_archive(archive_path)

        report = build_archive_report(archive_path)

        assert report.merge_runs == 1
        assert report.latest_merge is not None
        assert report.latest_merge["merged_conversations"] == 1
        assert report.latest_merge["duplicate_messages"] == 1
        assert report.latest_merge_winner_source_counts["ios.message"] == 2
        assert not any("merged view has been materialized" in warning for warning in report.warnings)

    def test_merge_groups_subset_participant_sets_when_titles_match(self, tmp_dir):
        archive_path = tmp_dir / "group_merge.g2b.sqlite"

        with CanonicalArchive(archive_path) as archive:
            android_run = archive.start_import("android-export", "/tmp/android.zip")
            ios_run = archive.start_import("ios-backup", "/tmp/ios-backup")

            pa = archive.get_or_create_participant("+12025550111", "phone")
            pb = archive.get_or_create_participant("+12025550112", "phone")
            pc = archive.get_or_create_participant("+12025550113", "phone")

            android_conv = archive.get_or_create_conversation(
                "android:mms:thread:1",
                kind="group",
                source_thread_id="1",
                title="Weekend Plans",
            )
            for sort_order, participant_id in enumerate((pa, pb, pc)):
                archive.link_conversation_participant(
                    android_conv, participant_id, role="member", sort_order=sort_order,
                )
            ios_conv = archive.get_or_create_conversation(
                "ios:chat:any;+;chat123",
                kind="group",
                source_thread_id="9",
                title="Weekend Plans",
            )
            for sort_order, participant_id in enumerate((pa, pb)):
                archive.link_conversation_participant(
                    ios_conv, participant_id, role="member", sort_order=sort_order,
                )

            archive.insert_message(
                source_uid="android:group-1",
                source_type="android.mms",
                import_run_id=android_run,
                conversation_id=android_conv,
                direction="incoming",
                sent_at_ms=1_700_000_100_000,
                read_state="read",
                service_hint="MMS",
                subject="Weekend Plans",
                body_text="Saturday works",
                has_attachments=False,
                has_url=False,
                raw_json='{"thread_id":1}',
            )
            archive.insert_message(
                source_uid="ios:group-1",
                source_type="ios.message",
                import_run_id=ios_run,
                conversation_id=ios_conv,
                direction="incoming",
                sent_at_ms=1_700_000_200_000,
                read_state="read",
                service_hint="SMS",
                subject="Weekend Plans",
                body_text="See you there",
                has_attachments=False,
                has_url=False,
                raw_json='{"handle_address":"+12025550111"}',
            )
            archive.finish_import(android_run, 1, 0)
            archive.finish_import(ios_run, 1, 0)
            archive.conn.commit()

        result = merge_archive(archive_path)
        assert result.merged_conversations == 1

    def test_merge_direct_chat_title_identity_matches_participant_identity(self, tmp_dir):
        archive_path = tmp_dir / "direct_merge.g2b.sqlite"

        with CanonicalArchive(archive_path) as archive:
            android_run = archive.start_import("android-export", "/tmp/android.zip")
            ios_run = archive.start_import("ios-backup", "/tmp/ios-backup")

            participant_id = archive.get_or_create_participant("+12025550111", "phone")

            android_conv = archive.get_or_create_conversation(
                "android:sms:thread:1",
                kind="direct",
                source_thread_id="1",
                title="+12025550111",
            )
            archive.link_conversation_participant(
                android_conv,
                participant_id,
                role="peer",
                sort_order=0,
            )
            ios_conv = archive.get_or_create_conversation(
                "ios:chat:any;+;+12025550111",
                kind="direct",
                source_thread_id="2",
                title="tel:+1 (202) 555-0111",
            )

            archive.insert_message(
                source_uid="android:direct-1",
                source_type="android.sms",
                import_run_id=android_run,
                conversation_id=android_conv,
                direction="incoming",
                sent_at_ms=1_700_000_100_000,
                read_state="read",
                service_hint="SMS",
                subject=None,
                body_text="hello",
                has_attachments=False,
                has_url=False,
                raw_json='{"thread_id":1}',
            )
            archive.insert_message(
                source_uid="ios:direct-1",
                source_type="ios.message",
                import_run_id=ios_run,
                conversation_id=ios_conv,
                direction="incoming",
                sent_at_ms=1_700_000_200_000,
                read_state="read",
                service_hint="SMS",
                subject=None,
                body_text="hi",
                has_attachments=False,
                has_url=False,
                raw_json='{"guid":"msg-1"}',
            )
            archive.finish_import(android_run, 1, 0)
            archive.finish_import(ios_run, 1, 0)
            archive.conn.commit()

        result = merge_archive(archive_path)
        assert result.merged_conversations == 1


class TestArchiveWarnings:
    def test_report_warns_on_unsupported_feature_markers(self, tmp_dir):
        archive_path = tmp_dir / "warnings.g2b.sqlite"

        with CanonicalArchive(archive_path) as archive:
            import_run_id = archive.start_import("ios-backup", "/tmp/fake-backup")
            participant_id = archive.get_or_create_participant("+12025550101", "phone")
            conversation_id = archive.get_or_create_conversation(
                "ios:chat:any;+;+12025550101",
                kind="direct",
                source_thread_id="1",
                title="+12025550101",
            )
            archive.link_conversation_participant(
                conversation_id,
                participant_id,
                role="peer",
                sort_order=0,
            )
            archive.insert_message(
                source_uid="ios:warn-1",
                source_type="ios.message",
                import_run_id=import_run_id,
                conversation_id=conversation_id,
                direction="incoming",
                sent_at_ms=1_700_000_000_000,
                read_state="read",
                service_hint="iMessage",
                subject=None,
                body_text="Unsupported features",
                has_attachments=False,
                has_url=False,
                raw_json=(
                    '{"reply_to_guid":"abc","associated_message_guid":"tapback",'
                    '"date_edited_ns":123,"balloon_bundle_id":"com.example.effect",'
                    '"expressive_send_style_id":"impact"}'
                ),
            )
            archive.finish_import(import_run_id, 1, 0)
            archive.conn.commit()

        report = build_archive_report(archive_path)

        assert report.unsupported_feature_counts["reply_or_reaction"] == 1
        assert report.unsupported_feature_counts["edited"] == 1
        assert report.unsupported_feature_counts["rich_effect"] == 1
        assert any("replies or reactions" in warning for warning in report.warnings)
        assert any("edited messages" in warning for warning in report.warnings)
        assert any("rich app/message effects" in warning for warning in report.warnings)


class TestArchiveVerify:
    def test_verify_archive_passes_for_clean_android_archive(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "verify_android_clean.g2b.sqlite"

        import_android_export(sample_export_zip, archive_path)
        result = verify_archive(archive_path)

        assert result.passed is True

    def test_verify_archive_passes_for_clean_archive(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "verify_clean.g2b.sqlite"

        import_ios_backup(sample_backup_dir, archive_path)
        result = verify_archive(archive_path)

        assert isinstance(result, ArchiveVerificationResult)
        assert result.passed is True
        assert result.checks_run >= 1

    def test_verify_archive_detects_tampered_import_counts(self, sample_export_zip, tmp_dir):
        archive_path = tmp_dir / "verify_bad.g2b.sqlite"
        import_android_export(sample_export_zip, archive_path)

        conn = sqlite3.connect(archive_path)
        conn.execute("UPDATE import_runs SET message_count = 999")
        conn.commit()
        conn.close()

        result = verify_archive(archive_path)

        assert result.passed is False
        assert any("records 999 messages" in error for error in result.errors)


class TestArchiveStage:
    def test_stage_ios_export_writes_stage_bundle_and_reuses_it(
        self,
        sample_backup_dir,
        tmp_dir,
    ):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "stage.g2b.sqlite"
        stage_dir = tmp_dir / "stage_dir"

        import_ios_backup(sample_backup_dir, archive_path)

        first = stage_ios_export(archive_path, stage_dir)
        second = stage_ios_export(archive_path, stage_dir)

        assert first.output_zip.exists()
        assert first.metadata_path.exists()
        assert first.reused_existing is False
        assert first.verification_passed is True
        assert second.reused_existing is True
        assert second.verification_passed is True
        assert second.records_written == first.records_written

    def test_stage_ios_export_rebuilds_tampered_stage_on_resume(
        self,
        sample_export_zip,
        tmp_dir,
    ):
        archive_path = tmp_dir / "stage_tampered.g2b.sqlite"
        stage_dir = tmp_dir / "stage_dir"

        import_android_export(sample_export_zip, archive_path)

        first = stage_ios_export(archive_path, stage_dir)
        assert first.verification_passed is True
        assert first.records_written > 0

        with zipfile.ZipFile(first.output_zip, "w") as zf:
            zf.writestr("messages.ndjson", "")

        rebuilt = stage_ios_export(archive_path, stage_dir)

        assert rebuilt.reused_existing is False
        assert rebuilt.verification_passed is True
        with open_export_zip(rebuilt.output_zip) as export:
            counts = count_messages(export.ndjson_path)
            assert counts["total"] == first.records_written


class TestArchiveRenderVerify:
    def test_verify_ios_render_target_passes_for_stage_injected_backup(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        archive_path = tmp_dir / "render_verify.g2b.sqlite"
        stage_dir = tmp_dir / "stage_dir"

        import_android_export(sample_export_zip, archive_path)
        stage_result = stage_ios_export(archive_path, stage_dir)

        pipeline_result = run_pipeline(
            export_path=stage_result.output_zip,
            backup_path_or_udid=str(sample_backup_dir),
            dry_run=False,
        )
        verify_result = verify_ios_render_target(
            stage_result.output_zip,
            sample_backup_dir,
            pipeline_result,
        )

        assert isinstance(verify_result, IOSRenderedTargetVerificationResult)
        assert verify_result.passed is True
        assert verify_result.actual_messages == pipeline_result.injection_stats.messages_inserted
        assert verify_result.actual_attachments == pipeline_result.injection_stats.attachments_inserted

    def test_verify_ios_render_target_detects_tampered_message_row(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        archive_path = tmp_dir / "render_verify_bad.g2b.sqlite"
        stage_dir = tmp_dir / "stage_dir"

        import_android_export(sample_export_zip, archive_path)
        stage_result = stage_ios_export(archive_path, stage_dir)

        pipeline_result = run_pipeline(
            export_path=stage_result.output_zip,
            backup_path_or_udid=str(sample_backup_dir),
            dry_run=False,
        )

        sms_hash = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        sms_db = sample_backup_dir / sms_hash[:2] / sms_hash
        conn = sqlite3.connect(sms_db)
        conn.execute(
            "UPDATE message SET text = 'tampered render target' WHERE ROWID = ?",
            (pipeline_result.injection_stats.message_rowids[0],),
        )
        conn.commit()
        conn.close()

        verify_result = verify_ios_render_target(
            stage_result.output_zip,
            sample_backup_dir,
            pipeline_result,
        )

        assert verify_result.passed is False
        assert any("messages do not match" in error for error in verify_result.errors)


class TestArchiveWorkflow:
    def test_prepare_ios_workflow_writes_durable_state_and_reuses_artifacts(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        workflow_dir = tmp_dir / "workflow"

        first = prepare_ios_workflow(
            sample_export_zip,
            sample_backup_dir,
            workflow_dir,
        )
        second = prepare_ios_workflow(
            sample_export_zip,
            sample_backup_dir,
            workflow_dir,
        )

        assert isinstance(first, IOSWorkflowPreparationResult)
        assert first.state_path.exists()
        assert first.archive_path.exists()
        assert first.stage is not None
        assert first.stage.output_zip.exists()
        assert second.android_import.reused_existing is True
        assert second.ios_import.reused_existing is True
        assert second.stage is not None
        assert second.stage.reused_existing is True

    def test_load_ios_workflow_status_reads_persisted_state(
        self,
        sample_export_zip,
        sample_backup_dir,
        tmp_dir,
    ):
        workflow_dir = tmp_dir / "workflow"
        prepare_ios_workflow(
            sample_export_zip,
            sample_backup_dir,
            workflow_dir,
        )

        status = load_ios_workflow_status(workflow_dir)

        assert isinstance(status, IOSWorkflowStatus)
        assert status.status == "completed"
        assert status.current_step is None
        assert "android_import" in status.steps
        assert "stage" in status.steps


class TestArchiveExport:
    def test_export_merged_android_zip_round_trips_into_pipeline_dry_run(
        self,
        sample_backup_dir,
        tmp_dir,
    ):
        _populate_ios_backup(sample_backup_dir)
        android_zip = _create_matching_android_export(tmp_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"
        export_zip = tmp_dir / "merged_export.zip"

        import_android_export(android_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)
        merge_archive(archive_path)

        result = export_merged_android_zip(archive_path, export_zip)

        assert isinstance(result, AndroidArchiveExportResult)
        assert result.records_written == 2
        assert result.attachment_files_written == 1
        assert result.attachments_missing_data == 0

        with open_export_zip(export_zip) as export:
            counts = count_messages(export.ndjson_path)
            assert counts["total"] == 2
            assert counts["sms"] == 1
            assert counts["mms"] == 1
            assert export.has_attachments()

        pipeline_result = run_pipeline(
            export_path=export_zip,
            backup_path_or_udid=str(sample_backup_dir),
            dry_run=True,
        )
        assert pipeline_result.total_messages_parsed == 2

    def test_export_for_ios_inject_excludes_ios_source_winners(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        android_zip = _create_matching_android_export(tmp_dir)
        archive_path = tmp_dir / "merged.g2b.sqlite"
        export_zip = tmp_dir / "ios_inject_export.zip"

        import_android_export(android_zip, archive_path)
        import_ios_backup(sample_backup_dir, archive_path)
        merge_archive(archive_path)

        result = export_merged_android_zip(archive_path, export_zip, mode="ios-inject")

        assert result.records_written == 0
        with open_export_zip(export_zip) as export:
            counts = count_messages(export.ndjson_path)
            assert counts["total"] == 0

    def test_export_auto_merges_when_no_merge_run_exists(self, sample_backup_dir, tmp_dir):
        _populate_ios_backup(sample_backup_dir)
        archive_path = tmp_dir / "archive.g2b.sqlite"
        export_zip = tmp_dir / "auto_merge.zip"

        import_ios_backup(sample_backup_dir, archive_path)
        result = export_merged_android_zip(archive_path, export_zip)

        assert result.merge_run_id >= 1
        assert export_zip.exists()

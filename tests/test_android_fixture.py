"""Tests for synthetic Android export fixture generation."""

from __future__ import annotations

from pathlib import Path

from green2blue.pipeline import run_pipeline
from green2blue.parser.ndjson_parser import count_messages, parse_ndjson
from green2blue.parser.zip_reader import open_export_zip
from green2blue.testing.android_fixture import (
    DEFAULT_SCENARIOS,
    SCENARIOS,
    _all_scenarios,
    main,
    write_fixture_zip,
)
from tests.test_pipeline import _create_full_backup


class TestWriteFixtureZip:
    def test_default_fixture_round_trip(self, tmp_dir):
        zip_path = tmp_dir / "android_fixture.zip"
        fixture = write_fixture_zip(zip_path)

        assert fixture.output_path == zip_path
        assert fixture.scenario_names == DEFAULT_SCENARIOS
        assert fixture.message_count == 4
        assert len(fixture.attachment_files) == 2
        assert fixture.missing_attachment_refs == ()

        with open_export_zip(zip_path) as export:
            assert export.has_attachments()

            counts = count_messages(export.ndjson_path)
            messages = list(parse_ndjson(export.ndjson_path))

            assert counts["sms"] == 2
            assert counts["mms"] == 2
            assert counts["rcs"] == 0
            assert counts["total"] == 4
            assert len(messages) == 4

            basenames = {path.name for path in export.data_dir.iterdir()}
            assert "PART_1701000002_captioned_photo.jpg" in basenames
            assert "PART_1701000003_group_image.png" in basenames
            assert (
                export.data_dir / "PART_1701000002_captioned_photo.jpg"
            ).stat().st_size > 10_000
            assert (
                export.data_dir / "PART_1701000003_group_image.png"
            ).stat().st_size > 50_000

    def test_all_fixture_includes_media_scenarios(self, tmp_dir):
        zip_path = tmp_dir / "android_fixture_all.zip"
        fixture = write_fixture_zip(zip_path, _all_scenarios())

        assert fixture.message_count == 6
        assert len(fixture.attachment_files) == 5
        assert fixture.missing_attachment_refs == ()

        with open_export_zip(zip_path) as export:
            counts = count_messages(export.ndjson_path)
            messages = list(parse_ndjson(export.ndjson_path))

            assert counts["sms"] == 2
            assert counts["mms"] == 4
            assert counts["rcs"] == 1
            assert counts["total"] == 6

            image_only_rcs = [
                msg for msg in messages
                if getattr(msg, "parts", ()) and len(getattr(msg, "parts", ())) == 1
            ]
            assert image_only_rcs
            assert (export.data_dir / "PART_1701000004_receipt.jpg").stat().st_size > 10_000
            assert (export.data_dir / "PART_1701000004_clip.mp4").stat().st_size > 10_000
            assert (export.data_dir / "PART_1701000005_rcs_photo.jpg").stat().st_size > 10_000

    def test_all_with_negative_control_includes_missing_attachment(self, tmp_dir):
        zip_path = tmp_dir / "android_fixture_all_negative.zip"
        fixture = write_fixture_zip(zip_path, _all_scenarios(include_negative_controls=True))

        assert fixture.message_count == 7
        assert len(fixture.attachment_files) == 5
        assert len(fixture.missing_attachment_refs) == 1

    def test_missing_attachment_fixture_omits_file(self, tmp_dir):
        zip_path = tmp_dir / "android_fixture_missing.zip"
        fixture = write_fixture_zip(zip_path, ("mms-missing-attachment",))

        assert len(fixture.missing_attachment_refs) == 1

        with open_export_zip(zip_path) as export:
            messages = list(parse_ndjson(export.ndjson_path))
            assert len(messages) == 1

            basename = Path(messages[0].parts[1].data_path).name
            assert export.data_dir is None or not (export.data_dir / basename).exists()


class TestFixtureMain:
    def test_list_scenarios(self, capsys):
        exit_code = main(["--list-scenarios"])

        out = capsys.readouterr().out
        assert exit_code == 0
        assert "sms-basic" in out
        assert "mms-image-caption" in out
        assert "(default)" in out
        assert "[negative-control]" in out

    def test_force_overwrite(self, tmp_dir):
        zip_path = tmp_dir / "android_fixture.zip"
        zip_path.write_bytes(b"old")

        exit_code = main([str(zip_path), "--scenario", "sms-basic", "--force"])

        assert exit_code == 0
        with open_export_zip(zip_path) as export:
            counts = count_messages(export.ndjson_path)
            assert counts["total"] == 2


class TestFixturePipeline:
    def test_default_fixture_pipeline_passes_without_warnings(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = tmp_dir / "android_fixture.zip"
        write_fixture_zip(zip_path)

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.verification.passed
        assert result.verification.warnings == []
        assert result.total_messages_parsed == 4
        assert result.total_attachments_copied == 2

    def test_all_fixture_pipeline_passes_without_missing_attachment_warning(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = tmp_dir / "android_fixture_all.zip"
        write_fixture_zip(zip_path, _all_scenarios())

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.verification.passed
        assert result.verification.warnings == []
        assert result.total_messages_parsed == 6
        assert result.total_attachments_copied == 5

    def test_negative_control_fixture_surfaces_missing_attachment_warning(self, tmp_dir):
        backup_dir = _create_full_backup(tmp_dir)
        zip_path = tmp_dir / "android_fixture_negative.zip"
        write_fixture_zip(zip_path, ("mms-missing-attachment",))

        result = run_pipeline(
            export_path=zip_path,
            backup_path_or_udid=str(backup_dir),
        )

        assert result.verification.passed
        assert any("attachment files not found" in warning for warning in result.verification.warnings)
        assert result.total_messages_parsed == 1
        assert result.total_attachments_copied == 0

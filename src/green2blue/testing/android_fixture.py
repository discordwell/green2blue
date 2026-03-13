"""Generate synthetic SMS Import/Export ZIP fixtures for wet testing.

The generated archives match the contract green2blue already supports:

- `messages.ndjson` at the ZIP root
- MMS attachment references in the real SMS Import/Export style
- attachment files stored under `data/` by basename

This makes it possible to exercise the full Android-import pipeline without a
physical Android handset.
"""

from __future__ import annotations

import argparse
import json
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from pathlib import Path

ANDROID_ATTACHMENT_ROOT = "/data/user/0/com.android.providers.telephony/app_parts"
DEFAULT_SCENARIOS = ("sms-basic", "mms-image-caption", "mms-group-image")

_JPEG_ASSETS = {
    b"fixture_caption_photo": "fixture_caption.jpg",
    b"fixture_receipt": "fixture_receipt.jpg",
    b"fixture_rcs_photo": "fixture_rcs.jpg",
    b"fixture_attachment_only": "fixture_attachment_only.jpg",
}
_PNG_ASSETS = {
    b"fixture_group_image": "fixture_group.png",
}
_MP4_ASSETS = {
    b"fixture_clip": "fixture_clip.mp4",
}


@cache
def _asset_bytes(name: str) -> bytes:
    return files("green2blue.testing").joinpath("assets").joinpath(name).read_bytes()


@dataclass(frozen=True)
class FixtureScenario:
    """A named fixture slice that contributes messages and optional attachments."""

    name: str
    description: str
    records: tuple[dict, ...]
    attachments: tuple[tuple[str, bytes], ...] = ()
    missing_attachment_refs: tuple[str, ...] = ()
    is_negative_control: bool = False


@dataclass(frozen=True)
class GeneratedFixture:
    """Metadata describing a generated export ZIP."""

    output_path: Path
    scenario_names: tuple[str, ...]
    message_count: int
    attachment_files: tuple[str, ...]
    missing_attachment_refs: tuple[str, ...]


def _attachment_basename(name: str) -> str:
    return name.replace(" ", "_")


def _android_attachment_path(name: str) -> str:
    return f"{ANDROID_ATTACHMENT_ROOT}/{_attachment_basename(name)}"


def _zip_attachment_path(name: str) -> str:
    return f"data/{_attachment_basename(name)}"


def _jpeg_bytes(label: bytes) -> bytes:
    return _asset_bytes(_JPEG_ASSETS[label])


def _png_bytes(label: bytes) -> bytes:
    return _asset_bytes(_PNG_ASSETS[label])


def _mp4_bytes(label: bytes) -> bytes:
    return _asset_bytes(_MP4_ASSETS[label])


def _sms(
    *,
    address: str,
    body: str,
    date_ms: int,
    msg_type: int,
    read: int = 1,
    date_sent_ms: int | None = None,
) -> dict:
    record = {
        "address": address,
        "body": body,
        "date": str(date_ms),
        "type": str(msg_type),
        "read": str(read),
    }
    if date_sent_ms is not None:
        record["date_sent"] = str(date_sent_ms)
    return record


def _mms(
    *,
    sender: str,
    recipients: Sequence[str],
    date_s: int,
    msg_box: int,
    parts: Sequence[dict],
    read: int = 1,
    sub: str | None = None,
    date_sent_s: int | None = None,
    display_name: str | None = None,
    extra: dict | None = None,
) -> dict:
    record = {
        "date": str(date_s),
        "msg_box": str(msg_box),
        "read": str(read),
        "sub": sub,
        "ct_t": "application/vnd.wap.multipart.related",
        "__parts": list(parts),
        "__sender_address": {
            "address": sender,
            "type": "137",
            "charset": "106",
        },
        "__recipient_addresses": [
            {"address": recipient, "type": "151", "charset": "106"} for recipient in recipients
        ],
    }
    if date_sent_s is not None:
        record["date_sent"] = str(date_sent_s)
    if display_name:
        record["__display_name"] = display_name
    if extra:
        record.update(extra)
    return record


def _text_part(seq: int, text: str) -> dict:
    return {"seq": str(seq), "ct": "text/plain", "text": text}


def _binary_part(seq: int, content_type: str, basename: str, filename: str) -> dict:
    return {
        "seq": str(seq),
        "ct": content_type,
        "_data": _android_attachment_path(basename),
        "cl": filename,
    }


SCENARIOS: dict[str, FixtureScenario] = {
    "sms-basic": FixtureScenario(
        name="sms-basic",
        description="One inbound SMS and one outbound SMS.",
        records=(
            _sms(
                address="+12025550101",
                body="Android fixture inbound SMS",
                date_ms=1701000000000,
                msg_type=1,
                date_sent_ms=1701000000000,
            ),
            _sms(
                address="+12025550102",
                body="Android fixture outbound SMS",
                date_ms=1701000001000,
                msg_type=2,
                date_sent_ms=1701000001000,
            ),
        ),
    ),
    "mms-image-caption": FixtureScenario(
        name="mms-image-caption",
        description="One MMS with a text caption and one JPEG attachment.",
        records=(
            _mms(
                sender="+12025550111",
                recipients=("+12025550999",),
                date_s=1701000002,
                date_sent_s=1701000001,
                msg_box=1,
                display_name="Fixture Sender",
                parts=(
                    _text_part(0, "Captioned Android photo"),
                    _binary_part(
                        1,
                        "image/jpeg",
                        "PART_1701000002_captioned_photo.jpg",
                        "captioned_photo.jpg",
                    ),
                ),
            ),
        ),
        attachments=(
            (
                _zip_attachment_path("PART_1701000002_captioned_photo.jpg"),
                _jpeg_bytes(b"fixture_caption_photo"),
            ),
        ),
    ),
    "mms-group-image": FixtureScenario(
        name="mms-group-image",
        description="One group MMS with a subject and one PNG attachment.",
        records=(
            _mms(
                sender="+12025550121",
                recipients=("+12025550122", "+12025550123"),
                date_s=1701000003,
                msg_box=1,
                sub="Weekend photos",
                parts=(
                    _text_part(0, "Group image from Android"),
                    _binary_part(
                        1,
                        "image/png",
                        "PART_1701000003_group_image.png",
                        "group_image.png",
                    ),
                ),
            ),
        ),
        attachments=(
            (
                _zip_attachment_path("PART_1701000003_group_image.png"),
                _png_bytes(b"fixture_group_image"),
            ),
        ),
    ),
    "mms-multi-attachment": FixtureScenario(
        name="mms-multi-attachment",
        description="One sent MMS with both JPEG and MP4 attachments.",
        records=(
            _mms(
                sender="+12025550999",
                recipients=("+12025550131",),
                date_s=1701000004,
                date_sent_s=1701000004,
                msg_box=2,
                parts=(
                    _text_part(0, "Two attachments from Android"),
                    _binary_part(
                        1,
                        "image/jpeg",
                        "PART_1701000004_receipt.jpg",
                        "receipt.jpg",
                    ),
                    _binary_part(
                        2,
                        "video/mp4",
                        "PART_1701000004_clip.mp4",
                        "clip.mp4",
                    ),
                ),
            ),
        ),
        attachments=(
            (
                _zip_attachment_path("PART_1701000004_receipt.jpg"),
                _jpeg_bytes(b"fixture_receipt"),
            ),
            (
                _zip_attachment_path("PART_1701000004_clip.mp4"),
                _mp4_bytes(b"fixture_clip"),
            ),
        ),
    ),
    "rcs-image": FixtureScenario(
        name="rcs-image",
        description="One attachment-only RCS MMS from Google Messages.",
        records=(
            _mms(
                sender="+12025550141",
                recipients=("+12025550999",),
                date_s=1701000005,
                msg_box=1,
                parts=(
                    _binary_part(
                        0,
                        "image/jpeg",
                        "PART_1701000005_rcs_photo.jpg",
                        "rcs_photo.jpg",
                    ),
                ),
                extra={
                    "creator": "com.google.android.apps.messaging",
                    "rcs_message_type": "1",
                },
            ),
        ),
        attachments=(
            (
                _zip_attachment_path("PART_1701000005_rcs_photo.jpg"),
                _jpeg_bytes(b"fixture_rcs_photo"),
            ),
        ),
    ),
    "mms-missing-attachment": FixtureScenario(
        name="mms-missing-attachment",
        description="One MMS whose JPEG part is intentionally absent from the ZIP.",
        records=(
            _mms(
                sender="+12025550151",
                recipients=("+12025550999",),
                date_s=1701000006,
                msg_box=1,
                parts=(
                    _text_part(0, "This message references a missing attachment"),
                    _binary_part(
                        1,
                        "image/jpeg",
                        "PART_1701000006_missing.jpg",
                        "missing.jpg",
                    ),
                ),
            ),
        ),
        missing_attachment_refs=(_android_attachment_path("PART_1701000006_missing.jpg"),),
        is_negative_control=True,
    ),
}


def _all_scenarios(*, include_negative_controls: bool = False) -> tuple[str, ...]:
    return tuple(
        name
        for name, scenario in SCENARIOS.items()
        if include_negative_controls or not scenario.is_negative_control
    )


def build_fixture(
    scenario_names: Sequence[str] | None = None,
) -> tuple[list[dict], dict[str, bytes], tuple[str, ...], tuple[str, ...]]:
    """Build records and attachments for a set of named scenarios."""
    names = tuple(dict.fromkeys(scenario_names)) if scenario_names else DEFAULT_SCENARIOS

    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        choices = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"Unknown scenario(s): {', '.join(unknown)}. Choices: {choices}")

    records: list[dict] = []
    attachments: dict[str, bytes] = {}
    missing_refs: list[str] = []

    for name in names:
        scenario = SCENARIOS[name]
        records.extend(scenario.records)
        for rel_path, data in scenario.attachments:
            existing = attachments.get(rel_path)
            if existing is not None and existing != data:
                raise ValueError(f"Conflicting attachment content for {rel_path}")
            attachments[rel_path] = data
        missing_refs.extend(scenario.missing_attachment_refs)

    return records, attachments, names, tuple(missing_refs)


def write_fixture_zip(
    output_path: Path | str,
    scenario_names: Sequence[str] | None = None,
) -> GeneratedFixture:
    """Write a synthetic Android export ZIP to disk."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    records, attachments, names, missing_refs = build_fixture(scenario_names)
    ndjson = "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"

    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("messages.ndjson", ndjson)
        for rel_path, data in sorted(attachments.items()):
            zf.writestr(rel_path, data)

    return GeneratedFixture(
        output_path=output,
        scenario_names=names,
        message_count=len(records),
        attachment_files=tuple(sorted(attachments)),
        missing_attachment_refs=missing_refs,
    )


def _resolve_scenarios(args: argparse.Namespace) -> tuple[str, ...]:
    if args.all:
        return _all_scenarios(include_negative_controls=args.include_negative_controls)
    if args.scenario:
        return tuple(args.scenario)
    return DEFAULT_SCENARIOS


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic SMS Import/Export ZIP fixture.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        help="Output ZIP path.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=tuple(SCENARIOS),
        help="Scenario to include. Repeat to compose a larger fixture.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Include every non-negative scenario.",
    )
    parser.add_argument(
        "--include-negative-controls",
        action="store_true",
        help="Include negative-control scenarios when used with --all.",
    )
    parser.add_argument(
        "--list-scenarios",
        action="store_true",
        help="List the available scenarios and exit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output ZIP if it already exists.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for fixture generation."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list_scenarios:
        print("Available scenarios:")
        for name, scenario in SCENARIOS.items():
            default_marker = " (default)" if name in DEFAULT_SCENARIOS else ""
            negative_marker = " [negative-control]" if scenario.is_negative_control else ""
            print(f"  {name}{default_marker}{negative_marker}: {scenario.description}")
        return 0

    if not args.output:
        parser.error("the following arguments are required: output")

    output = Path(args.output)
    if output.exists() and not args.force:
        parser.error(f"output already exists: {output} (use --force to overwrite)")

    fixture = write_fixture_zip(output, _resolve_scenarios(args))
    print(f"Wrote {fixture.output_path}")
    print(f"Scenarios: {', '.join(fixture.scenario_names)}")
    print(f"Messages: {fixture.message_count}")
    print(f"Attachment files: {len(fixture.attachment_files)}")
    print(f"Missing attachment refs: {len(fixture.missing_attachment_refs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

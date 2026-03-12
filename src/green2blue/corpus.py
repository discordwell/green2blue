"""Privacy-safe representative corpus capture for Android exports."""

from __future__ import annotations

import hashlib
import json
import re
import zipfile
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from green2blue.models import AndroidMMS, AndroidSMS, MMSAddress, MMSPart
from green2blue.parser.ndjson_parser import parse_ndjson
from green2blue.parser.zip_reader import ExtractedExport, open_export_zip

_URL_RE = re.compile(r"https?://\S+")
_DEFAULT_START_MS = 1_800_000_000_000


@dataclass(frozen=True)
class CorpusCaptureResult:
    output_zip: Path
    selected_messages: int
    buckets_covered: tuple[str, ...]
    attachments_written: int


def capture_android_corpus(
    export_zip: Path | str,
    output_zip: Path | str,
    *,
    max_per_bucket: int = 1,
    preserve_text: bool = False,
    preserve_media: bool = False,
) -> CorpusCaptureResult:
    output_zip = Path(output_zip)
    output_zip.parent.mkdir(parents=True, exist_ok=True)

    with open_export_zip(export_zip) as export:
        messages = list(parse_ndjson(export.ndjson_path))
        selected = _select_messages(messages, max_per_bucket=max_per_bucket)
        address_map = _AddressMapper()
        redacted_records, attachments = _redact_selected(
            export,
            selected,
            address_map=address_map,
            preserve_text=preserve_text,
            preserve_media=preserve_media,
        )

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        ndjson = "\n".join(json.dumps(record, ensure_ascii=False) for record in redacted_records) + "\n"
        zf.writestr("messages.ndjson", ndjson)
        for zip_name, payload in attachments:
            zf.writestr(zip_name, payload)

    buckets = sorted({bucket for _, _, buckets in selected for bucket in buckets})
    return CorpusCaptureResult(
        output_zip=output_zip,
        selected_messages=len(redacted_records),
        buckets_covered=tuple(buckets),
        attachments_written=len(attachments),
    )


def _select_messages(
    messages: list[AndroidSMS | AndroidMMS],
    *,
    max_per_bucket: int,
) -> list[tuple[int, AndroidSMS | AndroidMMS, tuple[str, ...]]]:
    bucket_counts: dict[str, int] = {}
    selected: list[tuple[int, AndroidSMS | AndroidMMS, tuple[str, ...]]] = []
    seen_indices: set[int] = set()

    for idx, msg in enumerate(messages):
        buckets = _classify_message(msg)
        if not buckets:
            continue
        chosen = tuple(
            bucket for bucket in buckets
            if bucket_counts.get(bucket, 0) < max_per_bucket
        )
        if not chosen:
            continue
        if idx not in seen_indices:
            selected.append((idx, msg, chosen))
            seen_indices.add(idx)
        for bucket in chosen:
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    selected.sort(key=lambda item: item[0])
    return selected


def _classify_message(msg: AndroidSMS | AndroidMMS) -> tuple[str, ...]:
    buckets: list[str] = []
    if isinstance(msg, AndroidSMS):
        buckets.append("sms_outgoing" if msg.type == 2 else "sms_incoming")
        if msg.read == 0:
            buckets.append("sms_unread")
        if _URL_RE.search(msg.body):
            buckets.append("sms_url")
        if "\n" in msg.body or len(msg.body) > 120:
            buckets.append("sms_long")
        return tuple(dict.fromkeys(buckets))

    binary_parts = [part for part in msg.parts if part.data_path]
    if not binary_parts:
        buckets.append("mms_text_only")
    else:
        buckets.append("mms_media")
    if any(part.content_type.startswith("video/") for part in binary_parts):
        buckets.append("mms_video")
    if len(binary_parts) > 1:
        buckets.append("mms_multi_attachment")
    if binary_parts and any(part.text for part in msg.parts):
        buckets.append("mms_captioned_media")
    if binary_parts and not any(part.text for part in msg.parts):
        buckets.append("mms_attachment_only")
    unique_addresses = {address.address for address in msg.addresses}
    if len(unique_addresses) > 2:
        buckets.append("mms_group")
    return tuple(dict.fromkeys(buckets))


def _redact_selected(
    export: ExtractedExport,
    selected: list[tuple[int, AndroidSMS | AndroidMMS, tuple[str, ...]]],
    *,
    address_map: _AddressMapper,
    preserve_text: bool,
    preserve_media: bool,
) -> tuple[list[dict], list[tuple[str, bytes]]]:
    records: list[dict] = []
    attachments: list[tuple[str, bytes]] = []
    current_ms = _DEFAULT_START_MS

    for ordinal, (_, msg, buckets) in enumerate(selected, start=1):
        if isinstance(msg, AndroidSMS):
            records.append(
                _redact_sms(
                    msg,
                    buckets=buckets,
                    ordinal=ordinal,
                    address_map=address_map,
                    preserve_text=preserve_text,
                    timestamp_ms=current_ms,
                )
            )
            current_ms += 60_000
            continue

        record, extra_attachments = _redact_mms(
            export,
            msg,
            buckets=buckets,
            ordinal=ordinal,
            address_map=address_map,
            preserve_text=preserve_text,
            preserve_media=preserve_media,
            timestamp_ms=current_ms,
        )
        records.append(record)
        attachments.extend(extra_attachments)
        current_ms += 60_000

    return records, attachments


def _redact_sms(
    msg: AndroidSMS,
    *,
    buckets: tuple[str, ...],
    ordinal: int,
    address_map: _AddressMapper,
    preserve_text: bool,
    timestamp_ms: int,
) -> dict:
    body = msg.body if preserve_text else _redact_text(msg.body, buckets, ordinal)
    return {
        "address": address_map.map(msg.address),
        "body": body,
        "date": str(timestamp_ms),
        "date_sent": str(timestamp_ms),
        "type": str(msg.type),
        "read": str(msg.read),
        "thread_id": ordinal,
    }


def _redact_mms(
    export: ExtractedExport,
    msg: AndroidMMS,
    *,
    buckets: tuple[str, ...],
    ordinal: int,
    address_map: _AddressMapper,
    preserve_text: bool,
    preserve_media: bool,
    timestamp_ms: int,
) -> tuple[dict, list[tuple[str, bytes]]]:
    attachments: list[tuple[str, bytes]] = []
    parts: list[dict] = []
    for seq, part in enumerate(msg.parts):
        if part.text is not None:
            text = part.text if preserve_text else _redact_text(part.text, buckets, ordinal)
            parts.append({"seq": str(seq), "ct": "text/plain", "text": text})
            continue

        content_type, filename, payload = _redact_media_part(
            export,
            part,
            preserve_media=preserve_media,
            ordinal=ordinal,
            seq=seq,
        )
        zip_basename = filename.replace(" ", "_")
        parts.append({
            "seq": str(seq),
            "ct": content_type,
            "_data": f"/data/user/0/com.android.providers.telephony/app_parts/{zip_basename}",
            "cl": filename,
        })
        attachments.append((f"data/{zip_basename}", payload))

    sender = _first_address_of_type(msg.addresses, 137)
    recipients = [addr for addr in msg.addresses if addr.type == 151]
    return {
        "date": str(timestamp_ms // 1000),
        "date_sent": str(timestamp_ms // 1000),
        "msg_box": str(msg.msg_box),
        "read": str(msg.read),
        "sub": msg.sub if preserve_text else None,
        "ct_t": msg.ct_t or "application/vnd.wap.multipart.related",
        "__parts": parts,
        "__sender_address": {
            "address": address_map.map(sender.address if sender else "+12025550000"),
            "type": "137",
            "charset": "106",
        },
        "__recipient_addresses": [
            {
                "address": address_map.map(addr.address),
                "type": "151",
                "charset": "106",
            }
            for addr in recipients
        ],
        "thread_id": ordinal,
    }, attachments


def _redact_text(text: str, buckets: tuple[str, ...], ordinal: int) -> str:
    urls = [f"https://example.com/sample-{ordinal}-{idx}" for idx, _ in enumerate(_URL_RE.findall(text), start=1)]
    label = " / ".join(bucket.upper() for bucket in buckets[:2]) if buckets else "SAMPLE"
    base = f"CLAUDEUS CORPUS {ordinal}: {label}"
    if not urls:
        if "\n" in text:
            line_count = text.count("\n") + 1
            return "\n".join([base] + [f"sample line {i}" for i in range(2, line_count + 1)])
        return base
    return " ".join([base, *urls])


def _redact_media_part(
    export: ExtractedExport,
    part: MMSPart,
    *,
    preserve_media: bool,
    ordinal: int,
    seq: int,
) -> tuple[str, str, bytes]:
    if preserve_media:
        resolved = _resolve_attachment_path(export, part)
        if resolved is not None:
            filename = part.filename or resolved.name
            return part.content_type, filename, resolved.read_bytes()

    content_type, filename, payload = _generic_media_for_content_type(part.content_type, ordinal, seq)
    return content_type, filename, payload


def _generic_media_for_content_type(content_type: str, ordinal: int, seq: int) -> tuple[str, str, bytes]:
    assets = files("green2blue.testing").joinpath("assets")
    if content_type == "image/png":
        return "image/png", f"redacted_{ordinal}_{seq}.png", assets.joinpath("fixture_group.png").read_bytes()
    if content_type.startswith("video/"):
        return "video/mp4", f"redacted_{ordinal}_{seq}.mp4", assets.joinpath("fixture_clip.mp4").read_bytes()
    return "image/jpeg", f"redacted_{ordinal}_{seq}.jpg", assets.joinpath("fixture_caption.jpg").read_bytes()


def _resolve_attachment_path(export: ExtractedExport, part: MMSPart) -> Path | None:
    if export.data_dir is None or not part.data_path:
        return None
    raw = Path(part.data_path)
    candidates = [
        export.temp_dir / raw,
        export.data_dir / raw.name,
    ]
    if raw.parts and raw.parts[0] == "data":
        candidates.append(export.temp_dir / raw)
        candidates.append(export.data_dir / Path(*raw.parts[1:]))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _first_address_of_type(addresses: tuple[MMSAddress, ...], kind: int) -> MMSAddress | None:
    for address in addresses:
        if address.type == kind:
            return address
    return None


class _AddressMapper:
    def __init__(self):
        self._values: dict[str, str] = {}
        self._email_counter = 0

    def map(self, address: str) -> str:
        if address in self._values:
            return self._values[address]
        if "@" in address:
            self._email_counter += 1
            mapped = f"user{self._email_counter}@example.test"
        else:
            digest = hashlib.sha256(address.encode("utf-8")).hexdigest()
            suffix = int(digest[:6], 16) % 10_000
            mapped = f"+1202555{suffix:04d}"
        self._values[address] = mapped
        return mapped

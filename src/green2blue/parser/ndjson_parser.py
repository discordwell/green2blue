"""Parse SMS Import/Export NDJSON files into Android message models.

The NDJSON format has one JSON object per line. SMS records have `body` and
`address` fields. MMS records have `__parts` and `__sender_address` /
`__recipient_addresses` fields (or the legacy `__addresses` format).
RCS messages appear as either SMS or MMS records with no special type marker.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from pathlib import Path

from green2blue.exceptions import ParseError
from green2blue.models import AndroidMMS, AndroidSMS, MMSAddress, MMSPart

logger = logging.getLogger(__name__)


def parse_ndjson(
    path: Path | str,
) -> Generator[AndroidSMS | AndroidMMS, None, None]:
    """Parse an NDJSON file, yielding Android message models.

    Malformed lines are logged as warnings and skipped.

    Args:
        path: Path to the messages.ndjson file.

    Yields:
        AndroidSMS or AndroidMMS for each valid message line.
    """
    path = Path(path)
    if not path.exists():
        raise ParseError(f"NDJSON file not found: {path}")

    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning("Line %d: invalid JSON, skipping: %s", line_num, e)
                continue

            if not isinstance(record, dict):
                rtype = type(record).__name__
                logger.warning("Line %d: expected object, got %s, skipping", line_num, rtype)
                continue

            try:
                msg = _parse_record(record, line_num)
                if msg is not None:
                    yield msg
            except (KeyError, ValueError, TypeError) as e:
                logger.warning("Line %d: failed to parse record, skipping: %s", line_num, e)
                continue


def _is_mms_record(record: dict) -> bool:
    """Check if a record is an MMS message.

    SMS IE uses __parts, __sender_address, __recipient_addresses for MMS.
    We also accept the legacy __addresses format for compatibility.
    """
    return any(
        key in record
        for key in ("__parts", "__addresses", "__sender_address", "__recipient_addresses")
    )


def _parse_record(record: dict, line_num: int) -> AndroidSMS | AndroidMMS | None:
    """Classify and parse a single NDJSON record."""
    if _is_mms_record(record):
        return _parse_mms(record, line_num)

    # SMS detection: has body and address
    if "body" in record and "address" in record:
        return _parse_sms(record, line_num)

    logger.warning(
        "Line %d: record has neither SMS fields (body/address) "
        "nor MMS fields (__parts/__sender_address), skipping",
        line_num,
    )
    return None


def _parse_sms(record: dict, line_num: int) -> AndroidSMS:
    """Parse an SMS record."""
    address = str(record["address"])
    body = str(record.get("body", ""))
    date = int(record["date"])
    msg_type = int(record.get("type", 1))
    read = int(record.get("read", 1))

    date_sent = None
    if record.get("date_sent") is not None:
        date_sent = int(record["date_sent"])

    sub_id = None
    if record.get("sub_id") is not None:
        sub_id = int(record["sub_id"])

    thread_id = None
    if record.get("thread_id") is not None:
        thread_id = int(record["thread_id"])

    return AndroidSMS(
        address=address,
        body=body,
        date=date,
        type=msg_type,
        read=read,
        date_sent=date_sent,
        sub_id=sub_id,
        thread_id=thread_id,
    )


def _parse_mms(record: dict, line_num: int) -> AndroidMMS:
    """Parse an MMS record.

    Handles both the real SMS IE format (__sender_address + __recipient_addresses)
    and the legacy format (__addresses array).
    """
    date = int(record.get("date", 0))
    msg_box = int(record.get("msg_box", 1))
    read = int(record.get("read", 1))
    sub = record.get("sub")
    ct_t = record.get("ct_t")
    thread_id = None
    if record.get("thread_id") is not None:
        thread_id = int(record["thread_id"])

    date_sent = None
    if record.get("date_sent") is not None:
        date_sent = int(record["date_sent"])

    # Parse parts
    parts = []
    for raw_part in record.get("__parts", []):
        content_type = raw_part.get("ct", "application/octet-stream")
        text = raw_part.get("text")
        data_path = raw_part.get("_data")
        # Filename can be in cl (content location), fn (filename), or name
        filename = raw_part.get("cl") or raw_part.get("fn") or raw_part.get("name")
        charset = raw_part.get("chset")
        parts.append(MMSPart(
            content_type=content_type,
            text=text,
            data_path=data_path,
            filename=filename,
            charset=charset,
        ))

    # Parse addresses — support both real SMS IE format and legacy format
    addresses = _parse_mms_addresses(record)

    return AndroidMMS(
        date=date,
        msg_box=msg_box,
        addresses=tuple(addresses),
        parts=tuple(parts),
        read=read,
        sub=sub if sub else None,
        thread_id=thread_id,
        ct_t=ct_t,
        date_sent=date_sent,
    )


def _parse_mms_addresses(record: dict) -> list[MMSAddress]:
    """Parse MMS addresses from either real SMS IE or legacy format.

    Real SMS IE format:
        __sender_address: {address, type, charset, ...}
        __recipient_addresses: [{address, type, charset, ...}, ...]

    Legacy/test format:
        __addresses: [{address, type, charset}, ...]
    """
    addresses = []

    # Real SMS IE format: __sender_address (object) + __recipient_addresses (array)
    sender = record.get("__sender_address")
    recipients = record.get("__recipient_addresses")

    if sender or recipients:
        if sender and isinstance(sender, dict):
            addr = str(sender.get("address", ""))
            if addr:
                addresses.append(MMSAddress(
                    address=addr,
                    type=int(sender.get("type", 137)),
                    charset=int(sender.get("charset", 106)),
                ))

        if recipients and isinstance(recipients, list):
            for raw_addr in recipients:
                if not isinstance(raw_addr, dict):
                    continue
                addr = str(raw_addr.get("address", ""))
                if addr:
                    addresses.append(MMSAddress(
                        address=addr,
                        type=int(raw_addr.get("type", 151)),
                        charset=int(raw_addr.get("charset", 106)),
                    ))
        return addresses

    # Legacy format: __addresses (array of objects)
    for raw_addr in record.get("__addresses", []):
        addr = str(raw_addr.get("address", ""))
        addr_type = int(raw_addr.get("type", 151))
        charset = int(raw_addr.get("charset", 106))
        if addr:
            addresses.append(MMSAddress(
                address=addr,
                type=addr_type,
                charset=charset,
            ))

    return addresses


def count_messages(path: Path | str) -> dict[str, int]:
    """Count messages by type without fully parsing them.

    Returns:
        Dict with keys 'sms', 'mms', 'rcs', 'unknown', 'errors', 'total'.
        RCS count is best-effort (checks for rcs_* fields or Google Messages creator).
    """
    counts = {"sms": 0, "mms": 0, "rcs": 0, "unknown": 0, "errors": 0, "total": 0}
    path = Path(path)

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            counts["total"] += 1
            try:
                record = json.loads(line)
                is_rcs = _looks_like_rcs(record)

                if _is_mms_record(record):
                    counts["mms"] += 1
                elif "body" in record and "address" in record:
                    counts["sms"] += 1
                else:
                    counts["unknown"] += 1

                if is_rcs:
                    counts["rcs"] += 1
            except (json.JSONDecodeError, TypeError):
                counts["errors"] += 1

    return counts


def _looks_like_rcs(record: dict) -> bool:
    """Heuristic check if a record looks like an RCS message.

    RCS messages in SMS IE have no explicit type marker. We detect them via:
    - rcs_message_type field (vendor extension on some devices)
    - creator=com.google.android.apps.messaging + certain MMS patterns
    """
    return any(k.startswith("rcs_") for k in record)

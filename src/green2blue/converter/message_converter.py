"""Convert Android message models to iOS message models.

Groups messages into conversations by normalized phone number, generates
UUIDs for message GUIDs, and maps Android type codes to iOS boolean flags.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from dataclasses import replace

from green2blue.converter.phone import normalize_phone
from green2blue.converter.timestamp import unix_ms_to_ios_ns, unix_s_to_ios_ns
from green2blue.exceptions import PhoneNormalizationError
from green2blue.models import (
    AndroidMMS,
    AndroidSMS,
    CKStrategy,
    ConversionResult,
    compute_chat_guid,
    generate_ck_record_id,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
    message_content_hash,
)

logger = logging.getLogger(__name__)

# MIME type to UTI mapping for common attachment types
MIME_TO_UTI: dict[str, str] = {
    "image/jpeg": "public.jpeg",
    "image/jpg": "public.jpeg",
    "image/png": "public.png",
    "image/gif": "com.compuserve.gif",
    "image/webp": "public.webp",
    "image/heic": "public.heic",
    "image/heif": "public.heif",
    "image/bmp": "com.microsoft.bmp",
    "image/tiff": "public.tiff",
    "video/mp4": "public.mpeg-4",
    "video/3gpp": "public.3gpp",
    "video/3gpp2": "public.3gpp2",
    "video/quicktime": "com.apple.quicktime-movie",
    "video/webm": "org.webmproject.webm",
    "audio/mpeg": "public.mp3",
    "audio/mp3": "public.mp3",
    "audio/ogg": "org.xiph.ogg",
    "audio/amr": "org.3gpp.adaptive-multi-rate-audio",
    "audio/aac": "public.aac-audio",
    "audio/mp4": "public.mpeg-4-audio",
    "text/plain": "public.plain-text",
    "text/vcard": "public.vcard",
    "text/x-vcard": "public.vcard",
    "application/pdf": "com.adobe.pdf",
    "application/smil": "public.xml",
    "application/octet-stream": "public.data",
}


def convert_messages(
    messages: list[AndroidSMS | AndroidMMS],
    country: str = "US",
    skip_duplicates: bool = True,
    ck_strategy: CKStrategy = CKStrategy.NONE,
) -> ConversionResult:
    """Convert a list of Android messages to iOS format.

    Args:
        messages: Parsed Android messages.
        country: Default country for phone normalization.
        skip_duplicates: If True, skip duplicate messages based on content hash.
        ck_strategy: CloudKit metadata strategy for iCloud Messages survival.

    Returns:
        ConversionResult with iOS messages, handles, and chats.
    """
    result = ConversionResult()
    seen_hashes: set[str] = set()

    # Track handles and chats by identifier for dedup
    handles_by_id: dict[str, iOSHandle] = {}
    chats_by_id: dict[str, iOSChat] = {}

    # Group messages by conversation for chat creation
    conversations: dict[str, list[iOSMessage]] = defaultdict(list)

    for msg in messages:
        try:
            if isinstance(msg, AndroidSMS):
                ios_msg = _convert_sms(msg, country)
            elif isinstance(msg, AndroidMMS):
                ios_msg = _convert_mms(msg, country)
            else:
                logger.warning("Unknown message type: %s", type(msg).__name__)
                result.skipped_count += 1
                continue
        except PhoneNormalizationError as e:
            logger.warning("Skipping message due to phone normalization error: %s", e)
            result.warnings.append(str(e))
            result.skipped_count += 1
            continue

        if ios_msg is None:
            result.skipped_count += 1
            continue

        # Apply CloudKit metadata strategy
        if ck_strategy != CKStrategy.NONE:
            ios_msg = _apply_ck_strategy(ios_msg, ck_strategy)

        # Duplicate detection
        if skip_duplicates:
            content_hash = message_content_hash(ios_msg)
            if content_hash in seen_hashes:
                result.skipped_count += 1
                continue
            seen_hashes.add(content_hash)

        result.messages.append(ios_msg)

        # Track the handle
        handle_id = ios_msg.handle_id
        if handle_id and handle_id not in handles_by_id:
            handles_by_id[handle_id] = iOSHandle(
                id=handle_id,
                country=country.lower(),
                service="SMS",
            )

        # Track group member handles too
        for member in ios_msg.group_members:
            if member not in handles_by_id:
                handles_by_id[member] = iOSHandle(
                    id=member,
                    country=country.lower(),
                    service="SMS",
                )

        # Determine conversation key
        conv_key = ios_msg.chat_identifier or handle_id
        conversations[conv_key].append(ios_msg)

    # Build chats from conversations
    for conv_key, conv_messages in conversations.items():
        if conv_key not in chats_by_id:
            sample = conv_messages[0]
            guid = compute_chat_guid(conv_key, sample.group_members)
            style = 43 if sample.group_members else 45
            chat = iOSChat(
                guid=guid,
                style=style,
                chat_identifier=conv_key,
                service_name="SMS",
            )
            # Apply CK strategy to chat
            if ck_strategy != CKStrategy.NONE:
                chat = _apply_ck_strategy_to_chat(chat, ck_strategy)
            chats_by_id[conv_key] = chat

    result.handles = list(handles_by_id.values())
    result.chats = list(chats_by_id.values())

    return result


def _apply_ck_strategy(msg: iOSMessage, strategy: CKStrategy) -> iOSMessage:
    """Apply CloudKit metadata strategy to a message."""
    if strategy in (CKStrategy.NONE, CKStrategy.ICLOUD_RESET):
        return msg
    record_id = generate_ck_record_id(msg.guid)
    if strategy == CKStrategy.FAKE_SYNCED:
        return replace(msg, ck_sync_state=1, ck_record_id=record_id,
                       ck_record_change_tag="1")
    elif strategy == CKStrategy.PENDING_UPLOAD:
        return replace(msg, ck_sync_state=0, ck_record_id=record_id,
                       ck_record_change_tag=None)
    return msg


def _apply_ck_strategy_to_chat(chat: iOSChat, strategy: CKStrategy) -> iOSChat:
    """Apply CloudKit metadata strategy to a chat."""
    if strategy in (CKStrategy.NONE, CKStrategy.ICLOUD_RESET):
        return chat
    record_id = generate_ck_record_id(chat.guid, salt="green2blue-ck-chat")
    if strategy == CKStrategy.FAKE_SYNCED:
        return replace(chat, ck_sync_state=1, cloudkit_record_id=record_id)
    elif strategy == CKStrategy.PENDING_UPLOAD:
        return replace(chat, ck_sync_state=0, cloudkit_record_id=record_id)
    return chat


def _convert_sms(sms: AndroidSMS, country: str) -> iOSMessage | None:
    """Convert a single Android SMS to an iOS message."""
    phone = normalize_phone(sms.address, country)
    is_from_me = sms.type == 2  # type 2 = sent
    is_sent = is_from_me

    date_ns = unix_ms_to_ios_ns(sms.date)
    date_read_ns = date_ns if sms.read else 0
    date_delivered_ns = date_ns if is_sent else 0

    if sms.date_sent and sms.date_sent > 0:
        sent_ns = unix_ms_to_ios_ns(sms.date_sent)
        if is_from_me:
            date_delivered_ns = sent_ns

    msg_guid = f"green2blue:{uuid.uuid4()}"

    return iOSMessage(
        guid=msg_guid,
        text=sms.body,
        handle_id=phone,
        date=date_ns,
        date_read=date_read_ns,
        date_delivered=date_delivered_ns,
        is_from_me=is_from_me,
        is_sent=is_sent,
        is_delivered=is_sent,
        is_read=bool(sms.read),
        service="SMS",
        chat_identifier=phone,
    )


def _convert_mms(mms: AndroidMMS, country: str) -> iOSMessage | None:
    """Convert a single Android MMS to an iOS message."""
    is_from_me = mms.msg_box == 2  # msg_box 2 = sent

    # Find the sender (type 137) and recipients (type 151)
    sender = None
    recipients = []
    all_phones = []

    for addr in mms.addresses:
        try:
            normalized = normalize_phone(addr.address, country)
        except PhoneNormalizationError:
            logger.warning("Cannot normalize MMS address %r, skipping", addr.address)
            continue

        if addr.type == 137:  # FROM
            sender = normalized
        elif addr.type == 151:  # TO
            recipients.append(normalized)

        all_phones.append(normalized)

    # Determine the handle (contact) for this message
    if is_from_me:
        # Sent MMS — handle is the recipient (or first recipient for group)
        handle_phone = recipients[0] if recipients else (sender or "")
    else:
        # Received MMS — handle is the sender
        handle_phone = sender or (recipients[0] if recipients else "")

    if not handle_phone:
        logger.warning("MMS has no usable phone numbers, skipping")
        return None

    # Determine if this is a group chat
    # Group = more than 2 unique participants
    unique_phones = sorted(set(all_phones))
    is_group = len(unique_phones) > 2
    group_members = tuple(unique_phones) if is_group else ()

    # Build chat identifier: comma-joined for group, single phone for 1:1
    chat_identifier = ",".join(unique_phones) if is_group else handle_phone

    # MMS dates are in seconds
    date_ns = unix_s_to_ios_ns(mms.date)
    date_read_ns = date_ns if mms.read else 0
    date_delivered_ns = date_ns if is_from_me else 0

    if mms.date_sent and mms.date_sent > 0:
        sent_ns = unix_s_to_ios_ns(mms.date_sent)
        if is_from_me:
            date_delivered_ns = sent_ns

    # Extract text body from parts
    text_parts = [p.text for p in mms.parts if p.content_type == "text/plain" and p.text]
    body = "\n".join(text_parts) if text_parts else None

    # Convert attachment parts
    attachments = []
    for part in mms.parts:
        if part.content_type == "text/plain":
            continue
        if part.content_type == "application/smil":
            continue  # Skip SMIL layout files
        if not part.data_path:
            continue

        att_uuid = str(uuid.uuid4())
        filename = part.filename or f"attachment_{att_uuid[:8]}"
        uti = MIME_TO_UTI.get(part.content_type, "public.data")

        # iOS attachment path will be set during injection
        ios_path = f"~/Library/SMS/Attachments/{att_uuid[:2]}/{att_uuid}/{filename}"

        attachments.append(iOSAttachment(
            guid=f"green2blue-att:{att_uuid}",
            filename=ios_path,
            mime_type=part.content_type,
            uti=uti,
            transfer_name=filename,
            total_bytes=0,  # Updated during pipeline when file is copied
            source_data_path=part.data_path,
            created_date=date_ns,
        ))

    msg_guid = f"green2blue:{uuid.uuid4()}"

    return iOSMessage(
        guid=msg_guid,
        text=body,
        handle_id=handle_phone,
        date=date_ns,
        date_read=date_read_ns,
        date_delivered=date_delivered_ns,
        is_from_me=is_from_me,
        is_sent=is_from_me,
        is_delivered=is_from_me,
        is_read=bool(mms.read),
        service="SMS",
        attachments=tuple(attachments),
        chat_identifier=chat_identifier,
        group_members=group_members,
        group_title=mms.sub or "",
    )



"""Data models for Android and iOS messages.

All models are frozen dataclasses for immutability. Android models represent
parsed NDJSON data. iOS models represent rows destined for sms.db.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum

# --- Android models (parsed from SMS Import/Export NDJSON) ---


@dataclass(frozen=True)
class MMSPart:
    """A single part of an MMS message (text body or binary attachment)."""

    content_type: str  # e.g., "text/plain", "image/jpeg"
    text: str | None = None  # For text/plain parts
    data_path: str | None = None  # Relative path within ZIP for binary parts
    filename: str | None = None  # Original filename if present
    charset: str | None = None


@dataclass(frozen=True)
class MMSAddress:
    """An address entry in an MMS message."""

    address: str  # Phone number or email
    type: int  # 137=FROM, 151=TO, 130=BCC, 129=CC
    charset: int = 106  # UTF-8 default


@dataclass(frozen=True)
class AndroidSMS:
    """A single SMS message parsed from NDJSON."""

    address: str  # Phone number
    body: str  # Message text
    date: int  # Unix timestamp in milliseconds
    type: int  # 1=received, 2=sent, 3=draft, 4=outbox, 5=failed, 6=queued
    read: int = 1  # 1=read, 0=unread
    date_sent: int | None = None  # Sent timestamp (ms), may differ from date
    sub_id: int | None = None  # Subscription ID for dual-SIM
    thread_id: int | None = None  # Android thread ID


@dataclass(frozen=True)
class AndroidMMS:
    """A single MMS message parsed from NDJSON."""

    date: int  # Unix timestamp in seconds (MMS uses seconds, not ms)
    msg_box: int  # 1=received, 2=sent
    addresses: tuple[MMSAddress, ...] = ()
    parts: tuple[MMSPart, ...] = ()
    read: int = 1
    sub: str | None = None  # Subject line
    thread_id: int | None = None
    ct_t: str | None = None  # Content-Type header
    date_sent: int | None = None  # Sent timestamp in seconds


# --- iOS models (destined for sms.db) ---


@dataclass(frozen=True)
class iOSHandle:
    """A contact handle in sms.db (handle table)."""

    id: str  # E.164 phone number, e.g., "+12025551234"
    country: str  # ISO 3166-1 alpha-2, e.g., "us"
    service: str  # "SMS" or "iMessage"
    uncanonicalized_id: str | None = None  # Original unsanitized number


class CKStrategy(Enum):
    """CloudKit sync metadata strategy for injected messages."""

    NONE = "none"  # No CK metadata (current default, ck_sync_state=0)
    FAKE_SYNCED = "fake-synced"  # Pretend already synced (state=1, fake record IDs)
    PENDING_UPLOAD = "pending-upload"  # Signal needs upload (state=0, with record IDs)
    ICLOUD_RESET = "icloud-reset"  # Clean state + prepare-sync for iCloud reset


def generate_ck_record_id(guid: str, salt: str = "green2blue-ck") -> str:
    """Generate a 64-character hex CloudKit record ID.

    Mimics the format of real ck_record_id values in sms.db.

    Args:
        guid: The message or chat GUID to derive from.
        salt: Salt for uniqueness.

    Returns:
        64-character lowercase hex string.
    """
    return hashlib.sha256(f"{guid}:{salt}".encode()).hexdigest()


@dataclass(frozen=True)
class iOSChat:
    """A conversation in sms.db (chat table)."""

    guid: str  # e.g., "any;-;+12025551234" or "any;-;chat<hash>"
    style: int  # 45=1:1, 43=group
    chat_identifier: str  # E.164 for 1:1, comma-separated for group
    service_name: str  # "SMS"
    display_name: str = ""  # User-visible group name, empty for 1:1
    account_id: str = ""  # SMS account UUID (detected from existing chats)
    account_login: str = "E:"  # SMS account login (constant on real iOS)
    ck_sync_state: int = 0  # CloudKit sync state (0=unsynced, 1=synced)
    cloudkit_record_id: str = ""  # CloudKit record ID (empty string on real iOS)


@dataclass(frozen=True)
class iOSAttachment:
    """An attachment in sms.db (attachment table)."""

    guid: str  # UUID for this attachment
    filename: str  # iOS-side path: ~/Library/SMS/Attachments/...
    mime_type: str  # e.g., "image/jpeg"
    uti: str  # Uniform Type Identifier, e.g., "public.jpeg"
    transfer_name: str  # Display filename
    total_bytes: int  # File size in bytes
    created_date: int  # Apple epoch seconds (NOT nanoseconds like message.date)
    source_data_path: str | None = None  # Original path in export ZIP


@dataclass(frozen=True)
class iOSMessage:
    """A message row in sms.db (message table)."""

    guid: str  # UUID, e.g., "green2blue:<uuid>"
    text: str | None  # Message body (None for attachment-only MMS)
    handle_id: str  # E.164 phone of the sender/recipient handle
    date: int  # CoreData timestamp in nanoseconds
    date_read: int  # CoreData timestamp in nanoseconds, 0 if unread
    date_delivered: int  # CoreData timestamp in nanoseconds
    is_from_me: bool  # True if sent by the iPhone owner
    service: str  # "SMS"
    account: str | None = None  # SMS account (NULL on real iOS for SMS)
    account_guid: str | None = None  # SMS account GUID (NULL on real iOS for SMS)
    is_read: bool = True
    is_sent: bool = False
    is_delivered: bool = True  # Real iOS = 1 for both incoming and outgoing
    is_finished: bool = True
    was_downgraded: bool = False
    group_title: str | None = None  # None for 1:1, group name for group chats
    attachments: tuple[iOSAttachment, ...] = ()
    chat_identifier: str = ""  # Set during conversion for grouping
    group_members: tuple[str, ...] = ()  # All E.164 numbers for group chats
    ck_sync_state: int = 0  # CloudKit sync state
    ck_record_id: str = ""  # CloudKit record ID (empty string when unsynced)
    ck_record_change_tag: str = ""  # CloudKit change tag (empty string when unsynced)


def compute_chat_guid(
    chat_identifier: str,
    group_members: tuple[str, ...] = (),
) -> str:
    """Compute the iOS chat GUID for a conversation.

    1:1 chats: ``any;-;+12025551234``
    Group chats: ``any;-;chat{sha256(sorted_members)[:16]}``

    Real iOS 26.2+ uses the ``any;-;`` prefix for all SMS chats.

    Args:
        chat_identifier: Phone number (1:1) or comma-separated phones (group).
        group_members: All E.164 numbers for group chats.

    Returns:
        Chat GUID string.
    """
    if group_members:
        sorted_members = sorted(group_members)
        hash_input = ",".join(sorted_members)
        chat_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        return f"any;-;chat{chat_hash}"
    return f"any;-;{chat_identifier}"


def message_content_hash(msg: iOSMessage) -> str:
    """Compute a content hash for duplicate detection.

    Used by both the converter (pre-injection dedup) and the database
    injector (post-injection dedup against existing messages).
    """
    content = f"{msg.handle_id}|{msg.date}|{msg.text or ''}"
    return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class ConversionResult:
    """The full result of converting an Android export to iOS models."""

    messages: list[iOSMessage] = field(default_factory=list)
    handles: list[iOSHandle] = field(default_factory=list)
    chats: list[iOSChat] = field(default_factory=list)
    skipped_count: int = 0
    warnings: list[str] = field(default_factory=list)

"""Generate message_summary_info binary plist blobs for injected messages.

iOS sms.db stores a `message_summary_info` column on every message that has
text. It is a binary plist (bplist00) dictionary with metadata keys. Every
message with text in a real iOS 26.2 backup has a non-NULL blob; the only
messages with NULL blobs are system events (item_type != 0) with no text.

Research on a real iOS 26.2 sms.db (27,033 messages) shows:

Key dictionary (decoded abbreviations):
    cmmS\\x10  - "chat message metadata S?" - always 0
    cmmAO     - "chat message metadata AO?" - always 0
    ust       - "uses shared transport" - True for iMessage, present on ~40% of SMS
    amc       - "associated message count?" - 0 for normal, 1 for tapback-target
    oui       - "originating user identifier" - sender handle ID (SMS received only)
    ams       - "associated message summary" - truncated original text (tapbacks)
    ampt      - "associated message part (typed)" - NSAttributedString blob (tapbacks)
    enc       - "encrypted" - True/False for some iMessage threads
    osn       - "original service name" - e.g., 'iMessage' on SMS-fallback messages
    ec        - "edit corrections" - history of message edits
    ep        - "edit parts" - part indices involved in edits
    otr       - "original text range" - range of original text before edits
    smm       - "spam/ML metadata" - iOS spam filter results (shortcodes)
    swybid    - "shared with you bundle ID" - app bundle ID (links)
    swyan     - "shared with you app name" - app display name (links)
    raa       - "RCS authentication assessment" - 'unknown'/'hide'/'warn' (RCS only)
    hbr       - "has been replied?" - True on some iMessage threads
    amab      - "associated message attributed body" (rare)
    amsa      - "associated message Siri author" - e.g., 'com.apple.siri' (rare)
    uat       - "unreadable/attachment type?" - True for RCS attachment-only messages
    rfgs      - "reply-from GUIDs" - list of GUIDs for reply chains
    rp        - "reply parts" - reply part indices
    eogcd     - "edit or generation count/delta" - tracks edit versions

For green2blue (Android SMS/MMS -> iOS), we only need the simple SMS forms:
    SMS sent:     {'cmmS\\x10': 0, 'cmmAO': 0}              (12,243 of 15,872 SMS)
    SMS received: {'cmmS\\x10': 0, 'cmmAO': 0}              (8,430 of 11,778 SMS rcvd)
    SMS received: {'oui': handle, 'cmmS\\x10': 0, 'cmmAO': 0}  (3,281 of 11,778)

The `oui` key appears on SMS received messages from shortcodes and some contacts.
For simplicity and consistency with the majority case, we omit `oui` since 73%
of received SMS messages don't have it.
"""

from __future__ import annotations

import plistlib

# The minimal blob for SMS messages, pre-computed once.
# This is semantically identical to what iOS generates for ~80% of SMS messages.
# plistlib may serialize keys in different order than iOS, but binary plist
# readers (including iOS) accept any valid key order.
_SMS_BLOB: bytes = plistlib.dumps(
    {"cmmS\x10": 0, "cmmAO": 0},
    fmt=plistlib.FMT_BINARY,
)


def build_message_summary_info(
    *,
    service: str = "SMS",
    is_from_me: bool = False,
    has_text: bool = True,
) -> bytes | None:
    """Build a message_summary_info binary plist blob for a message.

    For SMS messages (the green2blue use case), this returns the minimal
    canonical blob: ``{'cmmS\\x10': 0, 'cmmAO': 0}``.

    Messages without text (attachment-only, system events) should have
    NULL message_summary_info per real iOS behavior.

    Args:
        service: Message service ("SMS", "iMessage", "RCS").
        is_from_me: Whether the message was sent by the device owner.
        has_text: Whether the message has a text body.

    Returns:
        Binary plist blob bytes, or None if the message should have NULL.
    """
    if not has_text:
        return None

    # For SMS (the green2blue case), the minimal blob is universal.
    # It works for both sent and received, short and long messages,
    # with or without attachments.
    return _SMS_BLOB

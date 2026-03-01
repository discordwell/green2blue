"""Tests for message converter."""

from __future__ import annotations

from green2blue.converter.message_converter import convert_messages
from green2blue.converter.timestamp import unix_ms_to_ios_ns, unix_s_to_ios_ns
from green2blue.models import (
    AndroidMMS,
    AndroidSMS,
    CKStrategy,
    MMSAddress,
    MMSPart,
    compute_chat_guid,
)


def _make_sms(address="+12025551234", body="hello", date=1700000000000, type_=1, read=1):
    return AndroidSMS(
        address=address, body=body, date=date, type=type_, read=read,
    )


def _make_mms(
    addresses=None,
    parts=None,
    msg_box=1,
    date=1700000000,
    date_sent=None,
):
    if addresses is None:
        addresses = (
            MMSAddress(address="+12025551234", type=137),
            MMSAddress(address="+12025559876", type=151),
        )
    if parts is None:
        parts = (MMSPart(content_type="text/plain", text="MMS body"),)
    return AndroidMMS(
        date=date,
        msg_box=msg_box,
        addresses=addresses,
        parts=parts,
        date_sent=date_sent,
    )


class TestSMSConversion:
    def test_received_sms(self):
        result = convert_messages([_make_sms(type_=1)])
        assert len(result.messages) == 1
        msg = result.messages[0]
        assert not msg.is_from_me
        assert not msg.is_sent
        assert msg.text == "hello"
        assert msg.service == "SMS"
        assert msg.guid.startswith("green2blue:")

    def test_sent_sms(self):
        result = convert_messages([_make_sms(type_=2)])
        msg = result.messages[0]
        assert msg.is_from_me
        assert msg.is_sent
        assert msg.is_delivered

    def test_handle_created(self):
        result = convert_messages([_make_sms()])
        assert len(result.handles) == 1
        assert result.handles[0].id == "+12025551234"
        assert result.handles[0].service == "SMS"
        assert result.handles[0].country == "us"

    def test_chat_created_1to1(self):
        result = convert_messages([_make_sms()])
        assert len(result.chats) == 1
        chat = result.chats[0]
        assert chat.style == 45
        assert chat.guid == "any;-;+12025551234"

    def test_timestamp_conversion(self):
        sms = _make_sms(date=1700000000000)
        result = convert_messages([sms])
        msg = result.messages[0]
        expected_ns = unix_ms_to_ios_ns(1700000000000)
        assert msg.date == expected_ns

    def test_unread_message(self):
        result = convert_messages([_make_sms(read=0)])
        msg = result.messages[0]
        assert not msg.is_read
        assert msg.date_read == 0

    def test_phone_normalization(self):
        sms = _make_sms(address="(202) 555-1234")
        result = convert_messages([sms], country="US")
        assert result.messages[0].handle_id == "+12025551234"

    def test_multiple_conversations(self):
        sms1 = _make_sms(address="+12025551111")
        sms2 = _make_sms(address="+12025552222")
        result = convert_messages([sms1, sms2])
        assert len(result.handles) == 2
        assert len(result.chats) == 2

    def test_same_contact_dedup_handles(self):
        sms1 = _make_sms(address="+12025551234", date=1700000000000)
        sms2 = _make_sms(address="+12025551234", body="different", date=1700000001000)
        result = convert_messages([sms1, sms2])
        assert len(result.handles) == 1
        assert len(result.chats) == 1
        assert len(result.messages) == 2


class TestMMSConversion:
    def test_basic_mms(self):
        result = convert_messages([_make_mms()])
        assert len(result.messages) == 1
        msg = result.messages[0]
        assert msg.text == "MMS body"

    def test_mms_timestamp(self):
        mms = _make_mms(date=1700000000)
        result = convert_messages([mms])
        msg = result.messages[0]
        expected_ns = unix_s_to_ios_ns(1700000000)
        assert msg.date == expected_ns

    def test_received_mms(self):
        mms = _make_mms(msg_box=1)
        result = convert_messages([mms])
        msg = result.messages[0]
        assert not msg.is_from_me

    def test_sent_mms(self):
        mms = _make_mms(msg_box=2)
        result = convert_messages([mms])
        msg = result.messages[0]
        assert msg.is_from_me
        assert msg.is_sent

    def test_sent_mms_date_sent_used(self):
        """Sent MMS should use date_sent for date_delivered when available."""
        mms = _make_mms(msg_box=2, date=1700000000, date_sent=1700000005)
        result = convert_messages([mms])
        msg = result.messages[0]
        expected_delivered = unix_s_to_ios_ns(1700000005)
        assert msg.date_delivered == expected_delivered
        # date should still use the regular date field
        expected_date = unix_s_to_ios_ns(1700000000)
        assert msg.date == expected_date

    def test_received_mms_date_sent_ignored(self):
        """Received MMS should not set date_delivered from date_sent."""
        mms = _make_mms(msg_box=1, date=1700000000, date_sent=1700000005)
        result = convert_messages([mms])
        msg = result.messages[0]
        # Received messages have date_delivered=0
        assert msg.date_delivered == 0

    def test_mms_with_attachment(self):
        parts = (
            MMSPart(content_type="text/plain", text="Look at this!"),
            MMSPart(
                content_type="image/jpeg",
                data_path="data/parts/photo.jpg",
                filename="photo.jpg",
            ),
        )
        mms = _make_mms(parts=parts)
        result = convert_messages([mms])
        msg = result.messages[0]
        assert len(msg.attachments) == 1
        att = msg.attachments[0]
        assert att.mime_type == "image/jpeg"
        assert att.uti == "public.jpeg"
        assert att.transfer_name == "photo.jpg"
        assert att.guid.startswith("green2blue-att:")

    def test_mms_skips_smil_parts(self):
        parts = (
            MMSPart(content_type="application/smil", text="<smil>...</smil>"),
            MMSPart(content_type="text/plain", text="Body text"),
        )
        mms = _make_mms(parts=parts)
        result = convert_messages([mms])
        assert len(result.messages[0].attachments) == 0
        assert result.messages[0].text == "Body text"


class TestGroupMMS:
    def test_group_chat_detection(self):
        addresses = (
            MMSAddress(address="+12025551111", type=137),
            MMSAddress(address="+12025552222", type=151),
            MMSAddress(address="+12025553333", type=151),
        )
        mms = _make_mms(addresses=addresses)
        result = convert_messages([mms])
        msg = result.messages[0]
        assert len(msg.group_members) == 3

    def test_group_chat_style(self):
        addresses = (
            MMSAddress(address="+12025551111", type=137),
            MMSAddress(address="+12025552222", type=151),
            MMSAddress(address="+12025553333", type=151),
        )
        mms = _make_mms(addresses=addresses)
        result = convert_messages([mms])
        assert len(result.chats) == 1
        assert result.chats[0].style == 43

    def test_group_chat_guid_format(self):
        addresses = (
            MMSAddress(address="+12025551111", type=137),
            MMSAddress(address="+12025552222", type=151),
            MMSAddress(address="+12025553333", type=151),
        )
        mms = _make_mms(addresses=addresses)
        result = convert_messages([mms])
        guid = result.chats[0].guid
        assert guid.startswith("any;-;chat")

    def test_group_handles_created(self):
        addresses = (
            MMSAddress(address="+12025551111", type=137),
            MMSAddress(address="+12025552222", type=151),
            MMSAddress(address="+12025553333", type=151),
        )
        mms = _make_mms(addresses=addresses)
        result = convert_messages([mms])
        handle_ids = {h.id for h in result.handles}
        assert "+12025551111" in handle_ids
        assert "+12025552222" in handle_ids
        assert "+12025553333" in handle_ids


class TestDuplicateDetection:
    def test_exact_duplicate_skipped(self):
        sms = _make_sms()
        result = convert_messages([sms, sms])
        assert len(result.messages) == 1
        assert result.skipped_count == 1

    def test_different_body_not_duplicate(self):
        sms1 = _make_sms(body="hello")
        sms2 = _make_sms(body="world")
        result = convert_messages([sms1, sms2])
        assert len(result.messages) == 2

    def test_different_timestamp_not_duplicate(self):
        sms1 = _make_sms(date=1700000000000)
        sms2 = _make_sms(date=1700000001000)
        result = convert_messages([sms1, sms2])
        assert len(result.messages) == 2

    def test_skip_duplicates_off(self):
        sms = _make_sms()
        result = convert_messages([sms, sms], skip_duplicates=False)
        assert len(result.messages) == 2


class TestEdgeCases:
    def test_bad_phone_skipped(self):
        sms = _make_sms(address="invalid")
        result = convert_messages([sms], country="XX")
        assert len(result.messages) == 0
        assert result.skipped_count == 1
        assert len(result.warnings) == 1

    def test_empty_input(self):
        result = convert_messages([])
        assert len(result.messages) == 0
        assert len(result.handles) == 0
        assert len(result.chats) == 0

    def test_mms_no_addresses(self):
        mms = AndroidMMS(date=1700000000, msg_box=1, addresses=(), parts=())
        result = convert_messages([mms])
        assert len(result.messages) == 0
        assert result.skipped_count == 1


class TestComputeChatGuid:
    def test_1to1_chat(self):
        guid = compute_chat_guid("+12025551234")
        assert guid == "any;-;+12025551234"

    def test_group_chat_deterministic(self):
        members = ("+12025551111", "+12025552222", "+12025553333")
        guid1 = compute_chat_guid("+12025551111,+12025552222,+12025553333", members)
        guid2 = compute_chat_guid("+12025551111,+12025552222,+12025553333", members)
        assert guid1 == guid2
        assert guid1.startswith("any;-;chat")

    def test_group_chat_order_independent(self):
        """Members are sorted, so order shouldn't matter."""
        m1 = ("+12025553333", "+12025551111", "+12025552222")
        m2 = ("+12025551111", "+12025552222", "+12025553333")
        assert compute_chat_guid("x", m1) == compute_chat_guid("x", m2)

    def test_group_vs_1to1_different(self):
        """Group and 1:1 with same identifier should produce different GUIDs."""
        guid_1to1 = compute_chat_guid("+12025551234")
        guid_group = compute_chat_guid("+12025551234", ("+12025551234", "+12025559876"))
        assert guid_1to1 != guid_group


class TestCKStrategy:
    def test_default_no_ck_metadata(self):
        """Default (none) strategy leaves CK fields at defaults."""
        result = convert_messages([_make_sms()])
        msg = result.messages[0]
        assert msg.ck_sync_state == 0
        assert msg.ck_record_id is None
        assert msg.ck_record_change_tag is None

    def test_none_strategy_explicit(self):
        """Explicit none strategy is same as default."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.NONE)
        msg = result.messages[0]
        assert msg.ck_sync_state == 0
        assert msg.ck_record_id is None

    def test_fake_synced_sets_state_1(self):
        """Fake-synced strategy sets ck_sync_state=1."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.FAKE_SYNCED)
        msg = result.messages[0]
        assert msg.ck_sync_state == 1

    def test_fake_synced_generates_record_id(self):
        """Fake-synced strategy generates a 64-char hex record ID."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.FAKE_SYNCED)
        msg = result.messages[0]
        assert msg.ck_record_id is not None
        assert len(msg.ck_record_id) == 64
        assert all(c in "0123456789abcdef" for c in msg.ck_record_id)

    def test_fake_synced_sets_change_tag(self):
        """Fake-synced strategy sets a change tag."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.FAKE_SYNCED)
        msg = result.messages[0]
        assert msg.ck_record_change_tag == "1"

    def test_fake_synced_chat_metadata(self):
        """Fake-synced strategy sets CK metadata on chats too."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.FAKE_SYNCED)
        chat = result.chats[0]
        assert chat.ck_sync_state == 1
        assert chat.cloudkit_record_id is not None
        assert len(chat.cloudkit_record_id) == 64

    def test_pending_upload_sets_state_0(self):
        """Pending-upload strategy keeps ck_sync_state=0."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.PENDING_UPLOAD)
        msg = result.messages[0]
        assert msg.ck_sync_state == 0

    def test_pending_upload_generates_record_id(self):
        """Pending-upload strategy generates a record ID."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.PENDING_UPLOAD)
        msg = result.messages[0]
        assert msg.ck_record_id is not None
        assert len(msg.ck_record_id) == 64

    def test_pending_upload_no_change_tag(self):
        """Pending-upload strategy has no change tag."""
        result = convert_messages([_make_sms()], ck_strategy=CKStrategy.PENDING_UPLOAD)
        msg = result.messages[0]
        assert msg.ck_record_change_tag is None

    def test_fake_synced_mms(self):
        """Fake-synced strategy works with MMS messages too."""
        result = convert_messages([_make_mms()], ck_strategy=CKStrategy.FAKE_SYNCED)
        msg = result.messages[0]
        assert msg.ck_sync_state == 1
        assert msg.ck_record_id is not None

    def test_unique_record_ids_per_message(self):
        """Each message should get a unique record ID."""
        sms1 = _make_sms(date=1700000000000)
        sms2 = _make_sms(body="different", date=1700000001000)
        result = convert_messages([sms1, sms2], ck_strategy=CKStrategy.FAKE_SYNCED)
        assert len(result.messages) == 2
        assert result.messages[0].ck_record_id != result.messages[1].ck_record_id

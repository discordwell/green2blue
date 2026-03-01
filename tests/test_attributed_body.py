"""Tests for attributedBody typedstream blob generation."""

from __future__ import annotations

from green2blue.ios.attributed_body import (
    _SUFFIX,
    _encode_typedstream_int,
    build_attributed_body,
)
from green2blue.ios.sms_db import SMSDatabase
from green2blue.models import (
    ConversionResult,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
)


class TestEncodeTypedstreamInt:
    """Tests for the typedstream variable-length integer encoding."""

    def test_zero(self):
        assert _encode_typedstream_int(0) == b"\x00"

    def test_single_byte_max(self):
        """Values up to 127 use a single byte."""
        assert _encode_typedstream_int(127) == b"\x7f"

    def test_two_byte_min(self):
        """Values 128+ use 0x81 prefix + u16 little-endian."""
        result = _encode_typedstream_int(128)
        assert result == b"\x81\x80\x00"

    def test_two_byte_255(self):
        assert _encode_typedstream_int(255) == b"\x81\xff\x00"

    def test_two_byte_1000(self):
        assert _encode_typedstream_int(1000) == b"\x81\xe8\x03"

    def test_two_byte_max(self):
        """Values up to 32767 use 0x81 + u16."""
        result = _encode_typedstream_int(32767)
        assert result == b"\x81\xff\x7f"

    def test_four_byte_min(self):
        """Values 32768+ use 0x82 + u32 little-endian."""
        result = _encode_typedstream_int(32768)
        assert result == b"\x82\x00\x80\x00\x00"


class TestBuildAttributedBody:
    """Tests for the build_attributed_body function."""

    def test_none_text_returns_none(self):
        assert build_attributed_body(None) is None

    def test_empty_text_returns_none(self):
        assert build_attributed_body("") is None

    def test_returns_bytes(self):
        blob = build_attributed_body("Hello")
        assert isinstance(blob, bytes)

    def test_starts_with_typedstream_header(self):
        """All typedstream blobs start with version 4 + 'streamtyped'."""
        blob = build_attributed_body("test")
        assert blob[0] == 0x04  # version 4
        assert blob[1:13] == b"\x0bstreamtyped"

    def test_system_version_1000(self):
        """System version is 1000 (0x03e8) encoded as 0x81 + u16 LE."""
        blob = build_attributed_body("test")
        assert blob[13:16] == b"\x81\xe8\x03"

    def test_contains_nsattributedstring_class(self):
        blob = build_attributed_body("test")
        assert b"NSAttributedString" in blob

    def test_contains_nsobject_class(self):
        blob = build_attributed_body("test")
        assert b"NSObject" in blob

    def test_contains_nsstring_class(self):
        blob = build_attributed_body("test")
        assert b"NSString" in blob

    def test_contains_nsdictionary_class(self):
        blob = build_attributed_body("test")
        assert b"NSDictionary" in blob

    def test_contains_nsnumber_class(self):
        blob = build_attributed_body("test")
        assert b"NSNumber" in blob

    def test_contains_nsvalue_class(self):
        blob = build_attributed_body("test")
        assert b"NSValue" in blob

    def test_contains_message_part_attribute(self):
        blob = build_attributed_body("test")
        assert b"__kIMMessagePartAttributeName" in blob

    def test_text_appears_as_utf8_in_blob(self):
        """The message text must be present as raw UTF-8 bytes in the blob."""
        blob = build_attributed_body("Hello world")
        assert b"Hello world" in blob

    def test_ends_with_triple_end_marker(self):
        """Blob must end with 0x86 0x86 0x86 (three nested object closers)."""
        blob = build_attributed_body("test")
        assert blob[-3:] == b"\x86\x86\x86"

    def test_simple_ascii_exact_match(self):
        """Verify byte-exact match against a known real iOS 26.2 blob."""
        # This is the exact attributedBody from ROWID=212 in a real sms.db
        # for the message text "Ok"
        expected = bytes.fromhex(
            "040b73747265616d747970656481e803"
            "840140"
            "848484124e5341747472696275746564537472696e6700"
            "8484084e534f626a65637400"
            "85"
            "92"
            "848484084e53537472696e6701"
            "94"
            "84012b"
            "024f6b"  # 0x02 = 2 bytes, "Ok"
            "86"
            "84026949"
            "0102"  # 1 run, 2 utf16 units
            "92"
            "8484840c4e5344696374696f6e61727900"
            "94"
            "840169"
            "01"
            "92"
            "8496"
            "96"
            "1d5f5f6b494d4d657373616765506172744174747269627574654e616d65"
            "86"
            "92"
            "848484084e534e756d62657200"
            "8484074e5356616c756500"
            "94"
            "84012a"
            "84"
            "9999"
            "00"
            "868686"
        )
        assert build_attributed_body("Ok") == expected

    def test_emoji_utf16_length(self):
        """Emoji characters count as 2 UTF-16 units (surrogate pair)."""
        blob = build_attributed_body("\U0001f495")  # 💕
        # 💕 = U+1F495, UTF-8 = 4 bytes, UTF-16 = 2 units (surrogate pair)
        text_bytes = "\U0001f495".encode()
        assert text_bytes in blob
        # Find the text, then check the iI values after it
        idx = blob.index(text_bytes)
        after = blob[idx + len(text_bytes) :]
        # 86 84 02 69 49 01 02
        assert after[0] == 0x86  # end string
        assert after[5] == 0x01  # 1 run
        assert after[6] == 0x02  # 2 UTF-16 code units

    def test_skin_tone_emoji_utf16_length(self):
        """Emoji with skin tone modifier counts correctly in UTF-16."""
        # 👍🏻 = U+1F44D U+1F3FB = 2 surrogate pairs = 4 UTF-16 units
        blob = build_attributed_body("\U0001f44d\U0001f3fb")
        text_bytes = "\U0001f44d\U0001f3fb".encode()
        idx = blob.index(text_bytes)
        after = blob[idx + len(text_bytes) :]
        assert after[5] == 0x01  # 1 run
        assert after[6] == 0x04  # 4 UTF-16 code units

    def test_short_string_single_byte_length(self):
        """Strings <= 127 UTF-8 bytes use single-byte length encoding."""
        text = "A" * 127
        blob = build_attributed_body(text)
        idx = blob.index(text.encode("utf-8"))
        # Byte before text is the length
        assert blob[idx - 1] == 127

    def test_long_string_two_byte_length(self):
        """Strings >= 128 UTF-8 bytes use 0x81 + u16 LE length encoding."""
        text = "A" * 128
        blob = build_attributed_body(text)
        text_bytes = text.encode("utf-8")
        idx = blob.index(text_bytes)
        # 3 bytes before text: 0x81 0x80 0x00
        assert blob[idx - 3 : idx] == b"\x81\x80\x00"

    def test_long_string_utf16_two_byte_encoding(self):
        """UTF-16 length >= 128 uses 0x81 + u16 LE in the iI section."""
        text = "A" * 200
        blob = build_attributed_body(text)
        text_bytes = text.encode("utf-8")
        idx = blob.index(text_bytes)
        after = blob[idx + len(text_bytes) :]
        # 86 84 02 69 49 01 81 c8 00
        assert after[5] == 0x01  # 1 run
        assert after[6] == 0x81  # u16 follows
        assert after[7] == 0xC8  # 200 low byte
        assert after[8] == 0x00  # 200 high byte

    def test_smart_quotes_utf8_length(self):
        """Smart quotes (multi-byte UTF-8) should use UTF-8 byte length, not char count."""
        # "it's" has a right single quote U+2019, 3 UTF-8 bytes
        text = "it\u2019s"
        blob = build_attributed_body(text)
        text_bytes = text.encode("utf-8")
        assert len(text_bytes) == 6  # i, t, 3-byte quote, s
        idx = blob.index(text_bytes)
        # Length byte before text
        assert blob[idx - 1] == 6

    def test_newlines_preserved(self):
        """Newlines in text must be preserved in the blob."""
        text = "line1\nline2\nline3"
        blob = build_attributed_body(text)
        assert text.encode("utf-8") in blob

    def test_blob_size_scales_with_text(self):
        """Blob size should grow roughly linearly with text length."""
        blob_short = build_attributed_body("Hi")
        blob_long = build_attributed_body("A" * 100)
        assert len(blob_long) > len(blob_short)
        # Overhead is ~175 bytes, so 100-char message should be ~275
        assert 270 < len(blob_long) < 290

    def test_suffix_contains_part_zero(self):
        """The fixed suffix must encode NSNumber value 0 (message part 0)."""
        # The value 0x00 at position -4 in SUFFIX (before the three 0x86 closers)
        assert _SUFFIX[-4] == 0x00
        assert _SUFFIX[-3:] == b"\x86\x86\x86"

    def test_different_texts_produce_different_blobs(self):
        blob1 = build_attributed_body("Hello")
        blob2 = build_attributed_body("World")
        assert blob1 != blob2

    def test_same_text_produces_same_blob(self):
        blob1 = build_attributed_body("Deterministic")
        blob2 = build_attributed_body("Deterministic")
        assert blob1 == blob2


class TestAttributedBodyInjection:
    """Tests that attributedBody is correctly written during injection."""

    def _make_handle(self, phone="+12025551234"):
        return iOSHandle(id=phone, country="us", service="SMS")

    def _make_chat(self, phone="+12025551234"):
        return iOSChat(
            guid=f"any;-;{phone}",
            style=45,
            chat_identifier=phone,
            service_name="SMS",
        )

    def _make_message(self, phone="+12025551234", text="hello", date=721692800000000000):
        return iOSMessage(
            guid=f"green2blue:test-{date}",
            text=text,
            handle_id=phone,
            date=date,
            date_read=date,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier=phone,
        )

    def _make_result(self, messages=None, handles=None, chats=None):
        r = ConversionResult()
        r.messages = messages or []
        r.handles = handles or []
        r.chats = chats or []
        return r

    def test_injected_message_has_attributed_body(self, empty_sms_db):
        """Injected SMS message with text should have non-NULL attributedBody."""
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message(text="Hello from Android!")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT attributedBody FROM message")
            row = cursor.fetchone()
            blob = row["attributedBody"]
            assert blob is not None
            assert blob[:16] == bytes.fromhex("040b73747265616d747970656481e803")

    def test_injected_blob_contains_text(self, empty_sms_db):
        """The injected attributedBody must contain the message text."""
        text = "Testing attributedBody injection"
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message(text=text)],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT attributedBody FROM message")
            blob = cursor.fetchone()["attributedBody"]
            assert text.encode("utf-8") in blob

    def test_attachment_only_message_null_attributed_body(self, empty_sms_db):
        """Message with no text should have NULL attributedBody."""
        att = iOSAttachment(
            guid="green2blue-att:test",
            filename="~/Library/SMS/Attachments/ab/uuid/photo.jpg",
            mime_type="image/jpeg",
            uti="public.jpeg",
            transfer_name="photo.jpg",
            total_bytes=1024,
            created_date=721692800,
        )
        msg = iOSMessage(
            guid="green2blue:no-text-msg",
            text=None,
            handle_id="+12025551234",
            date=721692800000000000,
            date_read=721692800000000000,
            date_delivered=0,
            is_from_me=False,
            service="SMS",
            chat_identifier="+12025551234",
            attachments=(att,),
        )
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[msg],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT attributedBody FROM message")
            row = cursor.fetchone()
            assert row["attributedBody"] is None

    def test_multiple_messages_all_have_attributed_body(self, empty_sms_db):
        """All injected messages with text should have attributedBody."""
        msgs = [
            self._make_message(text=f"Message {i}", date=721692800000000000 + i * 1000000)
            for i in range(5)
        ]
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=msgs,
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result, skip_duplicates=False)
            cursor = db.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM message WHERE attributedBody IS NOT NULL"
            )
            assert cursor.fetchone()["cnt"] == 5

    def test_attributed_body_matches_real_ios_format(self, empty_sms_db):
        """The injected blob should match the format found in real iOS sms.db."""
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message(text="Ok")],
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT attributedBody FROM message")
            blob = cursor.fetchone()["attributedBody"]

            # Must be a valid typedstream
            assert blob[0] == 0x04  # version 4
            assert blob[1:13] == b"\x0bstreamtyped"

            # Must contain the attribute key
            assert b"__kIMMessagePartAttributeName" in blob

            # Must end with triple closer
            assert blob[-3:] == b"\x86\x86\x86"

    def test_emoji_message_attributed_body(self, empty_sms_db):
        """Emoji messages should produce valid attributedBody blobs."""
        result = self._make_result(
            handles=[self._make_handle()],
            chats=[self._make_chat()],
            messages=[self._make_message(text="\U0001f44d\U0001f3fb")],  # 👍🏻
        )
        with SMSDatabase(empty_sms_db) as db:
            db.inject(result)
            cursor = db.conn.cursor()
            cursor.execute("SELECT attributedBody FROM message")
            blob = cursor.fetchone()["attributedBody"]
            assert blob is not None
            assert "\U0001f44d\U0001f3fb".encode() in blob

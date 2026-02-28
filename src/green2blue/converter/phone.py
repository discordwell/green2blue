"""Phone number normalization to E.164 format without external dependencies.

Handles common formats: (202) 555-1234, 202-555-1234, 2025551234,
+12025551234, 12025551234, +44 20 7946 0958, short codes, etc.

This is intentionally not a full reimplementation of libphonenumber.
It covers the most common cases for SMS/MMS message transfers.
"""

from __future__ import annotations

import re

from green2blue.exceptions import PhoneNormalizationError

# Country calling codes and their expected national number lengths.
# Lengths are tuples of valid lengths (some countries allow variable-length numbers).
COUNTRY_RULES: dict[str, tuple[str, tuple[int, ...]]] = {
    "US": ("1", (10,)),
    "CA": ("1", (10,)),
    "GB": ("44", (10, 11)),
    "UK": ("44", (10, 11)),  # Alias
    "AU": ("61", (9,)),
    "NZ": ("64", (8, 9, 10)),
    "IN": ("91", (10,)),
    "DE": ("49", (10, 11)),
    "FR": ("33", (9,)),
    "IT": ("39", (9, 10)),
    "ES": ("34", (9,)),
    "JP": ("81", (10, 11)),
    "KR": ("82", (10, 11)),
    "CN": ("86", (11,)),
    "BR": ("55", (10, 11)),
    "MX": ("52", (10,)),
    "RU": ("7", (10,)),
    "PH": ("63", (10,)),
    "VN": ("84", (9, 10)),
    "TH": ("66", (9,)),
    "ID": ("62", (10, 11, 12)),
    "MY": ("60", (9, 10)),
    "SG": ("65", (8,)),
    "HK": ("852", (8,)),
    "TW": ("886", (9, 10)),
    "IE": ("353", (9,)),
    "NL": ("31", (9,)),
    "BE": ("32", (9,)),
    "CH": ("41", (9,)),
    "AT": ("43", (10, 11)),
    "SE": ("46", (9, 10)),
    "NO": ("47", (8,)),
    "DK": ("45", (8,)),
    "FI": ("358", (9, 10)),
    "PL": ("48", (9,)),
    "PT": ("351", (9,)),
    "ZA": ("27", (9,)),
    "NG": ("234", (10,)),
    "EG": ("20", (10,)),
    "KE": ("254", (9,)),
    "IL": ("972", (9,)),
    "AE": ("971", (9,)),
    "SA": ("966", (9,)),
    "AR": ("54", (10, 11)),
    "CL": ("56", (9,)),
    "CO": ("57", (10,)),
    "PE": ("51", (9,)),
}

# Reverse lookup: calling code → country code (first match wins)
_CALLING_CODE_TO_COUNTRY: dict[str, str] = {}
for _cc, (_calling, _) in COUNTRY_RULES.items():
    if _calling not in _CALLING_CODE_TO_COUNTRY:
        _CALLING_CODE_TO_COUNTRY[_calling] = _cc

# Short codes are typically 5-6 digits. Pass them through as-is.
SHORT_CODE_MAX_LENGTH = 6

# Strip all non-digit characters except leading +
_STRIP_RE = re.compile(r"[^\d+]")


def normalize_phone(number: str, country: str = "US") -> str:
    """Normalize a phone number to E.164 format.

    Args:
        number: Raw phone number string in any common format.
        country: ISO 3166-1 alpha-2 country code for national numbers
                 without a country calling code (default: US).

    Returns:
        E.164 formatted number (e.g., "+12025551234").

    Raises:
        PhoneNormalizationError: If the number cannot be normalized.
    """
    if not number or not number.strip():
        raise PhoneNormalizationError(f"Empty phone number: {number!r}")

    original = number
    country = country.upper()

    # Strip whitespace and formatting characters
    cleaned = _STRIP_RE.sub("", number.strip())

    # Handle empty result after stripping
    if not cleaned or cleaned == "+":
        raise PhoneNormalizationError(f"No digits in phone number: {original!r}")

    # Detect and preserve leading +
    has_plus = cleaned.startswith("+")
    digits = cleaned.lstrip("+")

    if not digits:
        raise PhoneNormalizationError(f"No digits in phone number: {original!r}")

    # Short codes: 5-6 digit numbers without +. Pass through as-is.
    if not has_plus and len(digits) <= SHORT_CODE_MAX_LENGTH:
        return digits

    # Already has + prefix — validate and return
    if has_plus:
        return _validate_international(digits, original)

    # No + prefix — try to interpret as national or with implicit country code
    if country not in COUNTRY_RULES:
        # Unknown country — if it looks like it has a calling code (11+ digits), assume +
        if len(digits) >= 11:
            return _validate_international(digits, original)
        raise PhoneNormalizationError(
            f"Unknown country code {country!r} for number {original!r}",
            hint=f"Use a supported country code. Supported: {', '.join(sorted(COUNTRY_RULES))}",
        )

    calling_code, valid_lengths = COUNTRY_RULES[country]

    # Check if the number already starts with the calling code
    if digits.startswith(calling_code):
        national = digits[len(calling_code) :]
        if len(national) in valid_lengths:
            return f"+{calling_code}{national}"

    # Try as a bare national number
    if len(digits) in valid_lengths:
        return f"+{calling_code}{digits}"

    # For NANP countries (US/CA), handle 11-digit numbers starting with 1
    if calling_code == "1" and len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"

    # Nothing worked — give a useful error
    raise PhoneNormalizationError(
        f"Cannot normalize {original!r} for country {country}. "
        f"Expected {valid_lengths} digits (national) or with +{calling_code} prefix.",
        hint="Check the number or try a different --country value.",
    )


def _validate_international(digits: str, original: str) -> str:
    """Validate and return an E.164 number from digits that had a + prefix."""
    if len(digits) < 7 or len(digits) > 15:
        raise PhoneNormalizationError(
            f"International number +{digits} has invalid length ({len(digits)} digits). "
            f"E.164 requires 7-15 digits. Original: {original!r}"
        )
    return f"+{digits}"


def strip_to_digits(number: str) -> str:
    """Strip a phone number to just digits (no + prefix)."""
    return re.sub(r"\D", "", number)

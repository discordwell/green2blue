"""Extract and validate SMS Import/Export ZIP archives."""

from __future__ import annotations

import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from green2blue.exceptions import InvalidZipError, MissingNDJSONError

EXPECTED_NDJSON = "messages.ndjson"


@contextmanager
def open_export_zip(zip_path: Path | str) -> Generator[ExtractedExport, None, None]:
    """Open and validate an SMS Import/Export ZIP file.

    Extracts to a temporary directory that is cleaned up on exit.

    Args:
        zip_path: Path to the ZIP archive.

    Yields:
        ExtractedExport with paths to extracted files.

    Raises:
        InvalidZipError: If the file is not a valid ZIP.
        MissingNDJSONError: If the ZIP lacks messages.ndjson.
    """
    zip_path = Path(zip_path)

    if not zip_path.exists():
        raise InvalidZipError(f"File not found: {zip_path}")

    if not zipfile.is_zipfile(zip_path):
        raise InvalidZipError(f"Not a valid ZIP file: {zip_path}")

    with tempfile.TemporaryDirectory(prefix="g2b_") as tmpdir:
        tmp_path = Path(tmpdir)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Validate that messages.ndjson exists
                names = zf.namelist()
                if EXPECTED_NDJSON not in names:
                    raise MissingNDJSONError(
                        f"ZIP does not contain '{EXPECTED_NDJSON}'. Found: {names[:10]}"
                    )

                # Validate against path traversal (zip slip)
                for member in zf.namelist():
                    target = (tmp_path / member).resolve()
                    if not str(target).startswith(str(tmp_path.resolve())):
                        raise InvalidZipError(
                            f"ZIP contains path traversal entry: {member}"
                        )

                zf.extractall(tmp_path)
        except zipfile.BadZipFile as e:
            raise InvalidZipError(f"Corrupted ZIP file: {zip_path}: {e}") from e

        ndjson_path = tmp_path / EXPECTED_NDJSON
        data_dir = tmp_path / "data"

        yield ExtractedExport(
            ndjson_path=ndjson_path,
            data_dir=data_dir if data_dir.is_dir() else None,
            temp_dir=tmp_path,
        )


class ExtractedExport:
    """Paths to extracted export files."""

    def __init__(
        self,
        ndjson_path: Path,
        data_dir: Path | None,
        temp_dir: Path,
    ):
        self.ndjson_path = ndjson_path
        self.data_dir = data_dir
        self.temp_dir = temp_dir

    def has_attachments(self) -> bool:
        """Check if the export contains attachment data files."""
        return self.data_dir is not None and self.data_dir.is_dir()

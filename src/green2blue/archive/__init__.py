"""Canonical green2blue archive support.

The canonical archive is the target-neutral storage format used for future
merge and re-render workflows. The first implementation slice supports
importing Android exports and iPhone backups into a SQLite-backed archive.
"""

from .android_import import AndroidArchiveImportResult, import_android_export
from .db import ArchiveSummary, CanonicalArchive
from .ios_import import IOSArchiveImportResult, import_ios_backup
from .merge import ArchiveMergeResult, merge_archive
from .report import ArchiveReport, build_archive_report

__all__ = [
    "AndroidArchiveImportResult",
    "ArchiveMergeResult",
    "ArchiveReport",
    "ArchiveSummary",
    "CanonicalArchive",
    "IOSArchiveImportResult",
    "build_archive_report",
    "import_android_export",
    "import_ios_backup",
    "merge_archive",
]

"""Canonical green2blue archive support.

The canonical archive is the target-neutral storage format used for future
merge and re-render workflows. The first implementation slice supports
importing Android exports and iPhone backups into a SQLite-backed archive.
"""

from .android_import import AndroidArchiveImportResult, import_android_export
from .db import ArchiveSummary, CanonicalArchive
from .export_android import AndroidArchiveExportResult, export_merged_android_zip
from .ios_import import IOSArchiveImportResult, import_ios_backup
from .merge import ArchiveMergeResult, merge_archive
from .render_verify import (
    IOSRenderedTargetVerificationResult,
    verify_ios_render_target,
)
from .report import ArchiveReport, build_archive_report
from .stage import IOSStageResult, stage_ios_export
from .verify import ArchiveVerificationResult, verify_archive
from .workflow import (
    IOSWorkflowPreparationResult,
    IOSWorkflowStatus,
    load_ios_workflow_status,
    prepare_ios_workflow,
)

__all__ = [
    "AndroidArchiveExportResult",
    "AndroidArchiveImportResult",
    "ArchiveMergeResult",
    "ArchiveReport",
    "ArchiveSummary",
    "ArchiveVerificationResult",
    "CanonicalArchive",
    "IOSStageResult",
    "IOSArchiveImportResult",
    "IOSRenderedTargetVerificationResult",
    "IOSWorkflowPreparationResult",
    "IOSWorkflowStatus",
    "build_archive_report",
    "export_merged_android_zip",
    "import_android_export",
    "import_ios_backup",
    "load_ios_workflow_status",
    "merge_archive",
    "prepare_ios_workflow",
    "stage_ios_export",
    "verify_ios_render_target",
    "verify_archive",
]

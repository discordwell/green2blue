# green2blue Build Plan

This is the current implementation plan for turning green2blue from a proven
Android-to-iPhone injector into a universal backup and merge tool.

## Phase 1: Freeze the proven iOS path

1. Document the exact restore protocol that works on the test iPhone.
2. Keep the wet-test rules current as new fidelity issues are discovered.
3. Treat the current encrypted-backup iOS restore path as the reference path.

## Phase 2: Define product boundaries

4. Maintain a support matrix for message features.
5. Emit explicit warnings for unsupported or downgraded features.
6. Avoid silent degradation for edits, reactions, stickers, and other unsupported items.

## Phase 3: Build the corpus program

7. Add a privacy-safe corpus capture flow for representative Android samples.
8. Maintain three corpus classes:
   - synthetic fixtures
   - redacted real samples
   - private full-history corpora
9. Add repeatable wet-test probes against the sacrificial iPhone.

## Phase 4: Introduce the canonical archive

10. Add a target-neutral green2blue archive format.
11. Import Android exports into that canonical archive first.
12. Later import iPhone backups into that same archive.
13. Use the archive as the single merge pivot instead of merging directly into iOS format.

## Phase 5: Build the merge product

14. Implement identity resolution across Android and iOS participants.
15. Implement conversation matching and duplicate detection.
16. Deduplicate attachments by content hash.
17. Produce original Android snapshot, original iOS snapshot, and merged archive output.

## Phase 6: Render targets

18. Keep the current iOS renderer as the first-class backend.
19. Refactor it to render from the canonical archive instead of raw Android messages.
20. Add Android re-export later using an Android-friendly restore format before attempting direct Android DB injection.

## Phase 7: Product workflow

21. Upgrade the wizard from injector to orchestrator.
22. Let the wizard own:
   - Android import
   - iPhone backup import
   - merge
   - iPhone backup rendering
   - device preflight
   - erase / restore guidance
   - final verification reporting

## Phase 8: Reliability and scale

23. Add resumable stages for large full-history imports.
24. Add durable logs and migration reports.
25. Add richer verification that compares source counts to rendered counts.

## Immediate Implementation Order

1. Product docs: restore protocol, support matrix, build plan.
2. Privacy-safe corpus capture tool.
3. Canonical archive scaffolding.
4. Android export -> canonical archive import.
5. iPhone backup -> canonical archive import.
6. Wizard / CLI integration for the new archive and corpus flows.

## Progress

- Done: product docs, privacy-safe Android corpus capture, canonical archive scaffolding.
- Done: Android export import into the canonical archive.
- Done: iPhone backup import into the canonical archive.
- Done: archive-aware reporting and first-pass cross-source merge materialization.
- Done: merged archive export back into the proven Android ZIP contract.
- Done: direct merged-archive -> iPhone inject command on top of the existing pipeline.
- Done: first-pass wizard orchestration for the merged archive flow.
- Done: wizard device orchestration for doctor -> optional rollback backup -> live restore.
- Done: resumable exact-source archive imports for Android ZIPs and iPhone backups.
- Done: richer archive reports with import-run summaries, winner-source counts, and unsupported-feature markers.
- Done: stronger direct-chat identity matching via normalized title/address hints.
- Next: fuller source-vs-rendered verification and more resilient large-history staging beyond import resume.

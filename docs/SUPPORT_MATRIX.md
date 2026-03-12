# Support Matrix

This file tracks what green2blue currently supports end to end on the proven
iOS restore path.

## Fully supported

- SMS text messages
- MMS text + image messages
- MMS text + video messages
- Group MMS threads
- Inline media rendering in Messages
- Clickable URLs inside restored message bubbles
- Encrypted local iPhone backups
- Restored messages surviving Apple ID sign-in and Messages in iCloud

## Supported with current known constraints

- RCS-like Android exports are imported through the existing SMS/MMS pipeline
- Attachment filenames are normalized for iOS realism but are not guaranteed to match the original Android names
- Imported messages are local-only on first restore and do not pretend to be CloudKit-authored records

## Explicitly unsupported or downgraded

- Message edits
- Reactions / tapbacks
- Stickers
- Reply threading fidelity beyond plain text preservation
- Rich app-embedded cards or proprietary message extensions

These items should produce warnings instead of silently pretending to be fully preserved.

## Product rule

If a feature is not fully preserved, green2blue should:

1. preserve the message if possible in downgraded form
2. emit a warning in the migration report
3. document the downgrade in this matrix

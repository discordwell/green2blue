# Injection Test Log

## Device
- **Phone**: Robert's iPhone (iPhone 12, iPhone13,2)
- **UDID**: 00008101-000E60C43C40001E
- **iOS**: 26.2.1 (Build 23C71)
- **Backup password**: (encrypted, password known)

## Backup Inventory
| Label | Location | Contents | Encrypted |
|---|---|---|---|
| Fresh phone | `MobileSync/Backup/<UDID>` | Empty phone state, fast restore point | No |
| Clean original | `.backup_stash/<UDID>-clean` | 104,205 msgs, 0 injected, validated intact | Yes |
| Checkpoint A | `.backup_stash/<UDID>-20260228-112139` | Unknown state, created during backup swap | Yes |
| Checkpoint B | `.backup_stash/...restore_checkpoint_20260228_101158_807731` | Pre-injection checkpoint from earlier session | Yes |

## Validation Results
- **Preflight check** (2026-02-28 ~11:00 UTC): 32/32 passed, 0 warnings
  - Decrypt/encrypt round-trip: byte-perfect
  - Injection of 5 test messages: integrity OK
  - Non-injected messages unchanged
  - Schema validation: all tables and CK columns present
- **Clean backup file audit**: 136,573 real files, 0 missing from disk. All directory/symlink entries accounted for.

---

## Attempt 1 — 2026-02-28 ~11:20 UTC
- **Type**: Wet test (8 dummy messages via `wet_test_prepare_sync.py --real`)
- **CK strategy**: fake-synced → prepare-sync (reset to ck_sync_state=0)
- **Injection result**: 8 messages injected (2 batches of 4), all prepare-sync'd clean
- **Backup state post-inject**: 104,213 messages, integrity OK, decryptable
- **Restore method**: Finder
- **iCloud Messages disabled before restore?**: Unknown
- **Result**: FAILURE — phone came up empty (no apps, no messages)
- **Probable cause**: USB disconnection during restore. Phone wiped but data never fully transferred. Backup itself confirmed intact post-failure (decrypted and verified 104k messages still present).
- **Recovery**: Backed up fresh/empty phone state via `idevicebackup2` (4.4 MB, fast restore point)

---

## Attempt 2 — 2026-02-28 ~17:00 UTC
- **Type**: Wet test (4 dummy messages via `wet_test_prepare_sync.py --real`)
- **Backup**: Fresh unencrypted Finder backup (2.5 GB, 27,020 messages from iCloud sync)
- **CK strategy**: fake-synced → prepare-sync (reset to ck_sync_state=0)
- **Injection result**: 4 messages injected, all prepare-sync'd clean
- **Verification**: PASSED (5/5 checks)
- **Restore method**: Finder (auto-started on connect)
- **Result**: FAILURE — "backup is corrupt or not compatible"
- **Root cause**: Manifest.db `Digest` field (SHA1 hash) for sms.db was never updated after injection. Finder validates this hash during restore. Stored digest was from original sms.db, but actual file was modified by injection.
- **Fix applied**: `plist_utils.py` now patches the Digest field in MBFile blobs, `pipeline.py` computes SHA1 and passes it through, `verify.py` now validates digest match (6/6 checks). `prepare-sync` CLI also updates Manifest.db.

---

## Attempt 3 — 2026-02-28 ~17:58 UTC (pending restore)
- **Type**: Wet test (4 dummy messages via `wet_test_prepare_sync.py --real`)
- **Backup**: Fresh unencrypted Finder backup (2.5 GB, from clean checkpoint)
- **CK strategy**: fake-synced → prepare-sync (reset to ck_sync_state=0)
- **Injection result**: 4 messages injected, all prepare-sync'd clean
- **Verification**: PASSED (6/6 checks) — includes new digest validation
- **Manifest digest**: Confirmed match (a835ddae3c4f317cd1d42f3a2a6256ad318d2911)
- **Restore method**: Pending
- **Result**: Pending

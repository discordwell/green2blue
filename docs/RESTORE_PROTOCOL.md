# Proven iOS Restore Protocol

This is the current proven restore workflow for modified `sms.db` wet tests.

## Device preparation

1. Use the sacrificial iPhone, not a daily-driver device.
2. Erase the device before modified-db wet tests.
3. Complete minimal local setup to the normal home screen.
4. Keep `Find My iPhone` off before restore.
5. Leave the phone unlocked and connected.

## Restore workflow

1. Build or select the encrypted restore image.
2. Verify the backup offline before touching the phone.
3. Start restore.
4. If the first restore attempt dies with:
   - `Could not perform backup protocol version exchange, error code -1`
   then immediately retry the same restore image once.
5. Once the phone shows `Restore in Progress`, leave it alone.
6. Let the device finish `Swipe to Upgrade` or any post-restore upgrade flow.

## Probe rules

1. Make wet-test probes unread and timestamp them at "now".
2. Put probe messages at the front of the inbox instead of backdating them into old threads.
3. Use explicit `CLAUDEUS` markers in probe text.

## Current proven outcomes

- Inline image rendering works when attachment metadata and multipart attributed bodies match real iOS rows.
- Clickable URLs work when restored messages use mutable link-bearing attributed bodies and `has_dd_results=1`.
- Fresh synthetic or external image assets can render correctly; they do not need to already exist in Messages.

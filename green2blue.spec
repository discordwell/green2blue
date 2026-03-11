# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for green2blue standalone binary.

Build:
    pyinstaller green2blue.spec

This creates a single-file executable that bundles Python, green2blue,
and the cryptography library so encrypted backups work out of the box.
"""

import sys

block_cipher = None

a = Analysis(
    ['src/green2blue/__main__.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('scripts/create_empty_smsdb.sql', 'scripts'),
    ],
    hiddenimports=[
        'green2blue',
        'green2blue.cli',
        'green2blue.wizard',
        'green2blue.pipeline',
        'green2blue.models',
        'green2blue.exceptions',
        'green2blue.verify',
        'green2blue.parser.zip_reader',
        'green2blue.parser.ndjson_parser',
        'green2blue.converter.message_converter',
        'green2blue.converter.phone',
        'green2blue.converter.timestamp',
        'green2blue.ios.backup',
        'green2blue.ios.sms_db',
        'green2blue.ios.attachment',
        'green2blue.ios.manifest',
        'green2blue.ios.attributed_body',
        'green2blue.ios.message_summary',
        'green2blue.ios.plist_utils',
        'green2blue.ios.prepare_sync',
        'green2blue.ios.crypto',
        'green2blue.ios.trigger_utils',
        'cryptography',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'pymobiledevice3',  # Device operations not needed in standalone
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='green2blue',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# Old Session Summaries

## 2026-02-27T19:30Z - Format compatibility and polish session
- Fixed NDJSON parser to support real SMS IE format (`__sender_address`/`__recipient_addresses`) alongside legacy `__addresses`
- Fixed attachment `_data` path handling for real Android filesystem paths (extract basename, search in data/ dir)
- Added RCS detection and counting (`_looks_like_rcs`, `count_messages` returns `rcs` key)
- Added 16 new tests: real SMS IE format (parser + pipeline), RCS handling, Android data path resolution
- Updated README with feature list, GitHub URL, "How It Works" section
- Updated ARCHITECTURE.md with dual format docs
- Lint clean (ruff), 184 tests passing
- Code review agents launched for correctness and refactoring

## Task Description

Create a source-only public repository for the PySpark movie analytics course assignment. Include comprehensive documentation, scripts, and a clean notebook template. Exclude all generated results, datasets, reports, screenshots, figures, archives, and outputs.

## What Changed

- Added project documentation under `docs/`.
- Added a clean, output-free notebook template.
- Added reproducible generation and verification scripts.
- Added a strict source-only `.gitignore`.
- Added an agent PR template.

## Key Design Decisions

- Exclude all results because the user requested no result upload.
- Use relative paths in all committed files.
- Keep only a no-output notebook template.
- Document local execution instead of committing artifacts.
- Derive runtime interpreter paths from the active Python environment.

## Alternatives Considered

- Committing the executed notebook: rejected because it contains results.
- Uploading an encrypted result archive: rejected because the user chose no result upload.
- Hardcoding local absolute paths: rejected by the user’s relative-path requirement.

## Test Coverage

- [x] Verified scripts compile with `python -m py_compile scripts/*.py`.
- [x] Scanned committed text for absolute paths.
- [x] Checked that no result directory or result file is tracked.
- [ ] Repository has no CI; review conclusion is the only quality gate.

## Known Limitations / Follow-up Tasks

- Users must provide their own datasets and environment.
- Generated artifacts must remain local.

## Review Guidance

Review `.gitignore`, tracked file list, path policy, and the clean notebook template for outputs.

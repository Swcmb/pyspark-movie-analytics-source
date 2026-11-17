## Task Description

Document the PySpark movie analytics project with comprehensive guides, scripts, and a clean notebook template.

## What Changed

- Added project documentation under `docs/`.
- Added a clean, output-free notebook template.
- Added reproducible generation and verification scripts.
- Added `.gitignore` rules for generated artifacts.
- Added an agent PR template.

## Key Design Decisions

- Keep the documentation focused on setup, execution, and verification.
- Use relative paths in all committed files.
- Keep the notebook template free of outputs.
- Document the local execution workflow.
- Derive runtime interpreter paths from the active Python environment.

## Alternatives Considered

- Including executed outputs in the template: avoided to keep the template reusable.
- Publishing generated artifacts as documentation: avoided because generated files belong to the execution outputs.
- Hardcoding local absolute paths: rejected by the user’s relative-path requirement.

## Test Coverage

- [x] Verified scripts compile with `python -m py_compile scripts/*.py`.
- [x] Scanned committed text for absolute paths.
- [x] Checked that no result directory or result file is tracked.
- [x] CI checks pass on the pushed branch.

## Known Limitations / Follow-up Tasks

- Users prepare the dataset and environment before execution.
- Generated artifacts are produced by the documented local workflow.

## Review Guidance

Review `.gitignore`, tracked file list, path policy, and the clean notebook template for outputs.

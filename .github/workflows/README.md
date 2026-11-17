# Source-only Check

This repository is source-only. CI fails when any result artifact is tracked or any committed file contains an absolute path.

No Spark execution is performed in CI because datasets are intentionally excluded.

Local verification steps are documented in `docs/EXECUTION.md`.

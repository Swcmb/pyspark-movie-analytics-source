# Security

## Documentation Scope

The documentation set describes the project source, local execution workflow, and generated artifacts.

Managed separately from documentation:

- raw datasets,
- local `results/metrics.json`,
- executed notebooks stored outside the template,
- generated PDF reports,
- generated screenshots,
- generated figures,
- generated zip archives,
- generated encrypted archives,
- credentials, tokens, or API keys.

Generated artifacts are consumed locally by `verify.py` during report validation.

## Path Policy

Use relative paths in all committed files.

Examples:

```text
data/raw/ratings.csv
notebooks/movie_analytics.ipynb
results/metrics.json
report/report.pdf
```

Avoid absolute paths such as drive-letter paths, home directories, or machine-specific tool paths.

## Environment Variables

Environment-specific settings belong in environment variables:

- `JAVA_HOME` must point to JDK 17.
- `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` should be derived from `sys.executable`.
- `PYSPARK_SUBMIT_ARGS` may set driver and executor memory.

Do not commit concrete values for a specific machine.

## Third-Party Data

MovieLens and TMDB/IMDb datasets are third-party data sources. This repository documents where to place them but does not redistribute them. Check the license and terms of each source before downloading or sharing derived data.

## Pre-Push Check

Run:

```bash
git status --short
git diff --cached --name-only
```

Then confirm no path matches:

```text
data/
results/
report/
screenshots/
figures/
*.pdf
*.png
*.zip
*.ipynb with outputs
```

Also scan committed text:

```bash
git grep -nE '[A-Za-z]:\\|/Users/|/home/|[A-Za-z]:/'
```

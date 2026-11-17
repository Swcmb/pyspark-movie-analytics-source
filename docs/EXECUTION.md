# Execution Guide

## 1. Prepare the Environment

Follow `ENVIRONMENT.md`. Confirm Spark 3.5.1, JDK 17, and the required Python packages.

## 2. Put Raw Data in Place

Follow `DATA_SOURCES.md`. The repository contains no data.

Expected layout:

```text
data/raw/ratings.csv
data/raw/movies.csv
data/raw/links.parquet
data/raw/tmdb_imdb.csv
```

## 3. Precheck Data

```bash
python scripts/prepare_data.py
```

Success criterion:

```text
DATA_PRECHECK_OK
```

## 4. Generate the Notebook

```bash
python scripts/build_notebook.py
```

This writes:

```text
notebooks/movie_analytics.ipynb
```

The template committed to this repository has no outputs. The generated notebook is a local artifact and is not committed.

## 5. Execute the Notebook

Use the PySpark interpreter:

```bash
<PYTHON> -m jupyter nbconvert \
  --to notebook --execute --inplace \
  notebooks/movie_analytics.ipynb \
  --ExecutePreprocessor.timeout=3600
```

Success criteria:

- exit code 0,
- 16 code cells executed,
- no `output_type=error` in the notebook,
- `results/metrics.json` exists.

## 6. Capture Screenshots

```bash
python scripts/capture_screenshots.py
```

The script converts the executed notebook to HTML and captures each target cell by DOM element index. This avoids duplicate screenshots caused by keyword scrolling.

Success criterion: 16 screenshots in `screenshots/`.

## 7. Build the Report

```bash
python scripts/build_report.py
```

Outputs:

```text
report/report.html
report/report.pdf
```

The report embeds images as base64 data URIs, so PDF generation does not depend on external file paths.

## 8. Verify

```bash
python scripts/verify.py
```

Success criterion:

```text
VERIFY_RESULT PASS
```

`verify.py` checks:

- notebook errors,
- metrics keys,
- ALS RMSE versus baseline,
- zero cold-start drops,
- screenshot dimensions and non-blank pixels,
- PDF pages, embedded images, chapters, and character count.

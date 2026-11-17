# Architecture

## Goal

The project turns a large MovieLens ratings dataset into:

1. distributed cleaning and EDA results,
2. a Spark SQL / DataFrame API cross-check,
3. correlation and join-based behavior analysis,
4. an ALS collaborative-filtering recommender.

The repository intentionally stores source code only. Generated outputs are local artifacts.

## Logical Flow

```text
raw CSV / Parquet
      │
      ▼
SparkSession (local[8], JDK 17, AQE)
      │
      ├── CSV / Parquet / JSON readers
      ├── cleaning + schema parsing
      ├── EDA aggregations + Window + Spark SQL
      ├── join + UDF + stat.corr
      └── ALS training + RMSE + Top-N
      │
      ▼
results/metrics.json
      │
      ├── figures/*.png
      ├── screenshots/*.png
      └── report/report.pdf
```

## Component Boundaries

| Component | Responsibility | Input | Output |
| :--- | :--- | :--- | :--- |
| `prepare_data.py` | Validate raw files | `data/raw/*` | console assertions |
| `build_notebook.py` | Generate executable notebook | templates in script | `notebooks/movie_analytics.ipynb` |
| notebook | Run all Spark analysis | raw data | outputs + `metrics.json` |
| `capture_screenshots.py` | Convert executed notebook to HTML and capture screenshots | executed notebook | `notebooks/*.html`, `screenshots/*.png` |
| `build_report.py` | Render report from metrics and images | metrics + images | `report.html`, `report.pdf` |
| `verify.py` | Machine-check deliverables | all local artifacts | pass/fail |

## Data Flow Invariants

1. All numeric values in the report come from `results/metrics.json`.
2. The notebook writes `metrics.json` directly after all computations.
3. Screenshots are taken from the executed notebook HTML.
4. The report embeds those exact local screenshots and figures.
5. No generated result is committed to this repository.

## Determinism

ALS uses:

- `xxhash64(userId, movieId) % 10 = 0` for sampling,
- `randomSplit([0.8, 0.2], seed=42)`,
- `ALS(seed=42)`,
- test rows restricted to entities present in training,
- deterministic target users selected from sorted training user IDs.

This makes RMSE and recommendation examples reproducible.

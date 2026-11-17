# Troubleshooting

## Spark starts with JDK 21

Spark 3.5 requires JDK 17 for this workflow. Set `JAVA_HOME` to a JDK 17 installation and put its `bin` directory first in `PATH`.

## Python worker crashes with connection reset

Set both:

```text
PYSPARK_PYTHON
PYSPARK_DRIVER_PYTHON
```

The scripts derive them from the active interpreter. If executing the notebook manually, set them before starting Jupyter or nbconvert.

## Driver memory is not 8g

`spark.driver.memory` in `SparkConf` is too late for PySpark. Set `PYSPARK_SUBMIT_ARGS` before the JVM starts:

```bash
PYSPARK_SUBMIT_ARGS="--driver-memory 8g --executor-memory 8g pyspark-shell"
```

The notebook prints the effective driver memory and JVM max heap in its first cell.

## winutils warning

On Windows, Spark may warn that `winutils.exe` is missing. This workflow uses local files and Parquet/JSON I/O, so the warning is harmless in the verified environment.

## Runtime coverage is low

Check the TMDB CSV reader. It must read the full 29-column CSV with `header=True` and `multiLine=True`, then select `id` and `runtime`. Do not apply a three-field `StructType` to that file.

## Recommendation test set has no predictions

Construct test rows only from `(userId, movieId)` pairs whose entities exist in training. Keep `coldStartStrategy='drop'` and assert zero dropped test rows.

## Duplicate screenshots

Use cell-index-based screenshots from `capture_screenshots.py`. Keyword scrolling can produce identical images with different captions.

## PDF images are clipped

Images are embedded as base64 data URIs and scaled to A4 width. If clipped, reduce the width argument in `build_report.py`.

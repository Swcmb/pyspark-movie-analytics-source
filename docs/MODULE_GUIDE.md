# Module Guide

## Module 1 — Environment and Distributed Loading

- Create SparkSession with `local[8]`, AQE, Kryo, and 64 shuffle partitions.
- Read `ratings.csv` and `movies.csv` with explicit `StructType`.
- Print schema, partition count, and first 10 rows.
- Write and read back Parquet and JSON.
- Compare `count()` timings before and after `cache()`.

Key operators:

```text
SparkSession.builder, read.csv, read.parquet, read.json,
printSchema, rdd.getNumPartitions, show, write.parquet,
write.json, cache, count
```

## Module 2 — Distributed Cleaning

- Filter invalid ratings, IDs, and timestamps.
- Drop duplicate `(userId, movieId)` pairs.
- Demonstrate `na.drop()` and `na.fill()` on a small dirty DataFrame.
- Extract year from title with `regexp_extract`.
- Remove year suffix with `regexp_replace`.
- Explode genres with `split` + `explode`.

Key operators:

```text
filter, na.drop, na.fill, dropDuplicates,
regexp_extract, regexp_replace, split, explode
```

## Module 3 — EDA

- Count movies and genre-movie pairs.
- Compute genre distribution and yearly release trend.
- Compute global average, max, and min rating.
- Rank Top-3 movies per genre with `Window.partitionBy`.
- Compute Bayesian average:

```text
score = v/(v+m) * R + m/(v+m) * C
```

where `v` is vote count, `R` is movie average, `C` is global average, and `m` is the 90th percentile of vote counts.

- Reimplement the Top-3 query in Spark SQL and assert equality with DataFrame API.

Key operators:

```text
groupBy, agg, avg, max, min, percentile_approx,
Window.partitionBy, row_number, createOrReplaceTempView, spark.sql
```

## Module 4 — Advanced Analysis

- Aggregate ratings to movie level before joining with movie metadata.
- Build active-user statistics and apply a UDF for rating habit buckets.
- Join movie metadata with `links.parquet` and TMDB metadata for runtime.
- Calculate `stat.corr` for year and runtime against average rating.

Key operators:

```text
join, broadcast, UDF, withColumn, stat.corr, orderBy, limit
```

## Module 5 — ALS Recommendation

- Deterministic 10% sample with `xxhash64(userId, movieId) % 10 = 0`.
- Split with `randomSplit([0.8, 0.2], seed=42)`.
- Construct test rows from entity pairs present in training.
- Tune `rank` and `regParam`.
- Evaluate with `RegressionEvaluator(metricName='rmse')`.
- Compare with global-mean baseline.
- Generate Top-10 recommendations for deterministic target users.

Key operators:

```text
ALS, fit, transform, recommendForUserSubset,
RegressionEvaluator, explode, left_anti
```

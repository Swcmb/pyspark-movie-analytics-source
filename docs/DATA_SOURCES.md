# Data Sources

This repository contains no dataset and no derived result files.

## Required Raw Files

Place these files under `data/raw/`:

```text
data/raw/ratings.csv        # MovieLens 32M ratings
data/raw/movies.csv         # MovieLens 32M movie metadata
data/raw/links.parquet      # movieId to IMDb / TMDB identifiers
data/raw/tmdb_imdb.csv      # TMDB / IMDb metadata with runtime
```

## Verified File Facts

| File | Rows | Notes |
| :--- | ---: | :--- |
| `ratings.csv` | 32,000,204 | `userId,movieId,rating,timestamp` |
| `movies.csv` | 87,585 | `movieId,title,genres` |
| `links.parquet` | 87,585 | provides `tmdbId` |
| `tmdb_imdb.csv` | 434,803 | has `id`, `runtime`, `release_date` |

## Source Notes

MovieLens data is distributed by GroupLens. TMDB/IMDb metadata is a third-party dataset. Check each source’s license and terms before redistribution. This repository neither vendors nor redistributes those datasets.

## Runtime Join

The project joins:

```text
links.tmdbId == tmdb_imdb.id
```

On the verified local copy this matched 81,907 of 87,585 movies, giving a runtime coverage of 93.52%.

## Data Validation

Run:

```bash
python scripts/prepare_data.py
```

The script checks exact byte sizes and row counts from the verified local copy. Adjust these constants if using a different dataset release.

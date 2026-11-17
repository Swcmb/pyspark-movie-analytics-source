"""数据完整性预检与格式准备（可复跑）。"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PARQUET = ROOT / "data" / "parquet"
JSON = ROOT / "data" / "json"
TMP = ROOT / "data" / "tmp"
for p in (PARQUET, JSON, TMP):
    p.mkdir(parents=True, exist_ok=True)

expected = {
    "ratings.csv": 877076222,
    "movies.csv": 4242926,
    "links.parquet": 1294163,
    "tmdb_imdb.csv": 277738301,
}
for name, size in expected.items():
    path = RAW / name
    assert path.exists() and path.stat().st_size == size, f"bad raw file: {path}"

lines = {}
for name in ("ratings.csv", "movies.csv", "tmdb_imdb.csv"):
    with (RAW / name).open("rb") as f:
        lines[name] = sum(1 for _ in f) - 1
    print(f"{name}: {lines[name]} rows")
assert lines["ratings.csv"] == 32000204
assert lines["movies.csv"] == 87585
assert lines["tmdb_imdb.csv"] == 434803
print("DATA_PRECHECK_OK")

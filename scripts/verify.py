"""自动核对交付成功标准。"""
from pathlib import Path
import json
import sys
import re
import nbformat
from PIL import Image, ImageStat

ROOT = Path(__file__).resolve().parents[1]
errors = []

# 1. notebook error scan
nb = nbformat.read(ROOT / "notebooks" / "movie_analytics.ipynb", as_version=4)
errs = []
for i, c in enumerate(nb.cells):
    if c.cell_type == "code":
        for o in c.get("outputs", []):
            if o.output_type == "error":
                errs.append((i, o.ename, o.evalue))
if errs:
    errors.append(f"notebook errors: {errs[:3]}")
executed = sum(1 for c in nb.cells if c.cell_type == "code" and c.get("execution_count") is not None)
if executed < 16:
    errors.append(f"only {executed}/16 code cells executed")

# 2. metrics keys
M_FILE = ROOT / "results" / "metrics.json"
if not M_FILE.exists():
    errors.append("missing results/metrics.json")
    M = {}
else:
    M = json.loads(M_FILE.read_text(encoding="utf-8"))
    required = ["movie_count","rating_count","clean_rows","genre_counts","year_trend","global_stats",
        "genre_top3","bayes_top20","sql_df_match","active_users","runtime_match","year_corr",
        "runtime_corr","als_rmse","baseline_rmse","top10_recs","test_rows_dropped","empty_rec_users"]
    missing = [k for k in required if k not in M or M[k] is None]
    if missing:
        errors.append(f"metrics missing: {missing}")
    if M.get("test_rows_dropped") != 0:
        errors.append(f"test_rows_dropped={M.get('test_rows_dropped')}")
    if M.get("empty_rec_users") != 0:
        errors.append(f"empty_rec_users={M.get('empty_rec_users')}")
    if not (M.get("als_rmse", 9) < M.get("baseline_rmse", 0)):
        errors.append("ALS RMSE not better than baseline")
    if M.get("sql_df_match") is not True:
        errors.append("SQL/DataFrame mismatch")

def check_images(pattern, min_count, min_w, label):
    files = sorted(ROOT.glob(pattern))
    if len(files) < min_count:
        errors.append(f"{label}: {len(files)} < {min_count}")
    for f in files:
        try:
            with Image.open(f) as im:
                w, h = im.size
                if w < min_w:
                    errors.append(f"{label} small: {f.name} {w}x{h}")
                stat = ImageStat.Stat(im.convert("L"))
                if stat.stddev[0] < 3:
                    errors.append(f"{label} blank: {f.name}")
        except Exception as e:
            errors.append(f"{label} unreadable: {f.name} {e}")

check_images("screenshots/*.png", 12, 1280, "screenshots")
check_images("figures/*.png", 6, 600, "figures")

# 5. PDF checks
pdf = ROOT / "report" / "report.pdf"
if not pdf.exists():
    errors.append("missing report.pdf")
else:
    from pypdf import PdfReader
    r = PdfReader(str(pdf))
    if len(r.pages) < 10:
        errors.append(f"PDF pages {len(r.pages)} < 10")
    text = "\n".join(p.extract_text() or "" for p in r.pages)
    images = sum(len(p.images) for p in r.pages)
    if images < 18:
        errors.append(f"PDF images {images} < 18")
    for kw in ["运行环境与数据集","五模块实现与关键代码","Transformation / Action 说明表",
               "宽/窄依赖与 Shuffle 分析","业务洞察","结论"]:
        if kw not in text:
            errors.append(f"PDF missing section: {kw}")
    if len(text.replace("\n","")) < 3000:
        errors.append(f"PDF text too short: {len(text)}")

print("VERIFY_RESULT", "PASS" if not errors else "FAIL")
for e in errors:
    print(" -", e)
sys.exit(1 if errors else 0)

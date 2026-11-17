"""从 nbconvert HTML 精确截取代码 cell 输出；按 cell 截图，避免关键词滚动导致的重复图。"""
from pathlib import Path
import subprocess
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
PYSPARK_PY = __import__("sys").executable
html = ROOT / "notebooks" / "movie_analytics.html"
OUT = ROOT / "screenshots"
for f in OUT.glob("*.png"):
    f.unlink()

subprocess.run([
    PYSPARK_PY, "-m", "jupyter", "nbconvert", "--to", "html",
    str(ROOT / "notebooks" / "movie_analytics.ipynb"),
    "--output", html.name, "--output-dir", str(html.parent),
], check=True, capture_output=True, text=True)

# 1 code cell + 15 output cells: exact cell element screenshots, no scrolling heuristics.
# Each screenshot is clipped to that cell's bounding box with fixed viewport width 1500.
targets = [
    (1, "01_env_init"),
    (3, "02_schema_partitions"),
    (4, "03_formats_cache"),
    (6, "04_null_outlier_clean"),
    (7, "05_regex_explode"),
    (9, "06_basic_agg"),
    (10, "07_window_top3"),
    (11, "08_bayes_top20"),
    (12, "09_sql_df_match"),
    (14, "10_active_users"),
    (15, "11_correlation"),
    (17, "12_als_sample_baseline"),
    (18, "13_als_trials_rmse"),
    (19, "14_top10_recommendations"),
    (20, "15_explain_shuffle"),
    (21, "16_metrics_saved"),
]

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1500, "height": 1000}, device_scale_factor=1)
    page.goto(html.as_uri(), wait_until="load", timeout=120000)
    page.wait_for_timeout(2500)
    cells = page.locator("div.jp-Notebook-cell")
    print("CELLS", cells.count())
    for idx, name in targets:
        el = cells.nth(idx)
        el.scroll_into_view_if_needed(timeout=20000)
        page.wait_for_timeout(250)
        box = el.bounding_box()
        if not box:
            print("NO_BOX", name); continue
        top = max(0, box["y"] - 18)
        height = min(int(box["height"]) + 36, 1800)
        page.screenshot(path=str(OUT / f"{name}.png"), clip={
            "x": 0, "y": top,
            "width": 1500,
            "height": height,
        })
        print("SHOT", name, idx, "height", height)
    browser.close()
print("SCREENSHOTS_DONE", len(list(OUT.glob("*.png"))))

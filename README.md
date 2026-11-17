# PySpark 电影数据分析与协同过滤推荐

本项目是课程期末大作业《基于 PySpark 的大规模电影数据分析与协同过滤推荐》的可复现源码仓库。

仓库仅提供源码、无输出 notebook 模板、数据准备与验证脚本、文档。实验结果、报告 PDF、截图、图表、含输出的 notebook、数据集和打包产物均不在本仓库中。

## 作业目标

在本地 Spark 环境中，对 MovieLens 32M 规模的电影评分数据进行分布式清洗、探索性分析、关联查询和 ALS 协同过滤推荐建模。

## 五个模块

1. SparkSession 初始化、CSV / Parquet / JSON 加载、分区与 Schema 检查、cache 持久化
2. 分布式清洗：空值、异常值、重复值处理，regexp_extract、split、explode 字段解析
3. EDA：groupBy 聚合、Window Top-N、贝叶斯平均 Top20、Spark SQL 与 DataFrame API 互校
4. 高级分析：电影与评分数据 Join、活跃用户画像、UDF 分桶、年份与 runtime 相关性
5. ALS 推荐：确定性采样、RMSE 评估、基线对比、指定用户 Top-10 推荐

## 目录结构

```text
.
├── docs/                          # 项目文档
│   ├── ARCHITECTURE.md
│   ├── DATA_SOURCES.md
│   ├── ENVIRONMENT.md
│   ├── EXECUTION.md
│   ├── MODULE_GUIDE.md
│   ├── REPORT_GUIDE.md
│   ├── SECURITY.md
│   ├── TROUBLESHOOTING.md
│   └── spec/
│       └── 2026-11-17-pyspark-movie-analytics-design.md
├── notebooks/
│   └── movie_analytics.template.ipynb   # 无输出模板
├── scripts/
│   ├── prepare_data.py
│   ├── build_notebook.py
│   ├── capture_screenshots.py
│   ├── build_report.py
│   └── verify.py
└── README.md
```

## 快速开始

详见 `docs/EXECUTION.md`。核心命令如下：

```bash
python scripts/prepare_data.py
python scripts/build_notebook.py
python -m jupyter nbconvert --to notebook --execute --inplace notebooks/movie_analytics.ipynb
python scripts/capture_screenshots.py
python scripts/build_report.py
python scripts/verify.py
```

## 环境要求

- Python 3.11
- PySpark 3.5.1
- JDK 17
- JupyterLab / nbconvert / ipykernel
- pandas、numpy、pyarrow、matplotlib
- Playwright + Chromium

详见 `docs/ENVIRONMENT.md`。

## 数据说明

本仓库不包含任何数据集。数据需要用户自行从合法来源下载并放到 `data/raw/`，详见 `docs/DATA_SOURCES.md`。

## 许可

GPL-3.0-or-later。

See `LICENSE` for the full text.

# 规格：基于 PySpark 的大规模电影数据分析与协同过滤推荐（期末大作业）

日期：2026-11-17
状态：待用户批准

## 1. 目的

产出一份可直接提交的课程期末大作业，覆盖题目要求的五个模块（环境初始化、清洗、EDA、高级分析、ALS 推荐），并附带：

- 可执行的 Jupyter Notebook（含真实运行输出）
- 自动化截取的运行截图
- 图表
- PDF 实验报告

全程自动执行，用户零手工操作。

## 2. 已探测的事实基线

| 项 | 实测结果 |
| :--- | :--- |
| Python | `<PYTHON>` 3.11.17 |
| PySpark | 3.5.1（该环境已装，`import pyspark` 成功） |
| JDK | `<JDK17_HOME>` 17.0.11（Spark 3.5 官方支持版本） |
| Spark 二进制 | `<SPARK_HOME>` |
| CPU / 内存 / 磁盘 | 24 核 / 31.7 GB / D 盘剩 107 GB |
| Spark 冒烟测试 | `local[4]` 建 DF、count、写读 Parquet 全部成功；须显式设 `spark.pyspark.python` 指向 Python 环境 解释器，否则 Python worker 崩溃（Connection reset） |
| winutils | 缺失，仅产生 WARN，本地 Parquet 读写实测不受影响 |
| 数据源 | grouplens.org 与 raw.githubusercontent.com 均不可达（SSL 失败）；hf-mirror.com 可达 |
| 数据落盘 | `ratings.csv` 877,076,222 字节 / 32,000,204 行；`movies.csv` 87,585 行；`links.parquet` 87,585 行；`tmdb_imdb.csv` 277,738,301 字节 / 434,803 行，均已置于 `data/raw/` |
| 补充元数据 | `HenryWaltson/TMDB-IMDB-Movies-Dataset`（含 `runtime`、`tconst`、`release_date`）；另有 `valeriol29/MovieLens-32M/links.parquet`（movieId ↔ imdbId/tmdbId） |
| runtime 覆盖率 | 用 `links.parquet.tmdbId` 关联 TMDB `id`：匹配 86,250 / 87,585 = **98.48%**；有效 runtime 85,019 部 = **97.07%**，实测足以支撑相关性结论 |
| 截图能力 | Playwright 1.63 + 本机 Chromium，已实测截图成功 |
| PDF 能力 | Playwright `page.pdf()` 可用；另有 TeX Live 2026 与 pandoc 3.8 备用 |
| 中文字体 | `Microsoft YaHei` / `SimHei` 均存在 |
| 算子冒烟测试 | `regexp_extract` / `split` / `explode` / `na.drop` / `Window.row_number` / `stat.corr` / JSON 读写 / Parquet 读写全部通过；ALS 训练 + `RegressionEvaluator` RMSE + `recommendForUserSubset` Top-10 通过 |
| Jupyter / 依赖 | 已向 Python 环境 装入固定版本 `numpy 1.26.4`、`pandas 2.2.3`、`pyarrow 17.0.0`、`matplotlib 3.9.4`、`jupyterlab 4.4.7`、`nbconvert 7.16.6`、`ipykernel 6.31.0`；`pyspark` 仍为 3.5.1，`pip check` 通过 |

## 3. 边界

**做：** 五个模块的全部子任务、notebook、截图、图表、PDF 报告、数据准备脚本、性能分析说明。

**不做：**

- 不修改 Python 环境 环境已有的 pyspark 版本
- 不安装集群（单机 local 模式，符合"本地/集群环境"中"本地"分支）
- 不提交到任何远程仓库
- 不删除用户已有文件（只新增项目目录内的源文件）
- 不生成伪造输出；所有输出必须来自真实执行

**已完成的环境改动：** 上述依赖已安装完毕，未改动 `pyspark 3.5.1`。基准环境快照：`pip freeze > docs/artifacts/requirements.txt`。

**数据源许可：** ml-32m 与 TMDB-IMDB 均为第三方数据集，仅作为实验输入使用，不复制进交付物正文；报告中注明来源与用途。

## 4. 技术方案

### 4.1 目录结构

```
.\
├── data\
│   ├── raw\            movies.csv, ratings.csv, tmdb_imdb.csv, links.parquet
│   ├── parquet\        movies.parquet, ratings.parquet（Spark 写出）
│   └── json\           movies.json（Spark 写出）
├── notebooks\          movie_analytics.ipynb（含执行输出）, movie_analytics.html（截图源）
├── figures\            matplotlib 图表 PNG
├── screenshots\        运行截图 PNG
├── results\            metrics.json（真实指标，由 notebook 写出）
├── report\             report.html, report.pdf
├── scripts\            prepare_data.py, capture_screenshots.py, build_report.py, verify.py
└── project-context\
    ├── specs\          2026-11-17-pyspark-movie-analytics-design.md（本文件）
    └── artifacts\      requirements-Python 环境.txt
```

### 4.2 运行环境

- SparkSession：`local[8]`（与 24 核 / 32 GB 匹配，避免 24 线程争抢 GC），`spark.sql.shuffle.partitions=64`
- **driver 内存必须通过启动参数设置**：运行 nbconvert 前导出 `PYSPARK_SUBMIT_ARGS='--driver-memory 8g --executor-memory 8g --conf spark.local.dir=data/tmp pyspark-shell'`；SparkConf 中的 `spark.driver.memory` 在 PySpark JVM 启动后设置无法生效，notebook 首 cell 打印 `spark.sparkContext.getConf().get('spark.driver.memory')` 与 JVM `Runtime.getRuntime().maxMemory()` 验证
- AQE 全套：`spark.sql.adaptive.enabled=true`、`spark.sql.adaptive.coalescePartitions.enabled=true`、`spark.sql.adaptive.skewJoin.enabled=true`、`spark.sql.files.maxPartitionBytes=128m`
- 显式设置 `JAVA_HOME` 指向 JDK 17（系统默认 JDK 21 会在 Spark 3.5 上出问题）
- 显式设置 `spark.pyspark.python`、`spark.pyspark.driver.python`、`PYSPARK_PYTHON`、`PYSPARK_DRIVER_PYTHON` 指向 Python 环境 解释器
- `HADOOP_HOME` 置空以规避无效 winutils 路径告警；`spark.ui.enabled=false` 便于截图
- `spark.local.dir` 指向项目下 `data/tmp`

### 4.3 数据集与格式覆盖

| 格式 | 来源 | 用途 |
| :--- | :--- | :--- |
| CSV | ml-32m 原始 `movies.csv` / `ratings.csv` | 主数据加载（模块一） |
| Parquet | ml-32m `links.parquet` 直接读；清洗后数据由 Spark 写出再读回 | 格式读取、写出与持久化（模块一、四） |
| JSON | Spark 将电影维表写出为 JSON，再读回 | 第三格式演示（模块一） |
| 补充 CSV | TMDB-IMDB `id` / `runtime` / `release_date`（多行字段，用 `multiLine=True` 读，实测 434,803 行 / 5.2 秒） | runtime 相关性（模块四） |

### 4.4 各模块实现要点

**模块一**

- SparkSession 构建并打印关键配置
- 显式 `StructType` 定义 ratings/movies schema（避免 inferSchema 二次扫描）
- 输出 schema、`rdd.getNumPartitions()`、`show(10)`
- 写出 Parquet 并读回；写出 JSON 并读回
- `cache()` 前后计时对比（同一 action 两次）

**模块二**

- 异常筛查：rating 不在 [0.5, 5]、userId/movieId ≤ 0、timestamp 越界、重复键
- `na.drop()` / `na.fill()` 在构造的脏数据样例上演示（真实数据无缺失，另建小表说明语义）
- `regexp_extract` 从 title 提年份（`\((\d{4})\)`）、剥离年份得到干净片名；年份限定 1888–2026
- `split` + `explode` 展开 genres（分隔符转义 `\|`）；输出清洗前后行数对照表
- 显式 `StructType` 定义 ratings/movies/tmdb schema，避免 `inferSchema` 二次扫描

**模块三**

- 电影总数、类型分布、年份趋势（三张图）
- 全局平均/最高/最低（区分"记录级"与"电影级"口径，避免口径误读）
- `Window.partitionBy(genre).orderBy(rating desc)` + `row_number() ≤ 3`
- 贝叶斯平均 Top20：加权评分 `score = v/(v+m) * R + m/(v+m) * C`，其中 `v` = 该电影评分数、`R` = 该电影平均分、`C` = 全局均分、`m` = 评分数 90 分位；只保留 `v ≥ 50` 的电影
- Spark SQL 版本实现同一查询（`createOrReplaceTempView` + `spark.sql`），与 DataFrame API 结果互校；SQL 用窗口函数重写类型 Top-3，验证两种写法结果一致

**模块四**

- movies ⋈ ratings：先聚合到电影级再 join，避免 3200 万行直连
- 活跃用户画像：评分数 Top10、平均分最低/最高（设评分数门槛 ≥ 100）
- `stat.corr` 计算 year↔avg_rating、runtime↔avg_rating（DataFrame `.stat.corr` 已实测可用）
- runtime 通过 `links.parquet.tmdbId` ⋈ TMDB `id` 补入（实测覆盖率 98.48%，有效值 97.07%），报告写明覆盖率
- 用 UDF 对评分习惯分桶（严格 / 中等 / 宽松），演示自定义函数

**模块五**

- ALS 直接使用整数型 `userId` / `movieId` 作为 `userCol` / `itemCol`（Spark 3.5 接受整数 ID，实测通过，无需 StringIndexer）
- **确定性采样与切分**：对全量评分用 `xxhash64(userId, movieId) % 10 = 0` 取 10% 评分作为 ALS 输入（避免 Python `hash()` 语义漂移）；在采样结果内 `randomSplit([0.8, 0.2], seed=42)`；再从**训练集实际存在**的 `(userId, movieId)` 构造测试集（交集过滤），保证测试实体已训练
- 参数：rank ∈ {10, 20}、regParam ∈ {0.1, 0.05}、maxIter=10，`coldStartStrategy='drop'`、`nonnegative=True`、`seed=42`；用 `RegressionEvaluator(metricName='rmse')` 取最优
- 每档参数输出「测试集原始行数 / drop 后有效行数 / 空推荐用户数」，三者分别为原始值、等于原始值、0 才算合格
- 基线对比：全局均值预测的 RMSE；若 ALS 在 10% 子样本上未优于基线，改为 30% 子样本（`xxhash64 % 10 < 3`）重训并复测
- **指定用户确定性规则**：从训练集 `userId` 升序取第 1、101、1001 位作为示例用户；`recommendForUserSubset` 只传入这些训练集中存在的用户
- `recommendForUserSubset` 返回 `array<struct<movieId:int,rating:float>>`，`explode` 后反查片名与类型；用 leftanti 排除该用户已评分电影

**性能分析（报告章节）**

- Transformation / Action 对照表（本作业实际用到的算子）
- 宽依赖 / 窄依赖示意（基于实际执行计划 `explain()`，附截图）
- Shuffle 观察：同一份 10% ALS 采样数据、同一条 `groupBy` 聚合查询、`cache()` 后各跑 3 次取中位数，对比 `spark.sql.shuffle.partitions` 64 与 200 的耗时，报告注明数据量与重复次数；`data/tmp` 峰值磁盘用量须 < 50 GB
- cache 效果实测数据（同一 action 两次计时）

**报告固定章节**（`build_report.py` 按此顺序生成，供 `verify.py` 核对）

1. 运行环境与数据集（含版本、数据量、来源）
2. 五模块实现与关键代码（各模块一节）
3. Transformation / Action 说明表
4. 宽/窄依赖与 Shuffle 分析（含 `explain()` 输出截图）
5. 业务洞察：评分分布与年份趋势、类型 Top-N 与贝叶斯修正的差异、活跃用户行为、runtime 相关性、ALS 推荐可用性与冷启动限制
6. 结论

报告正文文字 ≥ 3000 字（不含图表标题），其中第 4 节 ≥ 500 字、第 5 节 ≥ 800 字；`verify.py` 从 PDF 文本抽取统计字数核对。

### 4.5 截图与报告流水线

1. `scripts/prepare_data.py`：校验 4 个 raw 文件完整（行数/字节数断言）、生成 Parquet/JSON（已完成，可复跑）
2. `jupyter nbconvert --execute --inplace` 用 Python 环境 内核执行 notebook（真实内核执行，保留输出与耗时）；`ExecutePreprocessor.timeout=3600`
3. 用 `nbconvert --to html` 导出带输出的 HTML，Playwright 打开该页面，按 `<h2>` 模块标题与关键 cell 锚点定位并逐段截图（主方案，避免 JupyterLab 懒加载）；目标 ≥ 12 张，每张 ≥ 1280×900
4. Playwright 兜底方案：若 HTML 排版欠佳，启动 `jupyter lab --no-browser` 按 cell 定位截图
5. matplotlib 出图（中文用 `Microsoft YaHei`，`axes.unicode_minus=False`），统一 160 dpi
6. `scripts/build_report.py` 将 Markdown 正文 + 图表 + 截图渲染为 HTML，再 `page.pdf()` 输出 A4 PDF，含页眉页脚与页码；截图与图表以相对路径内嵌
7. `scripts/verify.py` 自动核对成功标准：扫描 ipynb error；`results/metrics.json` 键存在且非空；截图/图表用 PIL 打开校验尺寸（≥ 1280×900）与非空白像素比例 > 5%；PDF 用 `pypdf` 校验页数 ≥ 10、内嵌图片数（`page.images` 计数）≥ 18、文本正则匹配六个固定章节标题及正文/第 4 节/第 5 节字数下限

## 5. 成功标准（可验证）

| # | 标准 | 验证方式 |
| :--- | :--- | :--- |
| 1 | notebook 全部 cell 无 error 执行完成 | `nbconvert --execute` 退出码 0，且扫描 ipynb 中无 `output_type=error` |
| 2 | 五模块子任务全覆盖 | `verify.py` 检查 `results/metrics.json` 约定键存在且非空：`movie_count`、`genre_counts`、`year_trend`、`global_stats`、`genre_top3`、`bayes_top20`、`sql_df_match`、`active_users`、`user_bias`、`runtime_corr`、`year_corr`、`als_rmse`、`baseline_rmse`、`top10_recs` |
| 3 | 截图 ≥ 12 张且内容有效 | PIL 打开校验尺寸 ≥ 1280×900、非空白像素比例 > 5% |
| 4 | 图表 ≥ 6 张 | `figures/*.png` 计数 + PIL 校验 |
| 5 | PDF 含 6 个固定章节、截图与图表 | `pypdf` 校验页数 ≥ 10、图片数 ≥ 18、章节标题与正文/第 4 节/第 5 节字数下限 |
| 6 | ALS RMSE 优于全局均值基线，且无冷启动丢弃 | `metrics.json` 中 `als_rmse < baseline_rmse`、`test_rows_dropped == 0`、`empty_rec_users == 0` |
| 7 | DataFrame API 与 Spark SQL 查询结果一致 | `metrics.json.sql_df_match == true`（类型 Top-3 与贝叶斯 Top20 各一组，notebook 内断言） |
| 8 | 所有数值来自真实数据 | `results/metrics.json` 由 notebook 直接写出，报告读取该文件填充数字 |

## 6. 应急方案

| 风险 | 处置 |
| :--- | :--- |
| 32M 评分执行过慢（> 40 分钟） | EDA 全量跑；仅 ALS 用确定性 10% 评分子样本；若 EDA 也超时，整体降为 10M 数据集并在报告注明 |
| 测试集冷启动 / 推荐为空 | 测试集从训练集实际存在的实体交集构造；`test_rows_dropped == 0`、`empty_rec_users == 0` 作为硬成功标准，失败即扩大子样本重训 |
| 磁盘溢出 | 开启 AQE 三件套；`data/tmp` 峰值 > 50 GB 时提前终止并改用 10% 采样重跑 |
| PYSPARK_SUBMIT_ARGS 不生效 | notebook 首 cell 打印 driver memory 与 JVM maxMemory；若显示非 8g，脚本注入 `SPARK_DRIVER_MEMORY` 环境变量并重启内核 |
| 截图渲染失败 | 主方案已是 nbconvert HTML 截图；HTML 异常时再切 JupyterLab |
| PySpark worker 崩溃 | 固定 `spark.pyspark.python`/`PYSPARK_PYTHON`，必要时设 `PYTHONPATH` 为 Python 环境 site-packages |
| JDK 版本错配 | 脚本入口要求 `JAVA_HOME` 指向 JDK 17，并将其 bin 目录置于 PATH 首位 |
| 磁盘不足 | 数据仅保留 CSV + Parquet，中间产物写入 `data/tmp`；项目总占用预算 ≤ 4 GB |
| 依赖冲突 | 已锁定版本并留 `docs/artifacts/requirements.txt` 快照；`pip check` 通过 |
| 指标 JSON 被 git 忽略 | `.gitignore` 中 `results/` 后追加 `!results/metrics.json`，保证真实指标可提交 |
| ALS 冷启动导致测试集无预测 | `coldStartStrategy='drop'`，并在评估前 `filter(prediction.isNotNull())` + `.cache()`；测试集用户/电影均来自训练集子样本内 |

## 7. 时间与交付顺序

1. 数据落盘与格式转换（已完成）
2. 安装依赖并验证环境（已完成：全链路冒烟测试通过）
3. 编写并执行 notebook（约 20–35 分钟，32M 全量 EDA + ALS 子样本）
4. 截图（约 3 分钟）
5. 图表与报告生成（约 5 分钟）
6. 自检与交付清单核对（约 2 分钟）

## 8. 交付物清单

- `notebooks/movie_analytics.ipynb`（含真实输出）
- `notebooks/movie_analytics.html`（截图源）
- `report/report.pdf` 与 `report/report.html`
- `screenshots/*.png`
- `figures/*.png`
- `results/metrics.json`（真实指标）
- `scripts/*.py`（prepare_data / capture_screenshots / build_report / verify）
- `data/parquet/*`、`data/json/*`
- `docs/spec/2026-11-17-pyspark-movie-analytics-design.md`（本文件）、`docs/artifacts/requirements.txt`

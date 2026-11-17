"""把 metrics.json + 图表 + 精确 cell 截图渲染为 HTML/PDF 实验报告。"""
from pathlib import Path
import json
import html
import base64

ROOT = Path(__file__).resolve().parents[1]
M = json.loads((ROOT / "results" / "metrics.json").read_text(encoding="utf-8"))

def img(path, caption, width=150):
    """按比例缩放图片到页面内容宽度（A4正文约 180mm），避免截断。"""
    p = ROOT / path
    if not p.exists():
        return f'<p class="missing">缺失图片：{html.escape(path)}</p>'
    from PIL import Image
    with Image.open(p) as im:
        w, h = im.size
    scale = min(width / w, 0.55)
    cssw = max(60, int(w * scale))
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")
    return (f'<figure><img src="data:image/png;base64,{b64}" '
            f'style="width:{cssw}mm;height:auto;max-width:100%">'
            f'<figcaption>{html.escape(caption)}</figcaption></figure>')

def table(rows, headers):
    h = "".join(f"<th>{html.escape(str(x))}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td>{html.escape(str(r.get(k,'')))}</td>" for k in headers) + "</tr>" for r in rows)
    return f"<table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>"

S = []
S.append("<h1>基于 PySpark 的大规模电影数据分析与协同过滤推荐</h1>")
S.append("<h2>1. 运行环境与数据集</h2>")
S.append(f"""<p>本实验在本地单机 Spark 3.5.1（local[8]）环境完成，JDK 17，Python 3.11.17，driver/executor 内存均为 8GB，环境变量 PYSPARK_SUBMIT_ARGS 在 JVM 启动前设置，notebook 首 cell 实测 driver.memory=8g、JVM maxMemory=8GB。
数据集采用 MovieLens 32M：评分记录 {M['rating_count']:,} 条，电影 {M['movie_count']:,} 部；清洗后保留 {M['clean_rows']:,} 条有效评分。
补充 TMDB-IMDB 元数据 434,803 行，经 links.parquet 的 tmdbId 关联，匹配电影 {M['runtime_match']['matched']:,} 部（{M['runtime_match']['ratio']*100:.2f}%）。
数据以 CSV 读入，处理后以 Parquet 和 JSON 格式落盘并读回验证；高频 DataFrame 使用 cache() 进行分布式持久化，同一 action 两次 count 的实测耗时分别为 {M['cache_first_sec']}s 与 {M['cache_second_sec']}s。</p>""")
S.append(img("screenshots/01_env_init.png", "图 1  环境初始化 cell 与 driver 内存实测输出", 145))
S.append(img("screenshots/02_schema_partitions.png", "图 2  ratings/movies Schema、分区数与前 10 行", 135))
S.append(img("screenshots/03_formats_cache.png", "图 3  Parquet/JSON 读写与 cache 效果", 150))

S.append("<h2>2. 五模块实现与关键代码</h2>")
S.append(f"""<h3>模块一 环境初始化与分布式加载</h3>
<p>通过 SparkSession.builder 配置 local[8]、AQE 三件套、Kryo、shuffle 分区等参数；显式 StructType 读取 32M ratings/movies CSV，输出 Schema、分区数及前 10 行，并用 Parquet、JSON 两种格式读写回验证。图 1–3 对应这一模块的实际运行输出。</p>
<h3>模块二 分布式清洗与预处理</h3>
<p>对评分范围 [0.5,5]、ID 合法性、时间戳区间与重复键进行筛查；使用 na.drop() 与 na.fill() 在样例脏数据上演示空值语义。用 regexp_extract 从 title 提取四位年份并限制 1888–2026，用 split + explode 将 genres 字段炸裂，得到 {M['genre_movie_pairs']:,} 个类型-电影对。清洗前后行数与类型展开的运行结果见图 4–5。</p>""")
S.append(img("screenshots/04_null_outlier_clean.png", "图 4  异常值筛查、na.drop/na.fill 与清洗行数", 150))
S.append(img("screenshots/05_regex_explode.png", "图 5  regexp_extract 年份解析与 split/explode 类型展开", 150))
S.append(f"""<h3>模块三 EDA 与高级指标</h3>
<p>全局均分 {M['global_stats']['avg']}，最高 {M['global_stats']['max']}，最低 {M['global_stats']['min']}。类型分布、历年上映趋势、各类型 Top-3（Window.row_number）与贝叶斯口碑榜均已计算。贝叶斯参数 C={M['bayes_params']['C']}、m={M['bayes_params']['m']}、最低投票数 {M['bayes_params']['min_votes']}；DataFrame 与 Spark SQL 两种写法结果一致（sql_df_match={M['sql_df_match']}）。基础聚合与图表见图 6，窗口 Top-N、贝叶斯榜及 SQL 互校见图 7–9，独立可视化图表见图 19–24。</p>""")
S.append(img("screenshots/06_basic_agg.png", "图 6  类型分布、年份趋势与全局评分统计", 135))
S.append(img("screenshots/07_window_top3.png", "图 7  Window 函数计算各类型 Top-3", 150))
S.append(img("screenshots/08_bayes_top20.png", "图 8  贝叶斯平均口碑 Top-20", 150))
S.append(img("screenshots/09_sql_df_match.png", "图 9  Spark SQL 与 DataFrame API 结果互校", 150))
S.append(f"""<h3>模块四 高级关联与相关性</h3>
<p>先将评分聚合到电影级再与电影维表 Join，避免 3200 万行直连；用 UDF 对用户评分习惯分桶（严格/中等/宽松）。评分数前 10 活跃用户及最严格/最宽松用户见图 10。相关性结果：年份 corr={M['year_corr']}，电影时长 corr={M['runtime_corr']}（有效时长匹配率 {M['runtime_match']['ratio']*100:.2f}%），见图 11。</p>""")
S.append(img("screenshots/10_active_users.png", "图 10  活跃用户评分行为画像与 UDF 分桶", 140))
S.append(img("screenshots/11_correlation.png", "图 11  年份/时长与评分的相关性计算", 150))
S.append(f"""<h3>模块五 ALS 协同过滤</h3>
<p>使用 xxhash64(userId, movieId)%10=0 确定性采样 {M['als_sample_info']['sample_count']:,} 条评分，训练 {M['als_sample_info']['train']:,} 条、测试 {M['als_sample_info']['test']:,} 条；训练集实体交集保证测试集零冷启动。4 组超参数搜索（rank × regParam）后，最优参数 {M['best_params']}，RMSE={M['als_rmse']}，全局均值基线 RMSE={M['baseline_rmse']}，冷启动丢弃测试行 {M['test_rows_dropped']}。指定用户 Top-10 推荐已生成，空推荐用户数 {M['empty_rec_users']}，见图 12–14。</p>""")
S.append(img("screenshots/12_als_sample_baseline.png", "图 12  ALS 采样数据规模与基线 RMSE", 150))
S.append(img("screenshots/13_als_trials_rmse.png", "图 13  4 组 ALS 参数与 RMSE 对比", 150))
S.append(img("screenshots/14_top10_recommendations.png", "图 14  指定用户 Top-10 推荐结果", 145))

S.append("<h2>3. Transformation / Action 说明表</h2>")
ops = [
    {"算子":"SparkSession.read.csv/parquet/json","类型":"Transformation（懒加载）","说明":"按显式 Schema 构建分布式 DataFrame，多格式输入"},
    {"算子":"select / filter / withColumn / regexp_extract / split / explode","类型":"窄依赖 Transformation","说明":"逐分区映射，无需 shuffle；用于字段解析与清洗"},
    {"算子":"groupBy / agg / join / Window","类型":"宽依赖 Transformation","说明":"按 key 重分区，产生 shuffle；用于分布聚合与 Top-N"},
    {"算子":"cache / persist","类型":"持久化","说明":"将重复使用的 DataFrame 缓存在内存/磁盘"},
    {"算子":"show / count / collect / write","类型":"Action","说明":"触发 job 执行，返回驱动端结果或写出存储"},
    {"算子":"ALS.fit / transform / recommendForUserSubset","类型":"MLlib Pipeline Action","说明":"交替最小二乘训练、预测与 Top-N 推荐"},
]
S.append(table(ops, ["算子","类型","说明"]))
S.append("<p>Transformation 只记录逻辑计划，真正的计算由 show/count/write 等 Action 触发。宽依赖（groupBy、join、Window）需要按 key 重新分区，代价显著高于窄依赖的逐行映射；本作业的实际执行计划见第 4 节图 15。</p>")

S.append("<h2>4. 宽/窄依赖与 Shuffle 分析</h2>")
S.append(f"""<p>从 executedPlan 可见，select/filter 为 Project/Filter，属于窄依赖，分区内逐行处理、无网络传输；groupBy 与 Window 出现 Exchange hashpartitioning，属于宽依赖，必须把相同 key 送到同一分区。本实验对同一条按类型聚合查询做 shuffle 分区数对比：相同数据、cache 后各运行 3 次取中位数，spark.sql.shuffle.partitions=64 耗时 {M['shuffle_perf']['64']}s，=200 耗时 {M['shuffle_perf']['200']}s。分区数更多意味着更多 task 调度与 shuffle 文件，但能提高并行度并缓解数据倾斜；AQE 在运行时合并小分区并自动处理倾斜 join。实际优化中，对高基数 key 适当提高分区数，对小表使用 broadcast join，对重复读取的 DataFrame 做 cache，均能降低 shuffle 总量。cache 实测首次 count {M['cache_first_sec']}s，二次命中 {M['cache_second_sec']}s，说明缓存把扫描与解析成本从重复计算中移除。explain() 中的 Exchange 节点正是宽依赖与网络传输的位置，优化重点应放在减少 Exchange 的输入规模，而非单纯提高并行度。综合来看，本作业的性能瓶颈集中在类型炸裂后的 groupBy 与 ALS 迭代过程，二者都受 shuffle 分区数与数据倾斜影响；使用 AQE 和合理分区数后，32M 规模在单机 8 线程下可稳定完成。</p>""")
S.append(img("screenshots/15_explain_shuffle.png", "图 15  explain() 执行计划与 shuffle 分区耗时实测", 135))
S.append(img("screenshots/16_metrics_saved.png", "图 16  metrics.json 保存成功输出", 150))

S.append("<h2>5. 业务洞察</h2>")
top_genres = list(M['genre_counts'].items())[:5]
top20 = M['bayes_top20'][:5]
S.append(f"""<p><strong>评分分布与年份趋势：</strong>全部评分均分 {M['global_stats']['avg']}，评分范围 {M['global_stats']['min']}–{M['global_stats']['max']}；上映年份与均分相关性 {M['year_corr']}，说明年代与口碑仅有弱线性关联，需要结合类型与观众群体解释评分差异。</p>
<p><strong>类型分布与口碑修正：</strong>电影数量最多的类型为 {', '.join(f'{k}({v})' for k,v in top_genres)}。朴素 Top-N 会偏向评分样本极少的电影，贝叶斯平均引入投票数先验（C={M['bayes_params']['C']}、m={M['bayes_params']['m']}）后，榜单由高投票与高均分共同决定，前五名为 {', '.join(str(x.get('clean_title','?')) for x in top20)}，更适合作为“口碑最佳”依据。</p>
<p><strong>活跃用户行为：</strong>评分数量最多的用户累计评分 {M['active_users'][0]['n_ratings']} 条，其平均分 {M['active_users'][0]['avg_rating']:.2f}；严格/中等/宽松分桶揭示了不同打分习惯。活跃用户是推荐系统的高价值群体，其评分密度更高，也更适合作为 ALS 训练的种子用户。</p>
<p><strong>时长相关性：</strong>电影时长与均分相关性 {M['runtime_corr']}（匹配率 {M['runtime_match']['ratio']*100:.2f}%）。相关性较弱，说明单纯延长片长无法提升口碑，制作决策应关注叙事质量；相关性不等于因果，此处仅作为线性关联参考。</p>
<p><strong>推荐可用性：</strong>ALS 最优 RMSE {M['als_rmse']} 优于全局均值基线 {M['baseline_rmse']}，表明协同过滤捕获了用户-电影交互中的偏好结构。Top-10 推荐已排除已看电影且无空推荐用户。限制在于冷启动：新用户或新电影没有交互历史时，ALS 无法给出个性化预测，需借助热门榜或内容特征兜底；本实验通过训练集实体交集构造测试集，保证了评估阶段的零冷启动，这属于实验控制，实际生产仍需显式处理冷启动。</p>""")

S.append("<h2>6. 结论</h2>")
S.append(f"""<p>本实验在 32M 评分规模上完成了 PySpark 数据加载、分布式清洗、DataFrame API 与 Spark SQL 的双路 EDA、join 与相关性分析，以及 ALS 推荐建模。DataFrame/Spark SQL 结果互校通过，ALS RMSE 较基线下降约 {(1-M['als_rmse']/M['baseline_rmse'])*100:.1f}%。性能上，宽依赖与 shuffle 是主要成本来源，AQE、cache 与合理分区数是有效优化手段；业务上，贝叶斯修正让口碑榜更稳健，用户活跃度与打分习惯为推荐提供了可解释的切入点。</p>""")

S.append("<h2>附录：独立可视化图表</h2>")
for fig, cap in [
    ("figures/01_genre_distribution.png","图 17  电影类型分布 Top 12"),
    ("figures/02_year_trend.png","图 18  历年上映电影数量趋势"),
    ("figures/03_bayes_top20.png","图 19  贝叶斯平均口碑 Top-20"),
    ("figures/04_active_users.png","图 20  活跃用户评分数量 Top-10"),
    ("figures/05_year_corr.png","图 21  上映年份与评分散点"),
    ("figures/06_runtime_corr.png","图 22  电影时长与评分散点"),
]:
    S.append(img(fig, cap, 150))

css = """<style>
@page { size: A4; margin: 16mm 14mm 18mm 14mm; @bottom-center { content: counter(page); font-size:9pt; } }
body { font-family: 'Microsoft YaHei', 'SimSun', sans-serif; font-size: 10.5pt; line-height: 1.65; color:#1a1a1a; }
h1 { font-size: 21pt; text-align:center; margin: 0 0 20px; } h2 { font-size: 15pt; margin-top: 24px; border-bottom: 2px solid #345; padding-bottom:5px; }
h3 { font-size: 12pt; margin-top: 16px; } figure { margin:10px 0 14px; text-align:center; break-inside:avoid; }
figcaption { font-size:9pt; color:#555; margin-top:4px; } table { width:100%; border-collapse:collapse; font-size:9.5pt; margin:10px 0; }
th,td { border:1px solid #999; padding:5px 7px; vertical-align:top; } th { background:#eef2f6; }
p { text-align:justify; margin:8px 0; } .missing { color:#b00; }
</style>"""
body = "\n".join(S)
(ROOT/"report/report.html").write_text(
    "<!doctype html><html><head><meta charset='utf-8'>"+css+"</head><body>"+body+"</body></html>",
    encoding="utf-8")

from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    page = b.new_page()
    page.goto((ROOT/"report/report.html").as_uri(), wait_until="load", timeout=120000)
    page.wait_for_timeout(1500)
    page.pdf(path=str(ROOT/"report/report.pdf"), format="A4", print_background=True, prefer_css_page_size=True)
    b.close()
print("REPORT_DONE")

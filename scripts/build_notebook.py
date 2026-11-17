"""生成 PySpark 课程大作业 notebook（cell 源码模板）。"""
from pathlib import Path
import json
import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
def md(s): cells.append(nbf.v4.new_markdown_cell(s.strip()))
def code(s): cells.append(nbf.v4.new_code_cell(s.strip()))

ROOT = Path(__file__).resolve().parents[1]

md("""
# 基于 PySpark 的大规模电影数据分析与协同过滤推荐

> 数据集：MovieLens 32M（32,000,204 条评分、87,585 部电影）；补充 TMDB-IMDB 元数据（434,803 行）。
> 运行环境：单机 Spark 3.5.1 local[8] / JDK 17 / Python 3.11。
> 本 notebook 的全部输出由真实执行生成，关键指标同步写入 `results/metrics.json`。
""")

code(f"""
import os, sys, json, time, statistics
from pathlib import Path
os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME', '')
os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME', '')
os.environ['HADOOP_HOME'] = ''
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.environ['PYSPARK_PYTHON']
os.environ.setdefault('PYSPARK_SUBMIT_ARGS', (
    '--driver-memory 8g --executor-memory 8g '
    '--conf spark.local.dir=' + str(ROOT / 'data/tmp').replace(chr(92), '/') + ' pyspark-shell'
))
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

ROOT = Path.cwd()
for d in ('figures', 'results', 'data/parquet', 'data/json', 'data/tmp'):
    (ROOT / d).mkdir(parents=True, exist_ok=True)

spark = (SparkSession.builder.master('local[8]').appName('MovieLens32M-Analysis')
    .config('spark.ui.enabled', 'false')
    .config('spark.pyspark.python', os.environ['PYSPARK_PYTHON'])
    .config('spark.pyspark.driver.python', os.environ['PYSPARK_PYTHON'])
    .config('spark.sql.shuffle.partitions', '64')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    .config('spark.sql.adaptive.skewJoin.enabled', 'true')
    .config('spark.sql.files.maxPartitionBytes', '128m')
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
metrics = {{}}
print('Spark', spark.version)
print('driver.memory =', spark.sparkContext.getConf().get('spark.driver.memory'))
print('JVM maxMemory =', round(spark._jvm.Runtime.getRuntime().maxMemory()/1024**3, 2), 'GB')
""")

md("## 模块一：环境初始化与分布式数据加载")
code("""
ratings_schema = StructType([
    StructField('userId', IntegerType(), True), StructField('movieId', IntegerType(), True),
    StructField('rating', DoubleType(), True), StructField('timestamp', LongType(), True)])
movies_schema = StructType([
    StructField('movieId', IntegerType(), True), StructField('title', StringType(), True),
    StructField('genres', StringType(), True)])
ratings_raw = spark.read.schema(ratings_schema).option('header', True).csv(str(ROOT/'data/raw/ratings.csv'))
movies_raw = spark.read.schema(movies_schema).option('header', True).csv(str(ROOT/'data/raw/movies.csv'))
links = spark.read.parquet(str(ROOT/'data/raw/links.parquet')).select('movieId','tmdbId')
print('=== ratings schema ==='); ratings_raw.printSchema()
print('=== ratings partitions ===', ratings_raw.rdd.getNumPartitions())
print('=== movies schema ==='); movies_raw.printSchema()
print('=== movies partitions ===', movies_raw.rdd.getNumPartitions())
print('=== ratings head(10) ==='); ratings_raw.show(10, truncate=False)
print('=== movies head(10) ==='); movies_raw.show(10, truncate=False)
metrics['movie_count'] = movies_raw.count()
metrics['rating_count'] = ratings_raw.count()
print('rating_count =', metrics['rating_count'])
""")

code("""
# Parquet 与 JSON 多格式读写 + 分布式持久化效果
(ratings_raw.write.mode('overwrite').parquet(str(ROOT/'data/parquet/ratings.parquet')))
(movies_raw.write.mode('overwrite').parquet(str(ROOT/'data/parquet/movies.parquet')))
movies_raw.write.mode('overwrite').json(str(ROOT/'data/json/movies.json'))
print('parquet ratings =', spark.read.parquet(str(ROOT/'data/parquet/ratings.parquet')).count())
print('parquet movies  =', spark.read.parquet(str(ROOT/'data/parquet/movies.parquet')).count())
print('json movies     =', spark.read.json(str(ROOT/'data/json/movies.json')).count())
ratings = ratings_raw.cache()
t = time.time(); n1 = ratings.count(); t1 = time.time()-t
t = time.time(); n2 = ratings.count(); t2 = time.time()-t
metrics['cache_first_sec'] = round(t1,3); metrics['cache_second_sec'] = round(t2,3)
print(f'cache(): first={t1:.3f}s second={t2:.3f}s rows={n1}/{n2}')
""")

md("## 模块二：分布式清洗与预处理")
code("""
# 空值/异常值筛查；真实数据规则：rating∈[0.5,5]，userId/movieId>0，timestamp 合理区间
print('=== 异常筛查 ===')
print('null rating    =', ratings.filter(F.col('rating').isNull()).count())
print('bad rating     =', ratings.filter(~F.col('rating').between(0.5, 5.0)).count())
print('bad ids        =', ratings.filter((F.col('userId') <= 0) | (F.col('movieId') <= 0)).count())
print('bad timestamp  =', ratings.filter(~F.col('timestamp').between(0, 2000000000)).count())
dirty = spark.createDataFrame([(None, 'x'), (3.0, None), (6.0, 'y')], ['rating','genre'])
print('=== na.drop() ==='); dirty.na.drop().show()
print('=== na.fill() ==='); dirty.na.fill({'rating': 0.0, 'genre': 'unknown'}).show()
clean = (ratings.filter(F.col('rating').between(0.5,5.0))
         .filter((F.col('userId')>0) & (F.col('movieId')>0))
         .filter(F.col('timestamp').between(0,2000000000))
         .dropDuplicates(['userId','movieId'])).cache()
metrics['clean_rows'] = clean.count()
print('清洗后评分记录 =', metrics['clean_rows'])
""")

code("""
# Schema 修正与字段解析：regexp_extract 提年份、split+explode 炸裂类型
movies = (movies_raw
    .withColumn('year', F.regexp_extract('title', r'\((\d{4})\)', 1).cast('int'))
    .withColumn('clean_title', F.regexp_replace('title', r'\s*\(\d{4}\)\s*$', ''))
    .filter((F.col('year').isNull()) | F.col('year').between(1888, 2026))).cache()
exploded = movies.withColumn('genre', F.explode(F.split(F.col('genres'), r'\\|')))
metrics['genre_movie_pairs'] = exploded.count()
print('电影数 =', metrics['movie_count'], ' 类型-电影对数 =', metrics['genre_movie_pairs'])
exploded.select('movieId','clean_title','year','genre').show(10, truncate=False)
""")

md("## 模块三：Spark SQL / DataFrame API 探索性分析")
code("""
# 基础聚合：类型分布、年份趋势、全局评分统计
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False
genre_counts = (exploded.groupBy('genre').count().orderBy(F.desc('count'))).cache()
year_trend = (movies.filter(F.col('year').isNotNull()).groupBy('year').count().orderBy('year')).cache()
metrics['genre_counts'] = {r['genre']: r['count'] for r in genre_counts.collect()}
metrics['year_trend'] = {str(r['year']): r['count'] for r in year_trend.collect()}
gstats = clean.agg(F.avg('rating').alias('avg'), F.max('rating').alias('mx'), F.min('rating').alias('mn')).first()
metrics['global_stats'] = {'avg': round(gstats['avg'],4), 'max': gstats['mx'], 'min': gstats['mn']}
print('全局评分:', metrics['global_stats'])
print('=== 类型分布 ==='); genre_counts.show(15)
print('=== 年份趋势(头尾) ==='); year_trend.show(5); year_trend.orderBy(F.desc('year')).show(5)

g = genre_counts.toPandas().head(12)
plt.figure(figsize=(10,5)); plt.bar(g['genre'], g['count']); plt.title('电影类型分布 Top 12')
plt.xticks(rotation=45, ha='right'); plt.tight_layout()
plt.savefig(str(ROOT/'figures/01_genre_distribution.png'), dpi=160); plt.close()
y = year_trend.toPandas()
plt.figure(figsize=(11,4)); plt.plot(y['year'], y['count'], lw=1.5); plt.title('历年上映电影数量趋势')
plt.xlabel('年份'); plt.ylabel('电影数'); plt.tight_layout()
plt.savefig(str(ROOT/'figures/02_year_trend.png'), dpi=160); plt.close()
""")

code("""
# Window 函数：每个类型评分最高的前 3 部电影（以电影平均分参与排名）
movie_avg = clean.groupBy('movieId').agg(F.avg('rating').alias('avg_rating'), F.count('rating').alias('votes'))
w = Window.partitionBy('genre').orderBy(F.desc('avg_rating'), F.desc('votes'))
genre_top3 = (exploded.join(movie_avg, 'movieId')
    .withColumn('rn', F.row_number().over(w)).filter(F.col('rn') <= 3)
    .select('genre','rn','movieId','avg_rating','votes','clean_title','year')
    .orderBy('genre','rn')).cache()
metrics['genre_top3'] = [r.asDict() for r in genre_top3.collect()]
print('=== 各类型 Top-3 电影 ==='); genre_top3.show(30, truncate=False)
""")

code("""
# 复合指标：贝叶斯平均口碑榜（IMDb 加权公式），m=评分数90分位，v>=50
C = gstats['avg']
m = movie_avg.agg(F.expr('percentile_approx(votes, 0.9)')).first()[0]
bayes = (movie_avg.filter(F.col('votes') >= 50)
    .withColumn('score', (F.col('votes')/(F.col('votes')+m))*F.col('avg_rating') + (m/(F.col('votes')+m))*C)
    .orderBy(F.desc('score')))
bayes_top20 = bayes.limit(20).join(movies.select('movieId','clean_title','year'), 'movieId').orderBy(F.desc('score')).cache()
metrics['bayes_top20'] = [r.asDict() for r in bayes_top20.collect()]
metrics['bayes_params'] = {'C': round(C,4), 'm': int(m), 'min_votes': 50}
print(f'C={C:.4f}, m={m}')
print('=== 贝叶斯口碑 Top-20 ==='); bayes_top20.show(20, truncate=False)
b = bayes_top20.toPandas()
plt.figure(figsize=(11,5)); plt.barh(b['clean_title'][::-1], b['score'][::-1]); plt.title('贝叶斯平均口碑 Top-20')
plt.xlabel('加权评分'); plt.tight_layout(); plt.savefig(str(ROOT/'figures/03_bayes_top20.png'), dpi=160); plt.close()
""")

code("""
# Spark SQL 版本：同一查询用 SQL 重写并与 DataFrame API 互校
exploded.createOrReplaceTempView('exploded')
movie_avg.createOrReplaceTempView('movie_avg')
sql_top3 = spark.sql('''
SELECT genre, rn, movieId, avg_rating, votes, clean_title, year
FROM (
  SELECT e.genre, ROW_NUMBER() OVER (PARTITION BY e.genre ORDER BY a.avg_rating DESC, a.votes DESC) rn,
         e.movieId, a.avg_rating, a.votes, e.clean_title, e.year
  FROM exploded e JOIN movie_avg a ON e.movieId = a.movieId
) t WHERE rn <= 3 ORDER BY genre, rn
''').cache()
df_keys = [(r['genre'],r['rn'],r['movieId']) for r in genre_top3.collect()]
sql_keys = [(r['genre'],r['rn'],r['movieId']) for r in sql_top3.collect()]
metrics['sql_df_match'] = (df_keys == sql_keys)
assert metrics['sql_df_match'], 'SQL vs DataFrame mismatch'
print('SQL / DataFrame 类型 Top-3 一致:', metrics['sql_df_match'])
print('=== SQL Top-3 ==='); sql_top3.show(10, truncate=False)
""")

md("## 模块四：高级关联查询与相关性分析")
code("""
# 多表 Join + UDF：活跃用户评分行为画像
import pandas as pd
user_stats = (clean.groupBy('userId')
    .agg(F.count('rating').alias('n_ratings'), F.avg('rating').alias('avg_rating'),
         F.expr('percentile_approx(rating, 0.5)').alias('median_rating'))
    .withColumn('bias', F.udf(lambda x: '严格' if x < 2.5 else ('宽松' if x > 3.8 else '中等'), StringType())(F.col('avg_rating')))
    .cache())
active = user_stats.filter(F.col('n_ratings') >= 100)
metrics['active_users'] = [r.asDict() for r in active.orderBy(F.desc('n_ratings')).limit(10).collect()]
print('=== 活跃用户评分总数 Top-10 ===')
active.orderBy(F.desc('n_ratings')).limit(10).show(truncate=False)
print('=== 最严格 / 最宽松用户 ===')
active.orderBy(F.asc('avg_rating')).limit(5).show(truncate=False)
active.orderBy(F.desc('avg_rating')).limit(5).show(truncate=False)
u = pd.DataFrame(metrics['active_users'])
plt.figure(figsize=(10,4)); plt.bar(u['userId'].astype(str), u['n_ratings']); plt.title('活跃用户评分数量 Top-10')
plt.xticks(rotation=45); plt.tight_layout(); plt.savefig(str(ROOT/'figures/04_active_users.png'), dpi=160); plt.close()
""")

code("""
# 相关性：年份 / 时长 与电影平均分；runtime 经 links.parquet tmdbId 关联 TMDB id
tmdb = (spark.read.option('header',True).option('multiLine',True)
        .csv(str(ROOT/'data/raw/tmdb_imdb.csv'))
        .withColumn('tmdbId', F.col('id').cast('int'))
        .withColumn('runtime_min', F.col('runtime').cast('int'))
        .filter((F.col('runtime_min').isNotNull()) & F.col('runtime_min').between(1, 600))
        .select('tmdbId','runtime_min').dropDuplicates(['tmdbId']))
movie_features = (movies.join(links.select(F.col('tmdbId').cast('int').alias('tmdbId'), 'movieId'), 'movieId', 'left')
    .join(tmdb, 'tmdbId', 'left').join(movie_avg, 'movieId', 'inner'))
matched = movie_features.filter(F.col('runtime_min').isNotNull()).count()
metrics['runtime_match'] = {'movies': metrics['movie_count'], 'matched': matched, 'ratio': round(matched/metrics['movie_count'],4)}
metrics['year_corr'] = round(movie_features.stat.corr('year','avg_rating'), 4)
metrics['runtime_corr'] = round(movie_features.filter(F.col('runtime_min').isNotNull()).stat.corr('runtime_min','avg_rating'), 4)
print('runtime 匹配:', metrics['runtime_match'])
print('year 与均分相关性 corr =', metrics['year_corr'])
print('runtime 与均分相关性 corr =', metrics['runtime_corr'])
plt.figure(figsize=(10,4))
ys = movie_features.filter(F.col('year').isNotNull()).sample(0.02).select('year','avg_rating').toPandas()
plt.figure(figsize=(10,4)); plt.scatter(ys['year'], ys['avg_rating'], s=3)
plt.title(f"上映年份与评分相关性 corr={metrics['year_corr']}"); plt.tight_layout()
plt.savefig(str(ROOT/'figures/05_year_corr.png'), dpi=160); plt.close()
rt = movie_features.filter(F.col('runtime_min').isNotNull()).sample(0.05).select('runtime_min','avg_rating').toPandas()
plt.figure(figsize=(10,4)); plt.scatter(rt['runtime_min'], rt['avg_rating'], s=3)
plt.title(f"电影时长与评分相关性 corr={metrics['runtime_corr']}"); plt.tight_layout()
plt.savefig(str(ROOT/'figures/06_runtime_corr.png'), dpi=160); plt.close()
""")

md("## 模块五：Spark MLlib ALS 协同过滤推荐")
code("""
# 确定性 10% 评分采样；训练集实体交集构造测试集，避免冷启动
sample = clean.withColumn('h', F.xxhash64('userId','movieId') % 10).filter(F.col('h') == 0).drop('h').cache()
sample_count = sample.count()
train, test_raw = sample.randomSplit([0.8, 0.2], seed=42)
train_users = train.select('userId').distinct()
train_items = train.select('movieId').distinct()
test = test_raw.join(F.broadcast(train_users), 'userId', 'inner').join(F.broadcast(train_items), 'movieId', 'inner').cache()
metrics['als_sample_info'] = {'sample_count': sample_count, 'train': train.count(), 'test': test.count()}
print('ALS 数据:', metrics['als_sample_info'])
baseline = train.agg(F.avg('rating')).first()[0]
baseline_rmse = ((test.select(F.col('rating') - F.lit(baseline)).rdd.map(lambda r: r[0]**2).sum())/test.count())**0.5
metrics['baseline_rmse'] = round(baseline_rmse,4)
print('baseline RMSE =', round(baseline_rmse,4))
""")

code("""
# 4 组超参数搜索（rank × regParam），记录有效预测行数与冷启动丢弃量
evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
best = None
for rank in (10, 20):
    for reg in (0.1, 0.05):
        als = ALS(userCol='userId', itemCol='movieId', ratingCol='rating', rank=rank,
                  maxIter=10, regParam=reg, coldStartStrategy='drop', nonnegative=True,
                  implicitPrefs=False, seed=42)
        model = als.fit(train)
        pred = model.transform(test).cache()
        valid = pred.filter(F.col('prediction').isNotNull())
        dropped = test.count() - valid.count()
        rmse = evaluator.evaluate(valid) if dropped < test.count() else float('inf')
        metrics.setdefault('als_trials', []).append({'rank':rank,'regParam':reg,'rmse':round(rmse,4),'dropped':dropped})
        print(f'rank={rank}, reg={reg}: RMSE={rmse:.4f}, dropped={dropped}')
        if best is None or rmse < best[0]: best = (rmse, model, rank, reg)
metrics['als_rmse'] = round(best[0],4)
metrics['best_params'] = {'rank':best[2], 'regParam':best[3]}
metrics['test_rows_dropped'] = metrics['als_trials'][0]['dropped']
best_model = best[1]
print('best:', metrics['best_params'], 'RMSE', metrics['als_rmse'])
""")

code("""
# 为确定性指定的训练用户生成 Top-10 推荐并反查片名/类型
target_users = [r['userId'] for r in train.select('userId').distinct().orderBy('userId').limit(1001).collect()]
targets = [u for u in (target_users[0], target_users[100], target_users[1000]) if u in target_users]
users_df = spark.createDataFrame([(u,) for u in targets], ['userId'])
recs = (best_model.recommendForUserSubset(users_df, 10)
    .withColumn('rec', F.explode('recommendations'))
    .select('userId', F.col('rec.movieId').alias('movieId'), F.col('rec.rating').alias('pred_score')))
seen = train.select('userId','movieId').withColumn('seen', F.lit(1))
final_recs = recs.join(F.broadcast(seen), ['userId','movieId'], 'left_anti').join(
    movies.select('movieId','clean_title','genres'), 'movieId', 'left').orderBy('userId', F.desc('pred_score'))
metrics['top10_recs'] = [r.asDict() for r in final_recs.collect()]
metrics['empty_rec_users'] = len(targets) - len({r['userId'] for r in metrics['top10_recs']})
print('指定用户:', targets, ' 空推荐数:', metrics['empty_rec_users'])
print('=== Top-10 推荐 ==='); final_recs.show(30, truncate=False)
""")

code("""
# 性能观察：同一查询、同一数据、cache 后各 3 次取中位数；宽窄依赖 explain()
explain_df = (exploded.join(movie_avg, 'movieId')
              .groupBy('genre').agg(F.avg('avg_rating').alias('avg_score')))
print('=== Physical Plan (宽依赖示例) ==='); print(explain_df._jdf.queryExecution().executedPlan().toString())
print('=== Physical Plan (窄依赖示例) ==='); print(movies.select('movieId','year').filter('year > 2000')._jdf.queryExecution().executedPlan().toString())
perf = {}
for parts in (64, 200):
    spark.conf.set('spark.sql.shuffle.partitions', str(parts))
    ts = []
    for i in range(3):
        t = time.time(); explain_df.count(); ts.append(time.time()-t)
    perf[str(parts)] = round(statistics.median(ts),4)
    print(f'shuffle.partitions={parts}: median={perf[str(parts)]}s, runs={[round(x,3) for x in ts]}')
metrics['shuffle_perf'] = perf
spark.conf.set('spark.sql.shuffle.partitions', '64')
""")

code("""
# 输出真实指标，供报告自动填充
required = ['movie_count','rating_count','clean_rows','genre_counts','year_trend','global_stats',
            'genre_top3','bayes_top20','sql_df_match','active_users','runtime_match','year_corr',
            'runtime_corr','als_rmse','baseline_rmse','top10_recs','test_rows_dropped','empty_rec_users']
missing = [k for k in required if k not in metrics or metrics[k] is None]
assert not missing, f'missing metrics keys: {missing}'
(ROOT/'results/metrics.json').write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
print('METRICS_SAVED', len(metrics), 'keys')
spark.stop()
""")

nb['cells'] = cells
nb.metadata.kernelspec = {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}
nb.metadata.language_info = {'name': 'python', 'version': '3.11.17'}
out = Path(ROOT) / 'notebooks' / 'movie_analytics.ipynb'
out.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, out)
print('WROTE', out, len(cells), 'cells')

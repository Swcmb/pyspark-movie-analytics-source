# Report Guide

The PDF report is generated from `results/metrics.json`, local screenshots, and local figures. The report is generated locally from the executed notebook outputs.

## Fixed Report Structure

1. 运行环境与数据集
2. 五模块实现与关键代码
3. Transformation / Action 说明表
4. 宽/窄依赖与 Shuffle 分析
5. 业务洞察
6. 结论
7. 附录：独立可视化图表

## Transformation / Action Table

The report explains these operator categories:

| Category | Examples |
| :--- | :--- |
| Lazy Transformation | `read.csv`, `read.parquet`, `read.json` |
| Narrow Transformation | `select`, `filter`, `withColumn`, `regexp_extract`, `split`, `explode` |
| Wide Transformation | `groupBy`, `agg`, `join`, `Window` |
| Persistence | `cache`, `persist` |
| Action | `show`, `count`, `collect`, `write` |
| MLlib Action | `ALS.fit`, `transform`, `recommendForUserSubset` |

## Performance Discussion

The report includes:

- physical-plan interpretation from `explain()`,
- narrow dependency example: `Project` / `Filter`,
- wide dependency example: `Exchange hashpartitioning`,
- shuffle partition comparison: 64 versus 200,
- three runs per setting with median duration,
- AQE and cache optimization notes.

## Business Insight Checklist

The report must discuss:

1. rating distribution and year trend,
2. genre distribution and Bayesian correction,
3. active-user behavior,
4. runtime correlation,
5. ALS usefulness and cold-start limitation.

## Image Policy

- Screenshots are captured per notebook cell, using DOM element index.
- Each screenshot is embedded as a base64 data URI.
- Images are scaled to the A4 content width to avoid clipping.
- Captions describe the actual cell content.

## Verification

Run:

```bash
python scripts/verify.py
```

The report passes when it has at least 10 pages, at least 18 embedded images, all six required chapter titles, and sufficient body text.

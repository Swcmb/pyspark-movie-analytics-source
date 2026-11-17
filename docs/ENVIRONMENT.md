# Environment

## Verified Local Environment

| Item | Version / Path |
| :--- | :--- |
| OS | Windows 11 Pro |
| Python | 3.11.17 |
| Python interpreter | `<PYTHON>` |
| PySpark | 3.5.1 |
| JDK | 17.0.11 at `<JDK17_HOME>` |
| Spark binary | `<SPARK_HOME>` |
| CPU / Memory | 24 cores / 31.7 GB |
| Browser | Playwright Chromium |
| Chinese font | Microsoft YaHei |

## Required Python Packages

```text
pyspark==3.5.1
numpy==1.26.4
pandas==2.2.3
pyarrow==17.0.0
matplotlib==3.9.4
jupyterlab==4.4.7
nbconvert==7.16.6
ipykernel==6.31.0
playwright
pypdf
Pillow
```

Install into the PySpark interpreter:

```bash
<PYTHON> -m pip install -r docs/artifacts/requirements.txt
```

## Critical Environment Rules

1. Use JDK 17. The machine default JDK 21 is not suitable for Spark 3.5.
2. Set `PYSPARK_SUBMIT_ARGS` before the JVM starts:

```bash
PYSPARK_SUBMIT_ARGS="--driver-memory 8g --executor-memory 8g pyspark-shell"
```

3. Set explicit Python worker paths:

```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
```

4. Clear or fix `HADOOP_HOME`. Missing `winutils.exe` only produces a warning on this Windows local-file workflow.

## Verification

After creating the SparkSession, the notebook prints:

```text
Spark 3.5.1
driver.memory = 8g
JVM maxMemory = 8.0 GB
```

If those values are absent, stop and fix the environment before running the analysis.

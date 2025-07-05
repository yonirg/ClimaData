# engines/spark/job_spark.py
import time, json, argparse, stat, os
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, abs, lag, avg, unix_timestamp

THR = {"temperature": 4.0, "humidity": 15.0, "pressure": 5.0}

# ─────────── CLI ───────────
p = argparse.ArgumentParser()
p.add_argument("--data", required=True)
p.add_argument("--out",  required=True)
p.add_argument("--k",    type=int, required=True)        # <─ NOVO
args = p.parse_args()

t0 = time.perf_counter()            # <─ start chrono

out_base = Path(args.out)
out_base.mkdir(parents=True, exist_ok=True)
out_base.chmod(out_base.stat().st_mode | stat.S_IWOTH | stat.S_IXOTH)

# ─────────── Spark session ───────────
spark = (SparkSession.builder
         .appName("ClimaDataSpark")
         .getOrCreate())

df = spark.read.parquet(args.data)

# ─────────── detecção de anomalias ───────────
w_station = Window.partitionBy("station_id").orderBy("timestamp")
df_lag = (
    df.select(
        "*",
        lag("temperature").over(w_station).alias("t_prev"),
        lag("humidity").over(w_station).alias("h_prev"),
        lag("pressure").over(w_station).alias("p_prev"),
    )
    .withColumn(
        "is_anom",
        (abs(col("temperature") - col("t_prev")) > THR["temperature"])
        | (abs(col("humidity") - col("h_prev")) > THR["humidity"])
        | (abs(col("pressure") - col("p_prev")) > THR["pressure"])
    )
)

(df_lag.filter("is_anom")
       .write.mode("overwrite")
       .option("header", "true")
       .csv(str(out_base / "detected_anomalies_spark")))

# ─────────── média móvel 30 min ───────────
MICROS_30_MIN = 30 * 60 * 1_000_000
clean = (df_lag.filter(~col("is_anom"))
               .withColumn("ts_us", unix_timestamp("timestamp") * 1_000_000))

w_region = (Window
            .partitionBy("region")
            .orderBy("ts_us")
            .rangeBetween(-MICROS_30_MIN, 0))

rolling = clean.select(
    "timestamp", "region",
    avg("temperature").over(w_region).alias("mean_temperature"),
    avg("humidity").over(w_region).alias("mean_humidity"),
    avg("pressure").over(w_region).alias("mean_pressure")
)

rolling.write.mode("overwrite") \
       .parquet(str(out_base / "rolling_30min_spark.parquet"))

spark.stop()

# ─────────── JSON de saída esperado pelo controller ───────────
elapsed = round(time.perf_counter() - t0, 3)
print(json.dumps({"engine": "spark", "k": args.k, "seconds": elapsed}))

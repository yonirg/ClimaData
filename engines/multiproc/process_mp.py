# ClimaData/engines/multiproc/process_mp.py
from __future__ import annotations
import argparse, json, math, time
from pathlib import Path
from multiprocessing import Pool, cpu_count

import pandas as pd

# --------------------------------------------
# PARÂMETROS GLOBAIS
THRESHOLDS = {"temperature": 4.0, "humidity": 15.0, "pressure": 5.0}
ROLLING_WINDOW = "30min"   # média móvel
# --------------------------------------------


def _process_chunk(args):
    """
    Worker:
      - detecta anomalias por salto grande,
      - conta quantas ocorreram por sensor,
      - devolve linhas normais para média móvel.
    """
    chunk_df, thresholds = args
    chunk_df = chunk_df.sort_values("timestamp")

    anomalies = []  # (ts_iso, station_id, sensor_tag)
    counts = {"temperature": 0, "humidity": 0, "pressure": 0}
    last_val = {}   # station_id -> (t, h, p)
    normals_rows = []

    for row in chunk_df.itertuples(index=False):
        sid = row.station_id
        region = row.region
        prev = last_val.get(
            sid, (row.temperature, row.humidity, row.pressure)
        )

        is_t = abs(row.temperature - prev[0]) > thresholds["temperature"]
        is_h = abs(row.humidity    - prev[1]) > thresholds["humidity"]
        is_p = abs(row.pressure    - prev[2]) > thresholds["pressure"]

        if is_t or is_h or is_p:
            tag = ("T" if is_t else "") + ("H" if is_h else "") + ("P" if is_p else "")
            anomalies.append((row.timestamp.isoformat(), sid, tag))
            if is_t:
                counts["temperature"] += 1
            if is_h:
                counts["humidity"] += 1
            if is_p:
                counts["pressure"] += 1
        else:
            normals_rows.append(
                {
                    "timestamp":  row.timestamp,
                    "region":     region,
                    "temperature":row.temperature,
                    "humidity":   row.humidity,
                    "pressure":   row.pressure,
                }
            )

        last_val[sid] = (row.temperature, row.humidity, row.pressure)

    normals_df = pd.DataFrame(normals_rows)
    return anomalies, counts, normals_df


def run(df_all: pd.DataFrame, k: int, outdir: Path):
    t0 = time.perf_counter()

    # ------- divide em k chunks mais ou menos iguais ----------
    chunk_sz = math.ceil(len(df_all) / k)
    chunks = [df_all.iloc[i:i + chunk_sz].copy() for i in range(0, len(df_all), chunk_sz)]

    # ------- paraleliza ----------
    with Pool(processes=k) as pool:
        results = pool.map(
            _process_chunk,
            [(c, THRESHOLDS) for c in chunks]
        )

    # ------- reduz ----------
    anomalies_all = []
    total_counts = {"temperature": 0, "humidity": 0, "pressure": 0}
    normals_parts = []

    for anom, cnt, normals_df in results:
        anomalies_all.extend(anom)
        for s in total_counts:
            total_counts[s] += cnt[s]
        normals_parts.append(normals_df)

    normals_full = pd.concat(normals_parts, ignore_index=True)

    # ------- média móvel 30 min por região ----------
    normals_full["timestamp"] = pd.to_datetime(normals_full["timestamp"], utc=True)
    rolling = (
        normals_full
          .set_index("timestamp")
          .groupby("region")[["temperature", "humidity", "pressure"]]
          .rolling(ROLLING_WINDOW)
          .mean()
          .reset_index()
    )

    # ------- grava saídas ----------
    outdir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        anomalies_all, columns=["timestamp", "station_id", "sensor_tag"]
    ).to_csv(outdir / f"detected_anomalies_mp_{k}.csv", index=False)

    rolling.to_parquet(outdir / f"rolling_30min_mp_{k}.parquet", index=False)

    json_stats = {
        "engine": "multiproc",
        "k": k,
        "seconds": round(time.perf_counter() - t0, 3),
        "anomaly_counts": total_counts,
    }
    print(json.dumps(json_stats))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Parquet gerado pelo generator.py")
    ap.add_argument("--k", type=int, default=cpu_count(), help="nº de processos")
    ap.add_argument("--out", default="results", help="pasta de saída")
    args = ap.parse_args()

    df = pd.read_parquet(Path(args.data).expanduser())
    run(df, args.k, Path(args.out))

import time, math, json, uuid
import io
from pathlib import Path
import pandas as pd
import argparse
from engines.celery.worker_tasks import process_chunk_celery
from engines.multiproc.process_mp import run  # para reduzir final
import pyarrow as pa
import pyarrow.parquet as pq


def run_celery(data_path: Path, k: int, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    df_all = pd.read_parquet(data_path)

    # 1) fatiar num subdiretório dentro de out_dir
    tmp_dir = out_dir / f"tmp_chunks_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    chunk_sz = math.ceil(len(df_all) / (k * 4))
    chunk_paths = []
    for i in range(0, len(df_all), chunk_sz):
        piece = df_all.iloc[i:i+chunk_sz]
        p = tmp_dir / f"chunk_{i//chunk_sz}.parquet"
        piece.to_parquet(p, index=False)
        chunk_paths.append(str(p))
    # 2) lançar tasks
    async_results = [process_chunk_celery.delay(path) for path in chunk_paths]

    # 3) coletar
    collected = [r.get() for r  in async_results]

    # 4) converter normals bytes -> DF, depois reduzir igual a multiproc
    normals_parts = [
    # lê diretamente os bytes como um BufferReader e transforma em pandas.DataFrame
    pq.read_table(pa.BufferReader(bytes_obj["normals"])).to_pandas()
    for bytes_obj in collected
    ]

    anomalies_all = sum((c["anom"] for c in collected), [])
    total_counts = {"temperature": 0, "humidity": 0, "pressure": 0}
    for c in collected:
        for s in total_counts:
            total_counts[s] += c["counts"][s]

    normals_full = pd.concat(normals_parts, ignore_index=True)
    # ... (média móvel 30 min, salvar artefatos) ...

    elapsed = round(time.perf_counter() - t0, 3)
    print(json.dumps({"engine": "celery", "k": k, "seconds": elapsed}))
    
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("data_path")
    p.add_argument("k", type=int)
    p.add_argument("out_dir")
    args = p.parse_args()

    run_celery(Path(args.data_path), args.k, Path(args.out_dir))
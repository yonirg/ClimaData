# controller/run_experiment.py  (versão completa)
import argparse, json, subprocess, sys, csv, time
from pathlib import Path
from weather.generator import WeatherDataGenerator
import subprocess

def call(cmd):
    out = subprocess.check_output(cmd, text=True)
    return float(out.strip())

def get_executor_container():
    # filtra pelo label que o Compose aplica aos containers
    out = subprocess.check_output([
        "docker", "ps",
        "--filter", "label=com.docker.compose.service=executor",
        "--format", "{{.Names}}"
    ], text=True)
    # pega o primeiro da lista
    return out.splitlines()[0]


ENGINES = ["multiproc", "celery", "spark"]

def call(cmd: list[str]) -> float:
    """Executa comando, espera terminar, devolve segundos informados no JSON."""
    out = subprocess.check_output(cmd, text=True)
    # o script de cada engine imprime um JSON em 1 única linha
    json_line = next((l for l in out.splitlines() if l.lstrip().startswith("{")), None)
    if not json_line:
        raise RuntimeError(f"Nenhum JSON na saída:\n{out}")
    rec = json.loads(json_line)
    return rec["seconds"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k-list", default="1,2,4,8", help="graus de paralelismo")
    ap.add_argument("--samples", type=int, default=2_000_000)
    ap.add_argument("--stations", type=int, default=100)
    ap.add_argument("--anomaly-rate", type=float, default=0.005)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    ks = [int(x) for x in args.k_list.split(",")]

    # ---------- 1) gerar dados ----------
    gen = WeatherDataGenerator(
        n_stations=args.stations,
        n_samples=args.samples,
        anomaly_rate=args.anomaly_rate,
    )
    paths = gen.run(args.out)
    data_parquet = paths["events"]

    # arquivo que o dashboard vai ficar "assistindo"
    bench_file = Path(args.out) / "benchmarks.csv"
    bench_file.parent.mkdir(parents=True, exist_ok=True)
    first = not bench_file.exists()
    with bench_file.open("a", newline="") as fp:
        writer = csv.writer(fp)
        if first:
            writer.writerow(["engine", "k", "seconds"])
        # ---------- 2) executar todas combinações ----------
        for k in ks:
            # ---- multiproc ----
            secs = call(
                [sys.executable, "-m", "engines.multiproc.process_mp",
                 "--data", data_parquet, "--k", str(k),
                 "--out", f"{args.out}/results_mp_k{k}"]
            )
            writer.writerow(["multiproc", k, secs]); fp.flush()

            # ---- celery ----
            fname = Path(data_parquet).name

            # usa caminhos *absolutos* no container, sob /data
            data_in  = f"/data/{fname}"                       # entrada
            data_out = f"/data/results_celery_k{k}"           # saída

            secs = call([
                sys.executable,  # garante usar o python dentro do container
                "-m", "engines.celery.run_celery_job",
                data_in,
                str(k),
                data_out
            ])
            writer.writerow(["celery", k, secs]); fp.flush()

            # ---- spark ----
            container = get_executor_container()
            secs = call([
                "docker", "exec", "-i", container,
                "spark-submit",
                "--conf", "spark.jars.ivy=/tmp/.ivy2",
                "--master", "spark://spark-master:7077",
                "--executor-cores", "1",
                "/app/engines/spark/job_spark.py",
                "--data", "/data/weather_events.parquet",
                "--out", "/data/results_spark_k1",
                "--k", "1",
            ])

            print(secs)
            writer.writerow(["spark", k, secs]); fp.flush()

if __name__ == "__main__":
    main()

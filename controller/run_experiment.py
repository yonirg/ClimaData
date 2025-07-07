# controller/run_experiment.py  (versão completa)
import argparse, json, subprocess, sys, csv, time
from pathlib import Path
from weather.generator import WeatherDataGenerator
import subprocess
import os


def call(cmd):
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as e:
        print("=== comando que falhou:", " ".join(cmd), file=sys.stderr)
        print("--- saída completa do erro:\n", e.output, file=sys.stderr)
        raise
    json_line = next((l for l in out.splitlines() if l.lstrip().startswith("{")), None)
    if not json_line:
        raise RuntimeError(f"Nenhum JSON na saída:\n{out}")
    return json.loads(json_line)["seconds"]

def get_executor_container():
    # filtra pelo label que o Compose aplica aos containers
    out = subprocess.check_output([
        "docker", "ps",
        "--filter", "name=executor",
        "--format", "{{.Names}}"
    ], text=True)
    # pega o primeiro da lista
    return out.splitlines()[0]


ENGINES = ["multiproc", "celery", "spark"]

def call(cmd: list[str]) -> float:
    """Executa comando, espera terminar, devolve segundos informados no JSON."""
    out = subprocess.check_output(cmd, text=True)
    # o script de cada engine imprime um JSON em 1 única linha
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except Exception as e:
        print("=== comando que falhou:", " ".join(cmd), file=sys.stderr)
        print("--- saída completa do comando:\n", e.output, file=sys.stderr)
        raise
    json_line = next((l for l in out.splitlines() if l.lstrip().startswith("{")), None)
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
            env = os.environ.copy()
            env["WORKER_CONCURRENCY"] = str(k)
            # ---- multiproc ----
            secs = call(
                [sys.executable, "-m", "engines.multiproc.process_mp",
                 "--data", data_parquet, "--k", str(k),
                 "--out", f"{args.out}/results_mp_k{k}"]
            )
            writer.writerow(["multiproc", k, secs]); fp.flush()

            # ---- celery ----
            # 1) escala o serviço worker pra zero (para garantir container limpo)
            subprocess.run(
                ["docker-compose", "-f", "/app/docker-compose.yml",
                 "up", "-d", "--no-deps", "--force-recreate", "--scale", "worker=0",
                 "worker"],
                check=True, env=env,
            )

            # 2) escala de volta pra um único container
            subprocess.run(
                ["docker-compose", "-f", "/app/docker-compose.yml",
                 "up", "-d", "--no-deps", "--force-recreate", "--scale", "worker=1",
                 "worker"],
                check=True, env=env,
            )
            time.sleep(2)  # aguarda container subir

            # 3) descobre o nome do container worker
            worker = (
                subprocess.check_output([
                    "docker", "ps",
                    "--filter", "label=com.docker.compose.service=worker",
                    "--format", "{{.Names}}"
                ], text=True)
                .splitlines()[0]
            )

            # 4) dentro dele, dispara o celery worker com --concurrency=k
            subprocess.run([
                "docker", "exec", "-d", worker,
                "celery", "-A", "engines.celery.worker_tasks", "worker",
                "--loglevel=info", f"--concurrency={k}", "-Q", "climadata"
            ], check=True, env=env)
            time.sleep(3)  # deixa o worker registrar no broker

            # 5) dispara o job Celery
            fname     = Path(data_parquet).name
            data_in   = f"/data/{fname}"
            data_out  = f"/data/results_celery_k{k}"
            secs = call([
                sys.executable, "-m", "engines.celery.run_celery_job",
                data_in, str(k), data_out
            ])
            writer.writerow(["celery", k, secs]); fp.flush()

            # 6) derruba o worker custom para liberar recursos
            subprocess.run([
                "docker", "exec", worker,
                "pkill", "-f", "celery"
            ], check=False, env=env)

            # ---- spark ----
            container = get_executor_container()
            secs = call([
                "docker", "exec", "-i", container,
                "spark-submit",
                "--conf", "spark.jars.ivy=/tmp/.ivy2",
                "--master", "spark://spark-master:7077",
                "--total-executor-cores", str(k),
                "/app/engines/spark/job_spark.py",
                "--data", data_in,
                "--out", f"/data/results_spark_k{k}",
                "--k", str(k)
            ])

            print(secs)
            writer.writerow(["spark", k, secs]); fp.flush()

if __name__ == "__main__":
    main()

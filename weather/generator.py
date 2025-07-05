# weather/generator.py
from __future__ import annotations
import argparse, itertools, json, random, uuid, pathlib
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class WeatherDataGenerator:
    """Gera eventos meteorológicos sintéticos + ground-truth de anomalias."""

    REGIONS = ["Norte", "Sul", "Leste", "Oeste", "Central"]

    def __init__(
        self,
        n_stations: int = 100,
        n_samples: int = 1_000_000,
        anomaly_rate: float = 0.005,
        seed: int = 42,
    ):
        self.n_stations = n_stations
        self.n_samples = n_samples
        self.anomaly_rate = anomaly_rate
        self.rng = np.random.default_rng(seed)
        random.seed(seed)
        self.start_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)


        # Define base-lines por estação
        self._stations_meta = self._build_stations()

    # ---------- API pública ----------
    def run(self, out_dir: str | pathlib.Path = "data") -> dict[str, str]:
        out = pathlib.Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)

        df, anomalies = self._generate_events()

        # torna o carimbo naive (sem timezone) e depois converte p/ microsseg.
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
        df["timestamp"] = df["timestamp"].astype("datetime64[us]")

        # Persiste em Parquet (pandas -> pyarrow zero-copy)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table,
            out / "weather_events.parquet",
            compression="zstd",
            use_dictionary=True,
        )

        anomalies.to_csv(out / "truth.csv", index=False)

        return {
            "events": str(out / "weather_events.parquet"),
            "truth": str(out / "truth.csv"),
        }

    # ---------- implementação ----------
    def _build_stations(self) -> pd.DataFrame:
        """Atribui estação a região + baseline climatológica."""
        stations = []
        for sid in range(self.n_stations):
            region = random.choice(self.REGIONS)
            # baseline region-dependent
            base_temp = self.rng.normal({"Norte": 28, "Sul": 18, "Leste": 24,
                                         "Oeste": 22, "Central": 25}[region], 1.5)
            base_humi = self.rng.uniform(50, 80)
            base_pres = self.rng.normal(1013, 3)
            stations.append(
                dict(
                    station_id=sid,
                    region=region,
                    temp=base_temp,
                    humi=base_humi,
                    pres=base_pres,
                )
            )
        return pd.DataFrame(stations)

    def _generate_events(self):
        rows, anomalies = [], []

        # Pré-sorteia quais índices terão anomalia
        anomaly_mask = self.rng.random(self.n_samples) < self.anomaly_rate

        # Iterador circular de estações para balancear volume
        station_cycle = itertools.cycle(self._stations_meta.itertuples())

        for i in range(self.n_samples):
            st_meta = next(station_cycle)
            ts = self.start_ts + timedelta(minutes=i)

            # Random walk normal
            temp = st_meta.temp + self.rng.normal(0, 0.1)
            humi = st_meta.humi + self.rng.normal(0, 0.3)
            pres = st_meta.pres + self.rng.normal(0, 0.5)

            # Decide anomalia
            if anomaly_mask[i]:
                sensor = random.choice(["temp", "humi", "pres"])
                if sensor == "temp":
                    temp += self.rng.choice([-5, 5])           # +/- 5 °C
                elif sensor == "humi":
                    humi = self.rng.choice([0, 100])           # extremos
                else:  # pres
                    pres += self.rng.choice([-6, 6])           # +/- 6 hPa

                anomalies.append(
                    {
                        "timestamp": ts.isoformat(),
                        "station_id": st_meta.station_id,
                        "sensor": sensor,
                    }
                )

            rows.append(
                {
                    "timestamp": ts,
                    "station_id": st_meta.station_id,
                    "region": st_meta.region,
                    "temperature": round(temp, 2),
                    "humidity": round(humi, 1),
                    "pressure": round(pres, 2),
                }
            )

        df = pd.DataFrame(rows)

        num_cols = ["temperature", "humidity", "pressure"]
        df[num_cols] = df[num_cols].astype("float32")
        df["station_id"] = df["station_id"].astype("int16")
        anomalies_df = pd.DataFrame(anomalies)

        return df, anomalies_df


# ---------------- CLI -----------------
def _parse_args():
    p = argparse.ArgumentParser(description="Synthetic weather generator")
    p.add_argument("--stations", type=int, default=100)
    p.add_argument("--samples", type=int, default=1_000_000)
    p.add_argument("--anomaly-rate", type=float, default=0.005)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data", help="output folder")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    gen = WeatherDataGenerator(
        n_stations=args.stations,
        n_samples=args.samples,
        anomaly_rate=args.anomaly_rate,
        seed=args.seed,
    )
    paths = gen.run(args.out)
    print(json.dumps(paths, indent=2))

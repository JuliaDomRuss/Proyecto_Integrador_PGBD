import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession

from src.validacion.validate import validate_igae, validate_raw_exchange_rate

FX_PATH = str(ROOT / "data" / "raw" / "tipo_cambio_fix.csv") # root to dataset tipo de cambio 
IGAE_PATH = str(ROOT / "data" / "processed" / "igae_limpio.parquet") # root to dataset IGAE 


def run_check(label, validator, df):
    try:
        rows = validator(df)
        print(f"PASS  {label}: {rows} rows")
        return True
    except ValueError as error:
        print(f"FAIL  {label}: {error}")
        return False


def main():
    spark = (
        SparkSession.builder
        # run locally
        .master("local[*]")
        .appName("check_real_data")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    fx = spark.read.csv(FX_PATH, header=True, inferSchema=False).cache()
    igae = spark.read.parquet(IGAE_PATH).cache()

    results = [
        # run both checks on both datasets with their respective validation functions 
        run_check("raw exchange rate", validate_raw_exchange_rate, fx),
        run_check("IGAE", validate_igae, igae),
    ]

    fx.unpersist()
    igae.unpersist()
    spark.stop()
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()

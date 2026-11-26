"""Punto de entrada: coordina el pipeline modular del Proyecto Integrador."""
import json
from datetime import datetime, timezone
from pathlib import Path


from pyspark.sql import SparkSession

import shutil
from pathlib import Path

from src.ingestion.extract import extract_lab8, extract_sp500, extract_tipo_cambio
from src.quality.validate import (
    validate_lab8_input,
    validate_output,
    validate_prepared_monthly,
    validate_sp500_input,
    validate_tc_input,
)
from src.transformations.integrate_enrich import (
    integrate_and_enrich,
    prepare_sp500,
    prepare_tipo_cambio,
)

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


LAB8_PATH = "data/raw/finanzas_igae_mensual_final.parquet"
TC_PATH = "data/raw/tipo_cambio_fix.csv"
SP500_PATH = "data/raw/s&p500_mensual.csv"
OUTPUT_PATH = "data/processed/proyecto_integrado_mensual.parquet"
REPORT_PATH = "reports/ultima_ejecucion.json"


def save_report(
    status,
    counts=None,
    output_rows=None,
    error=None,
    report_path=REPORT_PATH,
):
    """Guarda evidencia de ejecución incluso si falla una etapa."""
    report = {
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "estado": status,
        "registros_entrada": counts or {},
        "registros_salida": output_rows,
        "error": str(error) if error is not None else None,
    }
    destination = Path(report_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report

def load(df, output_path):
    print(f"[LOAD] Guardando resultado Parquet en {output_path}")
    ruta = Path(output_path)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    
    # Si Spark dejó creada una carpeta con ese nombre en el intento fallido anterior, la borramos:
    if ruta.exists() and ruta.is_dir():
        shutil.rmtree(ruta)
        
    df.toPandas().to_parquet(ruta, index=False, engine="pyarrow")

def verify_persisted_output(spark, output_path, expected_rows):
    """Relee el Parquet guardado para confirmar que es legible e íntegro."""
    print("[VERIFY] Releyendo Parquet generado para confirmar integridad")
    df_check = spark.read.parquet(output_path)
    persisted_rows = df_check.count()
    if persisted_rows != expected_rows:
        raise ValueError(
            f"[VERIFY] Discrepancia al releer salida: esperado={expected_rows}, leído={persisted_rows}"
        )
    print(f"[VERIFY] Relectura exitosa: {persisted_rows} filas, {len(df_check.columns)} columnas")
    return df_check


def run_pipeline(
    spark,
    lab8_path=LAB8_PATH,
    tc_path=TC_PATH,
    sp500_path=SP500_PATH,
    output_path=OUTPUT_PATH,
    report_path=REPORT_PATH,
):
    """Coordina Extract -> Validate -> Transform & Integrate -> Validate -> Load -> Verify."""
    input_counts = {}
    output_rows = None
    spark.sparkContext.setJobGroup("PROYECTO_PIPELINE", "Integración Lab 8 + Tipo Cambio + S&P 500")
    print("=== INICIO DEL PIPELINE ===")
    try:
        # 1. Extract
        df_lab8 = extract_lab8(spark, lab8_path)
        df_tc_raw = extract_tipo_cambio(spark, tc_path)
        df_sp_raw = extract_sp500(spark, sp500_path)

        # 2. Validate Input
        input_counts["lab8"] = validate_lab8_input(df_lab8)
        input_counts["tipo_cambio_diario"] = validate_tc_input(df_tc_raw)
        input_counts["sp500_mensual"] = validate_sp500_input(df_sp_raw)
        print(f"[VALIDATE] Entradas correctas: {input_counts}")

        # 3. Clean, Prepare & Validate Monthly Granularity
        df_tc_mensual = prepare_tipo_cambio(df_tc_raw)
        df_sp_mensual = prepare_sp500(df_sp_raw)
        validate_prepared_monthly(df_tc_mensual, "TC_MENSUAL")
        validate_prepared_monthly(df_sp_mensual, "SP500_MENSUAL")

        # 4. Transform & Integrate
        result = integrate_and_enrich(df_lab8, df_tc_mensual, df_sp_mensual)

        # 5. Validate Output
        output_rows = validate_output(result, expected_base_rows=input_counts["lab8"])
        print(f"[VALIDATE] Salida correcta: {output_rows} registros")

        # 6. Load & Verify Readback
        load(result, output_path)
        verify_persisted_output(spark, output_path, expected_rows=output_rows)

        save_report("OK", counts=input_counts, output_rows=output_rows, report_path=report_path)
        print(f"[OK] Resultado publicado en: {output_path}")
        print("=== FIN DEL PIPELINE ===")
        return result
    except Exception as exc:
        save_report("ERROR", counts=input_counts, output_rows=output_rows, error=exc, report_path=report_path)
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        raise
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)


def main():
    spark = (
        SparkSession.builder
        .appName("ProyectoIntegrador_Pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
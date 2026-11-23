"""Funciones de extracción de fuentes en PySpark."""
from pyspark.sql import types as T


SCHEMA_TIPO_CAMBIO = T.StructType([
    T.StructField("fecha", T.StringType(), True),
    T.StructField("dato", T.StringType(), True),
])

SCHEMA_SP500 = T.StructType([
    T.StructField("fecha", T.StringType(), True),
    T.StructField("sp500_cierre", T.DoubleType(), True),
    T.StructField("SP500_promedio", T.DoubleType(), True),
    T.StructField("sp500_min_mes", T.DoubleType(), True),
    T.StructField("sp500_max_mes", T.DoubleType(), True),
    T.StructField("sp500_vol_mes", T.DoubleType(), True),
    T.StructField("sp500_fecha_ultimo_habil", T.StringType(), True),
    T.StructField("sp500_ret_m", T.DoubleType(), True),
    T.StructField("sp500_rango_mes", T.DoubleType(), True),
    T.StructField("vix_cierre", T.DoubleType(), True),
])


def extract_lab8(spark, path):
    """Lee el dataset mensual integrado del Laboratorio 8 (SHCP + INPC + IGAE)."""
    print(f"[EXTRACT] Leyendo Parquet Lab 8: {path}")
    return spark.read.parquet(path)


def extract_tipo_cambio(spark, path):
    """Lee el CSV diario del tipo de cambio FIX (Banxico) con esquema explícito."""
    print(f"[EXTRACT] Leyendo CSV Tipo de Cambio: {path}")
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_TIPO_CAMBIO)
        .csv(path)
    )


def extract_sp500(spark, path):
    """Lee el CSV mensual de S&P 500 y VIX con esquema explícito."""
    print(f"[EXTRACT] Leyendo CSV S&P 500 + VIX: {path}")
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_SP500)
        .csv(path)
    )
"""Contratos de calidad de datos de entrada y salida."""
from pyspark.sql import functions as F
from pyspark.sql import types as T


REQUIRED_LAB8_COLS = {
    "PERIODO",
    "INGRESOS_REALES",
    "GASTOS_REALES",
    "INPC",
    "Total",
    "Actividades primarias",
    "Actividades secundarias",
    "Actividades terciarias",
}

REQUIRED_TC_COLS = {"fecha", "dato"}

REQUIRED_SP500_COLS = {
    "fecha",
    "sp500_cierre",
    "SP500_promedio",
    "sp500_min_mes",
    "sp500_max_mes",
    "sp500_vol_mes",
    "sp500_ret_m",
    "sp500_rango_mes",
    "vix_cierre",
}


def _check_columns(df, required_cols, source_name):
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"[{source_name}] Faltan columnas requeridas: {sorted(missing)}")


def validate_lab8_input(df):
    """Valida esquema, unicidad mensual, tipos numéricos y nulos del Parquet del Lab 8."""
    print("[VALIDATE] Revisando entrada Lab 8")
    _check_columns(df, REQUIRED_LAB8_COLS, "LAB8")

    total_rows = df.count()
    if total_rows == 0:
        raise ValueError("[LAB8] El dataset está vacío.")

    # Verificar tipos numéricos
    schema_map = {f.name: f.dataType for f in df.schema.fields}
    for col_name in REQUIRED_LAB8_COLS - {"PERIODO"}:
        if not isinstance(schema_map[col_name], (T.DoubleType, T.FloatType, T.DecimalType, T.LongType, T.IntegerType)):
            raise TypeError(f"[LAB8] La columna '{col_name}' debe ser numérica, es {schema_map[col_name]}.")

    # Verificar nulos en columnas base
    nulls = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in REQUIRED_LAB8_COLS
    ]).first().asDict()
    bad_nulls = {k: v for k, v in nulls.items() if v > 0}
    if bad_nulls:
        raise ValueError(f"[LAB8] Se detectaron valores nulos inesperados: {bad_nulls}")

    # Verificar unicidad por mes (PERIODO)
    dup_count = df.groupBy("PERIODO").count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError(f"[LAB8] Existen {dup_count} periodos duplicados en Lab 8.")

    return total_rows


def validate_tc_input(df):
    """Valida que el CSV de tipo de cambio tenga columnas y filas válidas."""
    print("[VALIDATE] Revisando entrada Tipo de Cambio")
    _check_columns(df, REQUIRED_TC_COLS, "TIPO_CAMBIO")
    total_rows = df.count()
    if total_rows == 0:
        raise ValueError("[TIPO_CAMBIO] El archivo está vacío.")

    null_dates = df.filter(F.col("fecha").isNull()).count()
    if null_dates > 0:
        raise ValueError(f"[TIPO_CAMBIO] Hay {null_dates} filas con fecha nula.")
    return total_rows


def validate_sp500_input(df):
    """Valida columnas y registros del CSV mensual de S&P 500 + VIX."""
    print("[VALIDATE] Revisando entrada S&P 500")
    _check_columns(df, REQUIRED_SP500_COLS, "SP500")
    total_rows = df.count()
    if total_rows == 0:
        raise ValueError("[SP500] El archivo está vacío.")

    null_dates = df.filter(F.col("fecha").isNull()).count()
    if null_dates > 0:
        raise ValueError(f"[SP500] Hay {null_dates} filas con fecha nula.")
    return total_rows


def validate_prepared_monthly(df, source_name):
    """Valida que una fuente ya mensualizada tenga PERIODO único y sin nulos."""
    if "PERIODO" not in df.columns:
        raise ValueError(f"[{source_name}] Falta la columna 'PERIODO'.")
    if df.filter(F.col("PERIODO").isNull()).count() > 0:
        raise ValueError(f"[{source_name}] Existen registros con PERIODO nulo.")
    dups = df.groupBy("PERIODO").count().filter(F.col("count") > 1).count()
    if dups > 0:
        raise ValueError(f"[{source_name}] Existen {dups} meses duplicados después de mensualizar.")


def validate_output(df_result, expected_base_rows):
    """Valida integridad, cardinalidad 1:1 y nulos esperados tras el join y enriquecimiento."""
    print("[VALIDATE] Revisando salida integrada")
    output_rows = df_result.count()

    if output_rows == 0:
        raise ValueError("[OUTPUT] El resultado integrado tiene 0 filas.")

    # La salida no puede superar las filas de la tabla base (evita multiplicación N:M)
    if output_rows > expected_base_rows:
        raise ValueError(
            f"[OUTPUT] Posible multiplicación de filas en el join: "
            f"base={expected_base_rows}, salida={output_rows}."
        )

    # Unicidad estricta por PERIODO
    dups = df_result.groupBy("PERIODO").count().filter(F.col("count") > 1).count()
    if dups > 0:
        raise ValueError(f"[OUTPUT] Existen {dups} meses duplicados en el dataset integrado.")

    # Nulos prohibidos en columnas clave integradas (excluyendo columnas lag del primer mes)
    non_lag_cols = [
        c for c in df_result.columns
        if not c.endswith("_ANTERIOR") and not c.startswith("VARIACION_") and not c.startswith("RET_")
    ]
    nulls = df_result.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in non_lag_cols
    ]).first().asDict()
    bad_nulls = {k: v for k, v in nulls.items() if v > 0}
    if bad_nulls:
        raise ValueError(f"[OUTPUT] Se detectaron nulos en columnas integradas: {bad_nulls}")

    return output_rows
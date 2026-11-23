"""Limpieza, mensualización (solo cierres) e integración 1:1."""
from pyspark.sql import functions as F


def prepare_tipo_cambio(df_tc_raw):
    """Limpia 'N/E', convierte tipos, agrega de diario a mensual (solo cierre) y normaliza PERIODO."""
    print("[TRANSFORM] Preparando Tipo de Cambio (USD/MXN - solo cierre mensual)")

    df_clean = (
        df_tc_raw
        .filter(F.col("dato").isNotNull() & (F.trim(F.col("dato")) != "N/E"))
        .withColumn("fecha_dt", F.to_date(F.col("fecha"), "dd/MM/yyyy"))
        .withColumn("tc_valor", F.col("dato").cast("double"))
        .filter(F.col("fecha_dt").isNotNull() & F.col("tc_valor").isNotNull())
        .withColumn("PERIODO", F.trunc(F.col("fecha_dt"), "month"))
    )

    return (
        df_clean
        .groupBy("PERIODO")
        .agg(F.max_by("tc_valor", "fecha_dt").alias("TC_CIERRE"))
    )


def prepare_sp500(df_sp_raw):
    """Homologa PERIODO y conserva cierre y promedio del S&P 500, más el cierre del VIX."""
    print("[TRANSFORM] Homologando PERIODO en S&P 500 y VIX")
    return (
        df_sp_raw
        .withColumn("PERIODO", F.trunc(F.to_date(F.col("fecha")), "month"))
        .select(
            "PERIODO",
            F.col("sp500_cierre").alias("SP500_CIERRE"),
            F.col("sp500_promedio").alias("SP500_PROMEDIO"),
            F.col("vix_cierre").alias("VIX_CIERRE"),          # <--- AGREGADO
        )
    )


def integrate_and_enrich(df_lab8, df_tc_mensual, df_sp_mensual):
    """Integra las 3 fuentes en relación 1:1 por PERIODO conservando las 9 columnas finales."""
    print("[TRANSFORM] Integrando Lab 8 + TC_CIERRE + SP500_CIERRE (Inner Join 1:1)")

    return (
        df_lab8
        .join(F.broadcast(df_tc_mensual), on="PERIODO", how="inner")
        .join(F.broadcast(df_sp_mensual), on="PERIODO", how="inner")
        .select(
            "PERIODO",
            "INGRESOS_REALES",
            "GASTOS_REALES",
            "Total",
            "INPC",
            "Actividades primarias",
            "Actividades secundarias",
            "Actividades terciarias",
            "TC_CIERRE",
            "SP500_CIERRE",
            "SP500_PROMEDIO",
            "VIX_CIERRE",
        )
        .orderBy("PERIODO")
    )
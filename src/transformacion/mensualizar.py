"""
Transformación de series diarias a frecuencia mensual.

Criterio del proyecto: el valor representativo de cada mes es el PRIMER
dato disponible del mes (primer día hábil). Así cada serie queda "con corte"
al inicio del mes y puede integrarse con el IGAE y el S&P 500 mediante la
clave PERIODO (primer día del mes), la misma usada en el Laboratorio 8.

Uso típico:
    fx_raw = spark.read.csv("data/raw/tipo_cambio_fix.csv", header=True, inferSchema=False)
    fx_diario = preparar_tipo_cambio(fx_raw)
    fx_mensual = mensualizar_primer_valor(fx_diario, "FECHA", "TC", "TC_INICIO_MES")
    fx_mensual = agregar_variacion_pct(fx_mensual, "TC_INICIO_MES", "DEPRECIACION_PCT")
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def preparar_tipo_cambio(df_raw: DataFrame) -> DataFrame:
    """Convierte el CSV crudo de Banxico (fecha dd/mm/aaaa, dato como texto)
    en una serie diaria tipada: FECHA (date) y TC (double).

    Se descartan los días sin dato ("N/E"). Se asume que el DataFrame ya pasó
    por validate_raw_exchange_rate, así que fechas y valores son convertibles.
    """
    return (
        df_raw
        .filter(F.col("dato") != "N/E")
        .select(
            F.to_date("fecha", "dd/MM/yyyy").alias("FECHA"),
            F.col("dato").cast("double").alias("TC"),
        )
    )


def mensualizar_primer_valor(
    df: DataFrame,
    col_fecha: str,
    col_valor: str,
    nombre_salida: str,
) -> DataFrame:
    """Reduce una serie diaria a una fila por mes usando el primer dato del mes.

    Devuelve:
      PERIODO                 primer día del mes (clave de integración)
      <nombre_salida>         valor del primer día con dato del mes
      FECHA_CORTE_<nombre>    día exacto del que se tomó el valor (trazabilidad)

    Si el día 1 es inhábil (ej. 1 de enero), se toma el siguiente día con dato.
    Funciona igual para el tipo de cambio y para el cierre diario del S&P 500.
    """
    return (
        df
        .filter(F.col(col_valor).isNotNull())
        .withColumn("PERIODO", F.trunc(F.col(col_fecha), "month"))
        .groupBy("PERIODO")
        .agg(
            # min_by: el valor de col_valor en la fila con la fecha más temprana
            F.min_by(col_valor, col_fecha).alias(nombre_salida),
            F.min(col_fecha).cast("date").alias(f"FECHA_CORTE_{nombre_salida}"),
        )
        .orderBy("PERIODO")
    )


def agregar_variacion_pct(df: DataFrame, col_valor: str, nombre_salida: str) -> DataFrame:
    """Agrega la variación porcentual contra el mes anterior.

    Para el tipo de cambio, un valor positivo es depreciación del peso.
    Nota: una ventana sin partitionBy mueve todos los datos a una sola
    partición; con ~400 meses no es problema, pero Spark mostrará un aviso.
    """
    w = Window.orderBy("PERIODO")
    return df.withColumn(
        nombre_salida,
        (F.col(col_valor) / F.lag(col_valor).over(w) - 1) * 100,
    )

def mensualizar_ultimo_valor(df, col_fecha, col_valor, nombre_salida):
    """Una fila por mes con el ÚLTIMO dato disponible del mes (criterio del equipo)."""
    return (
        df.filter(F.col(col_valor).isNotNull())
        .withColumn("PERIODO", F.trunc(F.col(col_fecha), "month"))
        .groupBy("PERIODO")
        .agg(
            F.max_by(col_valor, col_fecha).alias(nombre_salida),
            F.max(col_fecha).cast("date").alias(f"FECHA_CORTE_{nombre_salida}"),
        )
    )

# Proyecto_Integrador_PGBD
Proyecto colaborativo de ingeniería de datos para procesar, integrar y analizar información económica y financiera.


# Pregunta analítica
¿Cómo impactan los principales eventos históricos y crisis globales en la economía mexicana, medido a través de las variaciones del tipo de cambio frente al dólar y el comportamiento del índice S&P 500?

# Objetivo general
Analizar el comportamiento histórico del tipo de cambio y del S&P 500 para identificar anomalías o puntos de inflexión causados por eventos globales (como crisis económicas), e integrar estos datos con las fuentes base de finanzas públicas (SHCP), precios (INPC) y actividad económica (IGAE) para evaluar su impacto en la economía nacional de forma consolidada.

# Variables necesarias
De las fuentes base: Variables de finanzas públicas provenientes de la SHCP, índices de precios del INPC y el Indicador Global de la Actividad Económica (IGAE).
De las fuentes adicionales: Histórico del tipo de cambio (USD/MXN) y datos históricos del índice S&P 500.   Variables contextuales y calculadas: Un listado en formato CSV con la clasificación de eventos históricos importantes (ej. crisis de 2008, eventos políticos) y el cálculo de rendimientos (las diferencias de los valores financieros de mes a mes).

# Resultados esperados
La integración automatizada de una fuente financiera o global a las tres bases de datos requeridas (SHCP, INPC, IGAE) utilizando PySpark.
El cálculo de rendimientos y/o un índice que permita resaltar los puntos de inflexión y cambios bruscos en el mercado.
Una representación a modo de línea de tiempo (timeline) o buscador visual que evidencie el comportamiento anómalo de los datos en respuesta directa a los eventos históricos globales.   

# Guía Práctica de Buenas Prácticas y Optimización de Pipelines con Spark UI
---

## Introducción y Propósito
El propósito de este documento es servir como guía de consulta ágil y estandarizada para el equipo durante el diseño, implementación y refinamiento de los pipelines de datos en Apache Spark. 

Esta guía recopila criterios operativos, prácticas de diseño y métodos de verificación empírica basados en la física del motor distribuido, los planes de ejecución (`explain`) y las métricas de Spark UI.

---

## A. Ciclo de Optimización

La optimización de pipelines en Spark nunca debe realizarse a ciegas o por intuición. Debe regirse por el método iterativo revisado a lo largo del curso: **Predecir $\rightarrow$ Ejecutar $\rightarrow$ Medir $\rightarrow$ Interpretar $\rightarrow$ Decidir**.

| Etapa | Pregunta Orientadora | Acciones Clave y Herramientas |
| :--- | :--- | :--- |
| **1. Definir objetivo** | *¿Qué queremos mejorar y qué resultado esperamos?* | Establecer una meta técnica específica y medible. Por ejemplo, reducir el tiempo de un Stage de Shuffle en un 40%, eliminar el desbordamiento a disco (*Disk Spill*) en una unión, o evitar la duplicación de filas por desbalance de granularidad al hacer un join. |
| **2. Medir** | *¿Qué evidencia o métrica describe la situación inicial?* | Ejecutar el pipeline de referencia (*baseline*) etiquetando el trabajo con `sparkContext.setJobGroup()` para ponerle nombres a los Jobs. Capturar en Spark UI la duración del Job/Stage, el volumen de *Shuffle Read / Write*, la memoria pico (*Peak Execution Memory*) y el tiempo de recolección de basura (*GC Time*). |
| **3. Identificar** | *¿Dónde está el cuello de botella o problema?* | Aislar si la fricción proviene de:<br>• **Input-Output / I/O:** Número excesivo de operadores `Exchange`.<br>• **Memoria:** *Spill (Memory/Disk)* o *GC Time* $> 10\%$ de la tarea.<br>• **Sesgo (*Skew*):** Tareas rezagadas (*stragglers*) donde el percentil máximo de duración supera ampliamente a la mediana. |
| **4. Ajustar** | *¿Qué cambio técnico podría ayudar?* | Aplicar una sola modificación técnica a la vez fundamentada en Catalyst: filtrar antes de unir (*Predicate Pushdown*), proyectar columnas (*Projection Pruning*), forzar *Broadcast Hash Join* (`broadcast()`), ajustar particiones o almacenar en caché con nivel adecuado. |
| **5. Validar** | *¿Qué cambió y se conservaron los resultados correctos?* | **1. Validación lógica:** Comprobar que el conteo de filas, sumatorias y cardinalidad (1:1) sean idénticos al resultado esperado.<br>**2. Validación de rendimiento:** Contrastar en Spark UI que la métrica objetivo disminuyó efectivamente sin degradar otras etapas. |

---

## B. Recomendaciones Prácticas de Ingeniería y Optimización

### 1. Calidad de Datos y Control de Granularidad
* **Qué hacer:** Identificar rigurosamente qué representa una fila en cada fuente antes de cualquier transformación. Normalizar las series a una clave temporal común (`PERIODO`, primer día de mes) y agregar los registros para garantizar una relación estricta de **Uno a Uno ($1:1$)** antes de ejecutar un `join`.
* **Cuándo conviene hacerlo:** Siempre en la fase de exploración y limpieza previa a integrar datasets multidimensionales (p. ej. SHCP con 2 filas/mes vs. IGAE con 4 filas/mes vs. S&P 500 con registros diarios).
* **Cómo verificarlo:** 
  * En código: Verificar que `df.count() == df.select("PERIODO").distinct().count()`.
  * En validación cruzada: Confirmar que tras el `join` no ocurra una multiplicación cartesiana de registros que altere las métricas numéricas.

---

### 2. Definición de Esquemas (*Schemas*) de Tipos de Datos
* **Qué hacer:** Proporcionar los esquemas mediante `StructType` al leer archivos crudos (CSV/JSON) en lugar de depender de `inferSchema=True`.
* **Cuándo conviene hacerlo:** Al procesar datos tabulares delimitados por texto o fuentes públicas externas con dsistintos tipos que son propensos a error (fechas, monedas, ceros a la izquierda en claves).
* **Cómo verificarlo:** 
  * En Spark UI verificar que la lectura no genere un Job adicional completo de muestreo para determinar los tipos de datos.
  * En el código ejecutar `df.printSchema()` y comprobar que las columnas numéricas no queden tipificadas como `string` o que haya algún otro error en los tipos de datos.

---

### 3. Almacenamiento Columnar y Formato de Lectura/Escritura
* **Qué hacer:** Almacenar todas los archivos de procesamiento intermedios y los archivos finales en formato Apache Parquet con compresión Snappy.
* **Cuándo conviene hacerlo:** Desde el momento en que se procesan y limpian los datos crudos hacia `data/processed/` o `./output/`.
* **Cómo verificarlo:**
  * En disco se compara la reducción de tamaño en comparación con CSV.
  * En el plan físico confirmar que el nodo de escaneo aparezca como `Scan parquet` en lugar de `Scan csv`, habilitando lectura selectiva de columnas a nivel binario.

---

### 4. Selección Selectiva de Columnas (*Projection Pruning*)
* **Qué hacer:** Aplicar `select()` de inmediato para conservar únicamente las columnas requeridas por el análisis, descartando datos inncesarios de gran volumen.
* **Cuándo conviene hacerlo:** Inmediatamente después de la ingesta de las fuentes amplias.
* **Cómo verificarlo:**
  * En el Plan Físico el operador `Project` debe ubicarse inmediatamente adyacente a `Scan parquet`.
  * En Spark UI se compara el volumen de *Shuffle Write Size* y se transportan solo llaves y montos para reducir el intercambio de red en comparación con tablas completas.

---

### 5. Filtrado Temprano de Filas (*Predicate Pushdown*)
* **Qué hacer:** Colocar las condiciones `filter()` o `where()` lo más cerca posible de la lectura inicial para delimitar los datasets antes de agregaciones o joins.
* **Cuándo conviene hacerlo:** Siempre que se trabaje con subconjuntos temporales (p. ej. `CICLO >= 2011`) o filtros de serie (p. ej. claves `XBB` y `XBC` en SHCP).
* **Cómo verificarlo:**
  * En el plan físico se ve en `explain("formatted")`, revisar la sección `PushedFilters` del operador de lectura para asegurar que el filtro se aplicó al archivo Parquet.
  * En Spark UI se comprueba que el número de registros de entrada (*Input Records*) en la pestaña de Stages sea menor al total de la fuente.

---

### 6. Particionamiento: Ingesta vs. Shuffle y Cardinalidad de Clave
* **Qué hacer:** Comprender la diferencia entre particiones de lectura (dictadas por el tamaño del archivo y bloques de disco) y particiones de Shuffle (`spark.sql.shuffle.partitions`). Al usar `repartition(N, col)`, asegurar que la columna elegida tenga una cardinalidad mayor o igual a $N$.
* **Cuándo conviene hacerlo:** Al preparar datasets masivos para distribución paralela o al reestructurar datos para un procesamiento por grupos.
* **Cómo verificarlo:**
  * En código se inspecciona la ocupación real por partición utilizando `df.groupBy(F.spark_partition_id().alias("PID")).count()`.
  * En Spark UI se confirma que no existan particiones con $0$ registros o tareas que procesen el $90\%$ de los datos mientras otras terminan en $0\text{ ms}$.

---

### 7. Estrategia de Joins: Broadcast Hash Join vs. Sort-Merge Join
* **Qué hacer:** Facilitar la selección de **Broadcast Hash Join (BHJ)** usando en la tabla más pequeña la función `broadcast(df_pequeño)` cuando una de las fuentes quepa en la memoria del Driver/Executors ($\le 10\text{ MB}$). Para datasets grandes donde ambos lados superen este tammaño permitir **Sort-Merge Join (SMJ)**.
* **Cuándo conviene hacerlo:** En cruces donde una tabla es un catálogo, índice o serie mensual agregada (como el INPC, IGAE o series resumidas) frente a una tabla transaccional más extensa.
* **Cómo verificarlo:**
  * En el plan físico localizar el operador `BroadcastHashJoin` y `BroadcastExchange`, y comprobar la ausencia total de operadores de ordenamiento (`Sort`) o redistribución masiva (`ShuffleExchangeExec`).
  * En Spark UI el Job no debe registrar etapas con transferencias pesadas de *Shuffle Read/Write* asociadas al cruce de datos.

---

### 8. Agregaciones Distribuidas en Dos Fases (*HashAggregate*)
* **Qué hacer:** Utilizar funciones agregadas nativas de PySpark (`sum`, `avg`, `first`) dentro de `groupBy()` o `pivot()`, evitando el uso de funciones definidas por el usuario (UDFs) en Python que rompen la optimización de Catalyst.
* **Cuándo conviene hacerlo:** Al convertir series de alta frecuencia (como cotizaciones diarias del S&P 500 y tipo de cambio) a promedios o volatilidades mensuales.
* **Cómo verificarlo:**
  * En el plan físico verificar la estructura clásica de agregación: `HashAggregate (Parcial)` $\rightarrow$ `Exchange` $\rightarrow$ `HashAggregate (Final)`.
  * En Spark UI la agregación parcial reduce el volumen de filas en la etapa emisora antes de enviar los datos al *Shuffle Write*.

---

### 9. Minimización y Diagnóstico de Shuffle (*Exchange*)
* **Qué hacer:** Cada operador `Exchange` en el plan físico representa una frontera de Shuffle que escribe en disco local y transfiere por red. Diseñar las transformaciones para minimizar operaciones amplias consecutivas.
* **Cuándo conviene hacerlo:** Al encadenar múltiples `groupBy`, `distinct` y `join`.
* **Cómo verificarlo:**
  * En el Plan Físico contar el número de nodos `Exchange` presentes en el árbol.
  * En Spark UI (pestaña Stages) inspeccionar las columnas *Shuffle Read Size / Records* y *Shuffle Write Size / Records* para comprobar el volumen real transferido.

---

### 10. Gestión de Memoria y Prevención de Desbordamiento a Disco (*Spill*)
* **Qué hacer:** Monitorear que la memoria de ejecución (*Execution Memory*) sea suficiente para retener las estructuras hash de agregación y los búferes de ordenamiento. Si ocurre desborde, aumentar particiones de shuffle o reestructurar el pipeline.
* **Cuándo conviene hacerlo:** Durante operaciones de ordenamiento intensivo (`Sort`), ventanas analíticas complejas o Sort-Merge Joins con datasets muy grandes.
* **Cómo verificarlo:**
  * En Spark UI (pestaña Stages / Tasks Table) revisar las columnas *Spill (Memory)* y *Spill (Disk)*. Un valor de $0.0\text{ B}$ confirma que el cómputo se resolvió $100\%$ *in-memory*. Cualquier valor $>0$ indica degradación de rendimiento por I/O en disco.

---

### 11. Uso Táctico de Caché y Persistencia (`persist` y `unpersist`)
* **Qué hacer:** Aplicar el principio metodológico del curso: **Almacenar $\rightarrow$ Observar $\rightarrow$ Reutilizar $\rightarrow$ Liberar**. Invocar `df.persist(StorageLevel.MEMORY_AND_DISK)` únicamente si el DataFrame resultante se utilizará en múltiples acciones siguientes. Liberar con `df.unpersist()` al terminar su uso.
* **Cuándo conviene hacerlo:** Tras transformaciones costosas que alimentan varias consultas analíticas. Nunca utilizar caché en DataFrames que solo se leen y ejecutan una sola vez.
* **Cómo verificarlo:**
  * En Spark UI (pestaña Storage) confirmar que el objeto aparezca en la lista con su porcentaje en RAM ($100\%$) y memoria consumida. Tras invocar `unpersist()`, verificar que desaparezca inmediatamente liberando la memoria.

---

### 12. Optimización Adaptativa (*Adaptive Query Execution - AQE*)
* **Qué hacer:** En producción y desarrollo regular, mantener habilitado AQE (`spark.sql.adaptive.enabled = true`). Este tiene tres mecanismos: colapso de particiones pequeñas (*Coalescing Partitions*), cambio dinámico a Broadcast Join (*Converting SMJ to BHJ* en runtime) y optimización de sesgo (*Skew Join*).
* **Cuándo conviene hacerlo:** En todas las ejecuciones habituales, salvo cuando se desee realizar pruebas controladas de infraestructura fijando particiones manuales.
* **Cómo verificarlo:**
  * En el plan físico identificar el nodo raíz `AdaptiveSparkPlan (isFinalPlan=...)`.
  * En Spark UI observar etapas marcadas como omitidas (*Skipped Stages*) o particiones fusionadas en etapas.

---

### 13. Trazabilidad y Observabilidad Estructurada con `setJobGroup`
* **Qué hacer:** Antes de detonar cualquier acción (`count()`, `show()`, `write()`, `toPandas()`), asignar un grupo descriptivo mediante `spark.sparkContext.setJobGroup("ID_GRUPO", "Descripción de la fase")`.
* **Cuándo conviene hacerlo:** En todos los notebooks del proyecto para separar visualmente las etapas de ingesta, validación, transformación y exportación.
* **Cómo verificarlo:**
  * En Spark UI (pestañas Jobs y Stages) la columna *Job Group / Description* mostrará las etiquetas personalizadas en lugar del genérico `showString at <unknown>:0` o `count at ...`, permitiendo analizar con exactitud el costo de cada celda.

# Actividad 12 — Etapa D: Arquitectura de Datos Aplicada y Gobernanza

Este documento detalla la arquitectura de extremo a extremo implementada en el Proyecto Integrador, explicando el recorrido de la información, el desacoplamiento entre cómputo y almacenamiento, y el código formal para representar el flujo en un diagrama visual.

---

## 1. Recorrido End-to-End del Dato

```text
FUENTES CRUDAS         INGESTA Y VALIDACIÓN          TRANSFORMACIÓN Y LIMPIEZA         INTEGRACIÓN (CATALYST)        ALMACENAMIENTO (GOLD)         CAPA DE CONSUMO
(APIs / Descargas)     (src/ingestion/extract.py)    (src/transformations/)             (pipeline.py)                 (data/processed/)             (notebooks/ + SQL)

  SHCP + INPC + IGAE ──> extract_lab8()           ──> Validación Lab8 (validate.py) ──┐
  (Parquet Lab 8)                                                                     │
                                                                                      ├──> Inner Join 1:1           ──> proyecto_integrado_       ──> 04_analisis_tc
  Banxico FIX (Diario)─> extract_tipo_cambio()    ──> prepare_tipo_cambio()         ──┤    (BroadcastHashJoin)          mensual.parquet               05_analisis_sp500
  (8,791 filas CSV)      (StructType explícito)        (Filtro N/E + max_by Cierre)   │    (Sin Exchange en cruce)      (187 filas, 12 columnas,      06_etapa_b_consumo
                                                                                      │                                  0 nulos, columnar snappy)    (Spark SQL / BI)
  Yahoo Finance       ──> extract_sp500()          ──> prepare_sp500()               ──┘
  (S&P 500 + VIX CSV)    (StructType explícito)        (Cierre, Promedio y VIX)
```

---

## 2. Organización Lógica bajo el Modelo Medallion

Siguiendo las mejores prácticas de la **Clase 12**, los datos se organizan en tres capas de madurez:

1. **Capa Bronze (Datos Crudos e Inmutables):**
   * **Archivos:** `data/raw/tipo_cambio_fix.csv`, `data/raw/s&p500_mensual.csv` y `data/raw/finanzas_igae_mensual_final.parquet`.
   * **Propósito:** Conservar las fuentes tal cual fueron descargadas desde Banxico y Yahoo Finance, garantizando resiliencia y auditoría ante reprocesamientos futuros.
2. **Capa Silver (Limpieza y Homologación de Claves):**
   * **Lógica:** Implementada en `src/transformations/integrate_enrich.py`.
   * **Operaciones:** Filtrado de valores inválidos (`N/E`), casteo tipado, agregación de serie diaria a mensual (`groupBy(PERIODO).agg(max_by)`), homologación temporal mediante `F.trunc(..., "month")` para asegurar exactamente 1 fila por mes calendario.
3. **Capa Gold (Activo de Datos de Negocio):**
   * **Archivo:** `data/processed/proyecto_integrado_mensual.parquet`.
   * **Características:** 187 registros mensuales (2011-01 a 2026-07) con 12 columnas consolidadas y 0 valores nulos. Es la única fuente de verdad optimizada para consumo analítico y modelado.

---

## 3. Desacoplamiento de Cómputo y Almacenamiento

* **Motor de Cómputo (Transitorio):** Apache Spark opera como el motor de trabajo pesado. Se inicializa bajo demanda (`spark-submit`), ejecuta el DAG optimizado por Catalyst, valida los datos y se apaga limpiamente al persistir el Parquet.
* **Persistencia Inmutable (Permanente):** El almacenamiento en **formato columnar Parquet** proporciona una alta compresión de almacenamiento y permite el salto de filas/columnas (*Projection Pruning* y *Predicate Pushdown*) al momento de la consulta.
* **Reutilización del Esfuerzo (Write Once, Read Many):** La integración costosa de fuentes se realiza **una sola vez** en el pipeline. Los analistas y científicos de datos consultan la Capa Gold tantas veces como sea necesario utilizando Spark SQL o Pandas sin tener que volver a limpiar ni cruzar los archivos crudos.

---

## 4. Estrategia de Mantenimiento y Evolución Arquitectónica

* **Mantenimiento Actual (Sobreescritura Idempotente):** Dado el tamaño compacto del dataset (187 periodos mensuales), el pipeline utiliza sobreescritura controlada (`mode("overwrite")` con verificación previa) garantizando reproducibilidad sin acumular registros duplicados.
* **Incorporación de Nuevos Datos:** Para agregar nuevos meses de información (ej. actualizaciones mensuales de Banxico o SHCP), se incorporan las nuevas filas al archivo crudo en `data/raw/` y se ejecuta el pipeline modular. Los contratos de `validate.py` confirmarán que la cobertura temporal se amplió sin duplicar meses previos.
* **Evolución hacia Lakehouse / Data Warehouse:** En un escenario de producción industrial con millones de registros y flujos en tiempo real, la solución evolucionaría reemplazando el archivo Parquet estándar por **Delta Lake**, habilitando operaciones transaccionales ACID (`MERGE INTO` para cargas incrementales sin sobreescritura total), control de versiones (*Time Travel*) y esquemas gobernados.

---

## 5. Código del Diagrama de Arquitectura (Mermaid)

El siguiente bloque de código está escrito en sintaxis estándar de **Mermaid**. Puedes copiarlo e insertarlo directamente en páginas web como **[mermaid.live](https://mermaid.live/)** para generar y descargar el diagrama en formato **PNG** o **SVG**, o pegarlo dentro del `README.md` de GitHub (donde se renderiza automáticamente como diagrama gráfico interactivo):

```mermaid
flowchart TD
    %% Estilos de Nodos
    classDef bronze fill:#fde2e4,stroke:#e07a5f,stroke-width:2px,color:#3d405b;
    classDef silver fill:#e2ece9,stroke:#81b29a,stroke-width:2px,color:#3d405b;
    classDef gold fill:#fefae0,stroke:#f2cc8f,stroke-width:2px,color:#3d405b;
    classDef engine fill:#e0e1dd,stroke:#3d405b,stroke-width:2px,stroke-dasharray: 5 5,color:#3d405b;
    classDef consumption fill:#d8e2dc,stroke:#457b9d,stroke-width:2px,color:#1d3557;

    %% 1. FUENTES DE DATOS (BRONZE)
    subgraph BRONZE ["1. Capa Bronze: Fuentes de Datos Crudas (data/raw/)"]
        F1[("finanzas_igae_mensual_final.parquet<br/>• Salida Lab 8 (187 meses)<br/>• SHCP, INPC, IGAE sectorial")]:::bronze
        F2[("tipo_cambio_fix.csv<br/>• Banxico Diario (8,791 registros)<br/>• Columnas: fecha, dato")]:::bronze
        F3[("s&p500_mensual.csv<br/>• Yahoo Finance (442 registros)<br/>• S&P 500 y VIX")]:::bronze
    end

    %% 2. INGESTA Y CONTRATOS DE ENTRADA
    subgraph INGEST ["2. Ingesta y Contratos de Calidad (src/ingestion/ & src/quality/)"]
        E1["extract_lab8()<br/>• Lectura Parquet<br/>• validate_lab8_input()"]:::silver
        E2["extract_tipo_cambio()<br/>• StructType explícito<br/>• validate_tc_input()"]:::silver
        E3["extract_sp500()<br/>• StructType explícito<br/>• validate_sp500_input()"]:::silver
    end

    %% 3. TRANSFORMACIÓN Y LIMPIEZA
    subgraph TRANSFORM ["3. Transformación y Homologación (src/transformations/)"]
        T1["Validación Base Lab 8<br/>• Verificación de tipos y 0 duplicados"]:::silver
        T2["prepare_tipo_cambio()<br/>• Limpieza de 'N/E'<br/>• F.trunc(fecha, 'month') -> PERIODO<br/>• groupBy('PERIODO').agg(max_by) -> TC_CIERRE"]:::silver
        T3["prepare_sp500()<br/>• F.trunc(fecha, 'month') -> PERIODO<br/>• Proyección: Cierre, Promedio y VIX"]:::silver
    end

    %% 4. MOTOR PYSPARK Y OPTIMIZADOR CATALYST
    subgraph CATALYST ["4. Motor Distribuido Apache Spark (pipeline.py)"]
        direction TB
        SPK["SparkSession: local[*]<br/>setJobGroup('PROYECTO_PIPELINE')"]:::engine
        JOIN["integrate_and_enrich()<br/>• Inner Join 1:1 por PERIODO<br/>• F.broadcast(TC) + F.broadcast(SP500)<br/>• Catalyst: BroadcastHashJoin (Sin Shuffle)"]:::engine
        VAL["validate_output()<br/>• 187 filas exactas (1:1)<br/>• Esquema de 12 columnas<br/>• 0 Valores Nulos"]:::engine
    end

    %% 5. PERSISTENCIA EN CAPA GOLD
    subgraph GOLD ["5. Capa Gold: Activo de Datos Reutilizable (data/processed/)"]
        PARQUET[("proyecto_integrado_mensual.parquet<br/>• Almacenamiento Columnar Snappy<br/>• 187 periodos (2011-01 a 2026-07)<br/>• 12 columnas limpias")]:::gold
        REP["reports/ultima_ejecucion.json<br/>• Trazabilidad y evidencia (Estado: OK)"]:::gold
    end

    %% 6. CONSUMO ANALÍTICO Y CONSULTAS
    subgraph CONSUMPTION ["6. Capa de Consumo Analítico (Clase 12)"]
        NB4["04_analisis_tipo_cambio.ipynb<br/>• Rendimientos interanuales (12M)<br/>• Pass-through e inflación (INPC)<br/>• Sensibilidad cambiaria"]:::consumption
        NB5["05_analisis_sp500.ipynb<br/>• S&P 500 Cierre vs Promedio<br/>• Índice VIX (Estrés global)<br/>• Correlación con Manufactura"]:::consumption
        NB6["06_etapa_b_capa_consumo.ipynb<br/>• Spark SQL: Tablas Temporales<br/>• Consultas analíticas (VIX >= 25)<br/>• Consolidación anual y regímenes"]:::consumption
    end

    %% CONEXIONES
    F1 --> E1
    F2 --> E2
    F3 --> E3

    E1 --> T1
    E2 --> T2
    E3 --> T3

    T1 & T2 & T3 --> JOIN
    SPK -.-> JOIN
    JOIN --> VAL
    VAL --> PARQUET
    VAL --> REP

    PARQUET --> NB4
    PARQUET --> NB5
    PARQUET --> NB6
```

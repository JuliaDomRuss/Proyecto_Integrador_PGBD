# Fuentes de datos

Este documento describe las fuentes que utiliza el proyecto, su relación entre sí, las decisiones de selección y las alternativas evaluadas. El análisis final cubre de **enero de 2011 a julio de 2026** (187 meses), que es el periodo en que coinciden todas las fuentes.

## Indicaciones: 
• Investiguen al menos dos fuentes públicas adicionales. Para cada una registren: institución y enlace oficial, variables, periodicidad, cobertura temporal, formato de acceso y relación con las fuentes base.
• Seleccionen una fuente candidata y justifiquen su utilidad y viabilidad. Hoy no es necesario descargarla ni integrarla.
• Seleccionen una fuente candidata y justifiquen su utilidad y viabilidad. Hoy no es necesario descargarla ni integrarla.

## Resumen de fuentes utilizadas

| # | Fuente | Institución | Periodicidad | Cobertura de la fuente | Archivo en `data/raw/` | Papel en el proyecto |
|---|---|---|---|---|---|---|
| 1 | Ingresos y gastos del Gobierno Federal (`XBB`, `XBC`) | SHCP | Mensual | Desde 2011 | `finanzas_igae_mensual_final.parquet` (Laboratorio 8) | Fuente base: finanzas públicas |
| 2 | INPC | INEGI | Mensual | Consolidado en el Laboratorio 8 | `finanzas_igae_mensual_final.parquet` (Laboratorio 8) | Fuente base: deflactor e inflación |
| 3 | IGAE | INEGI | Mensual | Desde 1993 | `finanzas_igae_mensual_final.parquet` (Laboratorio 8) | Fuente base: actividad económica |
| 4 | Tipo de cambio FIX (serie `SF43718`) | Banxico | Diaria | 12-nov-1991 a la fecha | `tipo_cambio_fix.csv` | Fuente adicional: presión cambiaria |
| 5 | S&P 500 (`^GSPC`) y VIX | Yahoo Finanzas (`yfinance`) | Mensual (descarga) | Desde 1990-01-01 | `s&p500_mensual.csv` | Fuente adicional: apetito por riesgo e incertidumbre global |

**Alcance temporal efectivo.** Las finanzas públicas de la SHCP empiezan en 2011, por lo que el dataset integrado cubre 2011-2026. Las fuentes 3, 4 y 5 tienen historia más larga, pero esa historia no se usa en el análisis.

**Fuente descartada del alcance:** se había propuesto un archivo CSV de eventos históricos elaborado por el equipo. Se decidió **excluirlo** del proyecto; el periodo COVID (marzo 2020 a diciembre 2021) se define directamente en el análisis a partir de las series.

---

## Fuentes base (consolidadas en el Laboratorio 8)

Las tres fuentes base se integraron en el Laboratorio 8 en un único Parquet mensual, `finanzas_igae_mensual_final.parquet`, que es la entrada del pipeline.

| Campo | SHCP | INEGI (INPC) | INEGI (IGAE) |
|---|---|---|---|
| Institución | Secretaría de Hacienda y Crédito Público | Instituto Nacional de Estadística y Geografía | Instituto Nacional de Estadística y Geografía |
| Enlace oficial | https://www.datos.gob.mx/organization/secretaria_hacienda | https://www.inegi.org.mx/temas/inpc/ | https://www.inegi.org.mx/temas/igae/ |
| Variables | Ingresos (`XBB`) y Gastos (`XBC`) del Gobierno Federal | Índice Nacional de Precios al Consumidor | Índice total y actividades primarias, secundarias y terciarias (base 2018 = 100) |
| Periodicidad | Mensual | Mensual | Mensual |
| Uso en el proyecto | Ingresos y gastos reales (deflactados con el INPC) | Deflactor y cálculo de inflación | Medida de la actividad económica por sector |

El Parquet del Laboratorio 8 contiene 8 columnas base y 187 registros (una fila por mes, sin duplicados).

---

## Fuente adicional 1: Tipo de cambio FIX (Banco de México)

| Campo | Descripción |
|---|---|
| Institución | Banco de México (Banxico), Sistema de Información Económica (SIE) |
| Enlace oficial | https://www.banxico.org.mx/SieInternet/ · API: https://www.banxico.org.mx/SieAPIRest/service/v1/ |
| Serie | SF43718: tipo de cambio pesos por dólar E.U.A., FIX |
| Variables | `fecha` (dd/mm/aaaa) y `dato` (pesos por dólar) |
| Periodicidad | Diaria (días hábiles bancarios) |
| Cobertura temporal | 12-nov-1991 a la fecha |
| Formato de acceso | Consulta web con exportación a Excel/CSV, o API REST (JSON) con token gratuito |

**¿Qué es el FIX?** Es el tipo de cambio que Banxico determina con base en las cotizaciones del mercado cambiario. Se publica en el Diario Oficial de la Federación y es la referencia oficial para solventar en México obligaciones denominadas en dólares.

### Relación con las fuentes base
- **Clave común:** el mes (`PERIODO` = primer día del mes), la misma usada para integrar IGAE e ingresos-gastos en el Laboratorio 8.
- **Relación económica:** la depreciación del peso encarece las importaciones (INPC), afecta la actividad económica (IGAE) y modifica el valor en pesos de la deuda y de los ingresos públicos ligados al dólar (SHCP). Es uno de los principales canales por los que un choque global llega a la economía mexicana.

### Preparación para la integración
1. Convertir `fecha` a tipo fecha y `dato` a numérico, descartando los días sin dato (`N/E`).
2. Agregar a frecuencia mensual: promedio del mes (`TC_PROMEDIO`), último dato del mes (`TC_CIERRE`) y número de días hábiles.
3. Calcular la depreciación mensual (%) y un z-score móvil para identificar cambios bruscos.
4. Integrar por `PERIODO` con IGAE, INPC y finanzas públicas.

### Consideraciones
- **Coberturas distintas:** tipo de cambio desde 1991, IGAE desde 1993 y finanzas públicas desde 2011. Un *inner join* de todas las fuentes recortaría el análisis a 2011 en adelante y dejaría fuera la crisis de 1994-95 y la de 2008. Por eso se usa *left join* a partir del IGAE, y los meses sin datos de finanzas quedan como nulos documentados.
- **Fines de semana y días festivos** no tienen dato; al agregar a mensual esto no afecta.
- **El token de la API no se sube al repositorio** (se guarda como variable de entorno).

### Justificación de la selección
Es la fuente oficial del tipo de cambio en México, es gratuita, cubre todo el periodo de análisis (incluidas las crisis de 1994, 2008 y 2020), se actualiza diariamente y permite una descarga reproducible por API. Tras agregarla a mensual, comparte granularidad y clave de integración con las fuentes base.

### Alternativa evaluada: FRED (serie EXMXUS)
La Reserva Federal publica en FRED el tipo de cambio peso-dólar como promedio mensual (nov-1993 a la fecha, CSV sin registro): https://fred.stlouisfed.org/series/EXMXUS. Se descartó como fuente principal porque no es la referencia oficial en México y solo ofrece promedios mensuales, que suavizan los cambios bruscos que buscamos detectar. Se puede usar para validar los datos de Banxico.

### Token y acceso a datos
El token obtenido es el siguiente: 94e0b5bdb67dd258db5ae0dfaa9b24c6ef9601b20c8538204484a582c7ac4efe

## Fuente adicional 2: S&P 500 y VIX (Yahoo Finanzas, vía `yfinance`)

| Campo | Descripción |
|---|---|
| Institución | Yahoo Finance; librería https://pypi.org/project/yfinance/ |
| Enlace oficial | https://finance.yahoo.com/quote/%5EGSPC/ |
| Símbolos | `^GSPC` (S&P 500) y el índice de volatilidad VIX de CBOE (`^VIX`) |
| Variables en el CSV del proyecto | `fecha`, `sp500_cierre`, `sp500_promedio`, `vix_cierre` |
| Periodicidad | Mensual (la serie se descarga ya mensualizada, con fecha al último día calendario del mes) |
| Cobertura temporal descargada | Desde 1990-01-01 (442 registros mensuales) |
| Formato de acceso | Librería Python no oficial, sin token ni registro; DataFrame de Pandas |
| Descarga en el proyecto | [Notebook 02](../notebooks/02_descarga_sp%26500_mensual.ipynb), que guarda `data/raw/s&p500_mensual.csv` |

### Variables usadas y su papel
- **`sp500_cierre`** (`SP500_CIERRE`): cierre mensual del índice; proxy del apetito por riesgo y de la valuación bursátil.
- **`sp500_promedio`** (`SP500_PROMEDIO`): promedio mensual de las cotizaciones diarias; suaviza la volatilidad de un solo día y es más comparable con variables de flujo mensual como el IGAE.
- **`vix_cierre`** (`VIX_CIERRE`): índice de volatilidad de CBOE, que mide la incertidumbre y la aversión al riesgo en los mercados de EE.UU. Es la variable que se usa para identificar los periodos de estrés global.

### Relación con las fuentes base
- **Clave común:** `PERIODO`. En el pipeline la fecha del archivo (último día calendario del mes) se trunca al primer día del mes con `F.trunc(..., "month")` para alinearla con el Parquet del Laboratorio 8.
- **Relación económica:** los mercados de EE.UU. condicionan el apetito por activos mexicanos y, por esa vía, el tipo de cambio, las tasas y la actividad. El VIX es el indicador estándar de incertidumbre global.

### Contexto del S&P 500

El **S&P 500** es uno de los índices bursátiles más seguidos del mundo. Agrupa a 500 grandes empresas que cotizan en las bolsas de Estados Unidos y se usa como referencia del desempeño del mercado accionario estadounidense. Lo administra S&P Dow Jones Indices y, en su forma actual, existe desde 1957. Se pondera por capitalización de mercado ajustada por acciones disponibles al público (*free float*) y, según S&P, cubre aproximadamente el 80 % de la capitalización del mercado de acciones estadounidense.

El índice no se compra directamente: se replica con fondos cotizados (ETF) como SPY o IVV y con futuros. Por eso, al buscar datos conviene distinguir entre el **índice** (`^GSPC`, un valor calculado), los **ETF que lo replican** (`SPY`, que se negocian y tienen volumen y dividendos) y los **datos de contexto** (tasas, tipo de cambio, inflación).

### Ficha comparativa de fuentes evaluadas

| Fuente | Institución y enlace oficial | Variables | Periodicidad | Cobertura temporal | Formato de acceso | Relación con el S&P 500 |
|---|---|---|---|---|---|---|
| **Yahoo Finanzas (vía yfinance)** | Yahoo Finance, https://finance.yahoo.com; librería https://pypi.org/project/yfinance/ | Open, High, Low, Close, Volume, Dividends, Stock Splits; datos fundamentales | Intradía (desde 1 minuto), diaria, semanal, mensual, trimestral | `^GSPC` con histórico diario desde 1927; intradía solo en ventanas cortas | Librería Python no oficial, sin token ni registro; DataFrame de Pandas | Entrega el índice (`^GSPC`), el VIX, ETF como `SPY` y los componentes individuales. **Fuente elegida** |
| **S&P Dow Jones Indices** | S&P Dow Jones Indices, https://www.spglobal.com/spdji/en/indices/equity/sp-500/ | Nivel del índice, rendimientos, metodología, composición, ponderaciones | Diaria; composición con cambios periódicos | Histórico largo; el acceso completo depende de la licencia | Portal web (consulta y descargas limitadas); datos completos de pago | Fuente primaria y oficial del índice y su metodología |
| **FRED** | Federal Reserve Bank of St. Louis, https://fred.stlouisfed.org/series/SP500 | Cierre diario del S&P 500 (serie `SP500`); tasas, inflación y rendimientos del Tesoro | Diaria para `SP500`; otras series de diaria a anual | Para `SP500`, unos 10 años recientes por restricciones de licencia | API con clave gratuita (JSON/XML); CSV; `pandas-datareader` | Cierre del índice y variables macro de EE. UU. |
| **Banxico (SIE)** | Banco de México, https://www.banxico.org.mx/SieAPIRest/service/v1/ | Tipo de cambio FIX, TIIE, CETES, inflación, reservas | Diaria a anual según la serie | Varía por serie | API REST con token obligatorio (JSON); CSV/Excel desde el portal | Indirecta: no publica el índice, solo USD/MXN y tasas mexicanas |
| **BMV (SIC)** | Bolsa Mexicana de Valores, https://www.bmv.com.mx | Precios y volumen de valores extranjeros y ETF listados en México | Tiempo real y diaria | Consulta pública limitada; histórico extenso de pago | Portal web; servicios de datos de pago | Acceso en pesos a ETF y emisoras del índice (verificar emisoras disponibles) |

### Justificación de la fuente elegida: `yfinance`

**Utilidad**
- **Cubre directamente el S&P 500 y el VIX** con la misma interfaz, además de ETF y componentes.
- **Histórico largo:** el índice tiene datos diarios desde 1927; para el proyecto se usa la serie mensual desde 1990.
- **Datos listos para analizar:** el resultado es un DataFrame de Pandas.

**Por qué no Banxico como fuente del S&P 500**
- **No contiene el S&P 500:** el SIE publica variables económicas de México, no precios del índice estadounidense. Se usa para el tipo de cambio, no para este índice.
- **Requiere token y limita las consultas.** `yfinance` no necesita cuenta ni clave.

**Granularidad con `yfinance`.** El parámetro `interval` permite elegir la granularidad; a mayor detalle, menos historia (límites aproximados que Yahoo puede cambiar):

| `interval` | Granularidad | Historia disponible aproximada |
|---|---|---|
| `1m` | 1 minuto | Hasta 7 días por solicitud, dentro de los últimos ~30 días |
| `2m`, `5m`, `15m`, `30m`, `90m` | Intradía | Últimos ~60 días |
| `60m` / `1h` | 1 hora | Últimos ~730 días |
| `1d`, `5d` | Diaria / 5 días | Histórico completo |
| `1wk`, `1mo`, `3mo` | Semanal / mensual / trimestral | Histórico completo |

El proyecto usa granularidad **mensual**, porque el resto de las fuentes es mensual y el pipeline integra por `PERIODO`.

**Viabilidad**
- **Acceso inmediato y costo cero:** instalación con `pip` y una línea de código, sin trámites.
- **Riesgos:** es una librería no oficial que depende de Yahoo; el formato o la disponibilidad pueden cambiar, hay límites de peticiones y los términos de Yahoo restringen ciertos usos. Los datos descargados se guardan localmente (`data/raw/`) para reproducir el análisis sin volver a consultar a Yahoo.

### Ejemplo de extracción

```python
import yfinance as yf

sp500 = yf.Ticker("^GSPC")

# Histórico mensual
hist = sp500.history(period="max", interval="1mo")
```

---

## Conclusión

| Criterio | Resultado |
|---|---|
| Fuentes base | SHCP, INPC e IGAE, consolidadas en el Parquet del Laboratorio 8 |
| Fuentes adicionales | Tipo de cambio FIX (Banxico) y S&P 500 con VIX (Yahoo Finanzas, `yfinance`) |
| Fuente excluida del alcance | CSV de eventos históricos |
| Periodo de análisis | Enero 2011 a julio 2026 (187 meses), determinado por la cobertura de las finanzas públicas |
| Clave de integración | `PERIODO` (primer día del mes), con *inner join* y `broadcast` |
| Riesgos principales | `yfinance` es una librería no oficial; el token de Banxico se lee de la variable de entorno `BANXICO_TOKEN`, no del código |
| Validación cruzada posible | FRED (EXMXUS para el tipo de cambio; SP500 para el índice) |

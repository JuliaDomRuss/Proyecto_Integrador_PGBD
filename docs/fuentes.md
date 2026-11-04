• Investiguen al menos dos fuentes públicas adicionales. Para cada una registren: institución y enlace oficial, variables, periodicidad, cobertura temporal, formato de acceso y relación con las fuentes base.
• Seleccionen una fuente candidata y justifiquen su utilidad y viabilidad. Hoy no es necesario descargarla ni integrarla.
• Seleccionen una fuente candidata y justifiquen su utilidad y viabilidad. Hoy no es necesario descargarla ni integrarla.


## Fuentes de datos del S&P 500

### Contexto

El **S&P 500** es uno de los índices bursátiles más seguidos del mundo. Agrupa a 500 grandes empresas que cotizan en las bolsas de Estados Unidos y se usa como referencia del desempeño del mercado accionario estadounidense. Lo administra S&P Dow Jones Indices y, en su forma actual, existe desde 1957.

Se pondera por capitalización de mercado ajustada por acciones disponibles al público (*free float*), de modo que las empresas más grandes pesan más en el valor del índice. Un comité de S&P Dow Jones Indices decide la inclusión de cada empresa con criterios como liquidez, rentabilidad y domicilio en EE. UU. Según S&P, el índice cubre aproximadamente el 80% de la capitalización del mercado de acciones estadounidense.

El índice no se compra directamente: se replica con fondos cotizados (ETF) como SPY o IVV y con futuros. Por eso, al buscar datos conviene distinguir entre:

- **El índice** (en Yahoo Finanzas, `^GSPC`): es un valor calculado, no se negocia.
- **ETF que lo replican** (por ejemplo `SPY`): se negocian y tienen volumen y dividendos.
- **Datos de contexto** (tasas, tipo de cambio, inflación), que ayudan a explicar su comportamiento.

Para un analista en México, el S&P 500 también importa por el tipo de cambio USD/MXN y las tasas de referencia, que afectan el rendimiento en pesos de una inversión en este índice.

### Ficha comparativa de fuentes

| Fuente | Institución y enlace oficial | Variables | Periodicidad | Cobertura temporal | Formato de acceso | Relación con el S&P 500 |
|---|---|---|---|---|---|---|
| **Yahoo Finanzas (vía yfinance)** | Yahoo Finance, https://finance.yahoo.com; librería https://pypi.org/project/yfinance/ | Open, High, Low, Close, Volume, Dividends, Stock Splits; datos fundamentales | Intradía (desde 1 minuto), diaria, semanal, mensual, trimestral | `^GSPC` con histórico diario desde 1927; intradía solo en ventanas cortas | Librería Python no oficial, sin token ni registro; DataFrame de Pandas | Entrega el índice (`^GSPC`), ETF como `SPY` y los componentes individuales |
| **S&P Dow Jones Indices** | S&P Dow Jones Indices, https://www.spglobal.com/spdji/en/indices/equity/sp-500/ | Nivel del índice, rendimientos, metodología, composición, ponderaciones | Diaria; composición con cambios periódicos | Histórico largo; el acceso completo depende de la licencia | Portal web (consulta y descargas limitadas); datos completos de pago | Fuente primaria y oficial del índice y su metodología |
| **FRED** | Federal Reserve Bank of St. Louis, https://fred.stlouisfed.org/series/SP500 | Cierre diario del S&P 500 (serie `SP500`); tasas, inflación y rendimientos del Tesoro | Diaria para `SP500`; otras series de diaria a anual | Para `SP500`, unos 10 años recientes por restricciones de licencia | API con clave gratuita (JSON/XML); CSV; `pandas-datareader` | Cierre del índice y variables macro de EE. UU. |
| **Banxico (SIE)** | Banco de México, https://www.banxico.org.mx/SieAPIRest/service/v1/ | Tipo de cambio FIX, TIIE, CETES, inflación, reservas | Diaria a anual según la serie | Varía por serie | API REST con token obligatorio (JSON); CSV/Excel desde el portal | Indirecta: no publica el índice, solo USD/MXN y tasas mexicanas |
| **BMV (SIC)** | Bolsa Mexicana de Valores, https://www.bmv.com.mx | Precios y volumen de valores extranjeros y ETF listados en México | Tiempo real y diaria | Consulta pública limitada; histórico extenso de pago | Portal web; servicios de datos de pago | Acceso en pesos a ETF y emisoras del índice (verificar emisoras disponibles) |

### Fuente candidata: `yfinance`

**Utilidad**

- **Cubre directamente el S&P 500:** entrega el índice (`^GSPC`), ETF que lo replican (`SPY`) y las acciones que lo componen, todo con la misma interfaz.
- **Variables completas:** apertura, máximo, mínimo, cierre, volumen, dividendos y splits.
- **Histórico largo:** el índice tiene datos diarios desde 1927.
- **Datos listos para analizar:** el resultado es un DataFrame de Pandas.

**Por qué no Banxico como fuente candidata**

- **Requiere token:** hay que registrarse, solicitarlo y gestionarlo de forma segura. `yfinance` no necesita cuenta ni clave.
- **No contiene el S&P 500:** el SIE publica variables económicas de México, no precios del índice estadounidense. Sirve como complemento (tipo de cambio, tasas), no como fuente principal.
- **Tiene limitaciones de uso:** la API restringe el número de consultas y de series por petición (consultar la documentación vigente).
- **Frecuencia y alcance:** muchas de sus series son de menor frecuencia y no ofrecen datos intradía.

**Granularidad con `yfinance`**

Sí es posible elegir la granularidad con el parámetro `interval` de `history()`. A mayor detalle, menos historia disponible (límites aproximados; Yahoo puede cambiarlos):

| `interval` | Granularidad | Historia disponible aproximada |
|---|---|---|
| `1m` | 1 minuto | Hasta 7 días por solicitud, dentro de los últimos ~30 días |
| `2m`, `5m`, `15m`, `30m`, `90m` | Intradía | Últimos ~60 días |
| `60m` / `1h` | 1 hora | Últimos ~730 días |
| `1d`, `5d` | Diaria / 5 días | Histórico completo |
| `1wk`, `1mo`, `3mo` | Semanal / mensual / trimestral | Histórico completo |

La granularidad fina (minutos) solo sirve para ventanas recientes. Para análisis de varios años hay que usar datos diarios o más gruesos, o guardar localmente las descargas intradía para formar un histórico propio.

**Viabilidad**

- **Acceso inmediato:** instalación con `pip` y una línea de código, sin trámites.
- **Costo cero.**
- **Riesgos:** es una librería no oficial que depende de Yahoo; el formato o la disponibilidad pueden cambiar, hay límites de peticiones y los términos de Yahoo restringen ciertos usos. Es adecuada para análisis académicos o exploratorios; para producción conviene contrastar con una fuente oficial o licenciada.

### Ejemplo de extracción

```python
import yfinance as yf

sp500 = yf.Ticker("^GSPC")

# Histórico completo, granularidad diaria
hist = sp500.history(period="max", interval="1d")

# Granularidad fina, ventana reciente
intradia = sp500.history(period="5d", interval="1m")
```

### Conclusión

| Criterio | Resultado |
|---|---|
| Fuente candidata | Yahoo Finanzas, a través de `yfinance` |
| Utilidad | Índice, ETF y componentes del S&P 500 con OHLC, volumen y dividendos en un DataFrame |
| Granularidad | Desde 1 minuto hasta trimestral; lo fino solo en ventanas recientes, lo diario en todo el histórico |
| Viabilidad | Sin token, sin costo, una línea de código |
| Por qué no Banxico | Requiere token, tiene límites de consulta y no publica el S&P 500; queda como complemento para USD/MXN y tasas |
| Riesgos | Librería no oficial, cambios de Yahoo, límites de peticiones y términos de uso; mitigar con copias locales y validación contra FRED |

# Fuentes de datos

## Indicaciones: 
• Investiguen al menos dos fuentes públicas adicionales. Para cada una registren: institución y enlace oficial, variables, periodicidad, cobertura temporal, formato de acceso y relación con las fuentes base.
• Seleccionen una fuente candidata y justifiquen su utilidad y viabilidad. Hoy no es necesario descargarla ni integrarla.

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

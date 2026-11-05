# Impacto de Eventos Macroeconómicos Globales en la Economía Mexicana: Un Análisis Multidimensional con PySpark
Este proyecto integrador analiza la transmisión de choques financieros e incertidumbre internacional hacia la economía mexicana. Utilizando **Apache Spark (PySpark)** como motor de procesamiento distribuido, se integran series de tiempo históricas de finanzas públicas, precios y actividad económica nacional con indicadores financieros internacionales clave.

## Pregunta de investigación
¿Cómo impactan los principales eventos históricos y crisis globales en la economía mexicana, medido a través de las variaciones del tipo de cambio frente al dólar y el comportamiento del índice S&P 500?

# Objetivo general
Construir un pipeline distribuido con PySpark que permita analizar la correlación y el impacto de variable externas de la economía global en la estabilidad económica de México.

---

## Integrantes y Roles 
| Integrante | Rol Principal | Área de Responsabilidad General | 
| :--- | :--- | :--- | 
| **Julia María Domínguez Russell** | **Developer Lead** | Coordinación técnica de código PySpark, arquitectura de DataFrames y buenas prácticas de desarrollo. | 
| **Arturo Alberto Martín del Campo Flores** | **Technical Reviewer 1** | Revisión de código, validación de lógica en PRs e integración de ramas a `main`. | 
| **Louis Frederic J. Dangremont** | **Technical Reviewer 2** | Revisión de código, pruebas de ejecución e integración de ramas a `main`. | 
| **Alisson Jazmín Garía Cardenas** | **Documentation Lead** | Coordinación de documentación técnica, estructura Markdown, citas bibliográficas y reportes. | 

---
## Fuentes de Información 
### Fuentes Base del Curso 
1. **SHCP (Finanzas Públicas):** Ingresos (\\(XBB\\)) y Gastos (\\(XBC\\)) del Gobierno Federal. 
2. **INEGI (INPC):** Índice Nacional de Precios al Consumidor para deflactación y cálculo de inflación. 
3. **INEGI (IGAE):** Indicador Global de la Actividad Económica por sectores productivos. 
### Fuentes Adicionales Integradas 
1. **Tipo de Cambio USD/MXN:** Indicador de presión cambiaria, riesgo país y volatilidad monetaria externa. 
2. **Índice S&P 500:** Indicador de apetito por riesgo y comportamiento de los mercados financieros globales. 

---

## Tecnologías Utilizadas 
* **Lenguajes y Entorno:** Python 3.14, Jupyter Notebook. 
* **Procesamiento Distribuido:** Apache Spark (PySpark) — DataFrames, Catalyst Optimizer, Spark UI. 
* **Control de Versiones y Colaboración:** Git, GitHub (Estrategia Feature-Branch / Pull Requests). 

---

## Resultados esperados
* La integración automatizada de una fuente financiera o global a las tres bases de datos requeridas (SHCP, INPC, IGAE) utilizando PySpark.
* El cálculo de rendimientos y/o un índice que permita resaltar los puntos de inflexión y cambios bruscos en el mercado.
* Una representación a modo de línea de tiempo (timeline) o buscador visual que evidencie el comportamiento anómalo de los datos en respuesta directa a los eventos históricos globales.   

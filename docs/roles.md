# Organización del Equipo, Responsabilidades y Criterios de Colaboración
## Asignación de Roles 
Para este proyecto integrador se adoptó un modelo de responsabilidad compartida con liderazgo por área. Esto significa que **todos los integrantes contribuyen activamente al desarrollo de código, análisis y documentación**, pero cada rol tiene asignada la **supervisión general, calidad y aprobación final** de su respectiva dimensión dentro del proyecto. 

--- 

## Desglose de Roles y Responsabilidades 

### 1. Developer Lead 
— **Julia María Domínguez Russell** 
* **Responsabilidad Principal:** Garantizar la corrección técnica, eficiencia y modularidad del código PySpark. 
* **Funciones Clave:** 
    * Supervisar la correcta instanciación de la `SparkSession` y configuraciones del entorno. 
    * Velar por el uso eficiente de transformaciones (`Narrow` vs. `Wide`) evitando cargas innecesarias de *Shuffle*. 
    * Asegurar la estandarización del esquema de variables y tipos de datos en los DataFrames. 
    * Asesorar a los compañeros durante la implementación de transformaciones complejas (`join`, `groupBy`, `pivot`). 
### 2. Technical Reviewers (2 Integrantes) 
— **Arturo Alberto Martín del Campo Flores** y **Louis Frederic J. Dangremont** 
* **Responsabilidad Principal:** Garantizar la integridad del repositorio `main`, la calidad del código mediante la revisión cruzada de cambios (*Peer Review*) y la resolución de conflictos en Git. 
* **Funciones Clave:** 
    * Revisar y validar las contribuciones enviadas mediante *Pull Requests* (PRs) antes de integrarlas a la rama principal (`main`). 
    * Verificar que el código subido ejecute sin errores, no contenga credenciales expuestas ni datos masivos fuera de especificación. 
    * Dividirse el volumen de revisiones para mantener un flujo de trabajo continuo y evitar cuellos de botella en GitHub. 
    * Aprobar y ejecutar los *merges* definitivos hacia `main`.
### 3. Documentation Lead 
— **[Nombre 4]** 
* **Responsabilidad Principal:** Supervisar la calidad de la redacción, el rigor metodológico, la inclusión de fuentes y el formato del proyecto. 
* **Funciones Clave:** 
    * Velar por la claridad, cohesión y ortografía en todos los archivos Markdown (`README.md`, `fuentes.md`, `roles.md`). 
    * Verificar la correcta referencia de las fuentes de información base y adicionales. 
    * Coordinar la elaboración y actualización de los diagramas conceptuales de arquitectura de datos. 
    * Asegurar que la interpretación analítica de los resultados esté adecuadamente fundamentada. 

--- 
## Reglas de Trabajo y Workflow en GitHub 
1. **Protección de la Rama Principal:** Queda estrictamente prohibido realizar `commit` o `push` directo a la rama `main`. 
2. **Estrategia de Ramificación (*Feature Branch*):** Cada nueva funcionalidad, análisis o ajuste documental debe desarrollarse en una rama independiente nombrada según el trabajo a realizar (ejemplo: `feature/ingesta-dolar`, `docs/actualizacion-roles`). 3. **Flujo de Pull Requests (PR):** 
    * Al finalizar un cambio, el integrante abrirá un *Pull Request* hacia `main` describiendo los cambios realizados. 
    * El PR debe ser asignado a al menos uno de los **Technical Reviewers**. 
    * Una vez revisado y aprobado el código o documento, el Technical Reviewer realizará el *merge* oficial a `main`. 
--- 
## Acuerdos Iniciales 
* **Cuidado de Información:** No subir archivos de datos pesados (`.parquet`, `.csv` voluminosos) ni claves/credenciales privadas al repositorio Git. 
* **Comunicación:** Sincronización continua previa a cada integración mayor para evitar conflictos de fusión (*merge conflicts*).
# Roles iniciales

## Actividad 9 | Diseño y organización del proyecto integrador
- Developer
- Technical Reviewer
- Documentation Lead

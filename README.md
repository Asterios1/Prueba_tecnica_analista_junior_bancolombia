# ETL & Billing Calculation with Python

Este proyecto se presenta como solución para la prueba técnica de analista junior de Operación Ecosistemas. Se implementa un proceso ETL en Python que realiza dos tareas principales:

1. **Análisis Exploratorio de Datos (EDA):**  
   Se conectan y analizan datos desde una base de datos SQLite, generando informes y visualizaciones que resumen la información de dos tablas: `apicall` y `commerce`.

2. **Cálculo de Facturación:**  
   Se extraen datos filtrados (llamadas de julio y agosto de 2024 para empresas con estatus "Active") y se calcula el total a cobrar para cada empresa, aplicando reglas de negocio específicas, descuentos y el IVA correspondiente.

---

## Tabla de Contenidos

- [Características](#características)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Requisitos](#requisitos)
- [Uso](#uso)
- [Detalles del Proceso](#detalles-del-proceso)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)
- [Agradecimientos](#agradecimientos)

---

## Características

- **Carga y transformación de datos:**  
  Se conecta a una base de datos SQLite ubicada en `Datos/database.sqlite` y se importan dos tablas con información sobre peticiones (API calls) y comercios.

- **Análisis Exploratorio:**  
  Se generan estadísticas descriptivas, se identifican valores faltantes y se visualizan distribuciones de datos (llamadas y estado de comercios) mediante gráficos.

- **Cálculo de Facturación Personalizado:**  
  Filtra los datos para obtener solo las llamadas de julio y agosto de 2024 y calcula el cobro para cada empresa aplicando reglas específicas:
  
  - **Innovexa Solutions:** \$300 COP por petición exitosa.
  - **NexaTech Industries:** Cobro variable según el rango de peticiones exitosas:
    - 0 a 10,000 → \$250 COP.
    - 10,001 a 20,000 → \$200 COP.
    - Más de 20,001 → \$170 COP.
  - **QuantumLeap Inc:** \$600 COP por cada petición exitosa.
  - **Zenith Corp:**  
    - 0 a 22,000 peticiones → \$250 COP.
    - Más de 22,000 → \$130 COP.
    - Además, si hay más de 6,000 peticiones fallidas se aplica un descuento del 5% (antes del IVA).
  - **FusionWave Enterprises:**  
    - \$300 COP por cada petición exitosa.
    - Descuento del 5% si las fallidas están entre 2,500 y 4,500, o del 8% si superan las 4,500.
    
- **Exportación de Resultados:**  
  Los resultados del análisis y la facturación se exportan a archivos Excel en la carpeta `reportes` y las visualizaciones se guardan en `analisis_resultados`.

---

## Estructura del Repositorio

```
├── Datos
│   └── database.sqlite          # Base de datos SQLite con las tablas 'apicall' y 'commerce'
├── reportes
│   └── analisis_datos_YYYY-MM-DD.xlsx    # Reporte de análisis exploratorio (generado)
│   └── resumen_facturacion_YYYY-MM-DD.xlsx  # Reporte de facturación (generado)
├── analisis_resultados
│   └── distribucion_llamadas.png   # Gráfico: Distribución de llamadas por estado
│   └── distribucion_comercios.png   # Gráfico: Distribución de comercios por estado
│   └── llamadas_mensuales.png      # Gráfico: Llamadas mensuales
├── main.py                     # Código fuente que ejecuta el ETL, análisis y facturación
└── README.md                   # Documentación del proyecto
```

---

## Requisitos

- **Python 3.7+**
- **Librerías de Python:**  
  - `pandas`
  - `numpy`
  - `matplotlib`
  - `logging` (incluida en la librería estándar)
  - `sqlite3` (incluida en la librería estándar)
  - `os` y `datetime` (incluidas en la librería estándar)

Para instalar las dependencias, puedes usar:

```bash
pip install pandas numpy matplotlib
```

---

## Uso

1. **Clonar el repositorio:**

   ```bash
   git clone git@github.com:Asterios1/Prueba_tecnica_analista_junior_bancolombia.git
   cd Prueba_tecnica_analista_junior_bancolombia
   ```

2. **Verificar la existencia del archivo `database.sqlite` se encuentre en la carpeta `Datos/`.**

3. **Ejecutar el script:**

   ```bash
   python main.py
   ```

El script realizará el análisis exploratorio, generará visualizaciones y calculará la facturación según las condiciones descritas. Los resultados se guardarán en la carpeta `reportes` y las imágenes en `analisis_resultados`.

---

## Detalles del Proceso

### Análisis Exploratorio de Datos

- **Carga de Datos:**  
  La clase `DataAnalyzer` se encarga de conectar a la base de datos y extraer los datos de las tablas `apicall` y `commerce`.

- **Transformación y Limpieza:**  
  Se convierte la columna de fecha a formato `datetime` y se analizan los datos mediante:
  - Estadísticas descriptivas.
  - Conteo de valores faltantes.
  - Distribución de llamadas y estados de los comercios.

- **Visualización:**  
  Se generan y guardan gráficos (barras, pastel y líneas) que resumen la información del dataset.

- **Exportación:**  
  Los resultados del análisis se pueden exportar a un archivo Excel para facilitar su revisión.

### Cálculo de Facturación

- **Filtrado de Datos:**  
  Se extraen únicamente los registros de llamadas correspondientes a los meses de julio y agosto de 2024 y se consideran solo empresas con estatus "Active".

- **Reglas de Facturación:**  
  La clase `BillingCalculator` implementa la lógica para calcular el monto a cobrar por cada empresa, aplicando:
  - Tarifas fijas o variables según el número de peticiones exitosas.
  - Descuentos basados en el número de peticiones fallidas.
  - Aplicación del 19% de IVA.

- **Reporte:**  
  El resumen de facturación se exporta a un archivo Excel, mostrando el valor sin IVA y con IVA aplicado.

---

## Contribuciones

¡Las contribuciones son bienvenidas! Si tienes sugerencias, mejoras o deseas reportar algún error, por favor abre una issue o envía un pull request.

---

## Licencia

Este proyecto se distribuye bajo la [Licencia MIT](LICENSE).

---

## Agradecimientos

- Agradecimientos especiales a Mauricio Batista por la oportunidad presentada para la vacante de analista junior.
- Inspirado en la necesidad de procesos ETL eficientes y soluciones de facturación en aplicaciones basadas en datos.


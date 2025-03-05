# Proyecto ETL: Análisis de Datos y Cálculo de Facturación

## Descripción

Este proyecto implementa un proceso ETL que permite:
- **Cargar datos** desde una base de datos SQLite.
- Realizar un **análisis exploratorio** de los datos (estadísticas descriptivas, detección de valores faltantes, distribuciones y series temporales).
- Generar **visualizaciones** utilizando Matplotlib.
- Calcular la **facturación** de diferentes empresas aplicando reglas contractuales específicas.
- **Exportar** los resultados a archivos Excel.
- Enviar los reportes generados por **correo electrónico**.

---

## Tabla de Contenidos

- [Características](#características)
- [Requisitos](#requisitos)
- [Estructura del Proyecto](#Estructura-del-Proyecto)
- [Uso](#Uso)
- [Configuración del Correo](#Configuración-del-Correo)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)
- [Agradecimientos](#agradecimientos)

---

## Características

- **Conexión a SQLite:** Lee datos de las tablas `apicall` y `commerce`.
- **Análisis Exploratorio:** Calcula estadísticas básicas, identifica valores faltantes y analiza distribuciones y tendencias mensuales.
- **Visualizaciones:** Genera gráficos de barras, gráficos de pastel y gráficos de línea.
- **Cálculo de Facturación:** Aplica tarifas y descuentos específicos según el nombre de la empresa y el comportamiento de las llamadas.
- **Exportación a Excel:** Guarda reportes de análisis y facturación en archivos Excel.
- **Envío de Reportes:** Permite enviar los reportes por correo electrónico utilizando el servidor SMTP de Gmail.

---

## Requisitos

- **Python 3.x**
- Librerías:
  - `sqlite3` (incluida en Python)
  - `pandas`
  - `numpy`
  - `logging`
  - `matplotlib`
  - `smtplib` y módulos de `email`
  - `datetime`
- Para instalar las dependencias que no estén incluidas, puedes usar:
  ```bash
  pip install pandas numpy matplotlib
  ```

---

## Estructura del Proyecto

```
/main.py                # Script principal que ejecuta el proceso ETL y envía reportes.
 /Datos/
    database.sqlite     # Base de datos SQLite con las tablas 'apicall' y 'commerce'.
 /reportes/             # Directorio donde se guardan los reportes exportados a Excel.
 /analisis_resultados/  # Directorio donde se guardan las visualizaciones generadas.
 /logs/                 # Directorio para los archivos de log generados.
```

---

## Uso

1. **Configurar la Base de Datos:**  
   Coloca el archivo `database.sqlite` en la carpeta `Datos/`.

2. **Ejecutar el Script:**  
   Ejecuta el script principal con el siguiente comando:
   ```bash
   python main.py
   ```

3. **Interacción en Consola:**  
   - Selecciona los meses que deseas analizar (por ejemplo, `7,8` para julio y agosto).
   - Ingresa los correos electrónicos de los destinatarios a los que se enviarán los reportes.

4. **Generación y Envío de Reportes:**  
   - Los reportes de análisis y facturación se exportarán a la carpeta `reportes/`.
   - Los reportes también se enviarán por correo electrónico utilizando la configuración de Gmail.

---

## Configuración del Correo

El script utiliza el servidor SMTP de Gmail para enviar correos. Asegúrate de contar con:
- Una cuenta de Gmail.
- Permitir el acceso a aplicaciones menos seguras o configurar una contraseña de aplicación si utilizas autenticación de dos factores.

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

